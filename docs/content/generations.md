---
sidebar:
  order: 6
---

# Generations, Readiness Gating, and DEPLOY

Every cluster is tagged with the **epoch** it was spawned under. A rollout spawns the new generation alongside the currently-serving one and cuts over **cluster-by-cluster in batches of `deployBatchSize`** — for each batch: spawn, wait until it's up, flip routing to it, then quiesce and kill the old one. No gap where a shard range is unserved, and only one batch is in a quiesce window at a time.

## Tagging

`REGISTER`'s payload carries a `generation` field (defaults to `0` if omitted — the only generation that exists before any rollout). `Hub` keeps this in two views:

- `clusters map[int]*Conn` — the **active routing table**, by `clusterID`. This is what `resolveTarget` routes business ops through. A `REGISTER` only claims a slot here if it's **empty** (cold start, or a crash-restart whose old conn was already removed); a new generation registering while the old one still holds the slot does **not** steal it. Routing flips only at cutover, via `Hub.PromoteClusters(gen, cids)`.
- `generations map[int]map[int]*Conn` — by generation, then `clusterID`. Readiness bookkeeping + the source `PromoteClusters`/`QuiesceClusters` pull a batch's conns from; a disconnect/reap only prunes the generation bucket the leaving `Conn` was actually tagged with.

## Readiness

`Hub.GenerationReady(gen)` is true once every `clusterID` in `[0, clusterCount)` has registered under `gen` **and** its heartbeat is still within `heartbeatTimeoutSecs`. The batch variant `Hub.AwaitClusters(ctx, gen, cids)` waits on just a subset — it blocks on a `sync.Cond`, broadcast on every register/heartbeat-reap/disconnect/promote, until those `cids` are all up under `gen` or `ctx` is done. (`AwaitGeneration` is `AwaitClusters` over the whole fleet.)

## Live generation counter

The live generation counter lives on the supervisor, not config. `Config.Generation`/`SM_GENERATION` only seed the value a cold-started manager begins at. Once running, `Supervisor` owns a mutable, in-memory generation counter, bumped by `Deploy`. This is deliberate: a static config value loaded once at `config.Load()` time can't support "trigger a redeploy while already running" — that needs runtime-mutable state driven by an inbound command, which is what `DEPLOY` is.

## DEPLOY — the rollout trigger

Any socket client can send `{op: "DEPLOY"}` (structural, not cluster-only). `Hub.handleDeploy` calls into a `Deployer` interface (implemented by `*Supervisor`, wired in via `Hub.SetDeployer` in `cmd/sm-manager/main.go` — this indirection exists so the hub doesn't need to import the supervisor package, which itself imports `server` for `*Hub`). `Supervisor.Deploy`:

1. Bumps its live counter `gen → gen+1`.
2. Splits `[0, clusterCount)` into contiguous batches of `deployBatchSize` (`0`/`>=clusterCount` → one whole-fleet batch, the old all-at-once behavior). Then, **per batch**:
   a. `spawnClusters(newGen, cids)` — spawn just this batch under the new generation, via the same `runCluster`/`spawnOnce` path crash-recovery uses. Each running cluster gets its own cancelable context keyed by `(gen, clusterID)` in `clusterCancels`, so any subset can be retired independently.
   b. `Hub.AwaitClusters(ctx, newGen, cids)` — block until this batch is registered and alive. On failure the roll **halts here**: the broken new batch is torn down (`cancelClusters(newGen, cids)`) and `Deploy` returns an error. Nothing is reversed — this batch was never promoted (step c hasn't run), so the old generation still owns its routing slot and keeps serving. Batches that already cut over stay on the new generation. Every `clusterID` is still served by exactly one healthy generation; the operator fixes the build and re-runs `DEPLOY` to finish the stragglers. This is canary-halt, not rollback — see Known gaps.
   c. `Hub.PromoteClusters(newGen, cids)` — flip the active routing slots for these `cids` to the new conns. Promote-before-quiesce means every `clusterID` routes to exactly one generation the whole time.
   d. `Hub.QuiesceClusters(ctx, oldGen, cids)` — `QUIESCE` the old batch's conns in parallel, wait for `QUIESCED` acks, then `cancelClusters(oldGen, cids)` kills those child processes for real (`exec.CommandContext` cancellation).
3. The response is `{ok:true, data:{generation:N}}` on a completed cutover, or `{ok:false, error}` — including if nothing is wired up yet (`SetDeployer` not called) or the deploy times out (`deployTimeoutSecs` bounds every blocking step above).

## Manager-initiated QUIESCE

Manager-initiated QUIESCE has its own correlation. `Hub.quiesceWaiters` (id → channel) lets the quiesce path wait for a specific `QUIESCED` without borrowing the `pending`/`routeResponse` machinery, which exists for forwarded business ops originated by some other client — a manager-sent `QUIESCE` has no such originating client to route the ack back to, it just needs to know the ack arrived.

## Known gaps, honestly

- **A halted roll leaves a mixed fleet on purpose — there is no rollback, by design.** These are gateway connections, not stateless requests. A cluster that already cut over has dropped its old shards and re-`IDENTIFY`'d on the new generation; "rolling it back" would mean a second disconnect/reconnect cycle for zero gain, so the manager never does it. When a mid-rollout batch fails readiness the roll **halts** (canary-style): cut-over batches stay new, the failed batch keeps its old generation (it was never promoted), un-started batches stay old. Every `clusterID` is served by exactly one healthy generation — a stable state, not a degraded one. Recovery is forward-only: fix the build, re-run `DEPLOY` to roll the stragglers. The one thing not built is an operator-triggered **manual** halt (stop before the next batch while watching metrics) — automatic halt on unhealthy readiness is the only trigger today.
- **Bot-side `QUIESCE`** is implemented as a real drain in `cluster.ts` (stop dispatch → `client.destroy()` → ack `QUIESCED`), but in-flight async handlers already running still complete — acceptable graceful drain, not a hard cut.

Smoke-tested end-to-end with fake cluster processes standing in for the bot: spawn → register → ready → promote → quiesce/ack → old pids actually killed → new generation serving heartbeats, repeatable across consecutive `DEPLOY` calls and across batch sizes.
