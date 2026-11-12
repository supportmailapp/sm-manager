# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Go process supervisor + IPC hub for the SupportMail sharding system. It spawns and babysits cluster processes (each hosting several discord.js gateway shards), routes messages between clusters and clients (the API) over a Unix socket, drives zero-downtime cluster-by-cluster rollouts, and hosts the public statuspage WebSocket. It is **bot-agnostic by design**: it only understands a fixed set of structural ops and a config-supplied allow-list of business-op *names*; it never parses business-op payloads. A second bot is a different `config.json`, not a code change.

This is one of several independent repos of the SupportMail stack (see `../CLAUDE.md` at the workspace root for how it relates to `sm-api`, `orbit`, and the `v2` bot whose clusters it spawns). `PROTOCOL.md` in this repo is the wire contract, hand-copied (not codegen'd) into `sm-manager`, `v2`/`supportmail` (`cluster.ts`), and `sm-api` — when it changes, update all three in the same sitting.

## Commands

```bash
cp config.example.json config.json   # first-time setup; config.json is gitignored
go vet ./...
go test ./...
go run ./cmd/sm-manager
go build -o sm-manager ./cmd/sm-manager
```

Single test: `go test ./internal/server -run TestRegisterMarksClusterAlive -v` (swap the package path and test name; `internal/server`, `internal/supervisor`, `internal/status`, `internal/protocol` and `internal/config` all have tests, `internal/telemetry` and `internal/wirejson` don't; the golden tests that pin wire bytes live in `server`, `status` and `config`).

No linter is configured beyond `go vet`; no Makefile or CI workflow exists in this repo.

There's also a separate VitePress docs site under `docs/` (its own pnpm project, unrelated to the Go build) — the source of the `docs.*.md` pages linked from `README.md` lives in `docs/content/*.md`.

**WSL socket gotcha**: Unix sockets need a short path on the *Linux* fs — a long `socketPath` or one under `/mnt/c/...` fails with `bind: invalid argument`. Keep the `/tmp/sm-manager.sock` default. For a real end-to-end run with a spawned `bun` cluster, `bun` must run natively — build `go build -o sm-manager.exe ./cmd/sm-manager` and run from PowerShell instead. (Plain `go build`/`go test` without spawning a real cluster process run fine from WSL.)

## Architecture

Standard layout: `cmd/sm-manager/main.go` is a thin wiring layer (config → server/supervisor/status, signal handling, Sentry init); everything else is import-restricted `internal/` packages.

- **`internal/protocol`** — the wire contract's Go side: op codes, `IsStructural`, request/response/target types, and the length-prefixed (4-byte big-endian uint32 + JSON, 16 MiB cap) frame codec.
- **`internal/config`** — `Config` struct, loaded from a JSON file (`SM_MANAGER_CONFIG` env, default `./config.json`) then overridden field-by-field by `SM_*` env vars. Nothing bot-specific is compiled in.
- **`internal/server`** — the socket hub (`hub.go`): in-memory cluster registry, structural-op handling, business-op allow-list gate + blind forward-by-target, request/response correlation by `id`, the active-routing table, generation tagging + readiness gating, `DEPLOY` handling via a `Deployer` interface, and manager-initiated `QUIESCE`/`QUIESCED` correlation. `server.go` owns the Unix listener, per-connection read loop, and heartbeat reaper.
- **`internal/supervisor`**: spawns and restarts cluster child processes (`exec.CommandContext`), injects the env contract (`CLUSTER_ID`/`SHARD_LIST`/`TOTAL_SHARDS`/`SM_MANAGER_SOCKET`/`GENERATION`), exponential-backoff crash restart, and implements the batched `Deploy` rollout. It also runs instance processes (`instances.go`), each individually, with their own env contract (see Instances below).
- **`internal/status`**: the public statuspage WebSocket: internet-facing, separate from the internal Unix socket, snapshot+delta fanout (clusters, plus instances in separate `instances`/`instance`/`removedInstance` fields that never carry guild ids), API-key auth, and IP-based rate-limiting/conn-capping behind Cloudflare+nginx.
- **`internal/telemetry`** — process-wide Sentry `Logger`/`Meter` singletons (`telemetry.Init` called once in `main` after `sentry.Init`; `Logger()`/`Meter()` lazily self-init to a safe no-op if a caller — e.g. a test — never calls `Init`).
- **`internal/wirejson`**: the `encoding/json/v2` option sets (Go 1.27). `Wire` keeps frames and status messages byte-compatible with what v1 `encoding/json` produced (null for nil slices/maps, HTML/U+2028 escaping, lenient UTF-8 for opaque payloads); `Config` is strict (case-sensitive, no duplicate or unknown keys, no nulls). Raw JSON values are `jsontext.Value`; a raw field that may be empty needs `omitzero`, not `omitempty`. Golden tests named `*Golden*` pin the exact bytes: run them after any wire-type change.

### Two-tier op codes (the design's core constraint)

- **Structural ops** (`REGISTER`, `HEARTBEAT`, `QUIESCE`, `QUIESCED`, `DEPLOY`, `GET_CLUSTER_STATUSES`, and the six instance ops `START_INSTANCE`, `STOP_INSTANCE`, `RESTART_INSTANCE`, `REMOVE_INSTANCE`, `SYNC_INSTANCES`, `GET_INSTANCE_STATUSES`; twelve in all), the only ops the manager understands and acts on. Fixed across every bot.
- **Business ops** — opaque to the manager. It checks the op name against `Config.AllowedBusinessOps`, then forwards the frame to the resolved target cluster *without ever decoding the payload*. The manager rejects unknown ops by name; the receiving cluster validates the payload itself. Never add payload-shape knowledge to this repo — that recouples it to one bot's domain and defeats the reuse design.

### Instances

An **instance** is a separately supervised single-process bot, identified by an opaque `instanceId`, with its own registry next to the cluster fleet (`internal/server/instances.go`, `internal/supervisor/instances.go`). It owns a set of guild ids (the exception register) and never appears in `GET_CLUSTER_STATUSES`; the manager knows nothing else about what an instance is. It is spawned from `instanceSpawnCommand` (never the cluster `spawnCommand`) with the env contract `INSTANCE_ID`, `SM_MANAGER_SOCKET` and `GENERATION` (no cluster id or shard list). An exit with `instanceNoRestartExitCode` (default 78) marks it `failed` and is not restarted; any other exit restarts with backoff. Every instance state change is published to the status sink (`Hub.instanceChanged`, outside `h.mu`), and `REMOVE_INSTANCE` removes it from the feed.

### Generations and rollouts

Every cluster is tagged with the epoch (generation) it was spawned under. `DEPLOY` rolls the fleet to a new generation **cluster-by-cluster in batches of `deployBatchSize`**: per batch, spawn the new generation, wait for it to register and go healthy (`Hub.AwaitClusters`), promote routing to it (`Hub.PromoteClusters` — flips *before* quiescing, so a `clusterID` always routes to exactly one generation), then `QUIESCE` and kill the old batch. The live generation counter is mutable runtime state on `*Supervisor`, not config — `Config.Generation`/`SM_GENERATION` only seed a cold start.

**No rollback on a failed batch, by design**: a batch that fails readiness halts the roll canary-style (already-cut-over batches stay on the new generation, the rest stay on the healthy old one) rather than reversing anything — these are live gateway connections, so "rolling back" a cut-over cluster would just be a second pointless reconnect. Recovery is forward-only: fix the build, re-run `DEPLOY`.

### Transport

Every peer (clusters, `sm-api`) connects to the same Unix socket path, that's the whole address, which is why it's configured rather than hardcoded (differs per environment). `resolveTarget` computes shard→cluster routing from topology (`shardId / shardsPerCluster`, or Discord's `(guildID >> 22) % totalShards` for a `guildId` target) rather than keeping a stored shard table. A `guildId` target consults the instance exception register first: a registered guild goes to its instance, and if that instance is not connected the sender gets `instance offline`; it never falls back to a cluster.

### Deliberate non-features

Don't "fix" these — they're intentional:
- No rollback on a failed rollout (see above).
- No business-op payload validation/parsing anywhere in this repo.
- No shared/persistent state — the registry rebuilds from cluster `REGISTER` on restart; rate-limiter state is per-process, in-memory, single-instance only.

For deeper detail on any of the above (readiness-gating internals, the quiesce-ack correlation mechanism, the statuspage WS trust boundary, full config key reference), read the corresponding page under `docs/content/` — it's kept in sync with the code and is more detailed than this file.
