---
sidebar:
  order: 4
---

# Configuration and Running

## Configuration

Config loads from a JSON file (`SM_MANAGER_CONFIG`, default `./config.json`), then env-var overrides are applied on top. A missing file is fine — defaults + env are enough. Copy the template to start:

```bash
cp config.example.json config.json
```

| JSON key                    | Env override                       | Default                | Meaning                                                                                                                                                 |
| --------------------------- | ---------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `socketPath`                | `SM_SOCKET_PATH`                   | `/tmp/sm-manager.sock` | Unix socket path all peers connect to                                                                                                                   |
| `spawnCommand`              | `SM_SPAWN_COMMAND`                 | —                      | Cluster process command (binary + args), e.g. `bun run --env-file=.env.production src/cluster.ts`. Must load the bot's env file itself — see Supervision. Empty = spawn nothing                                                           |
| `spawnDir`                  | `SM_SPAWN_DIR`                     | manager cwd            | Working directory for `spawnCommand` (the bot repo, e.g. `../bot`)                                                                                       |
| `shardsPerCluster`          | `SM_SHARDS_PER_CLUSTER`            | `1`                    | Shards each cluster hosts; also the shard→cluster divisor for routing                                                                                   |
| `clusterCount`              | `SM_CLUSTER_COUNT`                 | `1`                    | Number of clusters spawned; `clusterCount × shardsPerCluster` = total shards                                                                            |
| `heartbeatTimeoutSecs`      | `SM_HEARTBEAT_TIMEOUT_SECS`        | `30`                   | Cluster marked dead if no heartbeat within this window                                                                                                  |
| `allowedBusinessOps`        | `SM_ALLOWED_BUSINESS_OPS`          | `[]`                   | Flat list of business-op names the manager will forward                                                                                                 |
| `generation`                | `SM_GENERATION`                    | `0`                    | Starting epoch for a cold-started manager only — the live value is mutable runtime state on the supervisor, bumped by `DEPLOY`; see Generations, above  |
| `deployTimeoutSecs`         | `SM_DEPLOY_TIMEOUT_SECS`           | `120`                  | Bounds how long a `DEPLOY` blocks waiting for the new generation to become ready and the old one to quiesce, before giving up                           |
| `deployBatchSize`           | `SM_DEPLOY_BATCH_SIZE`             | `0`                    | Clusters swapped per rollout batch; `0` or `>= clusterCount` = whole fleet in one batch. `1` = one cluster at a time. See Generations, above            |
| `statusWsAddr`              | `SM_STATUS_WS_ADDR`                | — (disabled)           | `host:port` for the public statuspage WebSocket, e.g. `:4000`. Empty disables it. Separate from the internal Unix socket                                |
| `statusApiKeys`             | `SM_STATUS_API_KEYS`               | `[]` (open)            | API-key allowlist gating the public WS. Empty = open endpoint (dev only, logs a warning). The `check()` seam a richer key system slots in behind        |
| `statusRealIpHeader`        | `SM_STATUS_REAL_IP_HEADER`         | `CF-Connecting-IP`     | Header carrying the real client IP behind the proxy. Honored only when `RemoteAddr` ∈ `statusTrustedProxies`. Empty = trust no header, use `RemoteAddr` |
| `statusTrustedProxies`      | `SM_STATUS_TRUSTED_PROXIES`        | `127.0.0.1/32,::1/128` | CIDRs whose `RemoteAddr` is allowed to set `statusRealIpHeader` (i.e. nginx). Anti-spoof for the real-IP header — see Public statuspage WebSocket above |
| `statusHandshakeRatePerMin` | `SM_STATUS_HANDSHAKE_RATE_PER_MIN` | `60`                   | Per-IP `/ws` handshake attempts per minute (token bucket, checked before auth). `0` disables handshake rate-limiting                                    |
| `statusMaxConnsPerIp`       | `SM_STATUS_MAX_CONNS_PER_IP`       | `20`                   | Max concurrent live WS connections from one client IP. `0` disables                                                                                     |
| `statusMaxConns`            | `SM_STATUS_MAX_CONNS`              | `5000`                 | Max concurrent live WS connections across all IPs. `0` disables                                                                                         |

`SM_ALLOWED_BUSINESS_OPS`, `SM_STATUS_API_KEYS`, and `SM_STATUS_TRUSTED_PROXIES` are comma-separated. `SM_SPAWN_COMMAND` is space-separated.

Nothing bot-specific is compiled in — no payload shapes, no op vocabulary, no paths. `config.json` is gitignored (it's per-environment); `config.example.json` is the checked-in template.

## Running

```bash
cp config.example.json config.json
go vet ./...
go test ./...
go run ./cmd/sm-manager
```

Build a binary instead with `go build -o sm-manager ./cmd/sm-manager`.

Expected on startup (with a real `spawnCommand`) — spawn, then each cluster registering:

```log
cluster 1 spawned (pid 11382, shards [2 3] of 4, generation 0)
cluster 0 spawned (pid 11381, shards [0 1] of 4, generation 0)
cluster 0 registered (shards [0 1], generation 0) — connected, alive
```

On a child crash you'll see the exit code + escalating restart delay:

```log
cluster 0 exited after 3ms: exit status 1 — restarting in 1s
```

`go test ./...` exercises the hub headlessly — register-alive, unknown-op reject, forward-and-route-back, no-target reject, guild/shard routing, generation tagging, generation readiness (by count and by stale heartbeat), `AwaitGeneration`/`AwaitClusters` (resolves once ready / subset ready / returns on context deadline), the active-routing rules (a new generation registering does **not** steal the slot; a crash-restart refills an empty slot; `PromoteClusters` flips it), `DEPLOY` handling (no deployer configured / success / error propagation via a fake `Deployer`), and `QuiesceGeneration`/`QuiesceClusters` (sends `QUIESCE` to the right subset and awaits `QUIESCED` / times out without an ack) — without needing a live cluster. The supervisor's `batchClusterIDs`/`clusterIDs` splitting is unit-tested. The full `Supervisor.Deploy` rollout (real process spawn, kill, cutover) has no automated test; it was verified manually end-to-end against fake cluster processes (see Generations) rather than in `go test`.

> **WSL socket gotcha:** Unix sockets need a **short** path on the **Linux** fs. A long `socketPath` or one under `/mnt/c/...` fails with `bind: invalid argument`. Keep the `/tmp/sm-manager.sock` default. For a real end-to-end run the spawned `bun` must run natively — build `go build -o sm-manager.exe ./cmd/sm-manager` and run from PowerShell.

For running this as a long-lived service on a server (systemd unit, directory layout, redeploys), see [Deployment (systemd)](/deployment).
