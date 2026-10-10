# sm-manager

> Internal / not public yet. This documents the manager for us while it's being built.

Go process supervisor + IPC hub for the SupportMail sharding system. It supervises cluster processes (each hosting several discord.js gateway shards), routes messages between clusters and clients (the API) over a Unix socket, and — later — drives zero-downtime soft restarts and the public statuspage WebSocket.

The manager is **bot-agnostic on purpose**. It only understands four structural ops; everything bot-specific is either config or lives in the cluster/API. A second bot is a different config, not a code change. See `../sharding-rewrite-plan.md` for the full plan and `PROTOCOL.md` for the wire contract.

## Current Status

**Phase 1 done** (manager + bot sides — socket listener, registry, structural/business routing, allowed-op gate, process supervision with crash-restart, computed guild/shard→cluster routing; remaining Phase 1 work is API-side).  
**Phase 3 landed**: generation tagging, readiness gating, and a live `DEPLOY` control op that rolls the fleet to a new generation **cluster-by-cluster in configurable batches** (`deployBatchSize`), flipping routing per-batch only after the new clusters are ready and before quiescing the old ones — so no duplicate-routing window. Bot side drains for real on `QUIESCE`.  
**Phase 4 manager-half done**: `DEPLOY` is also the "manager control op" Phase 4 needed — the `sm-api`/dashboard side of wiring it up is separate work in another repo.  
**Phase 5 landed**: internet-facing statuspage WebSocket (`internal/status/`, separate from the Unix socket) streaming snapshot + health/status deltas, with a stub API-key gate **and** rate-limiting (per-IP handshake token bucket + per-IP/global connection caps, real client IP resolved from `CF-Connecting-IP` behind Cloudflare+nginx). Still stubbed: the real per-consumer key system.

## Documentation

- [**How the transport works**](docs/content/transport.md) — Unix socket, message framing, request/response correlation, target routing
- [**Process supervision**](docs/content/supervision.md) — environment contract, crash handling
- [**Generations, readiness gating, and DEPLOY (Phase 3)**](docs/content/generations.md) — generation tagging, readiness gating, live cluster-by-cluster rollouts, known gaps
- [**Two tiers of ops**](docs/content/ops.md) — structural vs. business ops, validation boundaries
- [**Public statuspage WebSocket (Phase 5)**](docs/content/status-ws.md) — trust boundary, real client IP resolution, rate-limit knobs
- [**Configuration and running**](docs/content/configuration.md) — config keys and overrides, getting started
- [**Layout and project status**](docs/content/layout.md) — source tree organization, phases yet to build
