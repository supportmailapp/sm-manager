---
sidebar:
  order: 1
---

# Layout

Standard Go layout: the entrypoint is a thin `package main` under `cmd/`; all reusable logic lives in `internal/` packages (import-restricted to this module).

```txt
/cmd/sm-manager/main.go        wire config → server, handle signals
/internal/protocol/            the shared wire contract, Go side
  protocol.go                 op codes, IsStructural, request/response/target types, FrameKind probe
  wire.go                     length-prefixed frame read/write, 16 MiB cap
/internal/config/config.go     Config struct, JSON-file load, env overrides, allowed-op set
/internal/server/              the socket hub
  server.go                   Unix listener, per-connection read loop, heartbeat reaper, signal shutdown
  hub.go                      in-memory registry, structural handling, business-op gate + forward, correlation, active-routing table (empty-slot claim + PromoteClusters cutover), generation tagging + readiness gating (GenerationReady/AwaitGeneration/AwaitClusters), DEPLOY handling (Deployer interface, handleDeploy) + manager-initiated QUIESCE/QUIESCED correlation (QuiesceGeneration/QuiesceClusters)
  hub_test.go                 routing, active-routing cutover, generation-readiness, DEPLOY, and quiesce tests against a fake conn
/internal/supervisor/          cluster process supervision
  supervisor.go               spawn per-cluster (context keyed by (gen,clusterID)), env-contract injection, prefixed log capture, crash backoff, batched Deploy (per batch: spawnClusters → AwaitClusters → PromoteClusters → QuiesceClusters → cancel old)
  supervisor_test.go          batchClusterIDs / clusterIDs splitting
/internal/status/              public statuspage WebSocket — internet-facing, separate from the Unix socket
  status.go                   Event/ClusterState wire types, Broadcaster (client fanout + last-known snapshot cache, drops slow clients), Sink interface the hub publishes through
  server.go                   TCP HTTP server, /ws upgrade (coder/websocket), /healthz, ctx-driven graceful shutdown
  auth.go                     API-key gate (constant-time allowlist; X-API-Key / bearer header, or "key.<value>" WS subprotocol for the browser client) — the check() seam a richer key system slots in behind
  clientip.go                 real-client-IP resolver — trusts CF-Connecting-IP only from a trusted-proxy RemoteAddr (nginx); anti-spoof for per-IP limiting behind Cloudflare+nginx
  ratelimit.go                per-IP handshake token bucket + per-IP/global concurrent-conn caps, idle-bucket sweeper (applied in handleWS around the key check)
  status_test.go              auth cases, snapshot-then-delta fanout, slow-client drop, end-to-end dial (real server + coder client, auth reject + snapshot + live delta)
  ratelimit_test.go           trusted-proxy IP resolution + spoof rejection, handshake token bucket (burst/refill/per-IP), conn caps (per-IP + global), idle-bucket sweep
```

Top-level: `config.example.json` (config template — SupportMail's 9 business ops + topology) and `PROTOCOL.md` (shared wire contract, copied verbatim into the bot and API repos).

## Deliberate non-features

These are design choices, not missing work:

- **No rollback on a failed rollout.** A batch that fails readiness **halts** the roll canary-style — cut-over batches stay new, the rest stay on a healthy old generation. Reversing a cut-over cluster just churns its gateway connection, so recovery is forward-only: fix the build, re-run `DEPLOY`. See Generations.
- **No payload knowledge.** The manager never validates, parses, or learns business-op payload shapes — that lives cluster-side. See Two Tiers of Ops.
- **No shared/persistent state.** The registry is in-memory, rebuilt from cluster registration on restart; limiter state is per-process. Single manager instance, no external store.
