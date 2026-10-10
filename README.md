# sm-manager

Go process supervisor and IPC hub for the SupportMail sharding system. It supervises cluster processes (each hosting several discord.js gateway shards), routes messages between clusters and clients (the API) over a Unix socket, drives zero-downtime cluster-by-cluster rollouts, and hosts the public statuspage WebSocket.

The manager is **bot-agnostic on purpose**. It only understands six structural ops (`REGISTER`, `HEARTBEAT`, `QUIESCE`, `QUIESCED`, `DEPLOY`, `GET_CLUSTER_STATUSES`) plus a config-supplied allow-list of business-op names, which it forwards without ever parsing the payload. A second bot is a different config, not a code change. See [`PROTOCOL.md`](PROTOCOL.md) for the wire contract.

## Features

- Unix-socket hub: cluster registry, structural-op handling, business-op allow-list gate, request/response correlation, computed guild/shard to cluster routing.
- Process supervision: spawns cluster processes, injects the env contract, restarts crashed clusters with exponential backoff.
- Generations and rollouts: generation tagging, readiness gating, and a live `DEPLOY` op that rolls the fleet to a new generation cluster-by-cluster in configurable batches (`deployBatchSize`). Routing flips per batch only after the new clusters are ready and before the old ones are quiesced, so there is no duplicate-routing window.
- Public statuspage WebSocket (`internal/status/`, separate from the Unix socket): snapshot plus health/status deltas, API-key gate, per-IP handshake rate-limiting and connection caps, real client IP resolved from `CF-Connecting-IP` behind Cloudflare and nginx.

Still stubbed: the real per-consumer API key system for the status WebSocket (today a flat allow-list).

## Documentation

Also published at [manager.docs.supportmail.dev](https://manager.docs.supportmail.dev).

- [**How the transport works**](docs/content/transport.md): Unix socket, message framing, request/response correlation, target routing
- [**Process supervision**](docs/content/supervision.md): environment contract, crash handling
- [**Generations, readiness gating, and DEPLOY**](docs/content/generations.md): generation tagging, readiness gating, live cluster-by-cluster rollouts, known gaps
- [**Two tiers of ops**](docs/content/ops.md): structural vs. business ops, validation boundaries
- [**Public statuspage WebSocket**](docs/content/status-ws.md): trust boundary, real client IP resolution, rate-limit knobs
- [**Configuration and running**](docs/content/configuration.md): config keys and overrides, getting started
- [**Layout and project status**](docs/content/layout.md): source tree organization, remaining work

## License

Licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0). You may use, modify, and self-host it. If you distribute a modified version, or let users interact with one over a network, you must make your modified source available under the same license.
