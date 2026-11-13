# SupportMail sharding protocol

Shared op-code contract between `sm-manager`, `supportmail` (cluster.ts), and `sm-api`. Copy this file into all three repos. When it changes, update the copy in every repo plus the matching type/struct file in the same sitting — there's no automated sync yet, only worth codegen once the op-code list outgrows manual maintenance.

## Envelope

Every message is a length-prefixed JSON frame. Requests get a matching response, correlated by `id`.

- **Request:** `{ id, op, target?, payload }`
- **Response:** `{ id, ok, data?, error? }`

`target` is only set for manager-routed requests (e.g. the API calling `SETUP_TICKETS` for a specific guild); cluster-to-manager registration/heartbeat messages omit it.

## Two tiers of op codes

This split is what keeps `sm-manager` reusable across bots:

- **Structural ops** — the only ops the manager understands and acts on. Identical for every bot. The manager `switch`es on these to drive registration, liveness, and the generation handoff.
- **Business ops** — opaque to the manager. It forwards them to the `target` cluster without inspecting the payload. This list is a contract between a bot's own cluster and its own API only; a different bot defines its own business ops without any change to `sm-manager`.

**Two places validation happens — and two places it must not:**

- The manager checks each inbound op against an allowed-op list from its config (structural ops always allowed, business ops from config). An unknown or misspelled op is rejected at the manager with `{ok: false, error}` rather than silently forwarded. This list is flat strings — op names only.
- The receiving cluster validates the _payload_ at its dispatch boundary (e.g. Zod), because that's where the payload is consumed and where the bot's types live.
- The manager never validates or parses payloads, and never learns their shapes — doing so would re-couple it to a specific bot's domain. The API does not re-validate what the cluster will validate.

| Op                          | Tier       | Direction               | Purpose                                                                                                                                                                                                                                                                        |
| --------------------------- | ---------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `REGISTER`                  | structural | cluster → manager       | Cluster announces itself on connect                                                                                                                                                                                                                                            |
| `HEARTBEAT`                 | structural | cluster → manager       | Periodic liveness + shard state                                                                                                                                                                                                                                                |
| `QUIESCE`                   | structural | manager → cluster       | Stop processing, close gateway sockets, ack when done                                                                                                                                                                                                                          |
| `QUIESCED`                  | structural | cluster → manager       | Ack for `QUIESCE`                                                                                                                                                                                                                                                              |
| `DEPLOY`                    | structural | any → manager           | Trigger a generation rollout (spawn new gen, await readiness, quiesce old gen); replies `{ok, data:{generation}}` once cutover completes or fails                                                                                                                              |
| `GET_CLUSTER_STATUSES`      | structural | client → manager        | Snapshot of every active cluster's state; replies `{ok, data:[]ClusterState}`. Manager reads its own registry, no target (sm-api's stats job)                                                                                                                                  |
| `START_INSTANCE`            | structural | client → manager        | Register an instance and its guild ids, start its process; replies `InstanceInfo`. Idempotent when running; error if a guild id maps to another instance; clears `failed`                                                                                                      |
| `STOP_INSTANCE`             | structural | client → manager        | SIGTERM, then SIGKILL after the grace period; replies `InstanceInfo` or `null` after exit. Keeps the mapping; unknown id is ok                                                                                                                                                 |
| `RESTART_INSTANCE`          | structural | client → manager        | Stop, then start under the current generation; replies `InstanceInfo`. Error `unknown instance` if never started or synced                                                                                                                                                     |
| `REMOVE_INSTANCE`           | structural | client → manager        | Stop if needed, then drop the registry entry and its guild mappings; replies `null`. Unknown id is ok                                                                                                                                                                          |
| `SYNC_INSTANCES`            | structural | client → manager        | Full desired instance list; replies `InstanceInfo[]`. Absent entries are stopped and removed; see Instances                                                                                                                                                                    |
| `GET_INSTANCE_STATUSES`     | structural | client → manager        | Snapshot of the instance registry; replies `InstanceInfo[]`. Instances never appear in `GET_CLUSTER_STATUSES`                                                                                                                                                                  |
| `VERIFY_GUILD_MEMBERSHIP`   | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `GET_GUILD_USERS`           | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `CHECK_MENTION_PERMISSIONS` | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `SETUP_TICKETS`             | business   | api → manager → cluster |                                                                                                                                                                                                                                                                                |
| `GET_FORUM_TAGS`            | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `SET_FORUM_TAGS`            | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `GET_GUILD_LIST`            | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `SYNC_GUILDS_WITH_DB`       | business   | any → manager → cluster |                                                                                                                                                                                                                                                                                |
| `HANDLE_DM_REPORT`          | business   | api → manager → cluster | Apply a DM-report action (or resync its log message) on the cluster owning the support guild                                                                                                                                                                                   |
| `REFRESH_WHITELABEL_GUILDS` | business   | api → manager → cluster | Reload the whitelabel guild set on the receiving cluster. sm-api sends it to every cluster by `target.clusterId`, not only to the one owning the guild; payload `{ guildId }`, replies `{ whitelabel: boolean }` (whether the guild is in that cluster's set after the reload) |
| `CLOSE_ALL_TICKETS`         | business   | api → manager → cluster | Close every open ticket of a guild; payload `{ guildId }`, replies `{ closed: number, failed: string[] }` (ids of tickets that could not be closed)                                                                                                                            |

The business rows above are SupportMail-specific. Another bot replaces them wholesale; the structural rows stay fixed.

## Instances

An instance is a separately supervised single-process bot, identified by an opaque `instanceId`. Instances are not clusters: they have their own registry, never appear in `GET_CLUSTER_STATUSES`, and own a set of guild ids (the exception register). The six instance ops are structural: the manager answers them itself, there is no `target`, and the payloads are:

- `START_INSTANCE`: `{ instanceId: string, guildIds: string[] }`, replies `InstanceInfo`.
- `STOP_INSTANCE`: `{ instanceId: string }`, replies `InstanceInfo` or `null`.
- `RESTART_INSTANCE`: `{ instanceId: string }`, replies `InstanceInfo`.
- `REMOVE_INSTANCE`: `{ instanceId: string }`, replies `null`.
- `SYNC_INSTANCES`: `{ instances: InstanceSyncEntry[] }`, replies `InstanceInfo[]`. The list is the full desired set. Entries absent from the list are stopped and removed. Listed entries are registered. `run: true` starts only an instance that is `stopped` or new, never one that is `failed`. `run: false` stops a running one. One `SYNC_INSTANCES` runs at a time; overlapping ones are queued.
  - **Errors.** A list that is invalid on its own (empty or repeated `instanceId`, empty guild id, a guild listed for two entries) is rejected with nothing changed. Past that point the sync applies in steps (remove unlisted, register listed, apply run flags) and does not roll back: if some entries fail, the reply is `{ ok: false, error }` with one `<instanceId>: <message>` per failed entry joined by `; ` (sorted), and no `data`. The entries that did not fail stay applied. A caller that needs the resulting state after such an error asks with `GET_INSTANCE_STATUSES`.
- `GET_INSTANCE_STATUSES`: `{}`, replies `InstanceInfo[]`.

`REGISTER` and `HEARTBEAT` also come from instances, with these payloads instead of the cluster ones:

- `REGISTER` from an instance: `{ instanceId: string, generation: number }`. Rejected with `unknown instance` if the manager did not start that id.
- `HEARTBEAT` from an instance: `{ instanceId: string, status?: unknown }`.

A connection keeps the identity it first registered with. A later `REGISTER` on the same connection for a different cluster id or generation, a different instance id, or switching between cluster and instance is rejected with `connection already registered as a different peer`. Registering again as the same identity (an instance may report a new generation) is allowed.

### Routing rule

A request whose `target.guildId` is in the exception register goes to that instance. If the instance is not connected, the manager answers `{ ok: false, error: "instance offline" }` and never falls back to a cluster. Any other guild id is routed to the cluster that owns it, as before.

## TypeScript (`protocol.ts` — `supportmail` and `sm-api`)

```ts
// Structural ops — known to the manager. Same for every bot.
export type StructuralOp =
  | "REGISTER"
  | "HEARTBEAT"
  | "QUIESCE"
  | "QUIESCED"
  | "DEPLOY"
  | "GET_CLUSTER_STATUSES"
  | "START_INSTANCE"
  | "STOP_INSTANCE"
  | "RESTART_INSTANCE"
  | "REMOVE_INSTANCE"
  | "SYNC_INSTANCES"
  | "GET_INSTANCE_STATUSES";

// Business ops — opaque to the manager. Bot-specific; this is SupportMail's set.
export type BusinessOp =
  | "VERIFY_GUILD_MEMBERSHIP"
  | "GET_GUILD_USERS"
  | "CHECK_MENTION_PERMISSIONS"
  | "SETUP_TICKETS"
  | "GET_FORUM_TAGS"
  | "SET_FORUM_TAGS"
  | "GET_GUILD_LIST"
  | "SYNC_GUILDS_WITH_DB"
  | "HANDLE_DM_REPORT"
  | "REFRESH_WHITELABEL_GUILDS"
  | "CLOSE_ALL_TICKETS";

export type OpCode = StructuralOp | BusinessOp;

export interface ShardRequest<T = unknown> {
  id: string; // correlation id, uuid
  op: OpCode;
  target?: { shardId?: number; clusterId?: number; guildId?: string };
  payload: T;
}

export interface ShardResponse<T = unknown> {
  id: string; // echoes request id
  ok: boolean;
  data?: T;
  error?: string;
}

// Instance shapes. Mirror `@supportmailapp/shared/protocol` (0.4.0).
export type InstanceRunState = "stopped" | "starting" | "running" | "failed";

export interface InstanceState {
  instanceId: string;
  state: InstanceRunState;
  generation: number;
  alive: boolean;
  lastBeat: number;
  exitCode?: number;
  status?: unknown; // opaque, from the instance's HEARTBEAT
}

// Internal socket only (the public status WebSocket omits guildIds).
export interface InstanceInfo extends InstanceState {
  guildIds: string[];
}

// One entry of the full desired list sent with SYNC_INSTANCES.
export interface InstanceSyncEntry {
  instanceId: string;
  guildIds: string[];
  run: boolean;
}
```

## Go (`protocol.go` — `sm-manager`)

The manager declares only the structural ops. Business ops are handled as raw strings and never enumerated here, so this file does not change when a bot adds or removes a business op.

```go
package protocol

import "encoding/json/jsontext"

type OpCode string

// Structural ops — the only ops the manager acts on. Everything else is
// forwarded by target without inspection.
const (
    OpRegister  OpCode = "REGISTER"
    OpHeartbeat OpCode = "HEARTBEAT"
    OpQuiesce   OpCode = "QUIESCE"
    OpQuiesced  OpCode = "QUIESCED"
    OpDeploy    OpCode = "DEPLOY"
    OpGetClusterStatuses OpCode = "GET_CLUSTER_STATUSES"

    // Instance ops: manager-answered ops that drive the instance registry.
    OpStartInstance       OpCode = "START_INSTANCE"
    OpStopInstance        OpCode = "STOP_INSTANCE"
    OpRestartInstance     OpCode = "RESTART_INSTANCE"
    OpRemoveInstance      OpCode = "REMOVE_INSTANCE"
    OpSyncInstances       OpCode = "SYNC_INSTANCES"
    OpGetInstanceStatuses OpCode = "GET_INSTANCE_STATUSES"
)

// IsStructural reports whether the manager should handle this op itself.
// If false, the message is routed to the target cluster untouched.
func IsStructural(op OpCode) bool {
    switch op {
    case OpRegister, OpHeartbeat, OpQuiesce, OpQuiesced, OpDeploy, OpGetClusterStatuses,
        OpStartInstance, OpStopInstance, OpRestartInstance, OpRemoveInstance,
        OpSyncInstances, OpGetInstanceStatuses:
        return true
    default:
        return false
    }
}

type Target struct {
    ShardId   *int    `json:"shardId,omitzero"`
    ClusterId *int    `json:"clusterId,omitzero"`
    GuildId   *string `json:"guildId,omitzero"`
}

type ShardRequest struct {
    ID      string          `json:"id"`
    Op      OpCode          `json:"op"`
    Target  *Target         `json:"target,omitzero"`
    Payload jsontext.Value  `json:"payload"`
}

type ShardResponse struct {
    ID    string          `json:"id"`
    OK    bool            `json:"ok"`
    Data  jsontext.Value  `json:"data,omitzero"`
    Error string          `json:"error,omitempty"`
}
```
