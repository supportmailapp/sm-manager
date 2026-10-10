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

| Op                          | Tier       | Direction               | Purpose                                                                                                                                           |
| --------------------------- | ---------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REGISTER`                  | structural | cluster → manager       | Cluster announces itself on connect                                                                                                               |
| `HEARTBEAT`                 | structural | cluster → manager       | Periodic liveness + shard state                                                                                                                   |
| `QUIESCE`                   | structural | manager → cluster       | Stop processing, close gateway sockets, ack when done                                                                                             |
| `QUIESCED`                  | structural | cluster → manager       | Ack for `QUIESCE`                                                                                                                                 |
| `DEPLOY`                    | structural | any → manager           | Trigger a generation rollout (spawn new gen, await readiness, quiesce old gen); replies `{ok, data:{generation}}` once cutover completes or fails |
| `GET_CLUSTER_STATUSES`      | structural | client → manager        | Snapshot of every active cluster's state; replies `{ok, data:[]ClusterState}`. Manager reads its own registry, no target (sm-api's stats job)     |
| `VERIFY_GUILD_MEMBERSHIP`   | business   | any → manager → cluster |                                                                                                                                                   |
| `GET_GUILD_USERS`           | business   | any → manager → cluster |                                                                                                                                                   |
| `CHECK_MENTION_PERMISSIONS` | business   | any → manager → cluster |                                                                                                                                                   |
| `SETUP_TICKETS`             | business   | api → manager → cluster |                                                                                                                                                   |
| `GET_FORUM_TAGS`            | business   | any → manager → cluster |                                                                                                                                                   |
| `SET_FORUM_TAGS`            | business   | any → manager → cluster |                                                                                                                                                   |
| `GET_GUILD_LIST`            | business   | any → manager → cluster |                                                                                                                                                   |
| `SYNC_GUILDS_WITH_DB`       | business   | any → manager → cluster |                                                                                                                                                   |
| `HANDLE_DM_REPORT`          | business   | api → manager → cluster | Apply a DM-report action (or resync its log message) on the cluster owning the support guild |

The business rows above are SupportMail-specific. Another bot replaces them wholesale; the structural rows stay fixed.

## TypeScript (`protocol.ts` — `supportmail` and `sm-api`)

```ts
// Structural ops — known to the manager. Same for every bot.
export type StructuralOp =
  | "REGISTER"
  | "HEARTBEAT"
  | "QUIESCE"
  | "QUIESCED"
  | "DEPLOY"
  | "GET_CLUSTER_STATUSES";

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
  | "HANDLE_DM_REPORT";

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
```

## Go (`protocol.go` — `sm-manager`)

The manager declares only the structural ops. Business ops are handled as raw strings and never enumerated here, so this file does not change when a bot adds or removes a business op.

```go
package protocol

import "encoding/json"

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
)

// IsStructural reports whether the manager should handle this op itself.
// If false, the message is routed to the target cluster untouched.
func IsStructural(op OpCode) bool {
    switch op {
    case OpRegister, OpHeartbeat, OpQuiesce, OpQuiesced, OpDeploy, OpGetClusterStatuses:
        return true
    default:
        return false
    }
}

type Target struct {
    ShardId   *int    `json:"shardId,omitempty"`
    ClusterId *int    `json:"clusterId,omitempty"`
    GuildId   *string `json:"guildId,omitempty"`
}

type ShardRequest struct {
    ID      string          `json:"id"`
    Op      OpCode          `json:"op"`
    Target  *Target         `json:"target,omitempty"`
    Payload json.RawMessage `json:"payload"`
}

type ShardResponse struct {
    ID    string          `json:"id"`
    OK    bool            `json:"ok"`
    Data  json.RawMessage `json:"data,omitempty"`
    Error string          `json:"error,omitempty"`
}
```
