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
	// OpDeploy triggers a generation rollout: spawn a new generation, wait for
	// every shard in it to become ready, then quiesce the old generation. Any
	// client (not just clusters) may send this — it's the manager control op
	// a dashboard/API sits behind.
	OpDeploy OpCode = "DEPLOY"
	// OpGetClusterStatuses returns a snapshot of every active cluster's current
	// state — the same view the statuspage WS broadcasts, but pulled on demand
	// over the internal socket. Manager-answered, no target; replies
	// `{ok, data:[]ClusterState}`. A plain client (sm-api's scheduled stats job)
	// sits behind this so it can read fleet-wide guild/member counts.
	OpGetClusterStatuses OpCode = "GET_CLUSTER_STATUSES"

	// Instance ops: manager-answered ops that drive the instance registry, a set
	// of separately supervised single-process bots that are not clusters. Sent by
	// plain clients; replies carry the instance state. See PROTOCOL.md\n\t// (canonical copy in the shared repo; this package is a hand-mirror of it).
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
	ID      string         `json:"id"`
	Op      OpCode         `json:"op"`
	Target  *Target        `json:"target,omitzero"`
	Payload jsontext.Value `json:"payload"`
}

type ShardResponse struct {
	ID    string         `json:"id"`
	OK    bool           `json:"ok"`
	Data  jsontext.Value `json:"data,omitzero"`
	Error string         `json:"error,omitempty"`
}

// FrameKind distinguishes an inbound request from an inbound response without
// fully decoding either. A frame carrying an "op" field is a request;
// otherwise it is a response correlated by "id".
type FrameKind struct {
	Op *OpCode `json:"op"`
	ID string  `json:"id"`
}
