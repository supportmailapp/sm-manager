package server

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"sm-manager/internal/protocol"
	"sm-manager/internal/status"
)

// InstanceRunner is implemented by the supervisor. It owns the instance
// processes; the hub only owns the registry.
type InstanceRunner interface {
	StartInstance(instanceID string) error // idempotent; error if no instanceSpawnCommand
	StopInstance(instanceID string)        // blocks until the process is gone; idempotent
}

// instance is one registry entry: an individually managed child process and
// the guild ids it owns. All fields except opMu are guarded by Hub.mu.
type instance struct {
	id       string
	guildIDs []string
	state    status.InstanceRunState
	exitCode int
	conn     *Conn

	generation int
	lastBeat   time.Time
	status     json.RawMessage // opaque HEARTBEAT status, forwarded verbatim

	// opMu serializes lifecycle ops (start/stop/restart/remove) per instance.
	opMu sync.Mutex
}

// InstanceInfo is the internal-socket view of an instance: the public state
// plus the guild ids it owns.
type InstanceInfo struct {
	status.InstanceState
	GuildIDs []string `json:"guildIds"`
}

// SetInstanceRunner wires the supervisor in. Called once at startup.
func (h *Hub) SetInstanceRunner(r InstanceRunner) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.instanceRunner = r
}

// SetInstanceState records a process lifecycle change reported by the
// supervisor. Unknown ids are ignored.
func (h *Hub) SetInstanceState(instanceID string, state status.InstanceRunState, exitCode int) {
	h.mu.Lock()
	inst := h.instances[instanceID]
	if inst != nil {
		inst.state = state
		inst.exitCode = exitCode
	}
	h.mu.Unlock()
	if inst != nil {
		h.instanceChanged(instanceID)
	}
}

// instanceChanged is the single hook point for publishing an instance's new
// state to the status feed. Called outside h.mu.
func (h *Hub) instanceChanged(instanceID string) {}

// instanceInfoLocked builds the view of one entry. Caller holds h.mu.
func (h *Hub) instanceInfoLocked(inst *instance) InstanceInfo {
	timeout := time.Duration(h.cfg.HeartbeatTimeoutSecs) * time.Second
	var lastBeat int64
	if !inst.lastBeat.IsZero() {
		lastBeat = inst.lastBeat.UnixMilli()
	}
	return InstanceInfo{
		InstanceState: status.InstanceState{
			InstanceID: inst.id,
			State:      inst.state,
			Generation: inst.generation,
			Alive:      inst.conn != nil && (timeout <= 0 || time.Since(inst.lastBeat) <= timeout),
			LastBeat:   lastBeat,
			ExitCode:   inst.exitCode,
			Status:     inst.status,
		},
		GuildIDs: append([]string{}, inst.guildIDs...),
	}
}

// snapshotInstances returns every registry entry sorted by id. Never nil.
func (h *Hub) snapshotInstances() []InstanceInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]InstanceInfo, 0, len(h.instances))
	for _, inst := range h.instances {
		out = append(out, h.instanceInfoLocked(inst))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out
}

// instanceRegisterPayload is the REGISTER body of an instance.
type instanceRegisterPayload struct {
	InstanceID string `json:"instanceId"`
	Generation int    `json:"generation"`
}

// handleInstanceRegister binds c to a registry entry. Only ids the manager
// already knows are accepted.
func (h *Hub) handleInstanceRegister(c *Conn, req *protocol.ShardRequest, p instanceRegisterPayload) {
	h.mu.Lock()
	inst := h.instances[p.InstanceID]
	if inst == nil {
		h.mu.Unlock()
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "unknown instance"})
		return
	}
	now := time.Now()
	inst.conn = c
	inst.generation = p.Generation
	inst.lastBeat = now
	inst.state = status.InstanceRunning
	inst.exitCode = 0
	h.mu.Unlock()

	id := p.InstanceID
	c.mu.Lock()
	c.instanceID = &id
	c.generation = p.Generation
	c.lastBeat = now
	c.mu.Unlock()

	log.Printf("instance %s registered (generation %d)", id, p.Generation)
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true})
	h.instanceChanged(id)
}

// handleInstanceHeartbeat refreshes the entry bound to c and stores the opaque
// status payload.
func (h *Hub) handleInstanceHeartbeat(c *Conn, req *protocol.ShardRequest, id string) {
	var p struct {
		Status json.RawMessage `json:"status"`
	}
	_ = json.Unmarshal(req.Payload, &p)

	now := time.Now()
	h.mu.Lock()
	if inst := h.instances[id]; inst != nil && inst.conn == c {
		inst.lastBeat = now
		if len(p.Status) > 0 {
			inst.status = append(json.RawMessage(nil), p.Status...)
		}
	}
	h.mu.Unlock()

	c.mu.Lock()
	c.lastBeat = now
	c.mu.Unlock()

	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true})
	h.instanceChanged(id)
}

// detachInstanceConnLocked clears the entry's conn if c is still the bound one
// and returns whether it did. A running instance falls back to starting; a
// failed or stopped one keeps its state. The entry and its guild mappings stay.
// Caller holds h.mu.
func (h *Hub) detachInstanceConnLocked(id string, c *Conn) bool {
	inst := h.instances[id]
	if inst == nil || inst.conn != c {
		return false
	}
	inst.conn = nil
	if inst.state == status.InstanceRunning {
		inst.state = status.InstanceStarting
	}
	return true
}

// reapInstancesLocked drops instance conns whose heartbeat has lapsed and
// returns the affected ids. Caller holds h.mu.
func (h *Hub) reapInstancesLocked(now time.Time, timeout time.Duration) []string {
	var reaped []string
	for id, inst := range h.instances {
		if inst.conn == nil || now.Sub(inst.lastBeat) <= timeout {
			continue
		}
		log.Printf("instance %s heartbeat lapsed (%s), marking dead", id, now.Sub(inst.lastBeat).Round(time.Second))
		_ = inst.conn.nc.Close()
		h.detachInstanceConnLocked(id, inst.conn)
		reaped = append(reaped, id)
	}
	return reaped
}
