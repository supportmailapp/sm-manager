package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
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

// Instance ops.
//
// Lock discipline: instance.opMu is always taken before Hub.mu, never the
// other way round, and neither h.mu nor a conn write lock is held while the
// runner is called (StopInstance blocks up to the grace period). Because the
// hub is the only caller of the runner and takes opMu around every call, it
// never issues a start and a stop for one id concurrently.

var (
	errNoInstanceRunner = errors.New("no instance runner configured")
	errUnknownInstance  = errors.New("unknown instance")
)

// InstanceSyncEntry is one element of the desired list sent with
// SYNC_INSTANCES.
type InstanceSyncEntry struct {
	InstanceID string   `json:"instanceId"`
	GuildIDs   []string `json:"guildIds"`
	Run        bool     `json:"run"`
}

type startInstancePayload struct {
	InstanceID string   `json:"instanceId"`
	GuildIDs   []string `json:"guildIds"`
}

type instanceIDPayload struct {
	InstanceID string `json:"instanceId"`
}

type syncInstancesPayload struct {
	Instances []InstanceSyncEntry `json:"instances"`
}

var jsonNull = json.RawMessage("null")

func (h *Hub) replyInstanceOK(c *Conn, req *protocol.ShardRequest, data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "failed to marshal reply"})
		return
	}
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true, Data: raw})
}

func (h *Hub) replyInstanceErr(c *Conn, req *protocol.ShardRequest, err error) {
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: err.Error()})
}

// handleInstanceOp runs one instance op to completion and replies. It runs in
// its own goroutine so a slow stop never blocks the caller's read loop.
func (h *Hub) handleInstanceOp(c *Conn, op protocol.OpCode, req *protocol.ShardRequest) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("instance op %s panicked: %v", op, r)
			_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "internal error"})
		}
	}()

	if op == protocol.OpGetInstanceStatuses {
		h.replyInstanceOK(c, req, h.snapshotInstances())
		return
	}

	h.mu.RLock()
	runner := h.instanceRunner
	h.mu.RUnlock()
	if runner == nil {
		h.replyInstanceErr(c, req, errNoInstanceRunner)
		return
	}

	switch op {
	case protocol.OpStartInstance:
		var p startInstancePayload
		if err := json.Unmarshal(req.Payload, &p); err != nil || p.InstanceID == "" {
			h.replyInstanceErr(c, req, errors.New("malformed payload: instanceId and guildIds required"))
			return
		}
		guilds, err := cleanGuildIDs(p.GuildIDs)
		if err != nil {
			h.replyInstanceErr(c, req, err)
			return
		}
		info, err := h.startInstance(runner, p.InstanceID, guilds)
		if err != nil {
			h.replyInstanceErr(c, req, err)
			return
		}
		h.replyInstanceOK(c, req, info)

	case protocol.OpStopInstance, protocol.OpRestartInstance, protocol.OpRemoveInstance:
		var p instanceIDPayload
		if err := json.Unmarshal(req.Payload, &p); err != nil || p.InstanceID == "" {
			h.replyInstanceErr(c, req, errors.New("malformed payload: instanceId required"))
			return
		}
		switch op {
		case protocol.OpStopInstance:
			h.replyStop(c, req, runner, p.InstanceID)
		case protocol.OpRestartInstance:
			h.replyRestart(c, req, runner, p.InstanceID)
		default:
			h.removeInstance(runner, p.InstanceID)
			h.replyInstanceOK(c, req, jsonNull)
		}

	case protocol.OpSyncInstances:
		var p syncInstancesPayload
		if err := json.Unmarshal(req.Payload, &p); err != nil {
			h.replyInstanceErr(c, req, errors.New("malformed payload: instances required"))
			return
		}
		if err := h.syncInstances(runner, p.Instances); err != nil {
			h.replyInstanceErr(c, req, err)
			return
		}
		h.replyInstanceOK(c, req, h.snapshotInstances())
	}
}

func (h *Hub) replyStop(c *Conn, req *protocol.ShardRequest, runner InstanceRunner, id string) {
	inst := h.lockInstance(id)
	if inst == nil {
		h.replyInstanceOK(c, req, jsonNull)
		return
	}
	runner.StopInstance(id)
	inst.opMu.Unlock()
	h.instanceChanged(id)
	info, ok := h.instanceInfo(id)
	if !ok {
		h.replyInstanceOK(c, req, jsonNull)
		return
	}
	h.replyInstanceOK(c, req, info)
}

func (h *Hub) replyRestart(c *Conn, req *protocol.ShardRequest, runner InstanceRunner, id string) {
	inst := h.lockInstance(id)
	if inst == nil {
		h.replyInstanceErr(c, req, errUnknownInstance)
		return
	}
	runner.StopInstance(id)
	err := h.spawnLocked(runner, id)
	inst.opMu.Unlock()
	h.instanceChanged(id)
	if err != nil {
		h.replyInstanceErr(c, req, err)
		return
	}
	info, _ := h.instanceInfo(id)
	h.replyInstanceOK(c, req, info)
}

// cleanGuildIDs rejects empty ids and drops duplicates, keeping order.
func cleanGuildIDs(in []string) ([]string, error) {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, g := range in {
		if g == "" {
			return nil, errors.New("malformed payload: empty guild id")
		}
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		out = append(out, g)
	}
	return out, nil
}

// instanceInfo returns the view of one entry.
func (h *Hub) instanceInfo(id string) (InstanceInfo, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	inst := h.instances[id]
	if inst == nil {
		return InstanceInfo{}, false
	}
	return h.instanceInfoLocked(inst), true
}

// lockInstance returns the entry for id with its opMu held, or nil when there
// is no such entry. The entry is re-checked after the lock is won, so a
// concurrent REMOVE is never acted on.
func (h *Hub) lockInstance(id string) *instance {
	for {
		h.mu.RLock()
		inst := h.instances[id]
		h.mu.RUnlock()
		if inst == nil {
			return nil
		}
		inst.opMu.Lock()
		if h.isCurrent(inst) {
			return inst
		}
		inst.opMu.Unlock()
	}
}

// isCurrent reports whether inst is still the registered entry for its id.
func (h *Hub) isCurrent(inst *instance) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.instances[inst.id] == inst
}

// setGuildsLocked replaces the guild mappings of inst. Caller holds h.mu and
// has already checked for conflicts.
func (h *Hub) setGuildsLocked(inst *instance, guildIDs []string) {
	for _, g := range inst.guildIDs {
		if h.guildInstance[g] == inst.id {
			delete(h.guildInstance, g)
		}
	}
	for _, g := range guildIDs {
		h.guildInstance[g] = inst.id
	}
	inst.guildIDs = guildIDs
}

// registerEntry creates or updates the entry for id and its guild mappings.
// The conflict check and the mapping insert happen under one h.mu hold, so two
// instances can never claim the same guild.
func (h *Hub) registerEntry(id string, guildIDs []string) (inst *instance, created bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, g := range guildIDs {
		if owner, ok := h.guildInstance[g]; ok && owner != id {
			return nil, false, fmt.Errorf("guild %s is already mapped to instance %s", g, owner)
		}
	}
	inst = h.instances[id]
	if inst == nil {
		inst = &instance{id: id, state: status.InstanceStopped}
		h.instances[id] = inst
		created = true
	}
	h.setGuildsLocked(inst, guildIDs)
	return inst, created, nil
}

// spawnLocked asks the runner to start id and, once it has accepted, shows the
// entry as starting right away (the supervisor reports the same asynchronously).
// Caller holds the entry's opMu, not h.mu.
func (h *Hub) spawnLocked(runner InstanceRunner, id string) error {
	if err := runner.StartInstance(id); err != nil {
		return err
	}
	h.mu.Lock()
	if inst := h.instances[id]; inst != nil && (inst.state == status.InstanceStopped || inst.state == status.InstanceFailed) {
		inst.state = status.InstanceStarting
		inst.exitCode = 0
	}
	h.mu.Unlock()
	return nil
}

// startInstance implements START_INSTANCE.
func (h *Hub) startInstance(runner InstanceRunner, id string, guildIDs []string) (InstanceInfo, error) {
	for {
		inst, created, err := h.registerEntry(id, guildIDs)
		if err != nil {
			return InstanceInfo{}, err
		}
		inst.opMu.Lock()
		if !h.isCurrent(inst) {
			// Removed between registering and winning the lock; register again.
			inst.opMu.Unlock()
			continue
		}

		h.mu.RLock()
		state := inst.state
		h.mu.RUnlock()

		var startErr error
		if state != status.InstanceRunning && state != status.InstanceStarting {
			startErr = h.spawnLocked(runner, id)
			if startErr != nil && created && state == status.InstanceStopped {
				// Nothing was ever started for an entry this call created: undo it.
				h.mu.Lock()
				if h.instances[id] == inst {
					h.setGuildsLocked(inst, nil)
					delete(h.instances, id)
				}
				h.mu.Unlock()
			}
		}
		inst.opMu.Unlock()
		h.instanceChanged(id)
		if startErr != nil {
			return InstanceInfo{}, startErr
		}
		info, _ := h.instanceInfo(id)
		return info, nil
	}
}

// removeInstance stops id if needed, then drops its entry and guild mappings.
// Unknown ids are a no-op.
func (h *Hub) removeInstance(runner InstanceRunner, id string) {
	inst := h.lockInstance(id)
	if inst == nil {
		return
	}
	runner.StopInstance(id)
	h.mu.Lock()
	if h.instances[id] == inst {
		h.setGuildsLocked(inst, nil)
		delete(h.instances, id)
	}
	h.mu.Unlock()
	inst.opMu.Unlock()
	h.instanceChanged(id)
}

// syncInstances implements SYNC_INSTANCES: make the registry match the list.
//  1. Validate the list on its own (no I/O, nothing changes on failure).
//  2. Stop and remove every entry that is not listed.
//  3. Register all listed entries in one atomic step.
//  4. Apply each entry's run flag, concurrently.
func (h *Hub) syncInstances(runner InstanceRunner, entries []InstanceSyncEntry) error {
	listed := make(map[string]struct{}, len(entries))
	owner := make(map[string]string)
	guilds := make([][]string, len(entries))
	for i, e := range entries {
		if e.InstanceID == "" {
			return errors.New("malformed payload: instanceId required")
		}
		if _, dup := listed[e.InstanceID]; dup {
			return fmt.Errorf("instance %s listed twice", e.InstanceID)
		}
		listed[e.InstanceID] = struct{}{}
		g, err := cleanGuildIDs(e.GuildIDs)
		if err != nil {
			return err
		}
		guilds[i] = g
		for _, gid := range g {
			if other, ok := owner[gid]; ok {
				return fmt.Errorf("guild %s listed for both %s and %s", gid, other, e.InstanceID)
			}
			owner[gid] = e.InstanceID
		}
	}

	// Phase 2: remove what is not listed.
	h.mu.RLock()
	var stale []string
	for id := range h.instances {
		if _, ok := listed[id]; !ok {
			stale = append(stale, id)
		}
	}
	h.mu.RUnlock()
	var wg sync.WaitGroup
	for _, id := range stale {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			h.removeInstance(runner, id)
		}(id)
	}
	wg.Wait()

	// Phase 3: register every listed entry atomically. Guilds held by another
	// listed entry are about to be rewritten, so only a holder outside the
	// list (created concurrently) is a conflict.
	var changed []string
	err := func() error {
		h.mu.Lock()
		defer h.mu.Unlock()
		for i := range entries {
			for _, g := range guilds[i] {
				if holder, ok := h.guildInstance[g]; ok {
					if _, inList := listed[holder]; !inList {
						return fmt.Errorf("guild %s is already mapped to instance %s", g, holder)
					}
				}
			}
		}
		for _, e := range entries {
			inst := h.instances[e.InstanceID]
			if inst == nil {
				inst = &instance{id: e.InstanceID, state: status.InstanceStopped}
				h.instances[e.InstanceID] = inst
			}
			h.setGuildsLocked(inst, nil) // free the old mappings first
		}
		for i, e := range entries {
			h.setGuildsLocked(h.instances[e.InstanceID], guilds[i])
			changed = append(changed, e.InstanceID)
		}
		return nil
	}()
	if err != nil {
		return err
	}
	for _, id := range changed {
		h.instanceChanged(id)
	}

	// Phase 4: apply run flags.
	var mu sync.Mutex
	var failures []string
	for _, e := range entries {
		wg.Add(1)
		go func(e InstanceSyncEntry) {
			defer wg.Done()
			if err := h.applyRunFlag(runner, e); err != nil {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("%s: %v", e.InstanceID, err))
				mu.Unlock()
			}
		}(e)
	}
	wg.Wait()
	if len(failures) > 0 {
		sort.Strings(failures)
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

// applyRunFlag brings one synced entry to its desired run state. A failed
// instance is never restarted here.
func (h *Hub) applyRunFlag(runner InstanceRunner, e InstanceSyncEntry) error {
	inst := h.lockInstance(e.InstanceID)
	if inst == nil {
		return nil // removed concurrently
	}
	defer func() {
		inst.opMu.Unlock()
		h.instanceChanged(e.InstanceID)
	}()

	h.mu.RLock()
	state := inst.state
	h.mu.RUnlock()

	if e.Run {
		if state == status.InstanceStopped {
			return h.spawnLocked(runner, e.InstanceID)
		}
		return nil
	}
	if state == status.InstanceRunning || state == status.InstanceStarting {
		runner.StopInstance(e.InstanceID)
	}
	return nil
}
