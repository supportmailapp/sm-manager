package server

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/getsentry/sentry-go/attribute"

	"sm-manager/internal/config"
	"sm-manager/internal/protocol"
	"sm-manager/internal/status"
	"sm-manager/internal/telemetry"
	"sm-manager/internal/wirejson"
)

// Deployer triggers a generation rollout — spawn a new generation, wait for
// it to become ready, quiesce the old one — and reports the generation it
// cut over to. Implemented by the supervisor; the hub only depends on this
// interface so it doesn't need to import the supervisor package (which in
// turn needs *Hub for AwaitGeneration/QuiesceGeneration).
type Deployer interface {
	Deploy(ctx context.Context) (generation int, err error)
}

// Conn is a single socket peer. A peer may be a cluster (after REGISTER) or a
// plain client (e.g. the API) that only ever sends business ops.
type Conn struct {
	id      uint64
	nc      writeCloser
	writeMu sync.Mutex

	mu         sync.Mutex
	clusterID  *int
	instanceID *string // set when the peer registered as an instance
	shards     []int
	generation int
	lastBeat   time.Time
	status     jsontext.Value // opaque per-cluster HEARTBEAT status payload, forwarded verbatim to the statuspage WS
}

// writeCloser is the subset of net.Conn the hub needs; narrowed for testability.
type writeCloser interface {
	Write(p []byte) (int, error)
	Close() error
}

func (c *Conn) send(v any) error {
	payload, err := json.Marshal(v, wirejson.Wire)
	if err != nil {
		return err
	}
	frame, err := protocol.EncodeFrame(payload)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.nc.Write(frame)
	return err
}

// sendRaw forwards an already-decoded frame body (used when routing a frame
// through untouched, without re-serializing it).
func (c *Conn) sendRaw(payload []byte) error {
	frame, err := protocol.EncodeFrame(payload)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.nc.Write(frame)
	return err
}

// Hub is the in-memory registry. Rebuilt from cluster registration on restart;
// no persistence here.
type Hub struct {
	cfg        *config.Config
	allowedOps map[protocol.OpCode]struct{}

	mu   sync.RWMutex
	cond *sync.Cond // broadcast on any registration/removal change; guards generation-readiness waiters

	clusters    map[int]*Conn         // by clusterID, set on REGISTER — routes business ops to whichever generation last registered that slot
	generations map[int]map[int]*Conn // generation → clusterID → Conn, used for readiness gating a rollout

	instances     map[string]*instance // by instance id; entries survive disconnects, removed only by the instance ops
	guildInstance map[string]string    // guild id → instance id (the exception register)

	pending        map[string]*Conn         // correlation id → originating conn
	quiesceWaiters map[string]chan struct{} // correlation id → waiter for a manager-initiated QUIESCE's QUIESCED ack
	nextID         uint64

	deployer Deployer // set once via SetDeployer; nil until the supervisor wires itself in

	instanceRunner InstanceRunner // set once via SetInstanceRunner; nil until the supervisor wires itself in

	statusSink status.Sink // set once via SetStatusSink; nil when the statuspage WS is disabled

	// pubMu serializes status publishes. A publish reads the current state and
	// then hands it to the sink; without this, two publishes for one id could
	// interleave and the older read be delivered last, leaving a stale entry (or
	// a ghost one after a removal) in the feed. Lock order: pubMu, then mu.
	pubMu sync.Mutex

	// syncMu lets one SYNC_INSTANCES run at a time. Lock order: syncMu, then an
	// instance's opMu, then mu.
	syncMu sync.Mutex
}

func NewHub(cfg *config.Config) *Hub {
	h := &Hub{
		cfg:            cfg,
		allowedOps:     cfg.AllowedSet(),
		clusters:       make(map[int]*Conn),
		generations:    make(map[int]map[int]*Conn),
		instances:      make(map[string]*instance),
		guildInstance:  make(map[string]string),
		pending:        make(map[string]*Conn),
		quiesceWaiters: make(map[string]chan struct{}),
	}
	h.cond = sync.NewCond(&h.mu)
	return h
}

// SetDeployer wires the supervisor in so an inbound DEPLOY has something to
// call. Must be set before Serve starts accepting connections.
func (h *Hub) SetDeployer(d Deployer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deployer = d
}

// SetStatusSink wires the public statuspage WS broadcaster in. Called once at
// startup before Serve. nil (never set) cleanly disables status publishing.
func (h *Hub) SetStatusSink(s status.Sink) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statusSink = s
}

// publishActive emits the current active-routing state for clusterID to the
// status sink, or a removal if no cluster currently holds that slot. It reads a
// fresh snapshot under its own lock rather than trusting caller-held state, and
// calls the sink OUTSIDE h.mu so the broadcaster's fanout never runs under the
// hub lock. Deltas reflect the active (routed) generation only — during a
// rollout a not-yet-promoted new generation isn't shown until PromoteClusters.
func (h *Hub) publishActive(clusterID int) {
	h.pubMu.Lock()
	defer h.pubMu.Unlock()
	h.mu.RLock()
	sink := h.statusSink
	c := h.clusters[clusterID]
	var (
		cs      status.ClusterState
		present bool
	)
	if c != nil {
		present = true
		timeout := time.Duration(h.cfg.HeartbeatTimeoutSecs) * time.Second
		c.mu.Lock()
		cs = status.ClusterState{
			ClusterID:  clusterID,
			Shards:     c.shards,
			Generation: c.generation,
			LastBeat:   c.lastBeat.UnixMilli(),
			Status:     c.status,
			Alive:      timeout <= 0 || time.Since(c.lastBeat) <= timeout,
		}
		c.mu.Unlock()
	}
	h.mu.RUnlock()

	if sink == nil {
		return
	}
	if present {
		sink.Publish(cs)
	} else {
		sink.Remove(clusterID)
	}
}

func (h *Hub) newConn(nc writeCloser) *Conn {
	h.mu.Lock()
	h.nextID++
	id := h.nextID
	h.mu.Unlock()
	return &Conn{id: id, nc: nc}
}

// removeConn drops a peer: unregister its cluster slot and purge any pending
// requests it originated (their responses can never arrive now).
func (h *Hub) removeConn(c *Conn) {
	h.mu.Lock()
	c.mu.Lock()
	cid := c.clusterID
	gen := c.generation
	iid := c.instanceID
	c.mu.Unlock()
	removedActive := false
	if cid != nil {
		if cur, ok := h.clusters[*cid]; ok && cur == c {
			delete(h.clusters, *cid)
			removedActive = true
			log.Printf("cluster %d disconnected", *cid)
			telemetry.Logger().Warn().Int("cluster_id", *cid).Int("generation", gen).Emit("cluster disconnected")
			telemetry.Meter().Count("sm_manager.cluster_disconnected", 1,
				sentry.WithAttributes(attribute.Int("cluster_id", *cid), attribute.Int("generation", gen)))
		}
		if members := h.generations[gen]; members != nil {
			if cur, ok := members[*cid]; ok && cur == c {
				delete(members, *cid)
			}
		}
	}
	// An instance conn only ever clears its own registry entry, never a cluster slot.
	detachedInstance := false
	if iid != nil {
		detachedInstance = h.detachInstanceConnLocked(*iid, c)
	}
	for id, origin := range h.pending {
		if origin == c {
			delete(h.pending, id)
		}
	}
	h.cond.Broadcast()
	h.mu.Unlock()

	// Publish outside the lock. Only the active conn losing its slot changes the
	// public view (a stale new/old-gen conn dropping doesn't).
	if removedActive {
		h.publishActive(*cid)
	}
	if detachedInstance {
		h.instanceChanged(*iid)
	}
}

// handleFrame dispatches one inbound frame from conn c.
func (h *Hub) handleFrame(c *Conn, raw []byte) {
	var kind protocol.FrameKind
	if err := json.Unmarshal(raw, &kind, wirejson.Wire); err != nil {
		log.Printf("conn %d: bad frame: %v", c.id, err)
		// Wire rejects repeated member names (anywhere in the frame, payload
		// included). A requester still waiting on an answer should hear about
		// it, so reply when the request's id can be read leniently. A frame
		// that is not JSON, or has no op or no id, stays a silent drop.
		var lenient protocol.FrameKind
		if json.Unmarshal(raw, &lenient, wirejson.Wire, jsontext.AllowDuplicateNames(true)) == nil &&
			lenient.Op != nil && lenient.ID != "" {
			_ = c.send(protocol.ShardResponse{ID: lenient.ID, OK: false, Error: "malformed request"})
		}
		return
	}

	// No "op" field → this is a response to a previously forwarded request.
	if kind.Op == nil {
		h.routeResponse(kind.ID, raw)
		return
	}

	op := *kind.Op
	if protocol.IsStructural(op) {
		h.handleStructural(c, op, raw)
		return
	}
	h.handleBusiness(c, op, raw)
}

// handleStructural processes the only ops the manager understands.
func (h *Hub) handleStructural(c *Conn, op protocol.OpCode, raw []byte) {
	var req protocol.ShardRequest
	if err := json.Unmarshal(raw, &req, wirejson.Wire); err != nil {
		_ = c.send(protocol.ShardResponse{ID: "", OK: false, Error: "malformed request"})
		return
	}
	switch op {
	case protocol.OpRegister:
		h.handleRegister(c, &req)
	case protocol.OpHeartbeat:
		h.handleHeartbeat(c, &req)
	case protocol.OpQuiesced:
		h.handleQuiesced(&req, raw)
	case protocol.OpQuiesce:
		// Manager → cluster only; never legitimately inbound.
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "QUIESCE is manager-originated"})
	case protocol.OpDeploy:
		h.handleDeploy(c, &req)
	case protocol.OpStartInstance, protocol.OpStopInstance, protocol.OpRestartInstance,
		protocol.OpRemoveInstance, protocol.OpSyncInstances, protocol.OpGetInstanceStatuses:
		// Own goroutine: a stop waits out the grace period and must not block the read loop.
		go h.handleInstanceOp(c, op, &req)
	case protocol.OpGetClusterStatuses:
		h.handleGetClusterStatuses(c, &req)
	}
}

// handleGetClusterStatuses answers a plain client (e.g. sm-api's stats job)
// with a snapshot of every active cluster's current state. Manager-answered,
// no target — it reads the manager's own registry, so no DB or bot knowledge
// is involved.
func (h *Hub) handleGetClusterStatuses(c *Conn, req *protocol.ShardRequest) {
	states := h.snapshotActive()
	data, err := json.Marshal(states, wirejson.Wire)
	if err != nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "failed to marshal cluster statuses"})
		return
	}
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true, Data: data})
}

// snapshotActive builds the current active-routing view of every cluster,
// mirroring the per-cluster state publishActive emits. Returns a non-nil
// (possibly empty) slice. Lock order matches publishActive: h.mu then c.mu.
func (h *Hub) snapshotActive() []status.ClusterState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	timeout := time.Duration(h.cfg.HeartbeatTimeoutSecs) * time.Second
	states := make([]status.ClusterState, 0, len(h.clusters))
	for cid, c := range h.clusters {
		c.mu.Lock()
		states = append(states, status.ClusterState{
			ClusterID:  cid,
			Shards:     c.shards,
			Generation: c.generation,
			LastBeat:   c.lastBeat.UnixMilli(),
			Status:     c.status,
			Alive:      timeout <= 0 || time.Since(c.lastBeat) <= timeout,
		})
		c.mu.Unlock()
	}
	return states
}

// registerPayload's Generation is the epoch this cluster process was spawned
// under (see Supervisor's GENERATION env var). Omitted/zero means generation
// 0 — the only generation that exists before any soft-restart rollout.
type registerPayload struct {
	ClusterID  int   `json:"clusterId"`
	Shards     []int `json:"shards"`
	Generation int   `json:"generation"`
}

// errAlreadyRegistered is the reply to a REGISTER that would change what a
// connection is bound to. Its text is part of the wire contract.
const errAlreadyRegistered = "connection already registered as a different peer"

// rebinds reports whether registering c as the given identity would change an
// identity it already holds. Registering again as the very same identity stays
// allowed (it refreshes the entry). An instance passes instanceID (clusterID
// nil); a cluster passes clusterID and generation (instanceID nil).
func (c *Conn) rebinds(instanceID *string, clusterID *int, generation int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.instanceID != nil:
		return instanceID == nil || *c.instanceID != *instanceID
	case c.clusterID != nil:
		return clusterID == nil || *c.clusterID != *clusterID || c.generation != generation
	}
	return false
}

func (h *Hub) handleRegister(c *Conn, req *protocol.ShardRequest) {
	// Probe first: an instanceId means the instance path, otherwise it is a cluster.
	var probe instanceRegisterPayload
	if err := json.Unmarshal(req.Payload, &probe, wirejson.Wire); err != nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "bad REGISTER payload"})
		return
	}
	if probe.InstanceID != "" {
		if c.rebinds(&probe.InstanceID, nil, 0) {
			_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: errAlreadyRegistered})
			return
		}
		h.handleInstanceRegister(c, req, probe)
		return
	}
	var p registerPayload
	if err := json.Unmarshal(req.Payload, &p, wirejson.Wire); err != nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "bad REGISTER payload"})
		return
	}
	if c.rebinds(nil, &p.ClusterID, p.Generation) {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: errAlreadyRegistered})
		return
	}
	c.mu.Lock()
	c.clusterID = &p.ClusterID
	c.shards = p.Shards
	c.generation = p.Generation
	c.lastBeat = time.Now()
	c.mu.Unlock()

	h.mu.Lock()
	members, ok := h.generations[p.Generation]
	if !ok {
		members = make(map[int]*Conn)
		h.generations[p.Generation] = members
	}
	members[p.ClusterID] = c
	// Promote into the active routing table only if the slot is unclaimed
	// (cold start, or a crash-restart of the currently-active cluster whose
	// old conn was already removed). During a rollout the slot stays held by
	// the outgoing generation until Deploy calls PromoteClusters at cutover —
	// this is what stops a freshly-registered new generation from stealing
	// routing before it's ready, closing the duplicate-routing window.
	if _, occupied := h.clusters[p.ClusterID]; !occupied {
		h.clusters[p.ClusterID] = c
	}
	h.mu.Unlock()
	h.cond.Broadcast()

	log.Printf("cluster %d registered (shards %v, generation %d) — connected, alive",
		p.ClusterID, p.Shards, p.Generation)
	telemetry.Logger().Info().
		Int("cluster_id", p.ClusterID).
		Int("generation", p.Generation).
		Int("shard_count", len(p.Shards)).
		Emit("cluster registered")
	telemetry.Meter().Count("sm_manager.cluster_registered", 1,
		sentry.WithAttributes(attribute.Int("cluster_id", p.ClusterID), attribute.Int("generation", p.Generation)))
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true})
	h.publishActive(p.ClusterID)
}

// handleQuiesced routes a QUIESCED ack. If it correlates to a manager-
// initiated QUIESCE (QuiesceGeneration), it wakes that waiter. Otherwise it
// falls back to normal response routing, in case some future non-manager
// peer correlates its own QUIESCE the same way a business op does.
func (h *Hub) handleQuiesced(req *protocol.ShardRequest, raw []byte) {
	h.mu.Lock()
	ch, ok := h.quiesceWaiters[req.ID]
	if ok {
		delete(h.quiesceWaiters, req.ID)
	}
	h.mu.Unlock()
	if ok {
		close(ch)
		return
	}
	h.routeResponse(req.ID, raw)
}

// handleDeploy triggers a generation rollout via the wired Deployer (the
// supervisor) and blocks until it completes, fails, or DeployTimeoutSecs
// elapses. This is the manager control op Phase 4 needed a surface for.
func (h *Hub) handleDeploy(c *Conn, req *protocol.ShardRequest) {
	h.mu.RLock()
	deployer := h.deployer
	h.mu.RUnlock()
	if deployer == nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "no deployer configured"})
		return
	}

	ctx := context.Background()
	if h.cfg.DeployTimeoutSecs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(h.cfg.DeployTimeoutSecs)*time.Second)
		defer cancel()
	}

	gen, err := deployer.Deploy(ctx)
	if err != nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: err.Error()})
		return
	}
	data, _ := json.Marshal(struct {
		Generation int `json:"generation"`
	}{Generation: gen}, wirejson.Wire)
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true, Data: data})
}

func (h *Hub) handleHeartbeat(c *Conn, req *protocol.ShardRequest) {
	c.mu.Lock()
	iid := c.instanceID
	c.mu.Unlock()
	if iid != nil {
		h.handleInstanceHeartbeat(c, req, *iid)
		return
	}
	c.mu.Lock()
	c.lastBeat = time.Now()
	if len(req.Payload) > 0 {
		// Store the opaque status payload for the statuspage WS. Copied so a
		// later reader isn't aliasing the decoded request buffer.
		c.status = append(c.status[:0], req.Payload...)
	}
	cid := c.clusterID
	c.mu.Unlock()
	if cid != nil {
		log.Printf("heartbeat from cluster %d", *cid)
	}
	_ = c.send(protocol.ShardResponse{ID: req.ID, OK: true})

	// Only the active (routed) conn drives the public status feed. A
	// not-yet-promoted new-generation conn heartbeating during a rollout does
	// not publish — publishActive reads the active slot, which is still the old
	// generation until PromoteClusters.
	if cid == nil {
		return
	}
	h.mu.RLock()
	active := h.clusters[*cid] == c
	h.mu.RUnlock()
	if active {
		h.publishActive(*cid)
	}
}

// handleBusiness gates against the allowed-op list, then forwards to the target
// cluster untouched. The manager never inspects or validates the payload.
func (h *Hub) handleBusiness(c *Conn, op protocol.OpCode, raw []byte) {
	var kind protocol.FrameKind
	_ = json.Unmarshal(raw, &kind, wirejson.Wire)

	if _, ok := h.allowedOps[op]; !ok {
		_ = c.send(protocol.ShardResponse{ID: kind.ID, OK: false, Error: "unknown op: " + string(op)})
		return
	}

	// Decode only the envelope needed for routing — never the payload.
	var req protocol.ShardRequest
	if err := json.Unmarshal(raw, &req, wirejson.Wire); err != nil {
		_ = c.send(protocol.ShardResponse{ID: kind.ID, OK: false, Error: "malformed request"})
		return
	}

	target, err := h.resolveTarget(req.Target)
	if err != nil {
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: err.Error()})
		return
	}

	h.mu.Lock()
	h.pending[req.ID] = c
	h.mu.Unlock()

	if err := target.sendRaw(raw); err != nil {
		h.mu.Lock()
		delete(h.pending, req.ID)
		h.mu.Unlock()
		_ = c.send(protocol.ShardResponse{ID: req.ID, OK: false, Error: "forward failed"})
		telemetry.Meter().Count("sm_manager.business_op_forward_failed", 1,
			sentry.WithAttributes(attribute.String("op", string(op))))
		return
	}
	telemetry.Meter().Count("sm_manager.business_op_forwarded", 1,
		sentry.WithAttributes(attribute.String("op", string(op))))
}

// errNoClusterForTarget and errInstanceOffline are the two routing failures a
// requester can see. Their text is part of the wire contract.
var (
	errNoClusterForTarget = errors.New("no cluster for target")
	errInstanceOffline    = errors.New("instance offline")
)

// resolveTarget picks the destination for a business op, in priority order:
// explicit clusterId, then explicit shardId, then a guildId. A guildId in the
// exception register goes to its instance; if that instance is not connected
// the result is errInstanceOffline and the request never falls back to a
// cluster. Any other guildId is mapped to a shard via Discord's sharding
// formula. The shard→cluster step is computed from topology (shardsPerCluster);
// there is no stored shard table.
func (h *Hub) resolveTarget(t *protocol.Target) (*Conn, error) {
	if t == nil {
		return nil, errNoClusterForTarget
	}
	var conn *Conn
	switch {
	case t.ClusterId != nil:
		conn = h.clusterConn(*t.ClusterId)
	case t.ShardId != nil:
		conn = h.clusterForShard(*t.ShardId)
	case t.GuildId != nil:
		h.mu.RLock()
		instID, registered := h.guildInstance[*t.GuildId]
		var instConn *Conn
		if registered {
			if inst := h.instances[instID]; inst != nil {
				instConn = inst.conn
			}
		}
		h.mu.RUnlock()
		if registered {
			if instConn == nil {
				return nil, errInstanceOffline
			}
			return instConn, nil
		}
		shard, ok := shardForGuild(*t.GuildId, h.cfg.TotalShards())
		if !ok {
			return nil, errNoClusterForTarget
		}
		conn = h.clusterForShard(shard)
	}
	if conn == nil {
		return nil, errNoClusterForTarget
	}
	return conn, nil
}

func (h *Hub) clusterConn(clusterID int) *Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clusters[clusterID]
}

// clusterForShard maps a shard id to its owning cluster via the contiguous
// topology (cluster c owns shards [c*shardsPerCluster, (c+1)*shardsPerCluster)).
func (h *Hub) clusterForShard(shard int) *Conn {
	if h.cfg.ShardsPerCluster <= 0 || shard < 0 {
		return nil
	}
	return h.clusterConn(shard / h.cfg.ShardsPerCluster)
}

// shardForGuild applies Discord's gateway sharding formula:
// shardId = (guildID >> 22) % totalShards. Returns false on an unparseable
// guild id or a non-positive shard count.
func shardForGuild(guildID string, totalShards int) (int, bool) {
	if totalShards <= 0 {
		return 0, false
	}
	id, err := strconv.ParseUint(guildID, 10, 64)
	if err != nil {
		return 0, false
	}
	return int((id >> 22) % uint64(totalShards)), true
}

// routeResponse sends a response/QUIESCED frame back to the peer that
// originated the correlated request.
func (h *Hub) routeResponse(id string, raw []byte) {
	h.mu.Lock()
	origin, ok := h.pending[id]
	if ok {
		delete(h.pending, id)
	}
	h.mu.Unlock()
	if !ok {
		log.Printf("response for unknown/expired id %q dropped", id)
		return
	}
	if err := origin.sendRaw(raw); err != nil {
		log.Printf("failed to deliver response %q: %v", id, err)
	}
}

// reapDead logs and drops clusters whose heartbeat has lapsed. Called on a tick.
func (h *Hub) reapDead() {
	timeout := time.Duration(h.cfg.HeartbeatTimeoutSecs) * time.Second
	now := time.Now()
	var reaped []int
	h.mu.Lock()
	for cid, c := range h.clusters {
		c.mu.Lock()
		last := c.lastBeat
		gen := c.generation
		c.mu.Unlock()
		if now.Sub(last) > timeout {
			log.Printf("cluster %d heartbeat lapsed (%s) — marking dead", cid, now.Sub(last).Round(time.Second))
			telemetry.Logger().Warn().
				Int("cluster_id", cid).
				Int("generation", gen).
				Int64("lapsed_ms", now.Sub(last).Round(time.Second).Milliseconds()).
				Emit("cluster heartbeat lapsed — marking dead")
			telemetry.Meter().Count("sm_manager.cluster_heartbeat_lapsed", 1,
				sentry.WithAttributes(attribute.Int("cluster_id", cid), attribute.Int("generation", gen)))
			delete(h.clusters, cid)
			if members := h.generations[gen]; members != nil {
				if cur, ok := members[cid]; ok && cur == c {
					delete(members, cid)
				}
			}
			_ = c.nc.Close()
			reaped = append(reaped, cid)
		}
	}
	reapedInstances := h.reapInstancesLocked(now, timeout)
	h.cond.Broadcast()
	h.mu.Unlock()

	for _, cid := range reaped {
		h.publishActive(cid) // slot now empty → emits a removal
	}
	for _, id := range reapedInstances {
		h.instanceChanged(id)
	}
}

// GenerationReady reports whether every cluster in [0, cfg.ClusterCount) has
// registered under generation gen and is still within its heartbeat window.
// This is the readiness gate a rollout step checks before cutting traffic
// over to a new generation or quiescing the old one (Phase 3).
func (h *Hub) GenerationReady(gen int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.generationReadyLocked(gen)
}

// generationReadyLocked is GenerationReady's body; caller must hold h.mu
// (read or write lock). It checks every cluster in the full topology.
func (h *Hub) generationReadyLocked(gen int) bool {
	return h.clustersReadyLocked(gen, h.allClusterIDs())
}

// clustersReadyLocked reports whether every clusterID in cids has registered
// under gen and is still within its heartbeat window. Caller holds h.mu. This
// is the per-batch readiness gate a cluster-by-cluster rollout checks — a
// batch only covers a subset of the fleet, so it can't wait on the whole
// generation being present.
func (h *Hub) clustersReadyLocked(gen int, cids []int) bool {
	members := h.generations[gen]
	if members == nil {
		return false
	}
	timeout := time.Duration(h.cfg.HeartbeatTimeoutSecs) * time.Second
	now := time.Now()
	for _, cid := range cids {
		c, ok := members[cid]
		if !ok {
			return false
		}
		c.mu.Lock()
		last := c.lastBeat
		c.mu.Unlock()
		if timeout > 0 && now.Sub(last) > timeout {
			return false
		}
	}
	return true
}

// allClusterIDs returns [0, cfg.ClusterCount).
func (h *Hub) allClusterIDs() []int {
	cids := make([]int, 0, h.cfg.ClusterCount)
	for cid := 0; cid < h.cfg.ClusterCount; cid++ {
		cids = append(cids, cid)
	}
	return cids
}

// PromoteClusters flips the active routing table for cids to their conns in
// generation gen. Deploy calls this at a batch's cutover — after the new
// generation's clusters for these ids are ready and before the old ones are
// quiesced — so business ops route to exactly one generation per clusterID at
// all times, switching atomically here.
func (h *Hub) PromoteClusters(gen int, cids []int) {
	h.mu.Lock()
	members := h.generations[gen]
	promoted := make([]int, 0, len(cids))
	for _, cid := range cids {
		if c := members[cid]; c != nil {
			h.clusters[cid] = c
			promoted = append(promoted, cid)
		}
	}
	h.cond.Broadcast()
	h.mu.Unlock()

	// Cutover flipped the active generation for these clusters — publish the
	// newly-routed state so the statuspage reflects the new generation.
	for _, cid := range promoted {
		h.publishActive(cid)
	}
}

// AwaitGeneration blocks until every cluster in generation gen has registered
// and is alive, or ctx is done first — whichever happens first. A rollout
// step calls this after spawning a new generation's clusters and before
// cutting traffic over / quiescing the old generation, so a slow or crash-
// looping new cluster blocks the handoff instead of half-completing it.
func (h *Hub) AwaitGeneration(ctx context.Context, gen int) error {
	return h.AwaitClusters(ctx, gen, h.allClusterIDs())
}

// AwaitClusters blocks until every clusterID in cids has registered under gen
// and is alive, or ctx is done first. This is the per-batch variant a
// cluster-by-cluster rollout waits on before promoting/quiescing that batch.
func (h *Hub) AwaitClusters(ctx context.Context, gen int, cids []int) error {
	stop := context.AfterFunc(ctx, h.cond.Broadcast)
	defer stop()

	h.mu.Lock()
	defer h.mu.Unlock()
	for !h.clustersReadyLocked(gen, cids) {
		if err := ctx.Err(); err != nil {
			return err
		}
		h.cond.Wait()
	}
	return nil
}

// QuiesceGeneration sends QUIESCE to every cluster currently tracked under
// gen and waits for each to ack with QUIESCED, in parallel, up to ctx's
// deadline. Called by a rollout step after the new generation is ready and
// before its own clusters are torn down. Returns a joined error naming every
// cluster that failed to send or ack in time — the caller decides whether a
// partial quiesce is still safe to cut over on.
func (h *Hub) QuiesceGeneration(ctx context.Context, gen int) error {
	h.mu.RLock()
	members := make([]*Conn, 0, len(h.generations[gen]))
	for _, c := range h.generations[gen] {
		members = append(members, c)
	}
	h.mu.RUnlock()
	return h.quiesceConns(ctx, members)
}

// QuiesceClusters quiesces only the cids' conns in generation gen — the
// per-batch variant Deploy uses after cutting a batch over, to drain and
// retire just that batch's outgoing clusters.
func (h *Hub) QuiesceClusters(ctx context.Context, gen int, cids []int) error {
	h.mu.RLock()
	genMembers := h.generations[gen]
	members := make([]*Conn, 0, len(cids))
	for _, cid := range cids {
		if c := genMembers[cid]; c != nil {
			members = append(members, c)
		}
	}
	h.mu.RUnlock()
	return h.quiesceConns(ctx, members)
}

// quiesceConns sends QUIESCE to each conn and waits for its QUIESCED ack, in
// parallel, up to ctx's deadline. Returns a joined error naming every conn
// that failed to send or ack in time.
func (h *Hub) quiesceConns(ctx context.Context, members []*Conn) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, c := range members {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()
			if err := h.quiesceOne(ctx, c); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// quiesceOne sends one QUIESCE and blocks for its QUIESCED ack or ctx.
func (h *Hub) quiesceOne(ctx context.Context, c *Conn) error {
	id := fmt.Sprintf("quiesce-%d", c.id)
	ch := make(chan struct{})
	h.mu.Lock()
	h.quiesceWaiters[id] = ch
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.quiesceWaiters, id)
		h.mu.Unlock()
	}()

	if err := c.send(protocol.ShardRequest{ID: id, Op: protocol.OpQuiesce}); err != nil {
		return fmt.Errorf("conn %d: send QUIESCE: %w", c.id, err)
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("conn %d: %w waiting for QUIESCED", c.id, ctx.Err())
	}
}
