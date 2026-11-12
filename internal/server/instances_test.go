package server

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"sync"
	"testing"
	"time"

	"sm-manager/internal/config"
	"sm-manager/internal/protocol"
	"sm-manager/internal/status"
)

// addInstance inserts a registry entry in state starting plus its guild
// mappings, the way the supervisor-driven ops will.
func addInstance(h *Hub, id string, guildIDs ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.instances[id] = &instance{id: id, guildIDs: guildIDs, state: status.InstanceStarting}
	for _, g := range guildIDs {
		h.guildInstance[g] = id
	}
}

func sendFrame(t *testing.T, h *Hub, c *Conn, id string, op protocol.OpCode, payload string) {
	t.Helper()
	raw, err := json.Marshal(protocol.ShardRequest{ID: id, Op: op, Payload: jsontext.Value(payload)})
	if err != nil {
		t.Fatal(err)
	}
	h.handleFrame(c, raw)
}

func registerInstance(t *testing.T, h *Hub, id string, generation int) (*Conn, *fakeConn) {
	t.Helper()
	fc := &fakeConn{}
	c := h.newConn(fc)
	payload, _ := json.Marshal(map[string]any{"instanceId": id, "generation": generation})
	sendFrame(t, h, c, "reg", protocol.OpRegister, string(payload))
	if resp := decodeResp(t, fc.last()); !resp.OK {
		t.Fatalf("instance register not ok: %+v", resp)
	}
	return c, fc
}

func TestInstanceRegisterBindsConn(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111")

	registerInstance(t, h, "wl_a", 3)

	snap := h.snapshotInstances()
	if len(snap) != 1 {
		t.Fatalf("want 1 instance, got %d", len(snap))
	}
	got := snap[0]
	if got.InstanceID != "wl_a" || got.State != status.InstanceRunning || got.Generation != 3 || !got.Alive {
		t.Fatalf("unexpected instance info: %+v", got)
	}
	if got.LastBeat == 0 {
		t.Fatal("lastBeat not set")
	}
	if len(got.GuildIDs) != 1 || got.GuildIDs[0] != "111" {
		t.Fatalf("guildIds = %v", got.GuildIDs)
	}
	if len(h.clusters) != 0 {
		t.Fatalf("instance register leaked into clusters: %v", h.clusters)
	}
}

func TestInstanceRegisterUnknownRejected(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	sendFrame(t, h, c, "r", protocol.OpRegister, `{"instanceId":"wl_zzz","generation":0}`)

	resp := decodeResp(t, fc.last())
	if resp.OK || resp.Error != "unknown instance" {
		t.Fatalf("want ok=false error=unknown instance, got %+v", resp)
	}
	if len(h.instances) != 0 || len(h.clusters) != 0 {
		t.Fatal("rejected register must not create state")
	}
}

func TestClusterRegisterUnaffected(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	sendFrame(t, h, c, "r", protocol.OpRegister, `{"clusterId":0,"shards":[0]}`)

	if resp := decodeResp(t, fc.last()); !resp.OK {
		t.Fatalf("cluster register not ok: %+v", resp)
	}
	if _, ok := h.clusters[0]; !ok {
		t.Fatal("cluster 0 not in registry")
	}
	if len(h.instances) != 0 {
		t.Fatalf("cluster register created an instance entry: %v", h.instances)
	}
}

func TestInstanceDisconnectKeepsMapping(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111", "222")
	c, _ := registerInstance(t, h, "wl_a", 1)

	h.removeConn(c)

	if h.guildInstance["111"] != "wl_a" || h.guildInstance["222"] != "wl_a" {
		t.Fatalf("guild mapping lost: %v", h.guildInstance)
	}
	snap := h.snapshotInstances()
	if len(snap) != 1 {
		t.Fatalf("entry removed on disconnect: %v", snap)
	}
	if snap[0].Alive || snap[0].State != status.InstanceStarting {
		t.Fatalf("want alive=false state=starting, got %+v", snap[0])
	}
}

func TestInstanceDisconnectKeepsFailedState(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111")
	c, _ := registerInstance(t, h, "wl_a", 1)

	h.SetInstanceState("wl_a", status.InstanceFailed, 78)
	h.removeConn(c)

	got := h.snapshotInstances()[0]
	if got.State != status.InstanceFailed || got.ExitCode != 78 {
		t.Fatalf("failed state must survive disconnect, got %+v", got)
	}
}

func TestStaleInstanceConnDoesNotClearNewerBinding(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111")
	oldConn, _ := registerInstance(t, h, "wl_a", 1)
	registerInstance(t, h, "wl_a", 2)

	h.removeConn(oldConn)

	got := h.snapshotInstances()[0]
	if !got.Alive || got.State != status.InstanceRunning || got.Generation != 2 {
		t.Fatalf("stale conn removal clobbered newer binding: %+v", got)
	}
}

func TestInstanceHeartbeatStoresStatus(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111")
	c, fc := registerInstance(t, h, "wl_a", 1)

	sendFrame(t, h, c, "hb", protocol.OpHeartbeat, `{"instanceId":"wl_a","status":{"guilds":1,"members":42}}`)

	if resp := decodeResp(t, fc.last()); !resp.OK || resp.ID != "hb" {
		t.Fatalf("heartbeat not acked: %+v", resp)
	}
	got := h.snapshotInstances()[0]
	if string(got.Status) != `{"guilds":1,"members":42}` {
		t.Fatalf("status = %s", got.Status)
	}
	if len(h.clusters) != 0 {
		t.Fatal("instance heartbeat touched clusters")
	}
}

func TestInstanceReapDeadClearsConn(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111")
	_, fc := registerInstance(t, h, "wl_a", 1)

	h.mu.Lock()
	h.instances["wl_a"].lastBeat = h.instances["wl_a"].lastBeat.Add(-time.Hour)
	h.mu.Unlock()
	h.reapDead()

	got := h.snapshotInstances()[0]
	if got.Alive || got.State != status.InstanceStarting {
		t.Fatalf("reaped instance should be starting and not alive: %+v", got)
	}
	if !fc.closed {
		t.Fatal("reaped instance conn not closed")
	}
	if h.guildInstance["111"] != "wl_a" {
		t.Fatal("reap dropped guild mapping")
	}
}

func TestInstancesNotInClusterStatuses(t *testing.T) {
	h := testHub()
	addInstance(h, "wl_a", "111")
	registerInstance(t, h, "wl_a", 1)

	if got := h.snapshotActive(); len(got) != 0 {
		t.Fatalf("instance leaked into cluster statuses: %+v", got)
	}
}

func TestSetInstanceStateUnknownIsNoop(t *testing.T) {
	h := testHub()
	h.SetInstanceState("wl_nope", status.InstanceFailed, 1)
	if len(h.instances) != 0 {
		t.Fatal("SetInstanceState created an entry")
	}
}

// routingHub builds a hub with cluster 0 registered (1 cluster, 1 shard) and
// returns it with the cluster's fakeConn.
func routingHub(t *testing.T) (*Hub, *fakeConn) {
	t.Helper()
	h := NewHub(&config.Config{
		AllowedBusinessOps:   []string{"GET_GUILD_LIST"},
		ShardsPerCluster:     1,
		ClusterCount:         1,
		HeartbeatTimeoutSecs: 30,
	})
	fc := &fakeConn{}
	c := h.newConn(fc)
	h.handleFrame(c, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: jsontext.Value(`{"clusterId":0,"shards":[0]}`)}))
	return h, fc
}

func sendGuildRequest(t *testing.T, h *Hub, guildID string) *fakeConn {
	t.Helper()
	fc := &fakeConn{}
	requester := h.newConn(fc)
	h.handleFrame(requester, mustMarshal(protocol.ShardRequest{
		ID:      "q1",
		Op:      protocol.OpCode("GET_GUILD_LIST"),
		Target:  &protocol.Target{GuildId: &guildID},
		Payload: jsontext.Value(`{}`),
	}))
	return fc
}

func frameCount(f *fakeConn) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.frames)
}

func TestGuildInRegisterRoutesToInstance(t *testing.T) {
	h, clusterConn := routingHub(t)
	const guild = "111111111111111111"
	addInstance(h, "wl_a", guild)
	_, instConn := registerInstance(t, h, "wl_a", 1)
	clusterBefore, instBefore := frameCount(clusterConn), frameCount(instConn)

	sendGuildRequest(t, h, guild)

	if got := frameCount(instConn); got != instBefore+1 {
		t.Fatalf("instance got %d new frames, want 1", got-instBefore)
	}
	var req protocol.ShardRequest
	if err := json.Unmarshal(instConn.last(), &req); err != nil || req.ID != "q1" {
		t.Fatalf("instance last frame not the request: %s (%v)", instConn.last(), err)
	}
	if got := frameCount(clusterConn); got != clusterBefore {
		t.Fatalf("cluster received %d frames for an instance guild", got-clusterBefore)
	}
}

func TestGuildInRegisterNeverFallsBackToCluster(t *testing.T) {
	h, clusterConn := routingHub(t)
	const guild = "111111111111111111"
	addInstance(h, "wl_a", guild) // never registered: no conn
	clusterBefore := frameCount(clusterConn)

	requester := sendGuildRequest(t, h, guild)

	resp := decodeResp(t, requester.last())
	if resp.ID != "q1" || resp.OK || resp.Error != "instance offline" {
		t.Fatalf("want ok:false instance offline, got %+v", resp)
	}
	if got := frameCount(clusterConn); got != clusterBefore {
		t.Fatalf("cluster received %d frames; must never fall back", got-clusterBefore)
	}
	if len(h.pending) != 0 {
		t.Fatalf("pending entry leaked: %v", h.pending)
	}
}

func TestInstanceDisconnectedAfterRegisterAnswersOffline(t *testing.T) {
	h, clusterConn := routingHub(t)
	const guild = "111111111111111111"
	addInstance(h, "wl_a", guild)
	ic, _ := registerInstance(t, h, "wl_a", 1)
	h.removeConn(ic)
	clusterBefore := frameCount(clusterConn)

	requester := sendGuildRequest(t, h, guild)

	if resp := decodeResp(t, requester.last()); resp.OK || resp.Error != "instance offline" {
		t.Fatalf("want instance offline, got %+v", resp)
	}
	if got := frameCount(clusterConn); got != clusterBefore {
		t.Fatal("cluster received a frame for an offline instance guild")
	}
}

func TestOtherGuildUsesShardArithmetic(t *testing.T) {
	h, clusterConn := routingHub(t)
	addInstance(h, "wl_a", "111111111111111111")
	registerInstance(t, h, "wl_a", 1)
	clusterBefore := frameCount(clusterConn)

	sendGuildRequest(t, h, "222222222222222222")

	if got := frameCount(clusterConn); got != clusterBefore+1 {
		t.Fatalf("cluster got %d new frames, want 1", got-clusterBefore)
	}
}

func TestUnroutableTargetKeepsClusterError(t *testing.T) {
	h, _ := routingHub(t)
	requester := sendGuildRequest(t, h, "not-a-snowflake")
	if resp := decodeResp(t, requester.last()); resp.OK || resp.Error != "no cluster for target" {
		t.Fatalf("want no cluster for target, got %+v", resp)
	}
}

// fakeRunner records runner calls. Like the real supervisor it reports state
// changes back to the hub.
type fakeRunner struct {
	h *Hub

	mu       sync.Mutex
	starts   []string
	stops    []string
	startErr error
	stopGate chan struct{} // when set, StopInstance blocks until it is closed

	inFlight map[string]int
	overlap  bool
}

func newFakeRunner(h *Hub) *fakeRunner {
	return &fakeRunner{h: h, inFlight: map[string]int{}}
}

func (f *fakeRunner) enter(id string) {
	f.mu.Lock()
	f.inFlight[id]++
	if f.inFlight[id] > 1 {
		f.overlap = true
	}
	f.mu.Unlock()
}

func (f *fakeRunner) leave(id string) {
	f.mu.Lock()
	f.inFlight[id]--
	f.mu.Unlock()
}

func (f *fakeRunner) StartInstance(id string) error {
	f.enter(id)
	defer f.leave(id)
	f.mu.Lock()
	f.starts = append(f.starts, id)
	err := f.startErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	f.h.SetInstanceState(id, status.InstanceStarting, 0)
	return nil
}

func (f *fakeRunner) StopInstance(id string) {
	f.enter(id)
	defer f.leave(id)
	f.mu.Lock()
	f.stops = append(f.stops, id)
	gate := f.stopGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	f.h.SetInstanceState(id, status.InstanceStopped, 0)
}

func (f *fakeRunner) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.starts)
}

func (f *fakeRunner) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stops)
}

func (f *fakeRunner) startedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.starts...)
}

// opHub is a testHub with a fakeRunner wired in.
func opHub() (*Hub, *fakeRunner) {
	h := testHub()
	r := newFakeRunner(h)
	h.SetInstanceRunner(r)
	return h, r
}

// opCall sends an instance op from a fresh client conn and waits for its reply.
func opCall(t *testing.T, h *Hub, op protocol.OpCode, payload string) protocol.ShardResponse {
	t.Helper()
	fc := &fakeConn{}
	c := h.newConn(fc)
	sendFrame(t, h, c, "op1", op, payload)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if raw := fc.last(); raw != nil {
			return decodeResp(t, raw)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no reply to %s", op)
	return protocol.ShardResponse{}
}

func instanceState(h *Hub, id string) (InstanceInfo, bool) {
	for _, i := range h.snapshotInstances() {
		if i.InstanceID == id {
			return i, true
		}
	}
	return InstanceInfo{}, false
}

func TestStartInstanceRegistersGuilds(t *testing.T) {
	h, r := opHub()

	resp := opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111","222"]}`)

	if !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
	var info InstanceInfo
	if err := json.Unmarshal(resp.Data, &info); err != nil || info.InstanceID != "wl_a" || info.State != status.InstanceStarting {
		t.Fatalf("bad reply data %s (%v)", resp.Data, err)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.guildInstance["111"] != "wl_a" || h.guildInstance["222"] != "wl_a" {
		t.Fatalf("guild mapping = %v", h.guildInstance)
	}
	if r.startCount() != 1 {
		t.Fatalf("runner starts = %d, want 1", r.startCount())
	}
}

func TestStartInstanceIdempotent(t *testing.T) {
	h, r := opHub()
	opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)
	h.SetInstanceState("wl_a", status.InstanceRunning, 0)

	resp := opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)

	if !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
	if r.startCount() != 1 {
		t.Fatalf("runner starts = %d, want 1", r.startCount())
	}
}

func TestStartInstanceClearsFailed(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_a", "111")
	h.SetInstanceState("wl_a", status.InstanceFailed, 78)

	resp := opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)

	if !resp.OK || r.startCount() != 1 {
		t.Fatalf("resp %+v, starts %d", resp, r.startCount())
	}
	if got, _ := instanceState(h, "wl_a"); got.State != status.InstanceStarting {
		t.Fatalf("state = %s", got.State)
	}
}

func TestStartInstanceGuildConflict(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_a", "111")

	resp := opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_b","guildIds":["111"]}`)

	if resp.OK || resp.Error == "" {
		t.Fatalf("want error, got %+v", resp)
	}
	if _, ok := instanceState(h, "wl_b"); ok {
		t.Fatal("conflicting instance was registered")
	}
	if h.guildInstance["111"] != "wl_a" || r.startCount() != 0 {
		t.Fatalf("mapping %v, starts %d", h.guildInstance, r.startCount())
	}
}

func TestStartInstanceRunnerErrorRollsBackNewEntry(t *testing.T) {
	h, r := opHub()
	r.startErr = errBoom

	resp := opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)

	if resp.OK || resp.Error != "boom" {
		t.Fatalf("want error boom, got %+v", resp)
	}
	if len(h.instances) != 0 || len(h.guildInstance) != 0 {
		t.Fatalf("entry not rolled back: %v %v", h.instances, h.guildInstance)
	}
}

func TestStopInstanceKeepsMapping(t *testing.T) {
	h, r := opHub()
	opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)

	resp := opCall(t, h, protocol.OpStopInstance, `{"instanceId":"wl_a"}`)

	if !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
	if r.stopCount() != 1 {
		t.Fatalf("runner stops = %d", r.stopCount())
	}
	if h.guildInstance["111"] != "wl_a" {
		t.Fatal("stop dropped the mapping")
	}
	got, ok := instanceState(h, "wl_a")
	if !ok || got.State != status.InstanceStopped {
		t.Fatalf("state = %+v ok=%v", got, ok)
	}
	var info InstanceInfo
	if err := json.Unmarshal(resp.Data, &info); err != nil || info.State != status.InstanceStopped {
		t.Fatalf("reply data %s (%v)", resp.Data, err)
	}
}

func TestStopUnknownInstanceIsOkNull(t *testing.T) {
	h, r := opHub()

	resp := opCall(t, h, protocol.OpStopInstance, `{"instanceId":"wl_nope"}`)

	if !resp.OK || string(resp.Data) != "null" {
		t.Fatalf("want ok data=null, got %+v", resp)
	}
	if r.stopCount() != 0 {
		t.Fatal("runner called for unknown instance")
	}
}

func TestRemoveInstanceDropsMapping(t *testing.T) {
	h, clusterConn := routingHub(t)
	runner := newFakeRunner(h)
	h.SetInstanceRunner(runner)
	const guild = "111111111111111111"
	opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["`+guild+`"]}`)

	resp := opCall(t, h, protocol.OpRemoveInstance, `{"instanceId":"wl_a"}`)

	if !resp.OK || string(resp.Data) != "null" {
		t.Fatalf("want ok data=null, got %+v", resp)
	}
	if runner.stopCount() != 1 {
		t.Fatalf("runner stops = %d", runner.stopCount())
	}
	if len(h.instances) != 0 || len(h.guildInstance) != 0 {
		t.Fatalf("not dropped: %v %v", h.instances, h.guildInstance)
	}
	// With the mapping gone the guild routes by shard arithmetic again.
	before := frameCount(clusterConn)
	sendGuildRequest(t, h, guild)
	if frameCount(clusterConn) != before+1 {
		t.Fatal("guild request did not reach the cluster after removal")
	}
}

func TestRemoveUnknownInstanceIsOkNull(t *testing.T) {
	h, _ := opHub()
	resp := opCall(t, h, protocol.OpRemoveInstance, `{"instanceId":"wl_nope"}`)
	if !resp.OK || string(resp.Data) != "null" {
		t.Fatalf("want ok data=null, got %+v", resp)
	}
}

func TestRestartUnknownInstance(t *testing.T) {
	h, r := opHub()

	resp := opCall(t, h, protocol.OpRestartInstance, `{"instanceId":"wl_nope"}`)

	if resp.OK || resp.Error != "unknown instance" {
		t.Fatalf("want unknown instance, got %+v", resp)
	}
	if r.startCount() != 0 || r.stopCount() != 0 {
		t.Fatal("runner called")
	}
}

func TestRestartStopsThenStarts(t *testing.T) {
	h, r := opHub()
	opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)
	h.SetInstanceState("wl_a", status.InstanceRunning, 0)

	resp := opCall(t, h, protocol.OpRestartInstance, `{"instanceId":"wl_a"}`)

	if !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
	if r.stopCount() != 1 || r.startCount() != 2 {
		t.Fatalf("stops %d starts %d, want 1 and 2", r.stopCount(), r.startCount())
	}
	if got, _ := instanceState(h, "wl_a"); got.State != status.InstanceStarting {
		t.Fatalf("state = %s", got.State)
	}
}

func TestSyncStartsStoppedButNotFailed(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_failed", "900")
	h.SetInstanceState("wl_failed", status.InstanceFailed, 78)

	resp := opCall(t, h, protocol.OpSyncInstances, `{"instances":[`+
		`{"instanceId":"wl_failed","guildIds":["900"],"run":true},`+
		`{"instanceId":"wl_new","guildIds":["901"],"run":true},`+
		`{"instanceId":"wl_off","guildIds":["902","903"],"run":false}]}`)

	if !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
	if got := r.startedIDs(); len(got) != 1 || got[0] != "wl_new" {
		t.Fatalf("started = %v, want only wl_new", got)
	}
	var infos []InstanceInfo
	if err := json.Unmarshal(resp.Data, &infos); err != nil || len(infos) != 3 {
		t.Fatalf("reply data %s (%v)", resp.Data, err)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for g, want := range map[string]string{"900": "wl_failed", "901": "wl_new", "902": "wl_off", "903": "wl_off"} {
		if h.guildInstance[g] != want {
			t.Fatalf("guildInstance[%s] = %q, want %q", g, h.guildInstance[g], want)
		}
	}
	if h.instances["wl_failed"].state != status.InstanceFailed {
		t.Fatal("failed instance was touched")
	}
	if h.instances["wl_off"].state != status.InstanceStopped {
		t.Fatalf("wl_off state = %s", h.instances["wl_off"].state)
	}
}

func TestSyncRunFalseStopsRunning(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_a", "111")
	h.SetInstanceState("wl_a", status.InstanceRunning, 0)

	resp := opCall(t, h, protocol.OpSyncInstances, `{"instances":[{"instanceId":"wl_a","guildIds":["111"],"run":false}]}`)

	if !resp.OK || r.stopCount() != 1 || r.startCount() != 0 {
		t.Fatalf("resp %+v stops %d starts %d", resp, r.stopCount(), r.startCount())
	}
}

func TestSyncRemovesUnlisted(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_old", "111")
	h.SetInstanceState("wl_old", status.InstanceRunning, 0)

	resp := opCall(t, h, protocol.OpSyncInstances, `{"instances":[]}`)

	if !resp.OK || string(resp.Data) != "[]" {
		t.Fatalf("resp %+v", resp)
	}
	if r.stopCount() != 1 {
		t.Fatalf("runner stops = %d", r.stopCount())
	}
	if len(h.instances) != 0 || len(h.guildInstance) != 0 {
		t.Fatalf("not removed: %v %v", h.instances, h.guildInstance)
	}
}

func TestSyncRejectsGuildListedTwice(t *testing.T) {
	h, r := opHub()

	resp := opCall(t, h, protocol.OpSyncInstances, `{"instances":[`+
		`{"instanceId":"wl_a","guildIds":["111"],"run":true},`+
		`{"instanceId":"wl_b","guildIds":["111"],"run":true}]}`)

	if resp.OK {
		t.Fatalf("want error, got %+v", resp)
	}
	if len(h.instances) != 0 || r.startCount() != 0 {
		t.Fatal("invalid sync had side effects")
	}
}

func TestSyncMovesGuildBetweenListedInstances(t *testing.T) {
	h, _ := opHub()
	addInstance(h, "wl_a", "111")
	h.SetInstanceState("wl_a", status.InstanceStopped, 0)

	resp := opCall(t, h, protocol.OpSyncInstances, `{"instances":[`+
		`{"instanceId":"wl_b","guildIds":["111"],"run":false},`+
		`{"instanceId":"wl_a","guildIds":["222"],"run":false}]}`)

	if !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
	if h.guildInstance["111"] != "wl_b" || h.guildInstance["222"] != "wl_a" {
		t.Fatalf("mapping = %v", h.guildInstance)
	}
}

func TestGetInstanceStatusesReplies(t *testing.T) {
	h, _ := opHub()
	addInstance(h, "wl_a", "111")

	resp := opCall(t, h, protocol.OpGetInstanceStatuses, `{}`)

	var infos []InstanceInfo
	if !resp.OK || json.Unmarshal(resp.Data, &infos) != nil || len(infos) != 1 || infos[0].InstanceID != "wl_a" {
		t.Fatalf("resp %+v", resp)
	}
}

func TestGetInstanceStatusesEmptyIsArray(t *testing.T) {
	h := testHub() // no runner needed
	resp := opCall(t, h, protocol.OpGetInstanceStatuses, `{}`)
	if !resp.OK || string(resp.Data) != "[]" {
		t.Fatalf("resp %+v", resp)
	}
}

func TestInstanceOpWithoutRunner(t *testing.T) {
	h := testHub()

	resp := opCall(t, h, protocol.OpStartInstance, `{"instanceId":"wl_a","guildIds":["111"]}`)

	if resp.OK || resp.Error != "no instance runner configured" {
		t.Fatalf("want no instance runner configured, got %+v", resp)
	}
	if len(h.instances) != 0 {
		t.Fatal("entry registered without a runner")
	}
}

func TestInstanceOpMalformedPayload(t *testing.T) {
	h, _ := opHub()
	resp := opCall(t, h, protocol.OpStartInstance, `{"guildIds":["111"]}`)
	if resp.OK {
		t.Fatalf("missing instanceId must fail: %+v", resp)
	}
	resp = opCall(t, h, protocol.OpStopInstance, `"not an object"`)
	if resp.OK {
		t.Fatalf("bad json must fail: %+v", resp)
	}
}

// A slow stop must not block the caller's read loop.
func TestInstanceOpDoesNotBlockReadLoop(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_a", "111")
	h.SetInstanceState("wl_a", status.InstanceRunning, 0)
	r.stopGate = make(chan struct{})

	fc := &fakeConn{}
	c := h.newConn(fc)
	done := make(chan struct{})
	go func() {
		sendFrame(t, h, c, "s", protocol.OpStopInstance, `{"instanceId":"wl_a"}`)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleFrame blocked on a slow stop")
	}
	if fc.last() != nil {
		t.Fatal("reply sent before the stop finished")
	}
	close(r.stopGate)
	deadline := time.Now().Add(2 * time.Second)
	for fc.last() == nil && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if resp := decodeResp(t, fc.last()); !resp.OK {
		t.Fatalf("not ok: %+v", resp)
	}
}

// Concurrent ops on one id must never reach the runner at the same time.
func TestInstanceOpsSerializedPerInstance(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_a", "111")
	h.SetInstanceState("wl_a", status.InstanceRunning, 0)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op := protocol.OpStopInstance
			payload := `{"instanceId":"wl_a"}`
			if i%2 == 0 {
				op = protocol.OpStartInstance
				payload = `{"instanceId":"wl_a","guildIds":["111"]}`
			}
			opCall(t, h, op, payload)
		}(i)
	}
	wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.overlap {
		t.Fatal("runner saw concurrent calls for one instance")
	}
}
