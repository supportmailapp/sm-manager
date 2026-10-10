package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"sm-manager/internal/config"
	"sm-manager/internal/protocol"
	"sm-manager/internal/status"
)

var errBoom = errors.New("boom")

// fakeConn captures frames written by the hub.
type fakeConn struct {
	mu     sync.Mutex
	frames [][]byte
	closed bool
}

func (f *fakeConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// strip the 4-byte length prefix so tests read JSON directly
	f.frames = append(f.frames, append([]byte(nil), p[4:]...))
	return len(p), nil
}

func (f *fakeConn) Close() error { f.closed = true; return nil }

func (f *fakeConn) last() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.frames) == 0 {
		return nil
	}
	return f.frames[len(f.frames)-1]
}

func testHub() *Hub {
	return NewHub(&config.Config{
		AllowedBusinessOps:   []string{"GET_GUILD_LIST"},
		HeartbeatTimeoutSecs: 30,
	})
}

func decodeResp(t *testing.T, raw []byte) protocol.ShardResponse {
	t.Helper()
	var r protocol.ShardResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return r
}

func TestRegisterMarksClusterAlive(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	req := protocol.ShardRequest{ID: "1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0,1]}`)}
	raw, _ := json.Marshal(req)
	h.handleFrame(c, raw)

	if resp := decodeResp(t, fc.last()); !resp.OK {
		t.Fatalf("register not ok: %+v", resp)
	}
	if _, ok := h.clusters[0]; !ok {
		t.Fatal("cluster 0 not in registry")
	}
}

func TestUnknownBusinessOpRejected(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	req := protocol.ShardRequest{ID: "9", Op: protocol.OpCode("NOT_A_REAL_OP"), Payload: json.RawMessage(`{}`)}
	raw, _ := json.Marshal(req)
	h.handleFrame(c, raw)

	resp := decodeResp(t, fc.last())
	if resp.OK {
		t.Fatal("unknown op should be rejected")
	}
	if resp.ID != "9" {
		t.Fatalf("response id = %q, want 9", resp.ID)
	}
}

func TestBusinessOpForwardedAndResponseRouted(t *testing.T) {
	h := testHub()

	// cluster 0 registers
	clusterFC := &fakeConn{}
	cluster := h.newConn(clusterFC)
	regRaw, _ := json.Marshal(protocol.ShardRequest{ID: "r", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0]}`)})
	h.handleFrame(cluster, regRaw)

	// API client sends a business op targeting cluster 0
	apiFC := &fakeConn{}
	api := h.newConn(apiFC)
	cid := 0
	bizRaw, _ := json.Marshal(protocol.ShardRequest{
		ID: "abc", Op: protocol.OpCode("GET_GUILD_LIST"),
		Target:  &protocol.Target{ClusterId: &cid},
		Payload: json.RawMessage(`{"foo":"bar"}`),
	})
	h.handleFrame(api, bizRaw)

	// cluster should have received the forwarded frame verbatim
	fwd := clusterFC.last()
	if fwd == nil {
		t.Fatal("cluster received no forwarded frame")
	}
	var fwdReq protocol.ShardRequest
	if err := json.Unmarshal(fwd, &fwdReq); err != nil || fwdReq.ID != "abc" {
		t.Fatalf("forwarded frame wrong: %s", fwd)
	}

	// cluster replies; response must route back to the api conn
	respRaw, _ := json.Marshal(protocol.ShardResponse{ID: "abc", OK: true, Data: json.RawMessage(`[]`)})
	h.handleFrame(cluster, respRaw)

	resp := decodeResp(t, apiFC.last())
	if !resp.OK || resp.ID != "abc" {
		t.Fatalf("api did not get routed response: %+v", resp)
	}
	if _, still := h.pending["abc"]; still {
		t.Fatal("pending entry not cleared")
	}
}

func TestShardForGuild(t *testing.T) {
	// Discord formula: (guildID >> 22) % totalShards.
	got, ok := shardForGuild("81384788765712384", 4)
	if !ok {
		t.Fatal("expected ok")
	}
	want := int((uint64(81384788765712384) >> 22) % 4)
	if got != want {
		t.Fatalf("shard = %d, want %d", got, want)
	}
	if _, ok := shardForGuild("not-a-number", 4); ok {
		t.Fatal("unparseable guild id should fail")
	}
	if _, ok := shardForGuild("123", 0); ok {
		t.Fatal("zero total shards should fail")
	}
}

func TestResolveTargetByGuildAndShard(t *testing.T) {
	// topology: 2 clusters × 2 shards = 4 total; cluster c owns shards [2c, 2c+1].
	h := NewHub(&config.Config{
		AllowedBusinessOps: []string{"GET_GUILD_LIST"},
		ShardsPerCluster:   2,
		ClusterCount:       2,
	})
	fc0, fc1 := &fakeConn{}, &fakeConn{}
	c0, c1 := h.newConn(fc0), h.newConn(fc1)
	h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0,1]}`)}))
	h.handleFrame(c1, mustMarshal(protocol.ShardRequest{ID: "r1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":1,"shards":[2,3]}`)}))

	// shard 3 → cluster 1
	shard := 3
	if got := h.resolveTarget(&protocol.Target{ShardId: &shard}); got != c1 {
		t.Fatalf("shard 3 resolved to wrong cluster")
	}

	// guild → shard → cluster; pick a guild that lands on a shard owned by c0.
	guild := "0" // (0 >> 22) % 4 = shard 0 → cluster 0
	if got := h.resolveTarget(&protocol.Target{GuildId: &guild}); got != c0 {
		t.Fatalf("guild 0 should route to cluster 0")
	}
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func TestRegisterTagsGeneration(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	req := protocol.ShardRequest{ID: "1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0,1],"generation":3}`)}
	h.handleFrame(c, mustMarshal(req))

	if resp := decodeResp(t, fc.last()); !resp.OK {
		t.Fatalf("register not ok: %+v", resp)
	}
	c.mu.Lock()
	gen := c.generation
	c.mu.Unlock()
	if gen != 3 {
		t.Fatalf("conn generation = %d, want 3", gen)
	}
	if h.generations[3][0] != c {
		t.Fatal("cluster 0 not tracked under generation 3")
	}
	// Still routable via the flat registry regardless of generation.
	if h.clusters[0] != c {
		t.Fatal("cluster 0 not in routing registry")
	}
}

func TestGenerationReady(t *testing.T) {
	h := NewHub(&config.Config{
		ClusterCount:         2,
		HeartbeatTimeoutSecs: 30,
	})
	c0 := h.newConn(&fakeConn{})
	c1 := h.newConn(&fakeConn{})

	h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":1}`)}))
	if h.GenerationReady(1) {
		t.Fatal("generation 1 should not be ready with only 1 of 2 clusters registered")
	}
	if h.GenerationReady(0) {
		t.Fatal("generation 0 has no registrations, should not be ready")
	}

	h.handleFrame(c1, mustMarshal(protocol.ShardRequest{ID: "r1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":1,"shards":[1],"generation":1}`)}))
	if !h.GenerationReady(1) {
		t.Fatal("generation 1 should be ready once both clusters registered")
	}
}

func TestGenerationReadyStaleHeartbeatNotReady(t *testing.T) {
	h := NewHub(&config.Config{
		ClusterCount:         1,
		HeartbeatTimeoutSecs: 1,
	})
	c0 := h.newConn(&fakeConn{})
	h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":5}`)}))

	if !h.GenerationReady(5) {
		t.Fatal("generation 5 should be ready right after registration")
	}

	c0.mu.Lock()
	c0.lastBeat = time.Now().Add(-2 * time.Second)
	c0.mu.Unlock()

	if h.GenerationReady(5) {
		t.Fatal("generation 5 should not be ready once its only cluster's heartbeat lapsed")
	}
}

func TestAwaitGenerationSucceedsOnceReady(t *testing.T) {
	h := NewHub(&config.Config{
		ClusterCount:         1,
		HeartbeatTimeoutSecs: 30,
	})
	c0 := h.newConn(&fakeConn{})

	go func() {
		time.Sleep(20 * time.Millisecond)
		h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":2}`)}))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.AwaitGeneration(ctx, 2); err != nil {
		t.Fatalf("AwaitGeneration returned error: %v", err)
	}
}

func TestAwaitGenerationReturnsOnContextDeadline(t *testing.T) {
	h := NewHub(&config.Config{
		ClusterCount:         1,
		HeartbeatTimeoutSecs: 30,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := h.AwaitGeneration(ctx, 0)
	if err == nil {
		t.Fatal("expected AwaitGeneration to return an error when generation never becomes ready")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("AwaitGeneration took too long to return: %s", elapsed)
	}
}

// fakeDeployer lets tests drive handleDeploy without a real supervisor.
type fakeDeployer struct {
	gen int
	err error
}

func (f *fakeDeployer) Deploy(ctx context.Context) (int, error) { return f.gen, f.err }

func TestHandleDeployNoDeployerConfigured(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	req := protocol.ShardRequest{ID: "d1", Op: protocol.OpDeploy, Payload: json.RawMessage(`{}`)}
	h.handleFrame(c, mustMarshal(req))

	resp := decodeResp(t, fc.last())
	if resp.OK {
		t.Fatal("DEPLOY with no deployer wired should fail")
	}
}

func TestHandleDeploySuccess(t *testing.T) {
	h := testHub()
	h.SetDeployer(&fakeDeployer{gen: 7})
	fc := &fakeConn{}
	c := h.newConn(fc)

	req := protocol.ShardRequest{ID: "d2", Op: protocol.OpDeploy, Payload: json.RawMessage(`{}`)}
	h.handleFrame(c, mustMarshal(req))

	resp := decodeResp(t, fc.last())
	if !resp.OK {
		t.Fatalf("DEPLOY should succeed: %+v", resp)
	}
	var data struct {
		Generation int `json:"generation"`
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if data.Generation != 7 {
		t.Fatalf("generation = %d, want 7", data.Generation)
	}
}

func TestHandleDeployPropagatesError(t *testing.T) {
	h := testHub()
	h.SetDeployer(&fakeDeployer{err: errBoom})
	fc := &fakeConn{}
	c := h.newConn(fc)

	req := protocol.ShardRequest{ID: "d3", Op: protocol.OpDeploy, Payload: json.RawMessage(`{}`)}
	h.handleFrame(c, mustMarshal(req))

	resp := decodeResp(t, fc.last())
	if resp.OK {
		t.Fatal("DEPLOY should surface the deployer's error")
	}
	if resp.Error != errBoom.Error() {
		t.Fatalf("error = %q, want %q", resp.Error, errBoom.Error())
	}
}

func TestQuiesceGenerationSendsAndAwaitsAck(t *testing.T) {
	h := NewHub(&config.Config{ClusterCount: 1, HeartbeatTimeoutSecs: 30})
	fc := &fakeConn{}
	c := h.newConn(fc)
	h.handleFrame(c, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":4}`)}))

	done := make(chan error, 1)
	go func() {
		done <- h.QuiesceGeneration(context.Background(), 4)
	}()

	// Wait for the QUIESCE frame to land on the cluster's conn, then ack it as
	// the cluster would.
	deadline := time.Now().Add(time.Second)
	var quiesceReq protocol.ShardRequest
	for {
		if raw := fc.last(); raw != nil {
			if err := json.Unmarshal(raw, &quiesceReq); err == nil && quiesceReq.Op == protocol.OpQuiesce {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("QUIESCE never sent to cluster")
		}
		time.Sleep(time.Millisecond)
	}

	ackReq, _ := json.Marshal(protocol.ShardRequest{ID: quiesceReq.ID, Op: protocol.OpQuiesced, Payload: json.RawMessage(`{}`)})
	h.handleFrame(c, ackReq)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("QuiesceGeneration returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("QuiesceGeneration did not return after QUIESCED ack")
	}
}

func TestQuiesceGenerationTimesOutWithoutAck(t *testing.T) {
	h := NewHub(&config.Config{ClusterCount: 1, HeartbeatTimeoutSecs: 30})
	fc := &fakeConn{}
	c := h.newConn(fc)
	h.handleFrame(c, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":9}`)}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := h.QuiesceGeneration(ctx, 9); err == nil {
		t.Fatal("expected QuiesceGeneration to error when no QUIESCED ack arrives")
	}
}

func TestRegisterDoesNotStealActiveRouting(t *testing.T) {
	// The routing-cutover fix: a new generation registering must NOT overwrite
	// the active routing slot held by the old generation. Routing only flips
	// on an explicit PromoteClusters at cutover.
	h := NewHub(&config.Config{
		AllowedBusinessOps: []string{"GET_GUILD_LIST"},
		ShardsPerCluster:   1,
		ClusterCount:       1,
	})
	cid := 0

	old := h.newConn(&fakeConn{})
	h.handleFrame(old, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":1}`)}))
	if got := h.resolveTarget(&protocol.Target{ClusterId: &cid}); got != old {
		t.Fatal("gen 1 should hold active routing after cold registration")
	}

	// New generation registers — must not steal routing yet.
	fresh := h.newConn(&fakeConn{})
	h.handleFrame(fresh, mustMarshal(protocol.ShardRequest{ID: "r1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":2}`)}))
	if got := h.resolveTarget(&protocol.Target{ClusterId: &cid}); got != old {
		t.Fatal("gen 2 register stole active routing before cutover — duplicate-routing window")
	}
	if h.generations[2][0] != fresh {
		t.Fatal("gen 2 conn not tracked under its generation")
	}

	// Cutover flips routing atomically.
	h.PromoteClusters(2, []int{0})
	if got := h.resolveTarget(&protocol.Target{ClusterId: &cid}); got != fresh {
		t.Fatal("PromoteClusters did not flip routing to gen 2")
	}
}

func TestRegisterRefillsEmptySlotOnCrashRestart(t *testing.T) {
	// A crash-restart of the active cluster (its conn removed first) must
	// re-take the routing slot on re-register, since the slot is now empty.
	h := NewHub(&config.Config{ShardsPerCluster: 1, ClusterCount: 1})
	cid := 0

	first := h.newConn(&fakeConn{})
	h.handleFrame(first, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":1}`)}))
	h.removeConn(first) // crash: socket closed

	second := h.newConn(&fakeConn{})
	h.handleFrame(second, mustMarshal(protocol.ShardRequest{ID: "r1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":1}`)}))
	if got := h.resolveTarget(&protocol.Target{ClusterId: &cid}); got != second {
		t.Fatal("respawned cluster did not reclaim the empty routing slot")
	}
}

func TestAwaitClustersSubsetReady(t *testing.T) {
	// A batch only waits on its own clusterIDs, not the whole fleet.
	h := NewHub(&config.Config{ClusterCount: 4, HeartbeatTimeoutSecs: 30})
	c0 := h.newConn(&fakeConn{})
	c1 := h.newConn(&fakeConn{})
	h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":2}`)}))
	h.handleFrame(c1, mustMarshal(protocol.ShardRequest{ID: "r1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":1,"shards":[1],"generation":2}`)}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.AwaitClusters(ctx, 2, []int{0, 1}); err != nil {
		t.Fatalf("subset [0,1] should be ready with only 2 of 4 clusters registered: %v", err)
	}
	// The full generation is not ready — clusters 2,3 never registered.
	if h.GenerationReady(2) {
		t.Fatal("full generation should not be ready with only 2 of 4 clusters")
	}
}

func TestQuiesceClustersSubset(t *testing.T) {
	// QuiesceClusters must only quiesce the named cids, not the whole generation.
	h := NewHub(&config.Config{ClusterCount: 2, HeartbeatTimeoutSecs: 30})
	fc0, fc1 := &fakeConn{}, &fakeConn{}
	c0, c1 := h.newConn(fc0), h.newConn(fc1)
	h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: "r0", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":1}`)}))
	h.handleFrame(c1, mustMarshal(protocol.ShardRequest{ID: "r1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":1,"shards":[1],"generation":1}`)}))

	done := make(chan error, 1)
	go func() { done <- h.QuiesceClusters(context.Background(), 1, []int{0}) }()

	// Ack cluster 0's QUIESCE as the cluster would.
	deadline := time.Now().Add(time.Second)
	var q protocol.ShardRequest
	for {
		if raw := fc0.last(); raw != nil {
			if err := json.Unmarshal(raw, &q); err == nil && q.Op == protocol.OpQuiesce {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("QUIESCE never sent to cluster 0")
		}
		time.Sleep(time.Millisecond)
	}
	h.handleFrame(c0, mustMarshal(protocol.ShardRequest{ID: q.ID, Op: protocol.OpQuiesced, Payload: json.RawMessage(`{}`)}))

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("QuiesceClusters returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("QuiesceClusters did not return after ack")
	}

	// Cluster 1 must not have been asked to quiesce.
	if raw := fc1.last(); raw != nil {
		var other protocol.ShardRequest
		if err := json.Unmarshal(raw, &other); err == nil && other.Op == protocol.OpQuiesce {
			t.Fatal("cluster 1 was quiesced but was not in the batch")
		}
	}
}

func TestBusinessOpNoTargetRejected(t *testing.T) {
	h := testHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	bizRaw, _ := json.Marshal(protocol.ShardRequest{ID: "t", Op: protocol.OpCode("GET_GUILD_LIST"), Payload: json.RawMessage(`{}`)})
	h.handleFrame(c, bizRaw)

	if resp := decodeResp(t, fc.last()); resp.OK {
		t.Fatal("business op with no resolvable target should fail")
	}
}

// fakeSink records status publishes/removals from the hub.
type fakeSink struct {
	mu       sync.Mutex
	pubs     []status.ClusterState
	removals []int
}

func (s *fakeSink) Publish(cs status.ClusterState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pubs = append(s.pubs, cs)
}
func (s *fakeSink) Remove(cid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removals = append(s.removals, cid)
}
func (s *fakeSink) lastPub() (status.ClusterState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pubs) == 0 {
		return status.ClusterState{}, false
	}
	return s.pubs[len(s.pubs)-1], true
}

func TestStatusSinkPublishesOnRegisterAndHeartbeat(t *testing.T) {
	h := testHub()
	sink := &fakeSink{}
	h.SetStatusSink(sink)
	fc := &fakeConn{}
	c := h.newConn(fc)

	reg := protocol.ShardRequest{ID: "1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0,1],"generation":1}`)}
	raw, _ := json.Marshal(reg)
	h.handleFrame(c, raw)

	cs, ok := sink.lastPub()
	if !ok || cs.ClusterID != 0 || cs.Generation != 1 || !cs.Alive {
		t.Fatalf("register did not publish active cluster: %+v ok=%v", cs, ok)
	}

	// Heartbeat carrying an opaque status payload should re-publish with it.
	hb := protocol.ShardRequest{ID: "2", Op: protocol.OpHeartbeat, Payload: json.RawMessage(`{"guilds":3,"shards":[]}`)}
	raw, _ = json.Marshal(hb)
	h.handleFrame(c, raw)

	cs, _ = sink.lastPub()
	if string(cs.Status) == "" {
		t.Fatalf("heartbeat status payload not forwarded: %+v", cs)
	}
}

func TestStatusSinkRemovesOnReap(t *testing.T) {
	h := NewHub(&config.Config{HeartbeatTimeoutSecs: 1})
	sink := &fakeSink{}
	h.SetStatusSink(sink)
	fc := &fakeConn{}
	c := h.newConn(fc)

	reg := protocol.ShardRequest{ID: "1", Op: protocol.OpRegister, Payload: json.RawMessage(`{"clusterId":0,"shards":[0],"generation":0}`)}
	raw, _ := json.Marshal(reg)
	h.handleFrame(c, raw)

	// Force the heartbeat stale, then reap.
	c.mu.Lock()
	c.lastBeat = time.Now().Add(-10 * time.Second)
	c.mu.Unlock()
	h.reapDead()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.removals) == 0 || sink.removals[len(sink.removals)-1] != 0 {
		t.Fatalf("reap did not emit removal: %+v", sink.removals)
	}
}

func TestGetClusterStatusesReturnsActiveSnapshot(t *testing.T) {
	h := testHub()

	// Register two clusters, each with an opaque HEARTBEAT status payload.
	for _, cid := range []int{0, 1} {
		fc := &fakeConn{}
		c := h.newConn(fc)
		reg := protocol.ShardRequest{
			ID:      "reg",
			Op:      protocol.OpRegister,
			Payload: json.RawMessage(`{"clusterId":` + string(rune('0'+cid)) + `,"shards":[` + string(rune('0'+cid)) + `],"generation":0}`),
		}
		raw, _ := json.Marshal(reg)
		h.handleFrame(c, raw)

		hb := protocol.ShardRequest{ID: "hb", Op: protocol.OpHeartbeat, Payload: json.RawMessage(`{"status":{"guilds":2,"members":10}}`)}
		raw, _ = json.Marshal(hb)
		h.handleFrame(c, raw)
	}

	// A plain client (no REGISTER) asks for the snapshot.
	client := &fakeConn{}
	cc := h.newConn(client)
	req := protocol.ShardRequest{ID: "snap", Op: protocol.OpGetClusterStatuses, Payload: json.RawMessage(`{}`)}
	raw, _ := json.Marshal(req)
	h.handleFrame(cc, raw)

	resp := decodeResp(t, client.last())
	if !resp.OK || resp.ID != "snap" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	var states []status.ClusterState
	if err := json.Unmarshal(resp.Data, &states); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("want 2 cluster states, got %d", len(states))
	}
	for _, cs := range states {
		if !cs.Alive {
			t.Errorf("cluster %d should be alive", cs.ClusterID)
		}
		if len(cs.Status) == 0 {
			t.Errorf("cluster %d missing opaque status payload", cs.ClusterID)
		}
	}
}
