package server

import (
	"encoding/json"
	"testing"
	"time"

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
	raw, err := json.Marshal(protocol.ShardRequest{ID: id, Op: op, Payload: json.RawMessage(payload)})
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
