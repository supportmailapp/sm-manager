package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sm-manager/internal/protocol"
	"sm-manager/internal/status"
)

// A connection keeps the identity it first registered with.
func TestReRegisterOnSameConnIsGuarded(t *testing.T) {
	const wantErr = "connection already registered as a different peer"
	register := func(h *Hub, c *Conn, fc *fakeConn, payload string) protocol.ShardResponse {
		sendFrame(t, h, c, "reg", protocol.OpRegister, payload)
		return decodeResp(t, fc.last())
	}

	t.Run("cluster to another cluster id", func(t *testing.T) {
		h := testHub()
		fc := &fakeConn{}
		c := h.newConn(fc)
		if resp := register(h, c, fc, `{"clusterId":0,"shards":[0],"generation":1}`); !resp.OK {
			t.Fatalf("first register: %+v", resp)
		}
		if resp := register(h, c, fc, `{"clusterId":1,"shards":[1],"generation":1}`); resp.OK || resp.Error != wantErr {
			t.Fatalf("want %q, got %+v", wantErr, resp)
		}
		if clusterOf(h, 1) != nil || generationOf(h, 1, 1) != nil || clusterOf(h, 0) != c {
			t.Fatal("rejected register changed the registry")
		}
	})

	t.Run("cluster to another generation", func(t *testing.T) {
		h := testHub()
		fc := &fakeConn{}
		c := h.newConn(fc)
		register(h, c, fc, `{"clusterId":0,"shards":[0],"generation":1}`)
		if resp := register(h, c, fc, `{"clusterId":0,"shards":[0],"generation":2}`); resp.OK || resp.Error != wantErr {
			t.Fatalf("want %q, got %+v", wantErr, resp)
		}
		if generationOf(h, 2, 0) != nil {
			t.Fatal("rejected register was tracked under generation 2")
		}
	})

	t.Run("same cluster again is allowed", func(t *testing.T) {
		h := testHub()
		fc := &fakeConn{}
		c := h.newConn(fc)
		register(h, c, fc, `{"clusterId":0,"shards":[0],"generation":1}`)
		if resp := register(h, c, fc, `{"clusterId":0,"shards":[0,1],"generation":1}`); !resp.OK {
			t.Fatalf("re-register as the same cluster refused: %+v", resp)
		}
	})

	t.Run("cluster to instance", func(t *testing.T) {
		h := testHub()
		addInstance(h, "wl_a", "111")
		fc := &fakeConn{}
		c := h.newConn(fc)
		register(h, c, fc, `{"clusterId":0,"shards":[0]}`)
		if resp := register(h, c, fc, `{"instanceId":"wl_a","generation":1}`); resp.OK || resp.Error != wantErr {
			t.Fatalf("want %q, got %+v", wantErr, resp)
		}
		if got, _ := instanceState(h, "wl_a"); got.Alive || got.State != status.InstanceStarting {
			t.Fatalf("rejected register bound the instance: %+v", got)
		}
	})

	t.Run("instance to another instance", func(t *testing.T) {
		h := testHub()
		addInstance(h, "wl_a", "111")
		addInstance(h, "wl_b", "222")
		c, fc := registerInstance(t, h, "wl_a", 1)
		if resp := register(h, c, fc, `{"instanceId":"wl_b","generation":1}`); resp.OK || resp.Error != wantErr {
			t.Fatalf("want %q, got %+v", wantErr, resp)
		}
		if got, _ := instanceState(h, "wl_b"); got.Alive {
			t.Fatalf("rejected register bound wl_b: %+v", got)
		}
	})

	t.Run("instance to cluster", func(t *testing.T) {
		h := testHub()
		addInstance(h, "wl_a", "111")
		c, fc := registerInstance(t, h, "wl_a", 1)
		if resp := register(h, c, fc, `{"clusterId":0,"shards":[0]}`); resp.OK || resp.Error != wantErr {
			t.Fatalf("want %q, got %+v", wantErr, resp)
		}
		if clusterCount(h) != 0 {
			t.Fatal("an instance connection became a cluster")
		}
	})

	t.Run("same instance again with a new generation is allowed", func(t *testing.T) {
		h := testHub()
		addInstance(h, "wl_a", "111")
		c, fc := registerInstance(t, h, "wl_a", 1)
		if resp := register(h, c, fc, `{"instanceId":"wl_a","generation":2}`); !resp.OK {
			t.Fatalf("re-register as the same instance refused: %+v", resp)
		}
		if got, _ := instanceState(h, "wl_a"); got.Generation != 2 {
			t.Fatalf("generation = %d, want 2", got.Generation)
		}
	})
}

// Two SYNC_INSTANCES must not overlap: while the first is stuck stopping an
// instance it removes, the second must not start applying its own list.
func TestOverlappingSyncsAreSerialized(t *testing.T) {
	h, r := opHub()
	addInstance(h, "wl_old", "111")
	h.SetInstanceState("wl_old", status.InstanceRunning, 0)
	r.stopGate = make(chan struct{})

	first := &fakeConn{}
	sendFrame(t, h, h.newConn(first), "s1", protocol.OpSyncInstances, `{"instances":[]}`)
	deadline := time.Now().Add(2 * time.Second)
	for r.stopCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if r.stopCount() != 1 {
		t.Fatal("the first sync never reached the stop")
	}

	second := &fakeConn{}
	// Lists wl_old too, so it has nothing of its own to remove and only the sync
	// lock keeps it from running ahead of the first one.
	sendFrame(t, h, h.newConn(second), "s2", protocol.OpSyncInstances,
		`{"instances":[{"instanceId":"wl_old","guildIds":["111"],"run":false},`+
			`{"instanceId":"wl_new","guildIds":["222"],"run":true}]}`)
	time.Sleep(100 * time.Millisecond)
	if r.startCount() != 0 || instanceCount(h) != 1 || instanceRunState(h, "wl_new") != "" {
		t.Fatalf("the second sync ran while the first was in progress: %d starts, %d instances",
			r.startCount(), instanceCount(h))
	}

	close(r.stopGate)
	for _, fc := range []*fakeConn{first, second} {
		deadline := time.Now().Add(3 * time.Second)
		for fc.last() == nil && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if fc.last() == nil {
			t.Fatal("a sync never replied")
		}
		if resp := decodeResp(t, fc.last()); !resp.OK {
			t.Fatalf("sync not ok: %+v", resp)
		}
	}
	if got := r.startedIDs(); len(got) != 1 || got[0] != "wl_new" {
		t.Fatalf("started = %v, want only wl_new", got)
	}
}

// gatedSink blocks inside its first PublishInstance until released.
type gatedSink struct {
	fakeSink
	entered chan struct{}
	gate    chan struct{}
	first   atomic.Bool
}

// Only the first call blocks. (A sync.Once would make a concurrent second call
// wait for the first one too, which would hide the bug under test.)
func (s *gatedSink) PublishInstance(is status.InstanceState) {
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		<-s.gate
	}
	s.fakeSink.PublishInstance(is)
}

// A publish that read an older state must not be delivered after a newer one.
func TestStatusPublishesAreSerialized(t *testing.T) {
	h := testHub()
	sink := &gatedSink{entered: make(chan struct{}), gate: make(chan struct{})}
	h.SetStatusSink(sink)
	addInstance(h, "wl_a", "111")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		h.SetInstanceState("wl_a", status.InstanceRunning, 0) // reads running, then stalls in the sink
	}()
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first publish never reached the sink")
	}
	go func() {
		defer wg.Done()
		h.SetInstanceState("wl_a", status.InstanceFailed, 78)
	}()
	time.Sleep(100 * time.Millisecond) // let the second one run into the first
	close(sink.gate)
	wg.Wait()

	got, ok := sink.lastInstPub("wl_a")
	if !ok || got.State != status.InstanceFailed || got.ExitCode != 78 {
		t.Fatalf("the feed ended on a stale state: %+v (ok=%v)", got, ok)
	}
}
