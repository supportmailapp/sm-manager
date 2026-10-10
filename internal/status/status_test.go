package status

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestAuthenticator(t *testing.T) {
	// No keys → open.
	open := newAuthenticator(nil)
	if !open.disabled() {
		t.Fatal("expected disabled with no keys")
	}
	if !open.check(httptest.NewRequest("GET", "/ws", nil)) {
		t.Fatal("open endpoint should accept any request")
	}

	a := newAuthenticator([]string{"secret", ""}) // empty key filtered out
	if a.disabled() {
		t.Fatal("expected enabled with a key")
	}
	cases := []struct {
		name   string
		set    func(*http.Request)
		expect bool
	}{
		{"no key", func(*http.Request) {}, false},
		{"wrong key", func(r *http.Request) { r.Header.Set("X-API-Key", "nope") }, false},
		{"header key", func(r *http.Request) { r.Header.Set("X-API-Key", "secret") }, true},
		{"bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer secret") }, true},
		{"subprotocol", func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Protocol", "sm-status.v1, key.secret")
		}, true},
		{"subprotocol wrong", func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Protocol", "sm-status.v1, key.nope")
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/ws", nil)
			tc.set(r)
			if got := a.check(r); got != tc.expect {
				t.Fatalf("check=%v want %v", got, tc.expect)
			}
		})
	}
}

func TestBroadcasterSnapshotThenDelta(t *testing.T) {
	b := NewBroadcaster()
	// Seed state before any client connects.
	b.Publish(ClusterState{ClusterID: 0, Generation: 1, Alive: true})

	c := &client{send: make(chan []byte, 8)}
	b.addClient(c)

	// First message must be the snapshot carrying the seeded cluster.
	snap := recvEvent(t, c)
	if snap.Type != "snapshot" || len(snap.Clusters) != 1 || snap.Clusters[0].ClusterID != 0 {
		t.Fatalf("bad snapshot: %+v", snap)
	}

	// A subsequent publish arrives as a delta.
	b.Publish(ClusterState{ClusterID: 1, Generation: 1, Alive: true})
	d := recvEvent(t, c)
	if d.Type != "delta" || d.Cluster == nil || d.Cluster.ClusterID != 1 {
		t.Fatalf("bad delta: %+v", d)
	}

	// Removal delta.
	b.Remove(1)
	rm := recvEvent(t, c)
	if rm.Type != "delta" || rm.Removed == nil || *rm.Removed != 1 {
		t.Fatalf("bad removal: %+v", rm)
	}
}

func TestBroadcasterDropsSlowClient(t *testing.T) {
	b := NewBroadcaster()
	c := &client{send: make(chan []byte, 1)}
	b.addClient(c) // fills the 1-slot buffer with the snapshot

	// This publish can't be delivered (buffer full) → client dropped + closed.
	b.Publish(ClusterState{ClusterID: 0})
	b.mu.Lock()
	_, still := b.clients[c]
	b.mu.Unlock()
	if still {
		t.Fatal("slow client should have been dropped")
	}
}

// TestServerEndToEnd runs the real WS server behind httptest and connects with
// the coder/websocket client, asserting auth gate + snapshot + live delta.
func TestServerEndToEnd(t *testing.T) {
	b := NewBroadcaster()
	b.Publish(ClusterState{ClusterID: 0, Generation: 2, Alive: true})

	srv := NewServer(":0", []string{"k1"}, Limits{}, b)
	ts := httptest.NewServer(srv.hs.Handler)
	defer ts.Close()
	wsBase := "ws" + strings.TrimPrefix(ts.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wrong key → rejected at handshake. Key travels as a subprotocol.
	badOpts := &websocket.DialOptions{Subprotocols: []string{statusSubprotocol, "key.bad"}}
	if _, _, err := websocket.Dial(ctx, wsBase+"/ws", badOpts); err == nil {
		t.Fatal("expected dial to fail with bad key")
	}

	// Good key → snapshot then delta.
	goodOpts := &websocket.DialOptions{Subprotocols: []string{statusSubprotocol, "key.k1"}}
	conn, _, err := websocket.Dial(ctx, wsBase+"/ws", goodOpts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// Server must negotiate only the non-secret protocol — never echo the key.
	if got := conn.Subprotocol(); got != statusSubprotocol {
		t.Fatalf("negotiated subprotocol = %q, want %q (key must not be echoed)", got, statusSubprotocol)
	}

	snap := readEvent(t, ctx, conn)
	if snap.Type != "snapshot" || len(snap.Clusters) != 1 {
		t.Fatalf("bad snapshot over the wire: %+v", snap)
	}

	b.Publish(ClusterState{ClusterID: 1, Generation: 2, Alive: true})
	d := readEvent(t, ctx, conn)
	if d.Type != "delta" || d.Cluster == nil || d.Cluster.ClusterID != 1 {
		t.Fatalf("bad delta over the wire: %+v", d)
	}
}

func recvEvent(t *testing.T, c *client) Event {
	t.Helper()
	select {
	case raw := <-c.send:
		var e Event
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return Event{}
	}
}

func readEvent(t *testing.T, ctx context.Context, conn *websocket.Conn) Event {
	t.Helper()
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return e
}
