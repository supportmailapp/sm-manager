package server

import (
	"strings"
	"testing"
	"time"

	"sm-manager/internal/config"
	"sm-manager/internal/protocol"
	"sm-manager/internal/status"
)

// Golden tests pin the exact bytes the hub writes on the internal socket and
// the inputs it must keep accepting. They never name the JSON package, so they
// run unchanged against whichever encoding/json implementation the build uses.
// In string literals below, the marker BS stands for a backslash and is
// replaced when the strings are built (see bs).

// bs turns the BS marker into a backslash.
func bs(s string) string { return strings.ReplaceAll(s, "BS", "\\") }

// goldenHub has no heartbeat timeout, so a connected peer is always alive and
// the output does not depend on the clock.
func goldenHub() *Hub {
	return NewHub(&config.Config{
		AllowedBusinessOps:   []string{"GET_GUILD_LIST"},
		HeartbeatTimeoutSecs: 0,
	})
}

// raw feeds one frame body to the hub.
func raw(h *Hub, c *Conn, frame string) { h.handleFrame(c, []byte(frame)) }

// reply waits for the nth frame written to fc and returns it as a string.
func reply(t *testing.T, fc *fakeConn, n int) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fc.mu.Lock()
		if len(fc.frames) >= n {
			s := string(fc.frames[n-1])
			fc.mu.Unlock()
			return s
		}
		fc.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no frame #%d written", n)
	return ""
}

func TestWireGoldenSend(t *testing.T) {
	zero, one := 0, 1
	guild := "5"
	cases := []struct {
		name string
		v    any
		want string
	}{
		{"ok without data", protocol.ShardResponse{ID: "1", OK: true}, `{"id":"1","ok":true}`},
		{"data null is kept", protocol.ShardResponse{ID: "1", OK: true, Data: []byte("null")}, `{"id":"1","ok":true,"data":null}`},
		{"data empty array is kept", protocol.ShardResponse{ID: "1", OK: true, Data: []byte("[]")}, `{"id":"1","ok":true,"data":[]}`},
		{"data empty object is kept", protocol.ShardResponse{ID: "1", OK: true, Data: []byte("{}")}, `{"id":"1","ok":true,"data":{}}`},
		{"data is compacted", protocol.ShardResponse{ID: "1", OK: true, Data: []byte(` { "a" : [ 1 , 2 ] } `)}, `{"id":"1","ok":true,"data":{"a":[1,2]}}`},
		{"error reply", protocol.ShardResponse{ID: "1", OK: false, Error: "unknown op: X"}, `{"id":"1","ok":false,"error":"unknown op: X"}`},
		{"empty id is kept", protocol.ShardResponse{ID: "", OK: false, Error: "malformed request"}, `{"id":"","ok":false,"error":"malformed request"}`},
		{"error html chars escaped", protocol.ShardResponse{ID: "1", Error: "unknown op: <x>&y"}, bs(`{"id":"1","ok":false,"error":"unknown op: BSu003cxBSu003eBSu0026y"}`)},
		{"error line separators escaped", protocol.ShardResponse{ID: "1", Error: "a" + string(rune(0x2028)) + "b" + string(rune(0x2029)) + "c"}, bs(`{"id":"1","ok":false,"error":"aBSu2028bBSu2029c"}`)},
		{"error control and quote chars", protocol.ShardResponse{ID: "1", Error: "a\"b\\c\nd\te\x01f"}, bs(`{"id":"1","ok":false,"error":"aBS"bBSBScBSndBSteBSu0001f"}`)},
		{"error unicode kept literal", protocol.ShardResponse{ID: "1", Error: "é€😀"}, `{"id":"1","ok":false,"error":"é€😀"}`},
		{"quiesce request has null payload", protocol.ShardRequest{ID: "q", Op: protocol.OpQuiesce}, `{"id":"q","op":"QUIESCE","payload":null}`},
		{"request payload kept", protocol.ShardRequest{ID: "q", Op: "X", Payload: []byte(`{}`)}, `{"id":"q","op":"X","payload":{}}`},
		{"request with full target", protocol.ShardRequest{ID: "q", Op: "X", Target: &protocol.Target{ShardId: &zero, ClusterId: &one, GuildId: &guild}, Payload: []byte(`{}`)},
			`{"id":"q","op":"X","target":{"shardId":0,"clusterId":1,"guildId":"5"},"payload":{}}`},
		{"request with empty target", protocol.ShardRequest{ID: "q", Op: "X", Target: &protocol.Target{}, Payload: []byte(`{}`)},
			`{"id":"q","op":"X","target":{},"payload":{}}`},
		{"empty guild id target is kept", protocol.ShardRequest{ID: "q", Op: "X", Target: &protocol.Target{GuildId: new(string)}, Payload: []byte(`{}`)}, `{"id":"q","op":"X","target":{"guildId":""},"payload":{}}`},
		{"nil slice and map stay null", map[string]any{"slice": []int(nil), "map": map[string]int(nil), "ptr": (*int)(nil)}, `{"map":null,"ptr":null,"slice":null}`},
		{"nil slice at top level", []InstanceInfo(nil), `null`},
		{"zero instance info keeps null guildIds", InstanceInfo{}, `{"instanceId":"","state":"","generation":0,"alive":false,"lastBeat":0,"guildIds":null}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeConn{}
			c := goldenHub().newConn(fc)
			if err := c.send(tc.v); err != nil {
				t.Fatalf("send: %v", err)
			}
			if got := string(fc.last()); got != tc.want {
				t.Fatalf("\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestWireGoldenClusterStatuses(t *testing.T) {
	h := goldenHub()

	// No clusters: an empty list, never null.
	client := &fakeConn{}
	cc := h.newConn(client)
	raw(h, cc, `{"id":"s","op":"GET_CLUSTER_STATUSES","payload":{}}`)
	if got, want := reply(t, client, 1), `{"id":"s","ok":true,"data":[]}`; got != want {
		t.Fatalf("empty\n got %s\nwant %s", got, want)
	}

	// One cluster with an opaque heartbeat payload that carries whitespace and
	// characters the encoder escapes.
	fc := &fakeConn{}
	c := h.newConn(fc)
	raw(h, c, `{"id":"r","op":"REGISTER","payload":{"clusterId":3,"shards":[6,7],"generation":2,"extra":true}}`)
	raw(h, c, `{"id":"h","op":"HEARTBEAT","payload":{ "guilds" : 2, "note":"<b>&" }}`)
	c.mu.Lock()
	c.lastBeat = time.UnixMilli(1700000000123)
	c.mu.Unlock()
	if string(c.status) != `{ "guilds" : 2, "note":"<b>&" }` {
		t.Fatalf("stored status not verbatim: %q", c.status)
	}
	raw(h, cc, `{"id":"s2","op":"GET_CLUSTER_STATUSES","payload":{}}`)
	want := bs(`{"id":"s2","ok":true,"data":[{"clusterId":3,"shards":[6,7],"generation":2,"alive":true,"lastBeat":1700000000123,"status":{"guilds":2,"note":"BSu003cbBSu003eBSu0026"}}]}`)
	if got := reply(t, client, 2); got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}

	// A cluster that registered without shards and never sent a status.
	h2 := goldenHub()
	fc2 := &fakeConn{}
	c2 := h2.newConn(fc2)
	raw(h2, c2, `{"id":"r","op":"REGISTER","payload":{"clusterId":0}}`)
	c2.mu.Lock()
	c2.lastBeat = time.UnixMilli(5)
	c2.mu.Unlock()
	cl2 := &fakeConn{}
	raw(h2, h2.newConn(cl2), `{"id":"s","op":"GET_CLUSTER_STATUSES"}`)
	want = `{"id":"s","ok":true,"data":[{"clusterId":0,"generation":0,"alive":true,"lastBeat":5}]}`
	if got := reply(t, cl2, 1); got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
}

func TestWireGoldenDeployReply(t *testing.T) {
	h := goldenHub()
	h.SetDeployer(&fakeDeployer{gen: 7})
	fc := &fakeConn{}
	c := h.newConn(fc)
	raw(h, c, `{"id":"d","op":"DEPLOY","payload":{}}`)
	if got, want := reply(t, fc, 1), `{"id":"d","ok":true,"data":{"generation":7}}`; got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
}

func TestWireGoldenInstanceReplies(t *testing.T) {
	h := goldenHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	// No instances: an empty list, never null.
	raw(h, c, `{"id":"g0","op":"GET_INSTANCE_STATUSES","payload":{}}`)
	if got, want := reply(t, fc, 1), `{"id":"g0","ok":true,"data":[]}`; got != want {
		t.Fatalf("empty\n got %s\nwant %s", got, want)
	}

	addInstance(h, "wl_a", "111", "222")
	addInstance(h, "wl_b")
	addInstance(h, "x&y", "<1>")
	h.mu.Lock()
	a := h.instances["wl_a"]
	a.state = status.InstanceRunning
	a.generation = 2
	a.lastBeat = time.UnixMilli(1700000000456)
	a.conn = &Conn{}
	a.status = []byte(`{ "guilds" : 1 }`)
	b := h.instances["wl_b"]
	b.state = status.InstanceFailed
	b.exitCode = 78
	h.mu.Unlock()

	raw(h, c, `{"id":"g1","op":"GET_INSTANCE_STATUSES","payload":{}}`)
	want := bs(`{"id":"g1","ok":true,"data":[` +
		`{"instanceId":"wl_a","state":"running","generation":2,"alive":true,"lastBeat":1700000000456,"status":{"guilds":1},"guildIds":["111","222"]},` +
		`{"instanceId":"wl_b","state":"failed","generation":0,"alive":false,"lastBeat":0,"exitCode":78,"guildIds":[]},` +
		`{"instanceId":"xBSu0026y","state":"starting","generation":0,"alive":false,"lastBeat":0,"guildIds":["BSu003c1BSu003e"]}]}`)
	if got := reply(t, fc, 2); got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
}

func TestWireGoldenInstanceOps(t *testing.T) {
	h, _ := opHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	raw(h, c, `{"id":"o1","op":"START_INSTANCE","payload":{"instanceId":"wl_a","guildIds":["111","222","111"]}}`)
	want := `{"id":"o1","ok":true,"data":{"instanceId":"wl_a","state":"starting","generation":0,"alive":false,"lastBeat":0,"guildIds":["111","222"]}}`
	if got := reply(t, fc, 1); got != want {
		t.Fatalf("start\n got %s\nwant %s", got, want)
	}

	// Unknown ids answer with data null, which must not be dropped.
	raw(h, c, `{"id":"o2","op":"REMOVE_INSTANCE","payload":{"instanceId":"nope"}}`)
	if got, want := reply(t, fc, 2), `{"id":"o2","ok":true,"data":null}`; got != want {
		t.Fatalf("remove unknown\n got %s\nwant %s", got, want)
	}
	raw(h, c, `{"id":"o3","op":"STOP_INSTANCE","payload":{"instanceId":"nope"}}`)
	if got, want := reply(t, fc, 3), `{"id":"o3","ok":true,"data":null}`; got != want {
		t.Fatalf("stop unknown\n got %s\nwant %s", got, want)
	}
	raw(h, c, `{"id":"o4","op":"RESTART_INSTANCE","payload":{"instanceId":"nope"}}`)
	if got, want := reply(t, fc, 4), `{"id":"o4","ok":false,"error":"unknown instance"}`; got != want {
		t.Fatalf("restart unknown\n got %s\nwant %s", got, want)
	}

	// wl_a is starting, so run=false stops it; the fake runner reports stopped.
	raw(h, c, `{"id":"o5","op":"SYNC_INSTANCES","payload":{"instances":[{"instanceId":"wl_a","guildIds":["111"],"run":false},{"instanceId":"wl_c","guildIds":null,"run":false}]}}`)
	want = `{"id":"o5","ok":true,"data":[` +
		`{"instanceId":"wl_a","state":"stopped","generation":0,"alive":false,"lastBeat":0,"guildIds":["111"]},` +
		`{"instanceId":"wl_c","state":"stopped","generation":0,"alive":false,"lastBeat":0,"guildIds":[]}]}`
	if got := reply(t, fc, 5); got != want {
		t.Fatalf("sync\n got %s\nwant %s", got, want)
	}

	// Malformed payloads keep their exact error text.
	for i, tc := range []struct{ frame, want string }{
		{`{"id":"e1","op":"START_INSTANCE","payload":{"instanceId":"","guildIds":[]}}`, `{"id":"e1","ok":false,"error":"malformed payload: instanceId and guildIds required"}`},
		{`{"id":"e2","op":"START_INSTANCE","payload":{"instanceId":5}}`, `{"id":"e2","ok":false,"error":"malformed payload: instanceId and guildIds required"}`},
		{`{"id":"e3","op":"STOP_INSTANCE","payload":[]}`, `{"id":"e3","ok":false,"error":"malformed payload: instanceId required"}`},
		{`{"id":"e4","op":"SYNC_INSTANCES","payload":{"instances":"x"}}`, `{"id":"e4","ok":false,"error":"malformed payload: instances required"}`},
	} {
		raw(h, c, tc.frame)
		if got := reply(t, fc, 6+i); got != tc.want {
			t.Fatalf("malformed %d\n got %s\nwant %s", i, got, tc.want)
		}
	}
}

func TestWireGoldenInstanceHeartbeatAndRegister(t *testing.T) {
	h := goldenHub()
	addInstance(h, "wl_a", "111")
	fc := &fakeConn{}
	c := h.newConn(fc)

	raw(h, c, `{"id":"r","op":"REGISTER","payload":{"instanceId":"wl_a","generation":4,"unknown":[1,2]}}`)
	if got, want := reply(t, fc, 1), `{"id":"r","ok":true}`; got != want {
		t.Fatalf("register\n got %s\nwant %s", got, want)
	}
	raw(h, c, `{"id":"h","op":"HEARTBEAT","payload":{"instanceId":"wl_a","status":{ "members" : 42 }}}`)
	if got, want := reply(t, fc, 2), `{"id":"h","ok":true}`; got != want {
		t.Fatalf("heartbeat\n got %s\nwant %s", got, want)
	}
	if got := h.snapshotInstances()[0]; string(got.Status) != `{ "members" : 42 }` || got.Generation != 4 {
		t.Fatalf("unexpected instance info: %+v status=%q", got, got.Status)
	}
	// A heartbeat without a status keeps the previous one.
	raw(h, c, `{"id":"h2","op":"HEARTBEAT","payload":{"instanceId":"wl_a"}}`)
	if got := h.snapshotInstances()[0]; string(got.Status) != `{ "members" : 42 }` {
		t.Fatalf("status lost: %q", got.Status)
	}
}

// Frames a client may send must keep decoding: unknown members, whitespace,
// escapes and odd-but-valid string content in opaque payloads that are only
// forwarded.
func TestWireGoldenBusinessFrameForwardedVerbatim(t *testing.T) {
	payloads := map[string]string{
		"whitespace":          `{ "a" : [ 1 , 2 ] }`,
		"big and exp numbers": `{"id":12345678901234567890,"n":1E2,"f":-0.0}`,
		"escaped html":        bs(`{"m":"BSu003cbBSu003e"}`),
		"lone surrogate":      bs(`{"m":"BSud83d"}`),
		"raw invalid utf8":    "{\"m\":\"\xff\"}",
		"unicode":             `{"m":"é€😀"}`,
		"null":                `null`,
		"nested":              `{"a":{"b":{"c":[[],{}]}}}`,
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			h := goldenHub()
			cl := &fakeConn{}
			cc := h.newConn(cl)
			raw(h, cc, `{"id":"r","op":"REGISTER","payload":{"clusterId":0,"shards":[0]}}`)

			caller := &fakeConn{}
			cc2 := h.newConn(caller)
			frame := `{"id":"abc","op":"GET_GUILD_LIST","target":{"clusterId":0},"payload":` + payload + `}`
			raw(h, cc2, frame)
			if got := reply(t, cl, 2); got != frame {
				t.Fatalf("not forwarded verbatim\n got %q\nwant %q", got, frame)
			}

			// The response frame is routed back untouched as well.
			resp := `{"id":"abc","ok":true,"data": [ 1 , ` + payload + ` ]}`
			raw(h, cc, resp)
			if got := string(caller.last()); got != resp {
				t.Fatalf("response not routed verbatim\n got %q\nwant %q", got, resp)
			}
		})
	}
}

func TestWireGoldenMalformedFrames(t *testing.T) {
	h := goldenHub()
	fc := &fakeConn{}
	c := h.newConn(fc)

	// Not JSON at all: logged and dropped, no reply.
	raw(h, c, `{"id":"m","op":"DEPLOY",`)
	raw(h, c, `[]`)
	raw(h, c, `not json`)
	if fc.last() != nil {
		t.Fatalf("unexpected reply to unparseable frame: %s", fc.last())
	}

	// Parses as a frame kind but not as a request: structural ops answer with
	// an empty id, business ops with the id.
	raw(h, c, `{"id":"m","op":"DEPLOY","payload":{},"target":5}`)
	if got, want := reply(t, fc, 1), `{"id":"","ok":false,"error":"malformed request"}`; got != want {
		t.Fatalf("structural\n got %s\nwant %s", got, want)
	}
	raw(h, c, `{"id":"m2","op":"GET_GUILD_LIST","payload":{},"target":5}`)
	if got, want := reply(t, fc, 2), `{"id":"m2","ok":false,"error":"malformed request"}`; got != want {
		t.Fatalf("business\n got %s\nwant %s", got, want)
	}
	raw(h, c, `{"id":"m3","op":"NOT_ALLOWED","payload":{}}`)
	if got, want := reply(t, fc, 3), `{"id":"m3","ok":false,"error":"unknown op: NOT_ALLOWED"}`; got != want {
		t.Fatalf("unknown op\n got %s\nwant %s", got, want)
	}

	// REGISTER payloads that do not decode.
	for i, payload := range []string{`[]`, `"x"`, `{"clusterId":"x"}`, `{"clusterId":1.5}`, `{"clusterId":0,"shards":"x"}`} {
		raw(h, c, `{"id":"b","op":"REGISTER","payload":`+payload+`}`)
		if got, want := reply(t, fc, 4+i), `{"id":"b","ok":false,"error":"bad REGISTER payload"}`; got != want {
			t.Fatalf("register %s\n got %s\nwant %s", payload, got, want)
		}
	}
}

func TestWireGoldenHeartbeatPayloadForms(t *testing.T) {
	h := goldenHub()
	fc := &fakeConn{}
	c := h.newConn(fc)
	raw(h, c, `{"id":"r","op":"REGISTER","payload":{"clusterId":0,"shards":[0]}}`)

	// No payload member: nothing stored.
	raw(h, c, `{"id":"h","op":"HEARTBEAT"}`)
	if len(c.status) != 0 {
		t.Fatalf("status stored for missing payload: %q", c.status)
	}
	// An explicit null payload is stored as the literal null, as before.
	raw(h, c, `{"id":"h","op":"HEARTBEAT","payload":null}`)
	if string(c.status) != `null` {
		t.Fatalf("status = %q, want null", c.status)
	}
	// Whitespace inside the payload is stored exactly as received.
	raw(h, c, `{"id":"h","op":"HEARTBEAT","payload":  { "a" : 1 }  }`)
	if string(c.status) != `{ "a" : 1 }` {
		t.Fatalf("status = %q", c.status)
	}
}

// Invalid UTF-8 and lone surrogate escapes that reach a decoded string are
// replaced by U+FFFD, so the reply is valid JSON.
func TestWireGoldenInvalidUTF8InDecodedStrings(t *testing.T) {
	h, _ := opHub()
	fc := &fakeConn{}
	c := h.newConn(fc)
	repl := string(rune(0xFFFD))

	raw(h, c, bs(`{"id":"u1","op":"START_INSTANCE","payload":{"instanceId":"wl_xBSud83d","guildIds":["1"]}}`))
	want := `{"id":"u1","ok":true,"data":{"instanceId":"wl_x` + repl + `","state":"starting","generation":0,"alive":false,"lastBeat":0,"guildIds":["1"]}}`
	if got := reply(t, fc, 1); got != want {
		t.Fatalf("lone surrogate\n got %q\nwant %q", got, want)
	}
	raw(h, c, "{\"id\":\"u2\",\"op\":\"START_INSTANCE\",\"payload\":{\"instanceId\":\"wl_y\xff\",\"guildIds\":[\"2\"]}}")
	want = `{"id":"u2","ok":true,"data":{"instanceId":"wl_y` + repl + `","state":"starting","generation":0,"alive":false,"lastBeat":0,"guildIds":["2"]}}`
	if got := reply(t, fc, 2); got != want {
		t.Fatalf("raw invalid byte\n got %q\nwant %q", got, want)
	}
}
