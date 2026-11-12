package status

import "testing"

// Golden tests pin the exact bytes the public status WebSocket writes. They
// build values without naming the JSON package, so they run unchanged against
// whichever encoding/json implementation the build uses.

func TestWireGoldenSnapshot(t *testing.T) {
	cases := []struct {
		name string
		ev   Event
		want string
	}{
		{
			name: "snapshot with clusters",
			ev: Event{Type: "snapshot", Clusters: []ClusterState{
				{ClusterID: 0, Shards: []int{0, 1}, Generation: 3, Alive: true, LastBeat: 1700000000123, Status: []byte(`{"guilds":2,"members":10}`)},
				{ClusterID: 1, Shards: []int{2, 3}, Generation: 3, Alive: false, LastBeat: 0},
			}},
			want: `{"type":"snapshot","clusters":[` +
				`{"clusterId":0,"shards":[0,1],"generation":3,"alive":true,"lastBeat":1700000000123,"status":{"guilds":2,"members":10}},` +
				`{"clusterId":1,"shards":[2,3],"generation":3,"alive":false,"lastBeat":0}]}`,
		},
		{
			name: "snapshot with no clusters omits the field",
			ev:   Event{Type: "snapshot", Clusters: []ClusterState{}},
			want: `{"type":"snapshot"}`,
		},
		{
			name: "snapshot with nil clusters omits the field",
			ev:   Event{Type: "snapshot"},
			want: `{"type":"snapshot"}`,
		},
		{
			name: "cluster without shards or status omits both",
			ev:   Event{Type: "snapshot", Clusters: []ClusterState{{ClusterID: 4, Generation: 0, Alive: true, LastBeat: 5}}},
			want: `{"type":"snapshot","clusters":[{"clusterId":4,"generation":0,"alive":true,"lastBeat":5}]}`,
		},
		{
			name: "empty shards slice omitted",
			ev:   Event{Type: "snapshot", Clusters: []ClusterState{{ClusterID: 4, Shards: []int{}, Alive: true}}},
			want: `{"type":"snapshot","clusters":[{"clusterId":4,"generation":0,"alive":true,"lastBeat":0}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(marshal(tc.ev)); got != tc.want {
				t.Fatalf("\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestWireGoldenDelta(t *testing.T) {
	zero := 0
	two := 2
	cases := []struct {
		name string
		ev   Event
		want string
	}{
		{
			name: "cluster delta",
			ev:   Event{Type: "delta", Cluster: &ClusterState{ClusterID: 2, Shards: []int{4}, Generation: 1, Alive: true, LastBeat: 99, Status: []byte(`{"a":1}`)}},
			want: `{"type":"delta","cluster":{"clusterId":2,"shards":[4],"generation":1,"alive":true,"lastBeat":99,"status":{"a":1}}}`,
		},
		{
			name: "removed cluster id 0 is kept",
			ev:   Event{Type: "delta", Removed: &zero},
			want: `{"type":"delta","removed":0}`,
		},
		{
			name: "removed cluster id 2",
			ev:   Event{Type: "delta", Removed: &two},
			want: `{"type":"delta","removed":2}`,
		},
		{
			name: "cluster zero values still emit non-omitted fields",
			ev:   Event{Type: "delta", Cluster: &ClusterState{}},
			want: `{"type":"delta","cluster":{"clusterId":0,"generation":0,"alive":false,"lastBeat":0}}`,
		},
		{
			name: "no payload at all",
			ev:   Event{Type: "delta"},
			want: `{"type":"delta"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(marshal(tc.ev)); got != tc.want {
				t.Fatalf("\n got %s\nwant %s", got, tc.want)
			}
		})
	}
}

// The status payload is an opaque raw JSON value the cluster supplies. Whatever
// shape it has, it must come out intact (compacted, HTML-escaped) and a
// present-but-empty object/array must not be dropped.
func TestWireGoldenOpaqueStatus(t *testing.T) {
	cases := []struct {
		name   string
		status string
		want   string
	}{
		{"empty object kept", `{}`, `"status":{}`},
		{"empty array kept", `[]`, `"status":[]`},
		{"null kept", `null`, `"status":null`},
		{"scalar kept", `0`, `"status":0`},
		{"whitespace compacted", ` { "a" : [ 1 , 2 ] , "b" : { } } `, `"status":{"a":[1,2],"b":{}}`},
		{"html chars escaped", `{"m":"<b>&</b>"}`, `"status":{"m":"\u003cb\u003e\u0026\u003c/b\u003e"}`},
		{"line separators escaped", "{\"m\":\"a\u2028b\u2029c\"}", `"status":{"m":"a\u2028b\u2029c"}`},
		{"already escaped html stays escaped", `{"m":"\u003cx\u003e"}`, `"status":{"m":"\u003cx\u003e"}`},
		{"exponent number untouched", `{"n":1E2,"f":1.50}`, `"status":{"n":1E2,"f":1.50}`},
		{"unicode kept literal", `{"m":"é€😀"}`, `"status":{"m":"é€😀"}`},
		{"escaped unicode preserved", `{"m":"\u00e9\ud83d\ude00"}`, `"status":{"m":"\u00e9\ud83d\ude00"}`},
		{"short escapes preserved", `{"m":"a\n\t\"\\\/"}`, `"status":{"m":"a\n\t\"\\\/"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(marshal(Event{Type: "delta", Cluster: &ClusterState{ClusterID: 1, Status: []byte(tc.status)}}))
			want := `{"type":"delta","cluster":{"clusterId":1,"generation":0,"alive":false,"lastBeat":0,` + tc.want + `}}`
			if got != want {
				t.Fatalf("\n got %s\nwant %s", got, want)
			}
		})
	}
}
