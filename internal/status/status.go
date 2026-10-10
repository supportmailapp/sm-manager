// Package status hosts the public, internet-facing statuspage WebSocket —
// separate from the manager's internal Unix socket. It broadcasts cluster
// health/status deltas to any number of subscribed statuspage clients.
//
// It is a leaf package: it never imports the server/hub. The hub pushes state
// changes in one direction via the Sink interface; the Broadcaster keeps its
// own last-known snapshot so a newly connected client can be brought up to date
// without calling back into the hub. This keeps the dependency edge one-way
// (server -> status) and cycle-free.
package status

import (
	"encoding/json"
	"sync"
)

// ClusterState is the manager's public view of one cluster. Status is the
// opaque per-cluster payload the cluster attaches to its HEARTBEAT (shard
// pings, guild/member counts, …) — the manager forwards it verbatim and never
// parses it, keeping the manager bot-agnostic.
type ClusterState struct {
	ClusterID  int             `json:"clusterId"`
	Shards     []int           `json:"shards,omitempty"`
	Generation int             `json:"generation"`
	Alive      bool            `json:"alive"`
	LastBeat   int64           `json:"lastBeat"` // unix ms; 0 if never seen
	Status     json.RawMessage `json:"status,omitempty"`
}

// Event is one message on the wire. "snapshot" carries the full active fleet
// (sent once, on connect); "delta" carries a single cluster that changed, or a
// Removed clusterId that went away.
type Event struct {
	Type     string         `json:"type"`
	Clusters []ClusterState `json:"clusters,omitempty"`
	Cluster  *ClusterState  `json:"cluster,omitempty"`
	Removed  *int           `json:"removed,omitempty"`
}

// Sink is the one-way channel the hub uses to push state changes into the
// broadcaster. Implemented by *Broadcaster. Kept as an interface so the hub can
// depend on the behaviour, not the concrete type, and so a nil sink cleanly
// means "status WS disabled".
type Sink interface {
	Publish(ClusterState)
	Remove(clusterID int)
}

// Broadcaster fans state deltas out to every connected statuspage client and
// remembers the current active-fleet snapshot for late joiners.
type Broadcaster struct {
	mu      sync.Mutex
	state   map[int]ClusterState // last-known active view, keyed by clusterId
	clients map[*client]struct{}
}

// client is one subscribed WS peer. send is buffered; a client that can't keep
// up is dropped rather than blocking the hub.
type client struct {
	send   chan []byte
	closed bool
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		state:   make(map[int]ClusterState),
		clients: make(map[*client]struct{}),
	}
}

// Publish records a cluster's new state and fans a delta out to all clients.
// Called by the hub whenever an active cluster registers, heartbeats, or is
// promoted into the routing table.
func (b *Broadcaster) Publish(cs ClusterState) {
	msg := marshal(Event{Type: "delta", Cluster: &cs})
	b.mu.Lock()
	b.state[cs.ClusterID] = cs
	b.fanoutLocked(msg)
	b.mu.Unlock()
}

// Remove drops a cluster from the active view and tells clients it's gone.
// Called by the hub on disconnect or heartbeat-reap of an active cluster.
func (b *Broadcaster) Remove(clusterID int) {
	cid := clusterID
	msg := marshal(Event{Type: "delta", Removed: &cid})
	b.mu.Lock()
	delete(b.state, clusterID)
	b.fanoutLocked(msg)
	b.mu.Unlock()
}

// fanoutLocked non-blockingly delivers msg to every client. A client whose
// buffer is full is assumed dead/slow and dropped — the status feed is
// best-effort and must never back-pressure the hub. Caller holds b.mu.
func (b *Broadcaster) fanoutLocked(msg []byte) {
	for c := range b.clients {
		select {
		case c.send <- msg:
		default:
			b.dropLocked(c)
		}
	}
}

// addClient registers c and immediately queues the current snapshot as its
// first message, atomically under the lock so no delta can slip ahead of it.
func (b *Broadcaster) addClient(c *client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[c] = struct{}{}
	snap := Event{Type: "snapshot", Clusters: make([]ClusterState, 0, len(b.state))}
	for _, cs := range b.state {
		snap.Clusters = append(snap.Clusters, cs)
	}
	// First message: guaranteed to fit — the channel is empty and buffered.
	c.send <- marshal(snap)
}

func (b *Broadcaster) removeClient(c *client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dropLocked(c)
}

// dropLocked removes c and closes its send channel once. Caller holds b.mu.
func (b *Broadcaster) dropLocked(c *client) {
	if _, ok := b.clients[c]; !ok {
		return
	}
	delete(b.clients, c)
	if !c.closed {
		c.closed = true
		close(c.send)
	}
}

func marshal(e Event) []byte {
	b, err := json.Marshal(e)
	if err != nil {
		// Event is all serializable types; a marshal error is a programmer bug.
		return []byte(`{"type":"error"}`)
	}
	return b
}
