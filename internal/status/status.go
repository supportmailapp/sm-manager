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
	"encoding/json/jsontext"
	"encoding/json/v2"
	"sort"
	"sync"

	"sm-manager/internal/wirejson"
)

// ClusterState is the manager's public view of one cluster. Status is the
// opaque per-cluster payload the cluster attaches to its HEARTBEAT (shard
// pings, guild/member counts, …) — the manager forwards it verbatim and never
// parses it, keeping the manager bot-agnostic.
type ClusterState struct {
	ClusterID  int            `json:"clusterId"`
	Shards     []int          `json:"shards,omitempty"`
	Generation int            `json:"generation"`
	Alive      bool           `json:"alive"`
	LastBeat   int64          `json:"lastBeat"` // unix ms; 0 if never seen
	Status     jsontext.Value `json:"status,omitzero"`
}

// Event is one message on the wire. "snapshot" carries the full active fleet
// (sent once, on connect); "delta" carries a single cluster that changed, or a
// Removed clusterId that went away. Instances ride in their own fields so a
// client that predates them sees the same bytes as before: a snapshot without
// instances has no "instances" key, and instance deltas use "instance" and
// "removedInstance", never "cluster" or "removed".
type Event struct {
	Type            string          `json:"type"`
	Clusters        []ClusterState  `json:"clusters,omitempty"`
	Instances       []InstanceState `json:"instances,omitempty"`
	Cluster         *ClusterState   `json:"cluster,omitzero"`
	Removed         *int            `json:"removed,omitzero"`
	Instance        *InstanceState  `json:"instance,omitzero"`
	RemovedInstance *string         `json:"removedInstance,omitzero"`
}

// Sink is the one-way channel the hub uses to push state changes into the
// broadcaster. Implemented by *Broadcaster. Kept as an interface so the hub can
// depend on the behaviour, not the concrete type, and so a nil sink cleanly
// means "status WS disabled".
type Sink interface {
	Publish(ClusterState)
	Remove(clusterID int)
	PublishInstance(InstanceState)
	RemoveInstance(instanceID string)
}

// Broadcaster fans state deltas out to every connected statuspage client and
// remembers the current active-fleet snapshot for late joiners.
type Broadcaster struct {
	mu      sync.Mutex
	state   map[int]ClusterState     // last-known active view, keyed by clusterId
	insts   map[string]InstanceState // last-known instance view, keyed by instanceId
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
		insts:   make(map[string]InstanceState),
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

// PublishInstance records an instance's new state and fans a delta out to all
// clients. Called by the hub on every instance state change.
func (b *Broadcaster) PublishInstance(is InstanceState) {
	msg := marshal(Event{Type: "delta", Instance: &is})
	b.mu.Lock()
	b.insts[is.InstanceID] = is
	b.fanoutLocked(msg)
	b.mu.Unlock()
}

// RemoveInstance drops an instance from the view and tells clients it's gone.
// Called by the hub when an instance entry is removed.
func (b *Broadcaster) RemoveInstance(instanceID string) {
	id := instanceID
	msg := marshal(Event{Type: "delta", RemovedInstance: &id})
	b.mu.Lock()
	delete(b.insts, instanceID)
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
	if len(b.insts) > 0 {
		snap.Instances = make([]InstanceState, 0, len(b.insts))
		for _, is := range b.insts {
			snap.Instances = append(snap.Instances, is)
		}
		sort.Slice(snap.Instances, func(i, j int) bool { return snap.Instances[i].InstanceID < snap.Instances[j].InstanceID })
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
	b, err := json.Marshal(e, wirejson.Wire)
	if err != nil {
		// Event is all serializable types; a marshal error is a programmer bug.
		return []byte(`{"type":"error"}`)
	}
	return b
}

// InstanceRunState is the lifecycle state of one managed instance process.
type InstanceRunState string

const (
	InstanceStopped  InstanceRunState = "stopped"
	InstanceStarting InstanceRunState = "starting"
	InstanceRunning  InstanceRunState = "running"
	InstanceFailed   InstanceRunState = "failed"
)

// InstanceState is the manager's public view of one instance. Status is the
// opaque payload the instance attaches to its HEARTBEAT, forwarded verbatim.
// It carries no guild ids: the public feed must not list what an instance owns.
type InstanceState struct {
	InstanceID string           `json:"instanceId"`
	State      InstanceRunState `json:"state"`
	Generation int              `json:"generation"`
	Alive      bool             `json:"alive"`
	LastBeat   int64            `json:"lastBeat"` // unix ms; 0 if never seen
	ExitCode   int              `json:"exitCode,omitzero"`
	Status     jsontext.Value   `json:"status,omitzero"`
}
