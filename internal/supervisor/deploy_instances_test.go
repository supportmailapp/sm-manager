package supervisor

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"sm-manager/internal/config"
	"sm-manager/internal/server"
)

// fakeFleet stands in for the hub's instance registry. It records each restart
// together with the generation the supervisor had at that moment.
type fakeFleet struct {
	s *Supervisor

	mu        sync.Mutex
	running   []string         // returned by RunningInstanceIDs
	gone      map[string]bool  // ids that are no longer running when their turn comes
	failOn    map[string]error // ids whose restart fails
	restarted []string         // ids actually restarted, in order
	attempts  []string         // every RestartRunningInstance call, in order
	gens      []int            // supervisor generation seen at each restart
	afterEach func(id string)  // optional hook run after each attempt
}

func (f *fakeFleet) RunningInstanceIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.running...)
}

func (f *fakeFleet) RestartRunningInstance(id string) (bool, error) {
	f.mu.Lock()
	f.attempts = append(f.attempts, id)
	hook := f.afterEach
	err := f.failOn[id]
	gone := f.gone[id]
	f.mu.Unlock()

	if hook != nil {
		defer hook(id)
	}
	if err != nil {
		return false, err
	}
	if gone {
		return false, nil
	}
	f.s.mu.Lock()
	gen := f.s.generation
	f.s.mu.Unlock()

	f.mu.Lock()
	f.restarted = append(f.restarted, id)
	f.gens = append(f.gens, gen)
	f.mu.Unlock()
	return true, nil
}

func (f *fakeFleet) snapshotRestarted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.restarted...)
}

func (f *fakeFleet) snapshotAttempts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.attempts...)
}

// newDeploySup builds a Supervisor ready for Deploy: a cluster command that
// crash-exits (so a halted roll has something to tear down), a given cluster
// count, starting generation 3, and a fake instance fleet. The supervisor is
// marked running without Run, so no old-generation clusters are spawned.
func newDeploySup(t *testing.T, clusterCount int, fleet *fakeFleet) *Supervisor {
	t.Helper()
	t.Setenv("SM_HELPER", "exit1")
	cfg := &config.Config{
		SocketPath:       "/tmp/sm-test.sock",
		ClusterCount:     clusterCount,
		ShardsPerCluster: 1,
		Generation:       3,
		SpawnCommand:     []string{os.Args[0], "-test.run=TestHelperProcess"},
	}
	s := New(cfg, server.NewHub(cfg))
	fleet.s = s
	s.fleet = fleet

	runCtx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.runCtx = runCtx
	s.mu.Unlock()
	t.Cleanup(func() {
		cancel()
		s.wg.Wait()
	})
	return s
}

func TestDeployRestartsRunningInstancesUnderNewGeneration(t *testing.T) {
	fleet := &fakeFleet{running: []string{"wl_a", "wl_b", "wl_c"}}
	s := newDeploySup(t, 0, fleet)
	logs := captureLog(t)

	gen, err := s.Deploy(context.Background())

	if err != nil || gen != 4 {
		t.Fatalf("Deploy = %d, %v; want 4, nil", gen, err)
	}
	if got := fleet.snapshotRestarted(); !reflect.DeepEqual(got, []string{"wl_a", "wl_b", "wl_c"}) {
		t.Fatalf("restarted = %v, want [wl_a wl_b wl_c] in order", got)
	}
	for i, g := range fleet.gens {
		if g != 4 {
			t.Fatalf("instance %d restarted under generation %d, want 4", i, g)
		}
	}
	if out := logs.String(); !strings.Contains(out, "instances restarted=3 failed=0") {
		t.Fatalf("completion log lacks the counts:\n%s", out)
	}
}

func TestDeployDoesNotStartInstancesThatAreNoLongerRunning(t *testing.T) {
	// wl_b was in the snapshot but was stopped (or removed) before its turn.
	fleet := &fakeFleet{
		running: []string{"wl_a", "wl_b"},
		gone:    map[string]bool{"wl_b": true},
	}
	s := newDeploySup(t, 0, fleet)

	if _, err := s.Deploy(context.Background()); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	if got := fleet.snapshotRestarted(); !reflect.DeepEqual(got, []string{"wl_a"}) {
		t.Fatalf("restarted = %v, want [wl_a]", got)
	}
	s.mu.Lock()
	n := len(s.instances)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("supervisor spawned %d instance(s) on its own", n)
	}
}

func TestDeployWithNoInstancesStillSucceeds(t *testing.T) {
	fleet := &fakeFleet{}
	s := newDeploySup(t, 0, fleet)

	if gen, err := s.Deploy(context.Background()); err != nil || gen != 4 {
		t.Fatalf("Deploy = %d, %v; want 4, nil", gen, err)
	}
	if got := fleet.snapshotAttempts(); len(got) != 0 {
		t.Fatalf("attempts = %v, want none", got)
	}
}

func TestHaltedDeployRestartsNoInstances(t *testing.T) {
	fleet := &fakeFleet{running: []string{"wl_a", "wl_b"}}
	s := newDeploySup(t, 1, fleet)
	// The new cluster never registers with the hub, so readiness times out.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := s.Deploy(ctx)

	if err == nil || !strings.Contains(err.Error(), "deploy halted") {
		t.Fatalf("Deploy err = %v, want a halted error", err)
	}
	if got := fleet.snapshotAttempts(); len(got) != 0 {
		t.Fatalf("instances restart attempts after a halted roll = %v, want none", got)
	}
}

func TestOneFailedInstanceRestartDoesNotStopTheRestOrFailDeploy(t *testing.T) {
	fleet := &fakeFleet{
		running: []string{"wl_a", "wl_b", "wl_c"},
		failOn:  map[string]error{"wl_b": errors.New("start refused")},
	}
	s := newDeploySup(t, 0, fleet)
	logs := captureLog(t)

	gen, err := s.Deploy(context.Background())

	if err != nil || gen != 4 {
		t.Fatalf("Deploy = %d, %v; want 4, nil", gen, err)
	}
	if got := fleet.snapshotRestarted(); !reflect.DeepEqual(got, []string{"wl_a", "wl_c"}) {
		t.Fatalf("restarted = %v, want [wl_a wl_c]", got)
	}
	out := logs.String()
	if !strings.Contains(out, "wl_b") || !strings.Contains(out, "start refused") {
		t.Fatalf("failure not logged with id and cause:\n%s", out)
	}
	if !strings.Contains(out, "instances restarted=2 failed=1") {
		t.Fatalf("completion log lacks the counts:\n%s", out)
	}
}

func TestDeployStopsRestartingInstancesWhenContextIsCancelled(t *testing.T) {
	fleet := &fakeFleet{running: []string{"wl_a", "wl_b", "wl_c"}}
	s := newDeploySup(t, 0, fleet)
	logs := captureLog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fleet.afterEach = func(id string) {
		if id == "wl_a" {
			cancel()
		}
	}

	gen, err := s.Deploy(ctx)

	if err != nil || gen != 4 {
		t.Fatalf("Deploy = %d, %v; want 4, nil (the cluster roll itself succeeded)", gen, err)
	}
	if got := fleet.snapshotAttempts(); !reflect.DeepEqual(got, []string{"wl_a"}) {
		t.Fatalf("attempts = %v, want only [wl_a]", got)
	}
	if out := logs.String(); !strings.Contains(out, "instances restarted=1 failed=0 skipped=2") {
		t.Fatalf("completion log lacks the skipped count:\n%s", out)
	}
}

func TestDeployWithoutFleetStillWorks(t *testing.T) {
	// A supervisor built without a hub (as the instance tests do) has no fleet.
	cfg := &config.Config{SpawnCommand: []string{"x"}}
	s := New(cfg, nil)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = runCtx

	if gen, err := s.Deploy(context.Background()); err != nil || gen != 1 {
		t.Fatalf("Deploy = %d, %v; want 1, nil", gen, err)
	}
}
