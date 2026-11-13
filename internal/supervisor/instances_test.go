package supervisor

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"sm-manager/internal/config"
	"sm-manager/internal/status"
)

// TestHelperProcess is not a real test: the instance tests re-execute the test
// binary with -test.run=TestHelperProcess and SM_HELPER set, and this plays the
// role of the instance child process.
func TestHelperProcess(t *testing.T) {
	mode := os.Getenv("SM_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "exit78":
		os.Exit(78)
	case "exit1":
		os.Exit(1)
	case "env":
		for _, k := range []string{"INSTANCE_ID", "GENERATION", "SM_MANAGER_SOCKET", "CLUSTER_ID", "SHARD_LIST", "TOTAL_SHARDS"} {
			if v, ok := os.LookupEnv(k); ok {
				fmt.Printf("ENV %s=%s\n", k, v)
			}
		}
		os.Exit(78)
	case "sleep":
		fmt.Println("ready")
		time.Sleep(60 * time.Second)
		os.Exit(0)
	case "trapterm":
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		fmt.Println("ready")
		<-ch
		os.Exit(0)
	case "ignoreterm":
		signal.Ignore(syscall.SIGTERM)
		fmt.Println("ready")
		time.Sleep(60 * time.Second)
		os.Exit(0)
	}
	os.Exit(2)
}

type stateRec struct {
	id    string
	state status.InstanceRunState
	code  int
}

type recorder struct {
	mu   sync.Mutex
	recs []stateRec
}

func (r *recorder) SetInstanceState(id string, state status.InstanceRunState, code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, stateRec{id, state, code})
}

func (r *recorder) snapshot() []stateRec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]stateRec(nil), r.recs...)
}

func (r *recorder) count(state status.InstanceRunState) int {
	n := 0
	for _, rec := range r.snapshot() {
		if rec.state == state {
			n++
		}
	}
	return n
}

// syncBuf is a goroutine-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// captureLog redirects the std logger into a buffer for the test's lifetime.
func captureLog(t *testing.T) *syncBuf {
	t.Helper()
	buf := &syncBuf{}
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return buf
}

// newInstanceSup builds a Supervisor with no cluster command and an instance
// command that re-executes this test binary in the given helper mode. Run is
// started in the background and stopped (and awaited) on cleanup.
func newInstanceSup(t *testing.T, helper string, graceSecs int) (*Supervisor, *recorder) {
	t.Helper()
	t.Setenv("SM_HELPER", helper)
	cfg := &config.Config{
		SocketPath:                "/tmp/sm-test.sock",
		InstanceSpawnCommand:      []string{os.Args[0], "-test.run=TestHelperProcess"},
		InstanceNoRestartExitCode: 78,
		InstanceStopGraceSecs:     graceSecs,
	}
	s := New(cfg, nil)
	rec := &recorder{}
	s.SetInstanceStateSink(rec)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	waitFor(t, "runCtx set", 5*time.Second, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.runCtx != nil
	})
	return s, rec
}

func TestInstanceNoRestartExitCode(t *testing.T) {
	s, rec := newInstanceSup(t, "exit78", 5)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "failed state", 10*time.Second, func() bool { return rec.count(status.InstanceFailed) == 1 })

	time.Sleep(1500 * time.Millisecond)
	got := rec.snapshot()
	want := []stateRec{
		{"wl_a", status.InstanceStarting, 0},
		{"wl_a", status.InstanceFailed, 78},
	}
	if len(got) != len(want) {
		t.Fatalf("states = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("states = %v, want %v", got, want)
		}
	}
}

func TestInstanceCrashRestarts(t *testing.T) {
	s, rec := newInstanceSup(t, "exit1", 5)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "second starting after backoff", 15*time.Second, func() bool { return rec.count(status.InstanceStarting) >= 2 })
	if n := rec.count(status.InstanceFailed); n != 0 {
		t.Fatalf("crash must not report failed, got %d", n)
	}
}

func TestInstanceEnvContract(t *testing.T) {
	buf := captureLog(t)
	s, rec := newInstanceSup(t, "env", 5)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "failed state", 10*time.Second, func() bool { return rec.count(status.InstanceFailed) == 1 })
	waitFor(t, "env output", 5*time.Second, func() bool {
		return strings.Contains(buf.String(), "ENV GENERATION=")
	})

	out := buf.String()
	for _, want := range []string{"INSTANCE_ID=wl_a", "GENERATION=", "SM_MANAGER_SOCKET=/tmp/sm-test.sock"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	for _, bad := range []string{"CLUSTER_ID=", "SHARD_LIST=", "TOTAL_SHARDS="} {
		if strings.Contains(out, bad) {
			t.Errorf("output must not contain %q:\n%s", bad, out)
		}
	}
	if !strings.Contains(out, "[instance wl_a out] ") {
		t.Errorf("output lacks instance log prefix:\n%s", out)
	}
}

func TestStopInstanceSendsSigterm(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM is not deliverable on Windows")
	}
	buf := captureLog(t)
	s, rec := newInstanceSup(t, "trapterm", 20)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "child ready", 10*time.Second, func() bool { return strings.Contains(buf.String(), "[instance wl_a out] ready") })

	start := time.Now()
	s.StopInstance("wl_a")
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("StopInstance took %s, want well under the 20s grace", elapsed)
	}
	snap := rec.snapshot()
	if last := snap[len(snap)-1]; last.state != status.InstanceStopped || last.code != 0 {
		t.Fatalf("final state = %v, want stopped", last)
	}
	if rec.count(status.InstanceFailed) != 0 {
		t.Fatalf("stop must not report failed: %v", snap)
	}
}

func TestStopInstanceKillsAfterGrace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM is not deliverable on Windows")
	}
	buf := captureLog(t)
	s, rec := newInstanceSup(t, "ignoreterm", 1)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "child ready", 10*time.Second, func() bool { return strings.Contains(buf.String(), "[instance wl_a out] ready") })

	start := time.Now()
	s.StopInstance("wl_a")
	elapsed := time.Since(start)
	if elapsed < 800*time.Millisecond {
		t.Fatalf("StopInstance returned after %s, before the 1s grace", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("StopInstance took %s, want about 1s", elapsed)
	}
	snap := rec.snapshot()
	if last := snap[len(snap)-1]; last.state != status.InstanceStopped {
		t.Fatalf("final state = %v, want stopped", last)
	}
}

func TestStartInstanceWithoutCommand(t *testing.T) {
	s := New(&config.Config{}, nil)
	rec := &recorder{}
	s.SetInstanceStateSink(rec)
	if err := s.StartInstance("wl_a"); err == nil {
		t.Fatal("expected error without instanceSpawnCommand")
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("no state may be recorded, got %v", got)
	}
}

func TestInstanceNilSinkIsSafe(t *testing.T) {
	s, rec := newInstanceSup(t, "exit78", 5)
	s.SetInstanceStateSink(nil)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("detached sink must not be called, got %v", got)
	}
	s.StopInstance("wl_a") // must not panic, idempotent on a finished instance
	s.StopInstance("wl_unknown")
}

// slotProbe records whether the supervisor still held the instance's slot when the
// failed state was reported.
type slotProbe struct {
	s      *Supervisor
	mu     sync.Mutex
	failed bool
	held   bool
}

func (p *slotProbe) SetInstanceState(id string, state status.InstanceRunState, code int) {
	if state != status.InstanceFailed {
		return
	}
	p.s.mu.Lock()
	_, held := p.s.instances[id]
	p.s.mu.Unlock()
	p.mu.Lock()
	p.failed, p.held = true, held
	p.mu.Unlock()
}

func TestInstanceSlotIsFreeWhenFailedIsReported(t *testing.T) {
	s, _ := newInstanceSup(t, "exit78", 5)
	probe := &slotProbe{s: s}
	s.SetInstanceStateSink(probe)
	if err := s.StartInstance("wl_a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "failed report", 10*time.Second, func() bool {
		probe.mu.Lock()
		defer probe.mu.Unlock()
		return probe.failed
	})
	probe.mu.Lock()
	held := probe.held
	probe.mu.Unlock()
	if held {
		t.Fatal("slot still taken at the failed report: a START reacting to it would be a no-op")
	}
}
