package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"sm-manager/internal/status"
)

// defaultInstanceStopGrace is used when cfg.InstanceStopGraceSecs is not
// positive, so a stopping instance can never be waited on forever.
const defaultInstanceStopGrace = 12 * time.Second

// InstanceStateSink receives instance process lifecycle changes. *server.Hub
// satisfies it; a nil sink is allowed and drops the reports.
type InstanceStateSink interface {
	SetInstanceState(instanceID string, state status.InstanceRunState, exitCode int)
}

// instanceRun is one running instance goroutine.
type instanceRun struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when the goroutine has exited
}

// SetInstanceStateSink wires in the receiver of instance lifecycle reports.
func (s *Supervisor) SetInstanceStateSink(sink InstanceStateSink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instanceSink = sink
}

func (s *Supervisor) reportInstance(instanceID string, state status.InstanceRunState, exitCode int) {
	s.mu.Lock()
	sink := s.instanceSink
	s.mu.Unlock()
	if sink != nil {
		sink.SetInstanceState(instanceID, state, exitCode)
	}
}

// StartInstance spawns and supervises the process for instanceID. It is
// idempotent: an instance that is already supervised is left alone. It
// implements server.InstanceRunner.
func (s *Supervisor) StartInstance(instanceID string) error {
	if len(s.cfg.InstanceSpawnCommand) == 0 {
		return errors.New("no instanceSpawnCommand configured")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runCtx == nil {
		return errors.New("supervisor not running")
	}
	if s.runCtx.Err() != nil {
		return errors.New("supervisor is shutting down")
	}
	if _, ok := s.instances[instanceID]; ok {
		return nil
	}

	ctx, cancel := context.WithCancel(s.runCtx)
	run := &instanceRun{cancel: cancel, done: make(chan struct{})}
	s.instances[instanceID] = run

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(run.done)
		// release drops the slot if it is still this run's. The deferred call covers
		// every way out of runInstance except the failed exit, which uses
		// releaseAndFail (see runInstance).
		release := func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.instances[instanceID] == run {
				delete(s.instances, instanceID)
			}
		}
		defer release()
		// Frees the slot and reports failed in one critical section. A START (which
		// needs s.mu) can therefore neither slip in between the two and have its
		// "starting" overwritten by this stale "failed", nor run before the slot is
		// free. The sink is called under s.mu: it only takes the hub's own locks and
		// never calls back into the supervisor.
		releaseAndFail := func(exitCode int) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.instances[instanceID] == run {
				delete(s.instances, instanceID)
			}
			if s.instanceSink != nil {
				s.instanceSink.SetInstanceState(instanceID, status.InstanceFailed, exitCode)
			}
		}
		s.runInstance(ctx, instanceID, releaseAndFail)
	}()
	return nil
}

// StopInstance stops the instance process (SIGTERM, then a kill after the
// grace period) and blocks until it is gone. Idempotent; unknown ids and
// instances whose loop already ended are a no-op. It implements
// server.InstanceRunner.
func (s *Supervisor) StopInstance(instanceID string) {
	s.mu.Lock()
	run := s.instances[instanceID]
	s.mu.Unlock()
	if run == nil {
		return
	}
	run.cancel()
	<-run.done
}

// runInstance keeps one instance process alive until ctx is cancelled or the
// process exits with the "do not restart" code. Crashes restart with the same
// backoff as clusters. releaseAndFail frees the supervisor's slot for the
// instance and reports it failed, atomically under s.mu. The slot must be free
// when the report is made, because a START reacting to that report while the
// slot is still taken would be a no-op and leave the hub showing "starting"
// with no process behind it; and no START may land between the release and the
// report, or the report would overwrite that START's "starting". A stop-driven
// exit needs no such care: the hub holds the instance's opMu and waits for this
// goroutine, so no START can overlap.
func (s *Supervisor) runInstance(ctx context.Context, instanceID string, releaseAndFail func(exitCode int)) {
	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			s.reportInstance(instanceID, status.InstanceStopped, 0)
			return
		}

		s.reportInstance(instanceID, status.InstanceStarting, 0)
		start := time.Now()
		err := s.spawnInstanceOnce(ctx, instanceID)
		ranFor := time.Since(start)

		if ctx.Err() != nil {
			// Stop or shutdown, not a crash.
			s.reportInstance(instanceID, status.InstanceStopped, 0)
			return
		}

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == s.cfg.InstanceNoRestartExitCode {
			log.Printf("instance %s exited with code %d, not restarting", instanceID, exitErr.ExitCode())
			releaseAndFail(exitErr.ExitCode())
			return
		}

		if ranFor >= healthyRuntime {
			backoff = backoffInitial
		}
		if err != nil {
			log.Printf("instance %s exited after %s: %v, restarting in %s",
				instanceID, ranFor.Round(time.Millisecond), err, backoff)
		} else {
			log.Printf("instance %s exited cleanly after %s, restarting in %s",
				instanceID, ranFor.Round(time.Millisecond), backoff)
		}

		select {
		case <-ctx.Done():
			s.reportInstance(instanceID, status.InstanceStopped, 0)
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// spawnInstanceOnce runs the instance process once and blocks until it exits.
func (s *Supervisor) spawnInstanceOnce(ctx context.Context, instanceID string) error {
	name := s.cfg.InstanceSpawnCommand[0]
	args := s.cfg.InstanceSpawnCommand[1:]

	s.mu.Lock()
	gen := s.generation
	s.mu.Unlock()

	grace := time.Duration(s.cfg.InstanceStopGraceSecs) * time.Second
	if grace <= 0 {
		grace = defaultInstanceStopGrace
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = s.cfg.SpawnDir
	cmd.Env = append(os.Environ(), s.instanceEnv(instanceID, gen)...)
	cmd.Stdout = newPrefixLogWriter(fmt.Sprintf("[instance %s out] ", instanceID))
	cmd.Stderr = newPrefixLogWriter(fmt.Sprintf("[instance %s err] ", instanceID))
	// Ask the process to shut down; fall back to a hard kill where signals are
	// unsupported (Windows). WaitDelay kills it if it outlives the grace period.
	cmd.Cancel = func() error {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = grace

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	log.Printf("instance %s spawned (pid %d, generation %d)", instanceID, cmd.Process.Pid, gen)

	return cmd.Wait()
}

// instanceEnv builds the env contract an instance process reads. Unlike a
// cluster it has no cluster id or shard list.
func (s *Supervisor) instanceEnv(instanceID string, gen int) []string {
	return []string{
		"INSTANCE_ID=" + instanceID,
		"SM_MANAGER_SOCKET=" + s.cfg.SocketPath,
		"GENERATION=" + strconv.Itoa(gen),
	}
}
