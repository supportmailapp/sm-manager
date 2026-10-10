// Package supervisor spawns and restarts the cluster processes. It knows the
// env contract the bot's cluster.ts expects (CLUSTER_ID, SHARD_LIST,
// TOTAL_SHARDS, SM_MANAGER_SOCKET, GENERATION) and nothing about business ops.
package supervisor

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/getsentry/sentry-go/attribute"

	"sm-manager/internal/config"
	"sm-manager/internal/server"
	"sm-manager/internal/telemetry"
)

const (
	// backoffInitial is the first restart delay after a crash.
	backoffInitial = 1 * time.Second
	// backoffMax caps the exponential restart delay.
	backoffMax = 30 * time.Second
	// healthyRuntime is how long a process must stay up before its crash
	// backoff resets — a process that survives this long is treated as a fresh
	// start, not part of a crash loop.
	healthyRuntime = 60 * time.Second
)

// clusterKey identifies one running cluster goroutine — a (generation,
// clusterID) pair, since a rollout can have the same clusterID running under
// two generations at once mid-cutover.
type clusterKey struct {
	gen int
	cid int
}

// Supervisor owns the cluster child processes across every generation that
// has ever existed. cfg.Generation only seeds the starting value — once Run
// is going, generation is live runtime state bumped by Deploy, not config.
type Supervisor struct {
	cfg *config.Config
	hub *server.Hub

	mu             sync.Mutex
	generation     int
	runCtx         context.Context
	clusterCancels map[clusterKey]context.CancelFunc // per-running-cluster cancel func

	deployMu sync.Mutex // serializes concurrent Deploy calls
	wg       sync.WaitGroup
}

func New(cfg *config.Config, hub *server.Hub) *Supervisor {
	return &Supervisor{
		cfg:            cfg,
		hub:            hub,
		generation:     cfg.Generation,
		clusterCancels: make(map[clusterKey]context.CancelFunc),
	}
}

// Run spawns cfg.ClusterCount clusters under the starting generation and
// keeps them alive until ctx is cancelled, restarting crashed ones with
// exponential backoff. It blocks until every cluster goroutine — across every
// generation Deploy has ever spawned — has exited.
func (s *Supervisor) Run(ctx context.Context) {
	if len(s.cfg.SpawnCommand) == 0 {
		log.Printf("supervisor: no spawnCommand configured — not spawning any clusters")
		return
	}

	s.mu.Lock()
	s.runCtx = ctx
	gen := s.generation
	s.mu.Unlock()

	s.spawnClusters(gen, clusterIDs(s.cfg.ClusterCount))
	s.wg.Wait()
	log.Printf("supervisor: all clusters stopped")
}

// spawnClusters starts one cluster goroutine per clusterID in cids, tagged
// with gen, each under its own context derived from runCtx so a global
// shutdown cancels them too and a later Deploy can retire any subset
// independently. Idempotent-safe only for cids not already running under gen.
func (s *Supervisor) spawnClusters(gen int, cids []int) {
	s.mu.Lock()
	runCtx := s.runCtx
	s.mu.Unlock()

	for _, cid := range cids {
		ctx, cancel := context.WithCancel(runCtx)
		s.mu.Lock()
		s.clusterCancels[clusterKey{gen: gen, cid: cid}] = cancel
		s.mu.Unlock()

		s.wg.Add(1)
		go func(clusterID int) {
			defer s.wg.Done()
			s.runCluster(ctx, clusterID, gen)
		}(cid)
	}
}

// cancelClusters stops the given clusters (in generation gen) for good — their
// runCluster loops see ctx.Err() != nil and return instead of restarting.
func (s *Supervisor) cancelClusters(gen int, cids []int) {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(cids))
	for _, cid := range cids {
		k := clusterKey{gen: gen, cid: cid}
		if cancel, ok := s.clusterCancels[k]; ok {
			cancels = append(cancels, cancel)
			delete(s.clusterCancels, k)
		}
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// Deploy bumps the live generation and rolls the fleet over to it
// cluster-by-cluster, in chunks of cfg.DeployBatchSize. For each batch it
// spawns the new generation's clusters for those ids, waits for them to
// register and go healthy, flips routing to them, then quiesces and retires
// the old generation's clusters for the same ids — so only one batch has a
// quiesce gap at a time. It implements server.Deployer, which is how an
// inbound DEPLOY op reaches here without the hub importing this package.
//
// If a batch never becomes ready, the roll halts canary-style: the broken new
// batch is torn down and Deploy returns an error. Nothing is reversed — the
// failed batch was never promoted, so its old generation still owns routing and
// keeps serving; already-cut-over batches stay on the new generation. Every
// clusterID is still served by exactly one healthy generation. There is no
// rollback by design (reversing a cut-over cluster only churns its gateway
// connection); recovery is forward-only — re-run Deploy. The error names where
// it stopped.
func (s *Supervisor) Deploy(ctx context.Context) (int, error) {
	if len(s.cfg.SpawnCommand) == 0 {
		return 0, fmt.Errorf("no spawnCommand configured — nothing to deploy")
	}

	s.deployMu.Lock()
	defer s.deployMu.Unlock()

	s.mu.Lock()
	if s.runCtx == nil {
		s.mu.Unlock()
		return 0, fmt.Errorf("supervisor not running")
	}
	oldGen := s.generation
	newGen := oldGen + 1
	s.generation = newGen
	s.mu.Unlock()

	batches := batchClusterIDs(s.cfg.ClusterCount, s.cfg.DeployBatchSize)
	log.Printf("deploy: rolling generation %d → %d in %d batch(es)", oldGen, newGen, len(batches))
	telemetry.Logger().Info().
		Int("old_generation", oldGen).
		Int("new_generation", newGen).
		Int("batches", len(batches)).
		Emit("deploy: rollout started")
	telemetry.Meter().Count("sm_manager.deploy_started", 1,
		sentry.WithAttributes(attribute.Int("new_generation", newGen)))

	for i, cids := range batches {
		log.Printf("deploy: batch %d/%d — spawning generation %d clusters %v", i+1, len(batches), newGen, cids)
		s.spawnClusters(newGen, cids)

		if err := s.hub.AwaitClusters(ctx, newGen, cids); err != nil {
			log.Printf("deploy: batch %d/%d clusters %v not ready, halting roll (old generation keeps serving): %v", i+1, len(batches), cids, err)
			telemetry.Logger().Error().
				Int("batch", i+1).
				Int("batches", len(batches)).
				Int("new_generation", newGen).
				Emitf("deploy: halted waiting for clusters %v: %v", cids, err)
			telemetry.Meter().Count("sm_manager.deploy_halted", 1,
				sentry.WithAttributes(attribute.Int("new_generation", newGen)))
			s.cancelClusters(newGen, cids)
			return newGen, fmt.Errorf("deploy halted at batch %d/%d (clusters %v): %w", i+1, len(batches), cids, err)
		}

		// New clusters are healthy: flip routing to them, then drain and retire
		// the old ones. Promote-before-quiesce means every clusterID routes to
		// exactly one generation throughout — no duplicate-routing window.
		s.hub.PromoteClusters(newGen, cids)
		if err := s.hub.QuiesceClusters(ctx, oldGen, cids); err != nil {
			log.Printf("deploy: batch %d/%d quiesce of old generation %d clusters %v incomplete, continuing: %v",
				i+1, len(batches), oldGen, cids, err)
		}
		s.cancelClusters(oldGen, cids)
		log.Printf("deploy: batch %d/%d cut over to generation %d", i+1, len(batches), newGen)
	}

	log.Printf("deploy: cutover to generation %d complete", newGen)
	telemetry.Logger().Info().Int("new_generation", newGen).Emit("deploy: cutover complete")
	telemetry.Meter().Count("sm_manager.deploy_completed", 1,
		sentry.WithAttributes(attribute.Int("new_generation", newGen)))
	return newGen, nil
}

// clusterIDs returns [0, count).
func clusterIDs(count int) []int {
	cids := make([]int, 0, count)
	for cid := 0; cid < count; cid++ {
		cids = append(cids, cid)
	}
	return cids
}

// batchClusterIDs splits [0, count) into contiguous chunks of up to batchSize.
// batchSize <= 0 or >= count collapses to a single whole-fleet batch.
func batchClusterIDs(count, batchSize int) [][]int {
	if batchSize <= 0 || batchSize > count {
		batchSize = count
	}
	if batchSize == 0 {
		return nil
	}
	var batches [][]int
	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}
		batch := make([]int, 0, end-start)
		for cid := start; cid < end; cid++ {
			batch = append(batch, cid)
		}
		batches = append(batches, batch)
	}
	return batches
}

// runCluster keeps one cluster process alive under generation gen, restarting
// it on crash, until ctx is cancelled (global shutdown or this generation
// being retired by Deploy).
func (s *Supervisor) runCluster(ctx context.Context, clusterID, gen int) {
	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return
		}

		start := time.Now()
		err := s.spawnOnce(ctx, clusterID, gen)
		ranFor := time.Since(start)

		if ctx.Err() != nil {
			// Shutdown/retirement, not a crash.
			return
		}

		if ranFor >= healthyRuntime {
			backoff = backoffInitial
		}

		if err != nil {
			log.Printf("cluster %d (generation %d) exited after %s: %v — restarting in %s",
				clusterID, gen, ranFor.Round(time.Millisecond), err, backoff)
			telemetry.Logger().Warn().
				Int("cluster_id", clusterID).
				Int("generation", gen).
				Int64("ran_for_ms", ranFor.Round(time.Millisecond).Milliseconds()).
				Int64("backoff_ms", backoff.Milliseconds()).
				Emitf("cluster %d exited: %v — restarting", clusterID, err)
		} else {
			log.Printf("cluster %d (generation %d) exited cleanly after %s — restarting in %s",
				clusterID, gen, ranFor.Round(time.Millisecond), backoff)
			telemetry.Logger().Info().
				Int("cluster_id", clusterID).
				Int("generation", gen).
				Int64("ran_for_ms", ranFor.Round(time.Millisecond).Milliseconds()).
				Int64("backoff_ms", backoff.Milliseconds()).
				Emit("cluster exited cleanly — restarting")
		}
		telemetry.Meter().Count("sm_manager.cluster_restart", 1,
			sentry.WithAttributes(
				attribute.Int("cluster_id", clusterID),
				attribute.Int("generation", gen),
				attribute.Bool("clean", err == nil),
			))

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// spawnOnce runs the cluster process once and blocks until it exits.
func (s *Supervisor) spawnOnce(ctx context.Context, clusterID, gen int) error {
	name := s.cfg.SpawnCommand[0]
	args := s.cfg.SpawnCommand[1:]

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = s.cfg.SpawnDir
	cmd.Env = append(os.Environ(), s.envFor(clusterID, gen)...)
	cmd.Stdout = newLogWriter(clusterID, "out")
	cmd.Stderr = newLogWriter(clusterID, "err")

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	log.Printf("cluster %d spawned (pid %d, shards %v of %d, generation %d)",
		clusterID, cmd.Process.Pid, s.cfg.ShardsFor(clusterID), s.cfg.TotalShards(), gen)

	return cmd.Wait()
}

// envFor builds the env contract the bot's cluster.ts reads.
func (s *Supervisor) envFor(clusterID, gen int) []string {
	shards := s.cfg.ShardsFor(clusterID)
	shardStrs := make([]string, len(shards))
	for i, sh := range shards {
		shardStrs[i] = strconv.Itoa(sh)
	}
	return []string{
		"CLUSTER_ID=" + strconv.Itoa(clusterID),
		"SHARD_LIST=" + strings.Join(shardStrs, ","),
		"TOTAL_SHARDS=" + strconv.Itoa(s.cfg.TotalShards()),
		"SM_MANAGER_SOCKET=" + s.cfg.SocketPath,
		"GENERATION=" + strconv.Itoa(gen),
	}
}

// logWriter prefixes each line of a child's stdout/stderr with its cluster id
// and stream name, so interleaved output stays attributable.
type logWriter struct {
	prefix string
	buf    []byte
}

func newLogWriter(clusterID int, stream string) *logWriter {
	return &logWriter{prefix: fmt.Sprintf("[cluster %d %s] ", clusterID, stream)}
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := w.buf[:i]
		w.buf = w.buf[i+1:]
		log.Printf("%s%s", w.prefix, line)
	}
	return len(p), nil
}
