package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"

	"sm-manager/internal/config"
	"sm-manager/internal/server"
	"sm-manager/internal/status"
	"sm-manager/internal/supervisor"
	"sm-manager/internal/telemetry"
)

func main() {
	// Loaded before sentry.Init since the DSN itself lives in config.json (or
	// its SM_SENTRY_* env overrides) — config errors here can't reach Sentry.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	sentryRelease := detectRelease()

	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              cfg.SentryDSN,
		Environment:      cfg.SentryEnvironment,
		Release:          sentryRelease,
		SendDefaultPII:   true,
		AttachStacktrace: true,
		EnableTracing:    true,
		TracesSampleRate: 1.0,
	}); err != nil {
		log.Fatalf("sentry.Init: %v", err)
	}
	defer sentry.Flush(2 * time.Second)
	defer recoverAndReport()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	telemetry.Init(ctx)
	logger := telemetry.Logger()

	log.Printf("sm-manager starting: socket=%s clusters=%d shards/cluster=%d allowedOps=%d generation=%d",
		cfg.SocketPath, cfg.ClusterCount, cfg.ShardsPerCluster, len(cfg.AllowedBusinessOps), cfg.Generation)
	logger.Info().
		Int("clusters", cfg.ClusterCount).
		Int("shards_per_cluster", cfg.ShardsPerCluster).
		Int("generation", cfg.Generation).
		Emit("sm-manager starting")
	telemetry.Meter().Gauge("sm_manager.clusters_configured", float64(cfg.ClusterCount))
	telemetry.Meter().Gauge("sm_manager.shards_configured", float64(cfg.TotalShards()))

	srv := server.New(cfg)
	if err := srv.Listen(); err != nil {
		logger.Fatal().Emitf("listen: %v", err)
		sentry.Flush(2 * time.Second)
		log.Fatalf("listen: %v", err)
	}
	defer srv.Close()

	// Public statuspage WebSocket (Phase 5) — internet-facing, separate from the
	// internal Unix socket. Disabled when no address is configured.
	var statusDone chan struct{}
	if cfg.StatusWSAddr != "" {
		bc := status.NewBroadcaster()
		srv.Hub().SetStatusSink(bc)
		statusSrv := status.NewServer(cfg.StatusWSAddr, cfg.StatusAPIKeys, status.Limits{
			RealIPHeader:    cfg.StatusRealIPHeader,
			TrustedProxies:  cfg.StatusTrustedProxies,
			HandshakePerMin: cfg.StatusHandshakeRatePerMin,
			MaxConnsPerIP:   cfg.StatusMaxConnsPerIP,
			MaxConns:        cfg.StatusMaxConns,
		}, bc)
		statusDone = make(chan struct{})
		go func() {
			defer recoverAndReport()
			if err := statusSrv.Run(ctx); err != nil {
				log.Printf("status WS: %v", err)
				logger.Error().Emitf("status WS: %v", err)
			}
			close(statusDone)
		}()
	}

	// Supervise the cluster processes. Runs under the same ctx so a signal tears
	// down both the socket server and every child. Wait for the children to exit
	// before returning so their shutdown is clean.
	sup := supervisor.New(cfg, srv.Hub())
	srv.Hub().SetDeployer(sup) // wire before Serve starts accepting DEPLOY requests
	supDone := make(chan struct{})
	go func() {
		defer recoverAndReport()
		sup.Run(ctx)
		close(supDone)
	}()

	if err := srv.Serve(ctx); err != nil {
		logger.Fatal().Emitf("serve: %v", err)
		sentry.Flush(2 * time.Second)
		log.Fatalf("serve: %v", err)
	}
	<-supDone
	if statusDone != nil {
		<-statusDone
	}
	log.Printf("sm-manager shutting down")
	logger.Info().Emit("sm-manager shutting down")
	_ = os.Stdout.Sync()
}

// recoverAndReport captures a panic to Sentry, flushes, then re-panics so the
// process still crashes with its normal stack trace on stderr.
func recoverAndReport() {
	if r := recover(); r != nil {
		sentry.CurrentHub().Recover(r)
		sentry.Flush(2 * time.Second)
		panic(r)
	}
}

// detectRelease is the fallback Sentry release when config.json's
// sentryRelease is empty. `go build` (no ldflags needed, so this works the
// same on Windows without WSL/shell scripting) automatically embeds VCS info
// into the binary when built from within a git checkout — read that back via
// debug.ReadBuildInfo. If it's unavailable (e.g. built outside a git repo, or
// via `go install` from the module cache), fall back to a millisecond
// timestamp so every build still gets a distinct release.
func detectRelease() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		var revision string
		var modified bool
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
			case "vcs.modified":
				modified = s.Value == "true"
			}
		}
		if revision != "" {
			if len(revision) > 12 {
				revision = revision[:12]
			}
			if modified {
				revision += "-dirty"
			}
			return revision
		}
	}
	return strconv.FormatInt(time.Now().UnixMilli(), 10)
}
