// Package telemetry holds the process-wide Sentry logger and meter, created
// once after sentry.Init so every package can emit structured logs and
// application metrics without each owning its own sentry.NewLogger/NewMeter
// plumbing. Call Init before any other package uses Logger/Meter.
package telemetry

import (
	"context"
	"sync"

	"github.com/getsentry/sentry-go"
)

var (
	once   sync.Once
	logger sentry.Logger
	meter  sentry.Meter
)

// Init creates the shared logger and meter against ctx. Call once, right
// after sentry.Init, before spawning any goroutine that logs or records
// metrics. Only the first call (whether this explicit one or a lazy call
// from Logger/Meter, e.g. in a test that never calls Init) takes effect —
// sentry.NewLogger/NewMeter safely no-op when no Sentry client is bound, so
// an uninitialized caller still gets a usable, silent implementation instead
// of a nil interface.
func Init(ctx context.Context) {
	once.Do(func() {
		logger = sentry.NewLogger(ctx)
		meter = sentry.NewMeter(ctx)
	})
}

// Logger returns the process-wide Sentry structured logger, lazily
// initializing it against context.Background() if Init was never called.
func Logger() sentry.Logger {
	Init(context.Background())
	return logger
}

// Meter returns the process-wide Sentry application-metrics meter, lazily
// initializing it against context.Background() if Init was never called.
func Meter() sentry.Meter {
	Init(context.Background())
	return meter
}
