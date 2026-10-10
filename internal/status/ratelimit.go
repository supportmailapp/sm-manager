package status

import (
	"context"
	"sync"
	"time"
)

// Limits configures the public WS's rate-limiting. Any field left at 0
// disables that particular limit; the whole limiter is a no-op when none are
// set (dev). See README "Public statuspage WebSocket".
type Limits struct {
	RealIPHeader    string   // header carrying the real client IP behind a proxy
	TrustedProxies  []string // CIDRs allowed to set RealIPHeader
	HandshakePerMin int      // per-IP /ws handshakes/min (token bucket); 0 = off
	MaxConnsPerIP   int      // max concurrent conns per IP; 0 = off
	MaxConns        int      // max concurrent conns overall; 0 = off
}

// limiter enforces per-IP handshake rate + per-IP/global concurrent-connection
// caps. All state is in-memory and process-local (single manager instance).
type limiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket // per-IP handshake buckets, lazily created
	active  map[string]int          // per-IP live connection count
	total   int                     // live connections across all IPs

	refillPerSec float64 // handshake token refill rate
	burst        float64 // handshake bucket capacity
	perIPMax     int
	globalMax    int

	now func() time.Time // injectable for tests
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(l Limits) *limiter {
	lim := &limiter{
		buckets:   make(map[string]*tokenBucket),
		active:    make(map[string]int),
		perIPMax:  l.MaxConnsPerIP,
		globalMax: l.MaxConns,
		now:       time.Now,
	}
	if l.HandshakePerMin > 0 {
		lim.refillPerSec = float64(l.HandshakePerMin) / 60.0
		lim.burst = float64(l.HandshakePerMin) // allow one minute's worth of burst
	}
	return lim
}

// allowHandshake consumes one handshake token for ip, returning false when the
// per-IP handshake budget is exhausted. Disabled (always true) when no rate is
// configured.
func (l *limiter) allowHandshake(ip string) bool {
	if l.refillPerSec <= 0 {
		return true
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[ip]
	if b == nil {
		// New IP starts with a full bucket.
		b = &tokenBucket{tokens: l.burst, last: now}
		l.buckets[ip] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * l.refillPerSec
			if b.tokens > l.burst {
				b.tokens = l.burst
			}
			b.last = now
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// acquire reserves a live-connection slot for ip, respecting the per-IP and
// global caps. On success the caller MUST pair it with release(ip).
func (l *limiter) acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.globalMax > 0 && l.total >= l.globalMax {
		return false
	}
	if l.perIPMax > 0 && l.active[ip] >= l.perIPMax {
		return false
	}
	l.total++
	l.active[ip]++
	return true
}

func (l *limiter) release(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[ip] > 0 {
		l.active[ip]--
		if l.active[ip] == 0 {
			delete(l.active, ip)
		}
	}
	if l.total > 0 {
		l.total--
	}
}

// sweep drops handshake buckets that have refilled to full and have no live
// connections, bounding the map for churning/one-shot IPs. Called on a ticker.
func (l *limiter) sweep() {
	if l.refillPerSec <= 0 {
		return
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for ip, b := range l.buckets {
		elapsed := now.Sub(b.last).Seconds()
		refilled := b.tokens + elapsed*l.refillPerSec
		if refilled >= l.burst && l.active[ip] == 0 {
			delete(l.buckets, ip)
		}
	}
}

// runSweeper periodically sweeps idle buckets until ctx is done.
func (l *limiter) runSweeper(ctx context.Context) {
	if l.refillPerSec <= 0 {
		return
	}
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.sweep()
		}
	}
}
