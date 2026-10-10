package status

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientIPTrustedProxy(t *testing.T) {
	r := newIPResolver("CF-Connecting-IP", []string{"127.0.0.1/32", "::1/128"})

	// Peer is a trusted proxy → honor the header.
	req := httptest.NewRequest("GET", "/ws", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("CF-Connecting-IP", "203.0.113.7")
	if got := r.clientIP(req); got != "203.0.113.7" {
		t.Fatalf("trusted proxy: got %q want visitor IP", got)
	}

	// Peer is NOT trusted (direct-to-origin) → ignore the spoofed header, use peer.
	req2 := httptest.NewRequest("GET", "/ws", nil)
	req2.RemoteAddr = "198.51.100.9:4444"
	req2.Header.Set("CF-Connecting-IP", "203.0.113.7") // spoof attempt
	if got := r.clientIP(req2); got != "198.51.100.9" {
		t.Fatalf("untrusted peer: got %q want peer IP (spoof ignored)", got)
	}

	// Trusted peer but header absent → fall back to peer.
	req3 := httptest.NewRequest("GET", "/ws", nil)
	req3.RemoteAddr = "127.0.0.1:5555"
	if got := r.clientIP(req3); got != "127.0.0.1" {
		t.Fatalf("no header: got %q want peer IP", got)
	}
}

func TestClientIPNoHeaderTrust(t *testing.T) {
	// Empty header name → never trust, always peer.
	r := newIPResolver("", []string{"127.0.0.1/32"})
	req := httptest.NewRequest("GET", "/ws", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("CF-Connecting-IP", "203.0.113.7")
	if got := r.clientIP(req); got != "127.0.0.1" {
		t.Fatalf("header disabled: got %q want peer IP", got)
	}
}

func TestHandshakeRateLimit(t *testing.T) {
	l := newLimiter(Limits{HandshakePerMin: 60}) // burst 60, refill 1/sec
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }

	// Drain the full burst.
	for i := 0; i < 60; i++ {
		if !l.allowHandshake("ip1") {
			t.Fatalf("token %d should be allowed within burst", i)
		}
	}
	// 61st is over budget.
	if l.allowHandshake("ip1") {
		t.Fatal("should be rate-limited after burst exhausted")
	}
	// A different IP has its own bucket.
	if !l.allowHandshake("ip2") {
		t.Fatal("distinct IP should have its own budget")
	}
	// After 1s, one token refilled → one more allowed, then blocked again.
	now = now.Add(time.Second)
	if !l.allowHandshake("ip1") {
		t.Fatal("one token should refill after 1s")
	}
	if l.allowHandshake("ip1") {
		t.Fatal("only one token refilled")
	}
}

func TestHandshakeRateDisabled(t *testing.T) {
	l := newLimiter(Limits{}) // no rate configured
	for i := 0; i < 1000; i++ {
		if !l.allowHandshake("ip1") {
			t.Fatal("disabled rate limit should always allow")
		}
	}
}

func TestConnCaps(t *testing.T) {
	l := newLimiter(Limits{MaxConnsPerIP: 2, MaxConns: 3})

	if !l.acquire("a") || !l.acquire("a") {
		t.Fatal("first two per-IP acquires should succeed")
	}
	if l.acquire("a") {
		t.Fatal("third from same IP exceeds per-IP cap")
	}
	if !l.acquire("b") {
		t.Fatal("different IP acquire should succeed (global has room)")
	}
	// Global cap now at 3 (a,a,b).
	if l.acquire("b") {
		t.Fatal("global cap of 3 reached")
	}
	// Releasing frees both per-IP and global slots.
	l.release("a")
	if !l.acquire("b") {
		t.Fatal("after release a slot should be free")
	}
}

func TestConnCapsDisabled(t *testing.T) {
	l := newLimiter(Limits{}) // no caps
	for i := 0; i < 1000; i++ {
		if !l.acquire("a") {
			t.Fatal("disabled caps should always allow")
		}
	}
}

func TestSweepDropsIdleBuckets(t *testing.T) {
	l := newLimiter(Limits{HandshakePerMin: 60})
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }

	l.allowHandshake("ip1") // creates a bucket, spends a token
	if len(l.buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(l.buckets))
	}
	// Advance well past a full refill; no active conns → swept.
	now = now.Add(5 * time.Minute)
	l.sweep()
	if len(l.buckets) != 0 {
		t.Fatalf("idle full bucket should be swept, got %d", len(l.buckets))
	}

	// A bucket with a live connection is retained even when full.
	l.allowHandshake("ip2")
	l.acquire("ip2")
	now = now.Add(5 * time.Minute)
	l.sweep()
	if _, ok := l.buckets["ip2"]; !ok {
		t.Fatal("bucket with active conn must not be swept")
	}
}
