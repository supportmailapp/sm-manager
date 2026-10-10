package status

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// writeTimeout bounds a single frame write to one slow client so it can't wedge
// its own writer goroutine.
const writeTimeout = 10 * time.Second

// statusSubprotocol is the non-secret WebSocket subprotocol the server accepts
// and echoes. The browser client offers this alongside the "key.<value>"
// subprotocol; the server negotiates this one (never the key), so the key is
// validated but never reflected back in the handshake response.
const statusSubprotocol = "sm-status.v1"

// Server is the internet-facing statuspage WebSocket. It is entirely separate
// from the internal Unix socket: different transport (TCP), different port,
// different (public) trust boundary. It serves one route — the WS upgrade —
// guarded by the auth stub, and streams the Broadcaster's snapshot + deltas.
type Server struct {
	addr string
	auth *authenticator
	ipr  *ipResolver
	lim  *limiter
	b    *Broadcaster
	hs   *http.Server
}

// NewServer builds the status WS server. addr is a host:port (e.g. ":4000").
// keys is the stub API-key allowlist (empty = open endpoint). limits configures
// real-client-IP resolution and rate-limiting for the public endpoint.
func NewServer(addr string, keys []string, limits Limits, b *Broadcaster) *Server {
	s := &Server{
		addr: addr,
		auth: newAuthenticator(keys),
		ipr:  newIPResolver(limits.RealIPHeader, limits.TrustedProxies),
		lim:  newLimiter(limits),
		b:    b,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s.hs = &http.Server{Addr: addr, Handler: mux}
	return s
}

// Run serves until ctx is cancelled, then shuts down gracefully. Blocks.
func (s *Server) Run(ctx context.Context) error {
	if s.auth.disabled() {
		log.Printf("status WS: no API keys configured — endpoint is OPEN (dev only). Set statusApiKeys / SM_STATUS_API_KEYS before prod.")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("status WS listening on %s (ws://%s/ws)", s.addr, s.addr)

	go s.lim.runSweeper(ctx)

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.hs.Shutdown(shutCtx)
	}()

	if err := s.hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	ip := s.ipr.clientIP(r)

	// Gate order: cheap per-IP handshake rate-limit first (so a flood can't
	// hammer the constant-time key check or the accept loop), then auth, then a
	// concurrent-connection slot held for the life of the socket.
	if !s.lim.allowHandshake(ip) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	if !s.auth.check(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.lim.acquire(ip) {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer s.lim.release(ip)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// OriginPatterns is left default (same-origin). The statuspage is served
		// from a different origin, so set InsecureSkipVerify-equivalent here only
		// once the real key auth (above) is the trust boundary. For now the auth
		// stub gates access; allow any origin so the browser client can connect.
		InsecureSkipVerify: true,
		// Negotiate only the non-secret subprotocol. Negotiation picks the first
		// client-offered protocol in this list, so the "key.<value>" one the
		// browser also offers is never selected/echoed. Non-browser clients that
		// offer no subprotocol simply get an empty negotiated value.
		Subprotocols: []string{statusSubprotocol},
	})
	if err != nil {
		log.Printf("status WS: accept failed: %v", err)
		return
	}
	defer conn.CloseNow()

	cl := &client{send: make(chan []byte, 32)}
	// CloseRead drains and discards inbound frames in the background (handling
	// pings/close for us) and returns a ctx cancelled when the peer goes away.
	// The statuspage is receive-only, so anything it sends is ignored.
	ctx := conn.CloseRead(r.Context())

	s.b.addClient(cl)
	defer s.b.removeClient(cl)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-cl.send:
			if !ok {
				// Broadcaster dropped us (slow consumer). Close politely.
				conn.Close(websocket.StatusPolicyViolation, "too slow")
				return
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
