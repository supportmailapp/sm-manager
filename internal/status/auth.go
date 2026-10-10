package status

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// keySubprotocolPrefix marks the WebSocket subprotocol that carries the API
// key. Browsers can't set headers on a WebSocket handshake but can offer
// subprotocols (which ride in Sec-WebSocket-Protocol), so the key travels as
// "key.<value>". This keeps it out of the URL/query — and thus out of access
// logs, browser history, and Referer headers. The server validates it but
// echoes back only the non-secret protocol (see statusSubprotocol), so the key
// is never reflected to the client.
const keySubprotocolPrefix = "key."

// authenticator is the first line of defense on the public WS endpoint. It is
// deliberately a STUB: today it does a flat, constant-time API-key allowlist
// check so the transport, wiring, and 401 path all exist and can be exercised
// end-to-end. The real key system (per-consumer keys, revocation, scopes,
// issuance for the statuspage) lands in Phase 5's auth/rate-limiting bullet and
// slots in behind this same check() seam without touching the server or the
// broadcaster.
//
// Rate-limiting is the sibling gate and lives in ratelimit.go (per-IP handshake
// token bucket + per-IP/global connection caps), applied in server.handleWS
// around this key check. This file is only the key gate.
type authenticator struct {
	keys map[string]struct{}
}

func newAuthenticator(keys []string) *authenticator {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k != "" {
			set[k] = struct{}{}
		}
	}
	return &authenticator{keys: set}
}

// disabled reports whether no keys are configured. With no keys the endpoint is
// open — acceptable for local/dev, but main logs a warning so it isn't shipped
// to prod unnoticed.
func (a *authenticator) disabled() bool { return len(a.keys) == 0 }

// check validates the request's API key. Key is read from the X-API-Key header,
// an Authorization: Bearer <key> header, or a "key.<value>" WebSocket
// subprotocol (the last is for the browser WebSocket() client, which can't set
// custom headers on the handshake). Comparison is constant-time to avoid
// leaking key material via timing. When no keys are configured, all requests
// pass.
func (a *authenticator) check(r *http.Request) bool {
	if a.disabled() {
		return true
	}
	presented := presentedKey(r)
	if presented == "" {
		return false
	}
	// Constant-time compare against each allowed key. The set is tiny (a
	// handful of keys) so a linear scan is fine.
	ok := false
	for k := range a.keys {
		if subtle.ConstantTimeCompare([]byte(presented), []byte(k)) == 1 {
			ok = true
		}
	}
	return ok
}

func presentedKey(r *http.Request) string {
	if v := r.Header.Get("X-API-Key"); v != "" {
		return v
	}
	if v := r.Header.Get("Authorization"); len(v) > 7 && v[:7] == "Bearer " {
		return v[7:]
	}
	// Browser path: the key rides as a "key.<value>" subprotocol.
	for _, proto := range offeredSubprotocols(r) {
		if strings.HasPrefix(proto, keySubprotocolPrefix) {
			return strings.TrimPrefix(proto, keySubprotocolPrefix)
		}
	}
	return ""
}

// offeredSubprotocols parses the comma-separated Sec-WebSocket-Protocol
// offer(s) into individual tokens.
func offeredSubprotocols(r *http.Request) []string {
	var out []string
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(header, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}
