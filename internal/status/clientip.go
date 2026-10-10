package status

import (
	"net"
	"net/http"
	"strings"
)

// ipResolver derives the real client IP for a request. Behind Cloudflare +
// nginx the transport peer (RemoteAddr) is nginx, not the visitor, so per-IP
// rate-limiting keyed on RemoteAddr would collapse every visitor into one
// bucket. The visitor IP rides in a trusted header (CF-Connecting-IP) that we
// honor ONLY when the immediate peer is a trusted proxy — otherwise a client
// hitting the origin directly could spoof it.
type ipResolver struct {
	header  string       // e.g. "CF-Connecting-IP"; empty = never trust a header
	trusted []*net.IPNet // RemoteAddr must be in one of these to honor header
}

// newIPResolver parses trusted-proxy CIDRs. Unparseable entries are skipped
// (logged by the caller if it cares); an empty/failed list means no header is
// ever trusted and clientIP always returns the socket peer.
func newIPResolver(header string, trustedCIDRs []string) *ipResolver {
	r := &ipResolver{header: strings.TrimSpace(header)}
	for _, c := range trustedCIDRs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, ipnet, err := net.ParseCIDR(c); err == nil {
			r.trusted = append(r.trusted, ipnet)
		}
	}
	return r
}

// clientIP returns the address to key rate-limiting on. It is the trusted
// header value when RemoteAddr is a trusted proxy and the header is present;
// otherwise the socket peer's IP.
func (r *ipResolver) clientIP(req *http.Request) string {
	peer := hostOnly(req.RemoteAddr)
	if r.header == "" || !r.isTrusted(peer) {
		return peer
	}
	v := req.Header.Get(r.header)
	if v == "" {
		return peer
	}
	// CF-Connecting-IP is a single IP; a forwarded-for style list would put the
	// original client leftmost. Take the first token defensively.
	if i := strings.IndexByte(v, ','); i >= 0 {
		v = v[:i]
	}
	if v = strings.TrimSpace(v); v != "" {
		return v
	}
	return peer
}

func (r *ipResolver) isTrusted(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range r.trusted {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// hostOnly strips the port from a host:port RemoteAddr, tolerating a bare host.
func hostOnly(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}
