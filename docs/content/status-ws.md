---
sidebar:
  order: 7
---

# Status WebSocket

Separate from the internal Unix socket: an **internet-facing** TCP HTTP server on `statusWsAddr`, one route (`/ws`, plus `/healthz`). It streams a `snapshot` on connect then `delta` frames as clusters register / heartbeat / promote / disconnect / reap (active/routed generation only). Receive-only — the manager never acts on anything a client sends. The cluster's opaque HEARTBEAT `status` payload is forwarded verbatim; the manager never parses it (stays bot-agnostic).

## Trust boundary — it's public

Unlike the Unix socket (filesystem-permission gated, local-only), this endpoint is reachable from the internet, so it has an inbound attack surface **even though it only emits** — accepting a connection costs a goroutine, a snapshot copy, and a fanout slot, and every handshake runs the constant-time key check. Two gates guard it, in this order per request:

1. **Handshake rate-limit** (per client IP) — a token bucket, checked _before_ auth so key-guessing / handshake floods can't hammer the auth path or the accept loop. Over budget → **429**.
2. **API-key auth** — the existing key allowlist (`internal/status/auth.go`; `X-API-Key` / `Authorization: Bearer` / `key.<value>` subprotocol). Bad/missing key → **401**.
3. **Concurrent-connection caps** — per-IP and global. A slot is acquired after auth and released when the socket closes. Over cap → **503**.

The key is a `PUBLIC_*` value shipped in the statuspage bundle — this is log hygiene + a first-line gate, not secrecy. Rate-limiting is the DoS floor, not the key system.

## Real client IP behind Cloudflare + nginx

In production the WS sits behind **Cloudflare → nginx (origin) → sm-manager**. To sm-manager the immediate peer (`RemoteAddr`) is **nginx** (loopback), not the visitor — so per-IP limiting keyed on `RemoteAddr` would lump every visitor into one bucket. The real visitor IP is read from a **trusted header** instead:

- `statusRealIpHeader` (default `CF-Connecting-IP`) names the header carrying the client IP. Cloudflare sets `CF-Connecting-IP` to the true client and nginx forwards it.
- The header is honored **only when `RemoteAddr` is in `statusTrustedProxies`** (default loopback `127.0.0.1/32,::1/128`). This is the anti-spoof rule: a client reaching the origin _directly_ (bypassing nginx) has a non-loopback `RemoteAddr`, so its forged `CF-Connecting-IP` is ignored and it's limited by its real socket address. Only nginx, sitting on loopback, is trusted to speak for the client.
- If nginx runs on a different host from sm-manager, set `statusTrustedProxies` to nginx's address(es). Set `statusRealIpHeader` empty to disable header trust entirely (limit purely on `RemoteAddr` — correct when there is no proxy in front).

> **Defense in depth, not the only line.** Cloudflare already absorbs volumetric floods at the edge and nginx can rate-limit too; the manager's own limiter is the last backstop so a request that slips through (or a direct-to-origin hit) still can't exhaust goroutines/FDs. Keep the origin firewalled to nginx only so the trusted-proxy rule holds.

## Rate-limit knobs

- `statusHandshakeRatePerMin` (default `60`) — handshake attempts per IP per minute (token-bucket refill; burst = one minute's worth). `0` disables handshake rate-limiting.
- `statusMaxConnsPerIp` (default `20`) — max concurrent live connections from one IP. `0` disables.
- `statusMaxConns` (default `5000`) — max concurrent live connections across all IPs. `0` disables.

Idle per-IP buckets are swept periodically so the tracking maps stay bounded. All limiter state is in-memory and per-process (single manager instance — no shared store needed).
