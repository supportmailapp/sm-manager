package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"sm-manager/internal/protocol"
)

// Config holds all bot-specific settings. A second bot deployment is a
// different config, not a code change. Nothing here is a compiled-in constant.
type Config struct {
	// SocketPath is the Unix socket the manager listens on.
	SocketPath string `json:"socketPath"`
	// SpawnCommand is the cluster process command (binary + args). Phase 1.
	SpawnCommand []string `json:"spawnCommand"`
	// SpawnDir is the working directory for SpawnCommand (the bot repo). Empty
	// means the manager's own cwd.
	SpawnDir string `json:"spawnDir"`
	// ShardsPerCluster and ClusterCount describe the shard topology. Phase 1.
	ShardsPerCluster int `json:"shardsPerCluster"`
	ClusterCount     int `json:"clusterCount"`
	// AllowedBusinessOps is a flat list of op-name strings the manager will
	// forward. Anything not structural and not on this list is rejected at the
	// manager. NO payload shapes live here — that would re-couple the manager
	// to a specific bot's domain.
	AllowedBusinessOps []string `json:"allowedBusinessOps"`
	// HeartbeatTimeoutSecs marks a cluster dead if no heartbeat arrives within
	// it.
	HeartbeatTimeoutSecs int `json:"heartbeatTimeoutSecs"`
	// Generation is the starting epoch stamped on every cluster the manager
	// spawns on a cold start (passed as the GENERATION env var). This is only
	// the seed value — once running, the live generation counter is mutable
	// runtime state owned by the Supervisor, bumped by an inbound DEPLOY op,
	// not by this config.
	Generation int `json:"generation"`
	// DeployTimeoutSecs bounds how long a DEPLOY op waits for the new
	// generation to become ready (and for the old generation to quiesce)
	// before giving up and rolling back. 0 means no bound (not recommended).
	DeployTimeoutSecs int `json:"deployTimeoutSecs"`
	// DeployBatchSize is how many clusters a rollout swaps at once. A rollout
	// spawns the new generation, cuts traffic over, and retires the old one in
	// chunks of this size, one chunk at a time — so only that many clusters
	// have a quiesce gap at any moment. 0 or a value >= ClusterCount means the
	// whole fleet in a single batch (the original all-at-once behavior).
	DeployBatchSize int `json:"deployBatchSize"`
	// StatusWSAddr is the host:port for the public statuspage WebSocket — the
	// internet-facing endpoint, separate from the internal Unix socket. Empty
	// disables it. (Phase 5.)
	StatusWSAddr string `json:"statusWsAddr"`
	// StatusAPIKeys is the stub allowlist gating the public WS. Empty means the
	// endpoint is open (dev only). This is the first-line-of-defense seam the
	// real key system replaces later.
	StatusAPIKeys []string `json:"statusApiKeys"`
	// StatusRealIPHeader names the header carrying the real client IP behind the
	// reverse proxy (Cloudflare → nginx). Honored only when RemoteAddr is in
	// StatusTrustedProxies. Empty = trust no header, rate-limit on RemoteAddr.
	StatusRealIPHeader string `json:"statusRealIpHeader"`
	// StatusTrustedProxies is the set of CIDRs whose RemoteAddr may set
	// StatusRealIPHeader (i.e. the origin nginx). Anti-spoof for the real-IP
	// header: a direct-to-origin client is limited on its socket address.
	StatusTrustedProxies []string `json:"statusTrustedProxies"`
	// StatusHandshakeRatePerMin caps per-IP /ws handshake attempts per minute
	// (token bucket, checked before auth). 0 disables handshake rate-limiting.
	StatusHandshakeRatePerMin int `json:"statusHandshakeRatePerMin"`
	// StatusMaxConnsPerIP caps concurrent live WS connections from one IP. 0 off.
	StatusMaxConnsPerIP int `json:"statusMaxConnsPerIp"`
	// StatusMaxConns caps concurrent live WS connections across all IPs. 0 off.
	StatusMaxConns int `json:"statusMaxConns"`
	// SentryDSN is the error/log/metric ingest endpoint. Empty disables Sentry.
	SentryDSN string `json:"sentryDsn"`
	// SentryEnvironment tags events (e.g. "production", "staging").
	SentryEnvironment string `json:"sentryEnvironment"`
}

// AllowedSet materializes AllowedBusinessOps into a lookup set.
func (c *Config) AllowedSet() map[protocol.OpCode]struct{} {
	set := make(map[protocol.OpCode]struct{}, len(c.AllowedBusinessOps))
	for _, op := range c.AllowedBusinessOps {
		set[protocol.OpCode(op)] = struct{}{}
	}
	return set
}

// Load reads config from the JSON file at path (env SM_MANAGER_CONFIG, else
// ./config.json), then applies env overrides for the most deployment-variable
// fields.
func Load() (*Config, error) {
	path := os.Getenv("SM_MANAGER_CONFIG")
	if path == "" {
		path = "config.json"
	}

	cfg := &Config{
		SocketPath:                "/tmp/sm-manager.sock",
		ShardsPerCluster:          1,
		ClusterCount:              1,
		HeartbeatTimeoutSecs:      30,
		DeployTimeoutSecs:         120,
		StatusRealIPHeader:        "CF-Connecting-IP",
		StatusTrustedProxies:      []string{"127.0.0.1/32", "::1/128"},
		StatusHandshakeRatePerMin: 60,
		StatusMaxConnsPerIP:       20,
		StatusMaxConns:            5000,
	}

	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(raw, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	case os.IsNotExist(err):
		// No file is allowed; rely on defaults + env.
	default:
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	applyEnvOverrides(cfg)

	if cfg.SocketPath == "" {
		return nil, fmt.Errorf("socketPath is required")
	}
	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("SM_SOCKET_PATH"); v != "" {
		cfg.SocketPath = v
	}
	if v := os.Getenv("SM_SHARDS_PER_CLUSTER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ShardsPerCluster = n
		}
	}
	if v := os.Getenv("SM_CLUSTER_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ClusterCount = n
		}
	}
	if v := os.Getenv("SM_HEARTBEAT_TIMEOUT_SECS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HeartbeatTimeoutSecs = n
		}
	}
	if v := os.Getenv("SM_ALLOWED_BUSINESS_OPS"); v != "" {
		parts := strings.Split(v, ",")
		ops := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				ops = append(ops, p)
			}
		}
		cfg.AllowedBusinessOps = ops
	}
	if v := os.Getenv("SM_SPAWN_COMMAND"); v != "" {
		cfg.SpawnCommand = strings.Fields(v)
	}
	if v := os.Getenv("SM_SPAWN_DIR"); v != "" {
		cfg.SpawnDir = v
	}
	if v := os.Getenv("SM_GENERATION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Generation = n
		}
	}
	if v := os.Getenv("SM_DEPLOY_TIMEOUT_SECS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.DeployTimeoutSecs = n
		}
	}
	if v := os.Getenv("SM_DEPLOY_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.DeployBatchSize = n
		}
	}
	if v := os.Getenv("SM_STATUS_WS_ADDR"); v != "" {
		cfg.StatusWSAddr = v
	}
	if v := os.Getenv("SM_STATUS_API_KEYS"); v != "" {
		cfg.StatusAPIKeys = splitCSV(v)
	}
	// LookupEnv so an explicit empty value disables header trust (unlike Getenv,
	// which can't distinguish empty from unset).
	if v, ok := os.LookupEnv("SM_STATUS_REAL_IP_HEADER"); ok {
		cfg.StatusRealIPHeader = strings.TrimSpace(v)
	}
	if v, ok := os.LookupEnv("SM_STATUS_TRUSTED_PROXIES"); ok {
		cfg.StatusTrustedProxies = splitCSV(v)
	}
	if v := os.Getenv("SM_STATUS_HANDSHAKE_RATE_PER_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.StatusHandshakeRatePerMin = n
		}
	}
	if v := os.Getenv("SM_STATUS_MAX_CONNS_PER_IP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.StatusMaxConnsPerIP = n
		}
	}
	if v := os.Getenv("SM_STATUS_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.StatusMaxConns = n
		}
	}
	if v := os.Getenv("SM_SENTRY_DSN"); v != "" {
		cfg.SentryDSN = v
	}
	if v := os.Getenv("SM_SENTRY_ENVIRONMENT"); v != "" {
		cfg.SentryEnvironment = v
	}
}

// splitCSV splits a comma-separated env value into trimmed, non-empty tokens.
func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// TotalShards is the full shard count across every cluster.
func (c *Config) TotalShards() int { return c.ClusterCount * c.ShardsPerCluster }

// ShardsFor returns the contiguous shard ids owned by clusterID under the
// current topology.
func (c *Config) ShardsFor(clusterID int) []int {
	start := clusterID * c.ShardsPerCluster
	shards := make([]int, 0, c.ShardsPerCluster)
	for i := 0; i < c.ShardsPerCluster; i++ {
		shards = append(shards, start+i)
	}
	return shards
}
