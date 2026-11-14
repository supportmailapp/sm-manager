package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Golden tests pin how config.json is decoded. They never name the JSON
// package, so they run unchanged against whichever encoding/json
// implementation the build uses.

func loadFile(t *testing.T, content string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SM_MANAGER_CONFIG", path)
	return Load()
}

func TestWireGoldenExampleConfig(t *testing.T) {
	t.Setenv("SM_MANAGER_CONFIG", filepath.Join("..", "..", "config.example.json"))
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	// AllowedBusinessOps[9] is a "" placeholder, replaced below by the loaded value.
	// That op is the one bot-specific name in the example file. The manager stays
	// bot-agnostic, so its tests do not spell bot-specific names out: they check
	// the name's shape (REFRESH_ prefix, _GUILDS suffix) and copy it across.
	want := &Config{
		SocketPath:                "/tmp/sm-manager.sock",
		SpawnCommand:              []string{"bun", "run", "--env-file=.env.production", "src/cluster.ts"},
		SpawnDir:                  "../bot",
		ShardsPerCluster:          2,
		ClusterCount:              1,
		AllowedBusinessOps:        []string{"VERIFY_GUILD_MEMBERSHIP", "GET_GUILD_USERS", "CHECK_MENTION_PERMISSIONS", "SETUP_TICKETS", "GET_FORUM_TAGS", "SET_FORUM_TAGS", "GET_GUILD_LIST", "SYNC_GUILDS_WITH_DB", "HANDLE_DM_REPORT", "", "CLOSE_ALL_TICKETS"},
		HeartbeatTimeoutSecs:      30,
		Generation:                0,
		DeployTimeoutSecs:         180,
		DeployBatchSize:           1,
		StatusWSAddr:              ":4000",
		StatusAPIKeys:             []string{"replace-me-before-prod"},
		StatusRealIPHeader:        "CF-Connecting-IP",
		StatusTrustedProxies:      []string{"127.0.0.1/32", "::1/128"},
		StatusHandshakeRatePerMin: 60,
		StatusMaxConnsPerIP:       20,
		StatusMaxConns:            5000,
		InstanceSpawnCommand:      []string{},
		InstanceNoRestartExitCode: 78,
		InstanceStopGraceSecs:     12,
		SentryDSN:                 "",
		SentryEnvironment:         "production",
	}
	// The tenth op is a bot-specific name; check its shape instead of
	// repeating the whole file in the test.
	if len(got.AllowedBusinessOps) != 11 || !strings.HasPrefix(got.AllowedBusinessOps[9], "REFRESH_") || !strings.HasSuffix(got.AllowedBusinessOps[9], "_GUILDS") {
		t.Fatalf("allowedBusinessOps = %v", got.AllowedBusinessOps)
	}
	want.AllowedBusinessOps[9] = got.AllowedBusinessOps[9]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("\n got %#v\nwant %#v", got, want)
	}
}

// Members the file omits keep their defaults.
func TestConfigDefaultsAndArrays(t *testing.T) {
	cfg, err := loadFile(t, `{"clusterCount":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterCount != 3 || cfg.SocketPath != "/tmp/sm-manager.sock" || cfg.HeartbeatTimeoutSecs != 30 ||
		cfg.StatusMaxConns != 5000 || !reflect.DeepEqual(cfg.StatusTrustedProxies, []string{"127.0.0.1/32", "::1/128"}) {
		t.Fatalf("defaults lost: %+v", cfg)
	}
	// A listed array replaces the default array instead of extending it.
	cfg, err = loadFile(t, `{"statusTrustedProxies":["10.0.0.0/8"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.StatusTrustedProxies, []string{"10.0.0.0/8"}) {
		t.Fatalf("trusted proxies = %v", cfg.StatusTrustedProxies)
	}
	// An empty array clears the default.
	cfg, err = loadFile(t, `{"statusTrustedProxies":[]}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.StatusTrustedProxies) != 0 {
		t.Fatalf("trusted proxies = %v", cfg.StatusTrustedProxies)
	}
}

// config.json is parsed strictly: a bad file must fail the load, and the error
// must name the file and the offending key.
func TestConfigStrictParsing(t *testing.T) {
	cases := []struct {
		name    string
		content string
		key     string // must appear in the error
	}{
		{"wrong-case key", `{"SocketPath":"/x.sock"}`, "SocketPath"},
		{"lower-case key", `{"clustercount":4}`, "clustercount"},
		{"unknown key", `{"somethingNew":1}`, "somethingNew"},
		{"duplicate key", `{"clusterCount":2,"clusterCount":3}`, "clusterCount"},
		{"null for a number", `{"heartbeatTimeoutSecs":null}`, "heartbeatTimeoutSecs"},
		{"null for a number among others", `{"socketPath":"/x.sock","clusterCount":null}`, "clusterCount"},
		{"null for a string", `{"statusRealIpHeader":null}`, "statusRealIpHeader"},
		{"null for the required string", `{"socketPath":null}`, "socketPath"},
		{"null for a string list", `{"statusTrustedProxies":null}`, "statusTrustedProxies"},
		{"null for an empty-default string list", `{"spawnCommand":null}`, "spawnCommand"},
		{"null inside a string list", `{"allowedBusinessOps":["A",null]}`, "allowedBusinessOps"},
		{"invalid UTF-8", "{\"spawnDir\":\"a\xffb\"}", "spawnDir"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("SM_MANAGER_CONFIG", path)
			cfg, err := Load()
			if err == nil {
				t.Fatalf("accepted %q as %+v", tc.content, cfg)
			}
			if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("error does not name the file %q and key %q: %v", path, tc.key, err)
			}
		})
	}
}

// Whitespace between tokens and after the value is fine.
func TestWireGoldenConfigWhitespace(t *testing.T) {
	cfg, err := loadFile(t, "\r\n\t{ \"clusterCount\" :\r\n 6 }\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterCount != 6 {
		t.Fatalf("clusterCount = %d", cfg.ClusterCount)
	}
}

func TestWireGoldenConfigRejects(t *testing.T) {
	for name, content := range map[string]string{
		"empty":             ``,
		"comment":           "// c\n{\"clusterCount\":1}",
		"trailing comma":    `{"clusterCount":1,}`,
		"bom":               "\xef\xbb\xbf{\"clusterCount\":1}",
		"string for number": `{"clusterCount":"3"}`,
		"fractional int":    `{"clusterCount":1.5}`,
		"exponent int":      `{"clusterCount":1e2}`,
		"array for object":  `[]`,
		"number for string": `{"socketPath":5}`,
		"string for array":  `{"spawnCommand":"bun run"}`,
		"two documents":     `{} {}`,
		"truncated":         `{"clusterCount":`,
	} {
		t.Run(name, func(t *testing.T) {
			if cfg, err := loadFile(t, content); err == nil {
				t.Fatalf("accepted %q as %+v", content, cfg)
			}
		})
	}
}

// Every field type Config uses must have a null rejector in wirejson.Config.
// This fails when a field of another type (bool, int64, map, struct, ...) is
// added, as a reminder to extend the rejectors and the strict-parsing cases.
// The comparison is on the concrete type, not on its Kind: the rejectors are
// registered for int, string and []string exactly, so a named type such as
// `type Secs int` has Kind Int but would slip past them.
func TestConfigFieldTypesAreCoveredByNullRejectors(t *testing.T) {
	covered := map[reflect.Type]bool{
		reflect.TypeFor[int]():      true,
		reflect.TypeFor[string]():   true,
		reflect.TypeFor[[]string](): true,
	}
	rt := reflect.TypeFor[Config]()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !covered[f.Type] {
			t.Errorf("field %s has type %s with no null rejector", f.Name, f.Type)
		}
	}
}
