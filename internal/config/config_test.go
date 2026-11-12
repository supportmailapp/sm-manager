package config

import (
	"path/filepath"
	"reflect"
	"testing"
)

func loadWithNoFile(t *testing.T) *Config {
	t.Helper()
	t.Setenv("SM_MANAGER_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestInstanceConfigDefaults(t *testing.T) {
	cfg := loadWithNoFile(t)
	if cfg.InstanceNoRestartExitCode != 78 {
		t.Fatalf("InstanceNoRestartExitCode = %d, want 78", cfg.InstanceNoRestartExitCode)
	}
	if cfg.InstanceStopGraceSecs != 12 {
		t.Fatalf("InstanceStopGraceSecs = %d, want 12", cfg.InstanceStopGraceSecs)
	}
	if len(cfg.InstanceSpawnCommand) != 0 {
		t.Fatalf("InstanceSpawnCommand = %v, want empty", cfg.InstanceSpawnCommand)
	}
}

func TestInstanceConfigEnvOverrides(t *testing.T) {
	t.Setenv("SM_INSTANCE_SPAWN_COMMAND", "bun run  src/instance.ts")
	t.Setenv("SM_INSTANCE_NO_RESTART_EXIT_CODE", "64")
	t.Setenv("SM_INSTANCE_STOP_GRACE_SECS", "5")
	cfg := loadWithNoFile(t)
	if want := []string{"bun", "run", "src/instance.ts"}; !reflect.DeepEqual(cfg.InstanceSpawnCommand, want) {
		t.Fatalf("InstanceSpawnCommand = %v, want %v", cfg.InstanceSpawnCommand, want)
	}
	if cfg.InstanceNoRestartExitCode != 64 || cfg.InstanceStopGraceSecs != 5 {
		t.Fatalf("got exit %d grace %d", cfg.InstanceNoRestartExitCode, cfg.InstanceStopGraceSecs)
	}
}
