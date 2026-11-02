package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"melodi_websocket/internal/config"
)

func TestConfigDefaultsAndOverrides(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := config.NewConfigManager(tmpDir, "config.json")

	overrides := config.FlatConfig{
		Port:       9090,
		Secret:     "custom_secret_123",
		ScaleMode:  "autonome",
		MelodiPort: 3000,
	}
	flagsPassed := map[string]bool{
		"port":   true,
		"secret": true,
	}

	active := mgr.InitConfig(overrides, flagsPassed)

	if active.Port != 9090 {
		t.Errorf("expected port 9090, got %d", active.Port)
	}
	if active.Secret != "custom_secret_123" {
		t.Errorf("expected secret custom_secret_123, got %s", active.Secret)
	}
	if active.ScaleMode != "autonome" {
		t.Errorf("expected scale_mode autonome, got %s", active.ScaleMode)
	}
}

func TestConfigUpdateAndMasking(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := config.NewConfigManager(tmpDir, "config.json")
	mgr.InitConfig(config.FlatConfig{Secret: "super_secret_key"}, map[string]bool{"secret": true})

	// Test UpdateConfig
	updates := map[string]any{
		"grace_period_seconds": 120,
		"log_level":            "DEBUG",
	}
	ok, updatedKeys, restartReq, msg := mgr.UpdateConfig(updates, true)
	if !ok {
		t.Fatalf("unexpected error updating config: %s", msg)
	}
	if restartReq {
		t.Errorf("dynamic update of grace_period and log_level should not require restart")
	}
	if len(updatedKeys) < 2 {
		t.Errorf("expected at least 2 updated keys, got: %v", updatedKeys)
	}

	cfg := mgr.Get()
	if cfg.GracePeriodSeconds != 120 {
		t.Errorf("expected grace_period_seconds 120, got %d", cfg.GracePeriodSeconds)
	}

	// Verify file persistence
	configFile := filepath.Join(tmpDir, "config.json")
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		t.Errorf("expected config.json to exist in %s", tmpDir)
	}

	// Test Masking with GetStructured(false)
	structuredMasked := mgr.GetStructured(false)
	if structuredMasked.Security.Secret == "super_secret_key" {
		t.Errorf("secret should be masked when includeSecrets=false")
	}

	// Test Unmasked with GetStructured(true)
	structuredFull := mgr.GetStructured(true)
	if structuredFull.Security.Secret != "super_secret_key" {
		t.Errorf("expected full secret, got: %s", structuredFull.Security.Secret)
	}
}
