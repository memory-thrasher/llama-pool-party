package config

import (
	"strings"
	"testing"
)

func setEnv(t *testing.T) {
	t.Helper()
	// Save and restore so tests don't leak state between cases.
	t.Cleanup(func() {
		for _, k := range []string{"COORD_ADDR", "COORD_DSN", "COORD_STATIC_DIR", "COORD_LOG_LEVEL"} {
			t.Setenv(k, "")
		}
	})
}

func TestDefaults(t *testing.T) {
	setEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want %q", cfg.Addr, DefaultAddr)
	}
	if cfg.DSN != "" {
		t.Errorf("DSN = %q, want empty default", cfg.DSN)
	}
	if cfg.StaticDir != DefaultStaticDir {
		t.Errorf("StaticDir = %q, want %q", cfg.StaticDir, DefaultStaticDir)
	}
	if cfg.LogLevel != DefaultLogLevel {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, DefaultLogLevel)
	}
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() = nil with empty DSN, want error")
	}
}

func TestEnvOverride(t *testing.T) {
	setEnv(t)
	t.Setenv("COORD_ADDR", "0.0.0.0:9999")
	t.Setenv("COORD_DSN", "host=127.0.0.1 port=5432 user=postgres dbname=postgres")
	t.Setenv("COORD_STATIC_DIR", "/srv/portal")
	t.Setenv("COORD_LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Addr != "0.0.0.0:9999" {
		t.Errorf("Addr = %q, want 0.0.0.0:9999", cfg.Addr)
	}
	if !strings.Contains(cfg.DSN, "host=127.0.0.1") {
		t.Errorf("DSN = %q, want it to come from env", cfg.DSN)
	}
	if cfg.StaticDir != "/srv/portal" {
		t.Errorf("StaticDir = %q, want /srv/portal", cfg.StaticDir)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", cfg.LogLevel)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestInvalidLogLevel(t *testing.T) {
	setEnv(t)
	t.Setenv("COORD_LOG_LEVEL", "verbose")
	if _, err := Load(); err == nil {
		t.Error("Load() with invalid log level: got nil, want error")
	}
}
