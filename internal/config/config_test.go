package config

import (
	"strings"
	"testing"
)

func setEnv(t *testing.T) {
	t.Helper()
	// Clear all four COORD_* vars at the START of each test so results are hermetic
	// even when the environment already defines them (t.Setenv restores the
	// original value on cleanup).
	for _, k := range envKeys {
		t.Setenv(k, "")
	}
	// Backstop: leave nothing behind even if a future change skips the loop above.
	t.Cleanup(func() {
		for _, k := range envKeys {
			t.Setenv(k, "")
		}
	})
}

var envKeys = []string{"COORD_ADDR", "COORD_DSN", "COORD_STATIC_DIR", "COORD_LOG_LEVEL", "POOL_CONT_IDLE_MS", "POOL_CONT_PREFER_MS", "POOL_JOB_EXPIRY_MS", "POOL_RUNNING_CAP_MS", "POOL_BATCH_WEIGHT", "POOL_BATCH_EXPIRY_MS"}

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
	if cfg.ContIdleMS != DefaultContIdleMS {
		t.Errorf("ContIdleMS = %d, want %d", cfg.ContIdleMS, DefaultContIdleMS)
	}
	if cfg.ContPreferMS != DefaultContPreferMS {
		t.Errorf("ContPreferMS = %d, want %d", cfg.ContPreferMS, DefaultContPreferMS)
	}
	if cfg.JobExpiryMS != DefaultJobExpiryMS {
		t.Errorf("JobExpiryMS = %d, want %d", cfg.JobExpiryMS, DefaultJobExpiryMS)
	}
	if cfg.RunningCapMS != DefaultRunningCapMS {
		t.Errorf("RunningCapMS = %d, want %d", cfg.RunningCapMS, DefaultRunningCapMS)
	}
	if cfg.BatchWeight != DefaultBatchWeight {
		t.Errorf("BatchWeight = %v, want %v", cfg.BatchWeight, DefaultBatchWeight)
	}
	if cfg.BatchExpiryMS != DefaultBatchExpiryMS {
		t.Errorf("BatchExpiryMS = %d, want %d", cfg.BatchExpiryMS, DefaultBatchExpiryMS)
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
	t.Setenv("POOL_CONT_IDLE_MS", "30000")
	t.Setenv("POOL_CONT_PREFER_MS", "300000")
	t.Setenv("POOL_JOB_EXPIRY_MS", "60000")
	t.Setenv("POOL_RUNNING_CAP_MS", "45000")
	t.Setenv("POOL_BATCH_WEIGHT", "0.25")
	t.Setenv("POOL_BATCH_EXPIRY_MS", "7200000")

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
	if cfg.ContIdleMS != 30000 {
		t.Errorf("ContIdleMS = %d, want 30000", cfg.ContIdleMS)
	}
	if cfg.ContPreferMS != 300000 {
		t.Errorf("ContPreferMS = %d, want 300000", cfg.ContPreferMS)
	}
	if cfg.JobExpiryMS != 60000 {
		t.Errorf("JobExpiryMS = %d, want 60000", cfg.JobExpiryMS)
	}
	if cfg.RunningCapMS != 45000 {
		t.Errorf("RunningCapMS = %d, want 45000", cfg.RunningCapMS)
	}
	if cfg.BatchWeight != 0.25 {
		t.Errorf("BatchWeight = %v, want 0.25", cfg.BatchWeight)
	}
	if cfg.BatchExpiryMS != 7200000 {
		t.Errorf("BatchExpiryMS = %d, want 7200000", cfg.BatchExpiryMS)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestBatchWeightInvalidFallsBack(t *testing.T) {
	setEnv(t)
	for _, bad := range []string{"abc", "-1", "0", "1e30"} {
		t.Setenv("POOL_BATCH_WEIGHT", bad)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() with POOL_BATCH_WEIGHT=%s: %v", bad, err)
		}
		if cfg.BatchWeight != DefaultBatchWeight {
			t.Errorf("BatchWeight with %s = %v, want the default %v", bad, cfg.BatchWeight, DefaultBatchWeight)
		}
	}
}

func TestInvalidLogLevel(t *testing.T) {
	setEnv(t)
	t.Setenv("COORD_LOG_LEVEL", "verbose")
	if _, err := Load(); err == nil {
		t.Error("Load() with invalid log level: got nil, want error")
	}
}
