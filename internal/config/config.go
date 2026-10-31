// Package config loads coordinator configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Defaults shared across the service.
const (
	DefaultAddr      = "127.0.0.1:8080"
	DefaultStaticDir = "./static"
	DefaultLogLevel  = "info"
)

// Defaults for the pool protocol plane (WS-06).
const (
	// DefaultDataDir is where request bodies are staged while queued (POOL_DATA_DIR).
	DefaultDataDir = "./data"
	// DefaultOfferLeaseMS is the default lease a router holds on an accepted
	// job before the coordinator may re-offer it (POOL_OFFER_LEASE_MS).
	DefaultOfferLeaseMS = 5000
	// DefaultMaxBounces is the per-job offer-bounce limit before the
	// job is parked and retried after a delay (POOL_MAX_BOUNCES, D-G1).
	DefaultMaxBounces = 3
	// DefaultHeartbeatMS is the SSE heartbeat interval on /pool/subscribe
	// (POOL_HEARTBEAT_MS).
	DefaultHeartbeatMS = 25000
)

// Config is the coordinator's runtime configuration, populated from env vars.
type Config struct {
	// Addr is the listen address (COORD_ADDR).
	Addr string
	// DSN is the postgres connection string (COORD_DSN).
	DSN string
	// StaticDir is the portal's static asset directory (COORD_STATIC_DIR).
	StaticDir string
	// LogLevel is the logging verbosity (COORD_LOG_LEVEL).
	LogLevel string
	// DataDir is where request bodies are staged while queued (POOL_DATA_DIR).
	DataDir string
	// OfferLeaseMS is the default offer lease after accept (POOL_OFFER_LEASE_MS).
	OfferLeaseMS int
	// MaxBounces caps offer rejections per job before parking (POOL_MAX_BOUNCES).
	MaxBounces int
	// HeartbeatMS is the SSE heartbeat interval (POOL_HEARTBEAT_MS).
	HeartbeatMS int
}

// env returns the value of name, or def when unset/empty.
func env(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		Addr:      env("COORD_ADDR", DefaultAddr),
		DSN:       env("COORD_DSN", ""),
		StaticDir: env("COORD_STATIC_DIR", DefaultStaticDir),
		LogLevel:  env("COORD_LOG_LEVEL", DefaultLogLevel),
		DataDir: env("POOL_DATA_DIR", DefaultDataDir),
		OfferLeaseMS: atoi(env("POOL_OFFER_LEASE_MS", ""), DefaultOfferLeaseMS),
		MaxBounces:  atoi(env("POOL_MAX_BOUNCES", ""), DefaultMaxBounces),
		HeartbeatMS: atoi(env("POOL_HEARTBEAT_MS", ""), DefaultHeartbeatMS),
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return cfg, fmt.Errorf("config: invalid COORD_LOG_LEVEL %q", cfg.LogLevel)
	}
	return cfg, nil
}

// Validate returns an error if the configuration is not usable.
// atoi parses v as a positive int, falling back to def when v is empty
// or not a positive integer.
func atoi(v string, def int) int {
	if v == "" {
		return def
	}
	n := 0
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return def
		}
		n = n*10 + int(ch-'0')
		if n > 1<<30 {
			return def
		}
	}
	if n == 0 {
		return def
	}
	return n
}

func (c Config) Validate() error {
	if c.DSN == "" {
		return errors.New("config: COORD_DSN is required for database-backed operation")
	}
	return nil
}
