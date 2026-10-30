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
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return cfg, fmt.Errorf("config: invalid COORD_LOG_LEVEL %q", cfg.LogLevel)
	}
	return cfg, nil
}

// Validate returns an error if the configuration is not usable.
func (c Config) Validate() error {
	if c.DSN == "" {
		return errors.New("config: COORD_DSN is required for database-backed operation")
	}
	return nil
}
