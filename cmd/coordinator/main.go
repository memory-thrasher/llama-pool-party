// Command coordinator is the pool coordinator entrypoint (WS-02/WS-06).
// It loads config from the environment, applies the embedded schema to
// postgres, and serves the HTTP plane (protocol endpoints, dispatch
// engine, fake-router harness) on cfg.Addr.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/pooldb"
	"pool-coordinator/internal/schema"
	"pool-coordinator/internal/server"
)

const Version = "0.1.0-dev"

// conninfoSecretKeys are libpq conninfo keys whose values must never reach logs:
// a conninfo can carry a password (COORD_DSN may include one as of WS-06).
var conninfoSecretKeys = []string{"password", "passfile", "sslkey", "sslpassword"}

// redactDSN replaces the values of secret-bearing conninfo keys in dsn
// with '*****' so the DSN is safe to print.
func redactDSN(dsn string) string {
	words := strings.Fields(dsn)
	for i, w := range words {
		for _, k := range conninfoSecretKeys {
			if _, found := strings.CutPrefix(w, k+"="); found {
				words[i] = k + "=*****"
				break
			}
		}
	}
	return strings.Join(words, " ")
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: config load failed: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: config validate failed: %v\n", err)
		os.Exit(1)
	}

	// The coordinator is postgres-backed (D-A2); apply the embedded schema
	// before serving.
	db := pooldb.New(cfg.DSN)
	if err := schema.Apply(ctx, cfg.DSN); err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: schema apply failed: %v\n", err)
		os.Exit(1)
	}

	srv := server.New(cfg, db, cfg.DataDir, os.Stdout)

	fmt.Printf("pool-coordinator %s starting\n", Version)
	fmt.Printf("  addr:      %s\n", cfg.Addr)
	fmt.Printf("  data dir:   %s\n", cfg.DataDir)
	fmt.Printf("  log level:  %s\n", cfg.LogLevel)
	fmt.Printf("  started:  %s\n", time.Now().UTC().Format("2006-01-02T15:04:05Z"))

	if err := srv.Run(ctx, cfg.Addr); err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: serve failed: %v\n", err)
		os.Exit(1)
	}
}
