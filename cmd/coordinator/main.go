// Command coordinator is the pool coordinator entrypoint (WS-02 skeleton).
// It loads config from the environment and prints a startup banner with the
// version. The HTTP server itself is WS-06 scope.
package main

import (
	"fmt"
	"os"
	"time"

	"pool-coordinator/internal/config"
)

const Version = "0.0.0-dev"

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: config load failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("pool-coordinator %s starting\n", Version)
	fmt.Printf("  addr:      %s\n", cfg.Addr)
	fmt.Printf("  dsn:       %s\n", cfg.DSN)
	fmt.Printf("  static:    %s\n", cfg.StaticDir)
	fmt.Printf("  log lvl:  %s\n", cfg.LogLevel)
	fmt.Printf("  started:  %s\n", time.Now().UTC().Format(time.RFC3339))
}
