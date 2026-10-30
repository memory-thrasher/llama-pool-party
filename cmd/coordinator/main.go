// Command coordinator is the pool coordinator entrypoint (WS-02 skeleton).
// It loads config from the environment and prints a startup banner with the
// version. The HTTP server itself is WS-06 scope.
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"pool-coordinator/internal/config"
)

const Version = "0.0.0-dev"

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

	fmt.Printf("pool-coordinator %s starting\n", Version)
	fmt.Printf("  addr:      %s\n", cfg.Addr)
	fmt.Printf("  dsn:       %s\n", redactDSN(cfg.DSN))
	fmt.Printf("  static:    %s\n", cfg.StaticDir)
	fmt.Printf("  log lvl:  %s\n", cfg.LogLevel)
	fmt.Printf("  started:  %s\n", time.Now().UTC().Format(time.RFC3339))
}
