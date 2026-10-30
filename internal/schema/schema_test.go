package schema

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// envPGDSN is the environment variable carrying the libpq-style conninfo DSN
// for the test's target database. It must be reachable for the integration
// test to run; otherwise the test skips cleanly.
const envPGDSN = "PGDSN"

// defaultPGDSN is the conninfo that works on the build host (peer auth as the
// `sid` OS user over the local socket, no TCP password needed).
const defaultPGDSN = "user=sid dbname=postgres"

// psqlConn runs psql for a read-only conninfo check. Returns an error when the
// server is unreachable so callers can skip cleanly.
func psqlConn(dsn string) error {
	cmd := exec.Command("psql", "-v", "ON_ERROR_STOP=1", "-q", "-X", "-d", dsn, "-c", "select 1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("psql connect %q: %v: %s", dsn, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// targetDSN returns the PGDSN from the environment, defaulting to defaultPGDSN.
func targetDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv(envPGDSN))
	if dsn == "" {
		dsn = defaultPGDSN
	}
	return dsn
}

// ensureScratchDB creates pool_dev_scratch (in targetDB's cluster) if it is missing.
func ensureScratchDB(ctx context.Context, pgDSN string) (scratchDSN string, err error) {
	rows, err := psqlQuery(ctx, pgDSN, "select datname from pg_database where datname = 'pool_dev_scratch'")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		if err := psqlExec(ctx, pgDSN, "create database pool_dev_scratch"); err != nil {
			return "", fmt.Errorf("create pool_dev_scratch: %w", err)
		}
	}
	scratchDSN = strings.ReplaceAll(pgDSN, "dbname=postgres", "dbname=pool_dev_scratch")
	return scratchDSN, nil
}

// TestApplyTwiceScratchDB applies schema v1 and the bootstrap script to the
// scratch DB pool_dev_scratch twice each, proving idempotency end to end.
// Skips cleanly when postgres is unreachable.
func TestApplyTwiceScratchDB(t *testing.T) {
	pgDSN := targetDSN(t)
	if err := psqlConn(pgDSN); err != nil {
		t.Skipf("postgres unreachable via PGDSN=%q: %v", pgDSN, err)
	}

	ctx := context.Background()
	scratchDSN, err := ensureScratchDB(ctx, pgDSN)
	if err != nil {
		t.Fatalf("ensure scratch db: %v", err)
	}
	t.Logf("scratch DSN: %s", scratchDSN)

	// 1. schema v1 applied twice
	if err := Apply(ctx, scratchDSN); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := Apply(ctx, scratchDSN); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	t.Log("Apply x2: OK")

	// 2. bootstrap applied twice
	bootstrapPath, err := bootstrapSQLPath()
	if err != nil {
		t.Fatalf("bootstrap path: %v", err)
	}
	if err := ApplyFile(ctx, scratchDSN, bootstrapPath); err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	if err := ApplyFile(ctx, scratchDSN, bootstrapPath); err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	t.Log("bootstrap x2: OK")

	// 3. exactly one admin row
	rows, err := psqlQuery(ctx, scratchDSN, "select email, role from users where email = 'admin'")
	if err != nil {
		t.Fatalf("admin count query: %v", err)
	}
	if len(rows) != 1 || rows[0] != "admin,admin" {
		t.Fatalf("admin rows = %v, want exactly one row 'admin,admin'", rows)
	}
	t.Logf("admin rows: %v (want exactly 1)", rows)
}

// bootstrapSQLPath locates sql/bootstrap_admin.sql relative to this source file.
func bootstrapSQLPath() (string, error) {
	root, err := repoRootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "sql", "bootstrap_admin.sql"), nil
}
