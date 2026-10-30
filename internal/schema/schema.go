// Package schema applies the coordinator's postgres schema (schema/0001_init.sql)
// idempotently.
//
// The coordinator is stdlib-only by design (no external Go Postgres driver), so
// Apply shells out to the psql(1) client. The DSN is a libpq-style
// keyword/value conninfo string, passed to psql as the database argument:
//
//	Apply(ctx, "user=sid dbname=pool_dev_scratch")
package schema

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// schemaSQLPath returns the repo-root location of the schema v1 script, resolved
// relative to this source file (embed cannot reach outside the package).
func schemaSQLPath() (string, error) {
	_, thisFile, ok := sourceFile()
	if !ok {
		return "", fmt.Errorf("schema: unable to locate source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "../../schema/0001_init.sql"), nil
}

// psqlPath returns the psql binary, overridable for tests.
var psqlPath = "psql"

// osReadFile is os.ReadFile, kept as a variable for testability.
var osReadFile = os.ReadFile

// sourceFile returns the absolute path of the file containing this call's
// package, used to locate sibling SQL scripts without build tags.
func sourceFile() (string, string, bool) {
	_, file, _, ok := runtime.Caller(0)
	return "", file, ok
}

// repoRootDir returns the repo root (two levels up from internal/schema),
// used by tests to locate sql/bootstrap_admin.sql.
func repoRootDir() (string, error) {
	_, thisFile, ok := sourceFile()
	if !ok {
		return "", fmt.Errorf("schema: unable to locate source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "../.."), nil
}

// Apply runs schema v1 against the database named by dsn, idempotently.
// The script is schema/0001_init.sql (idempotent by construction: every
// statement is CREATE ... IF NOT EXISTS / INSERT ... ON CONFLICT DO NOTHING).
func Apply(ctx context.Context, dsn string) error {
	path, err := schemaSQLPath()
	if err != nil {
		return err
	}
	b, err := osReadFile(path)
	if err != nil {
		return fmt.Errorf("schema: read %s: %w", path, err)
	}
	return ApplySQL(ctx, dsn, string(b))
}

// ApplyFile runs the SQL script at path against the database named by dsn.
func ApplyFile(ctx context.Context, dsn, path string) error {
	b, err := osReadFile(path)
	if err != nil {
		return fmt.Errorf("schema: read %s: %w", path, err)
	}
	return ApplySQL(ctx, dsn, string(b))
}

// ApplySQL runs raw SQL against dsn, idempotently (callers must make it so).
func ApplySQL(ctx context.Context, dsn, sqlStr string) error {
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("schema: empty DSN")
	}
	if strings.TrimSpace(sqlStr) == "" {
		return fmt.Errorf("schema: empty SQL")
	}
	cmd := exec.CommandContext(ctx, psqlPath,
		"-v", "ON_ERROR_STOP=1",
		"-q", "-X",
		"-d", dsn,
		"-c", sqlStr,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("schema Apply: psql failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// psqlExec runs a single statement against dsn.
func psqlExec(ctx context.Context, dsn, statement string) error {
	cmd := exec.CommandContext(ctx, psqlPath,
		"-v", "ON_ERROR_STOP=1",
		"-q", "-X",
		"-d", dsn,
		"-c", statement,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("psql exec: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// psqlQuery runs a query against dsn and returns the concatenated rows
// (each row's columns joined with ',').
func psqlQuery(ctx context.Context, dsn, query string) ([]string, error) {
	cmd := exec.CommandContext(ctx, psqlPath,
		"-At", "-F,", "-q", "-X",
		"-d", dsn,
		"-c", query,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("psql query: %v", err)
	}
	var rows []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			rows = append(rows, line)
		}
	}
	return rows, nil
}
