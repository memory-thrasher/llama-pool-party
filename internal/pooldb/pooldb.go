// Package pooldb is the coordinator's postgres access layer.
//
// The coordinator is stdlib-only by design (no external Go Postgres driver),
// so all access shells out to the psql(1) client, exactly like the schema
// package. DSNs are libpq-style conninfo strings (e.g. "user=sid
// dbname=pool_dev_scratch"); every statement runs under ON_ERROR_STOP=1 so a
// failed statement fails the call.
package pooldb

import (
	"context"
	"crypto/rand"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// psqlPath returns the psql binary, overridable for tests.
var psqlPath = "psql"

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// DB is a postgres handle (a DSN, in practice).
type DB struct {
	dsn string
}

// New returns a handle for the database named by the libpq-style dsn.
func New(dsn string) *DB {
	return &DB{dsn: dsn}
}

// DSN returns the handle's conninfo.
func (db *DB) DSN() string { return db.dsn }

// sq quotes s as a SQL text literal. Callers validate values first (UUIDs via
// quuid, tags via the router-side allow-list); sq doubles single quotes, which
// is sufficient for text columns.
func sq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// quuid validates s as a UUID and returns it quoted lowercase, or "" when s is
// not a valid UUID. Callers treat "" as a reject signal.
func quuid(s string) string {
	if !uuidRE.MatchString(s) {
		return ""
	}
	return sq(strings.ToLower(s))
}

// Exec runs one SQL statement; fails the call on any error.
func (db *DB) Exec(ctx context.Context, stmt string) error {
	cmd := exec.CommandContext(ctx, psqlPath,
		"-v", "ON_ERROR_STOP=1",
		"-q", "-X",
		"-d", db.dsn,
		"-c", stmt,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pooldb Exec: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ExecMulti runs a multi-statement script (the schema v1 script, e.g.); fails
// the call on any statement error.
func (db *DB) ExecMulti(ctx context.Context, script string) error {
	cmd := exec.CommandContext(ctx, psqlPath,
		"-v", "ON_ERROR_STOP=1",
		"-q", "-X",
		"-d", db.dsn,
		"-c", script,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pooldb ExecMulti: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Query runs a query and returns the rows, each row's columns joined with ','.
func (db *DB) Query(ctx context.Context, query string) ([]string, error) {
	cmd := exec.CommandContext(ctx, psqlPath,
		"-v", "ON_ERROR_STOP=1",
		"-At", "-F,", "-q", "-X",
		"-d", db.dsn,
		"-c", query,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("pooldb Query: %v", err)
	}
	var rows []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			rows = append(rows, line)
		}
	}
	return rows, nil
}

// FirstInt is a convenience wrapper for int-returning scalar queries.
func (db *DB) FirstInt(ctx context.Context, query string) (int, error) {
	s, err := db.oneString(ctx, query)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(s)
}

// FirstBool is a convenience wrapper for boolean-returning scalar queries
// (postgres renders booleans as "t"/"f"; "select 1 ..." existence checks return "1").
func (db *DB) FirstBool(ctx context.Context, query string) (bool, error) {
	s, err := db.oneString(ctx, query)
	if err != nil {
		return false, err
	}
	return s == "t" || s == "1", nil
}

// FirstString returns the first row's first column as a string, or "" (nil
// error) when there is no row. For scalar lookups (user ids, job ids, ...).
func (db *DB) FirstString(ctx context.Context, query string) (string, error) {
	s, err := db.oneString(ctx, query)
	if err != nil {
		return "", nil
	}
	return s, nil
}

// Count is a convenience wrapper for count(*) queries.
func (db *DB) Count(ctx context.Context, query string) (int, error) {
	s, err := db.oneString(ctx, query)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(s)
}

// I64 renders an int64 SQL literal.
func I64(n int64) string { return strconv.FormatInt(n, 10) }

// S renders a string SQL literal; empty renders NULL.
func S(s string) string {
	if s == "" {
		return "NULL"
	}
	return sq(s)
}

// NewUUID returns a random RFC 4122 v4 UUID string.
func NewUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("pooldb: crypto/rand: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// UUID renders a UUID SQL literal; empty renders NULL (callers validate first).
func UUID(s string) string {
	if q := quuid(s); q != "" {
		return q
	}
	return "NULL"
}

func (db *DB) oneString(ctx context.Context, query string) (string, error) {
	rows, err := db.Query(ctx, query)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", fmt.Errorf("pooldb: no rows")
	}
	return rows[0], nil
}
