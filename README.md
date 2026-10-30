# pool-coordinator

The pool coordinator: a small Go service (plus postgres schema and SQL bootstrap) for
the llama.cpp-pool volunteer GPU network. This repo is the standing home for the
coordinator workstream; the protocol contract lives in `protocol/` (WS-01).

## Layout

- `cmd/coordinator` - service entrypoint; loads config, prints a startup banner.
- `internal/config` - env-based configuration (`COORD_ADDR`, `COORD_DSN`,
  `COORD_STATIC_DIR`, `COORD_LOG_LEVEL`) with unit tests.
- `internal/schema` - idempotent application of the schema (`Apply`), with an
  integration test that applies it to a scratch database twice.
- `schema/0001_init.sql` - schema v1: users, tokens, routers, models, classes,
  queue, records, continuity, exclusions. Plain `CREATE ... IF NOT EXISTS` /
  `ON CONFLICT DO NOTHING` throughout.
- `sql/bootstrap_admin.sql` - idempotent bootstrap of the `admin` user (D-J4).

## Build & test

Requires Go (see `go.mod`) and, for the integration test, a reachable local
postgres.

```sh
make build   # go build ./...
make vet    # go vet ./...
make test   # go test ./... (integration test skips cleanly if postgres is down)
```

## Schema

```sh
make schema-apply   # apply schema v1 to the scratch DB (pool_dev_scratch), idempotent
make bootstrap     # apply sql/bootstrap_admin.sql to the scratch DB, idempotent
```

`PGDSN` selects the target database conninfo (default: `user=sid dbname=postgres`,
peer auth over the unix socket on the build host). The scratch DB is always
`pool_dev_scratch`.

## Config

| Env var         | Default          | Purpose                  |
|-----------------|----------------|--------------------------|
| `COORD_ADDR`    | `127.0.0.1:8080` | listen address (WS-06)   |
| `COORD_DSN`     | *(empty)*        | postgres DSN (required at runtime) |
| `COORD_STATIC_DIR`| `./static`     | portal asset dir (WS-07) |
| `COORD_LOG_LEVEL` | `info`        | log verbosity            |

## Notes

- The HTTP server itself is WS-06 scope; this tree currently only loads config
  and prints a banner.
- No external Go dependencies: `internal/schema` shells out to `psql`.
- The `admin/admin` bootstrap row is intentional (D-J4 recovery path); it is the
  only realistic credential in this repo.
