# pool-coordinator build/test targets (WS-02).
#
# PGDSN: libpq-style conninfo DSN for the control database (used to create the
# scratch database pool_dev_scratch).
#   The default is the DSN that works on the build host: peer auth as the local
#   `sid` OS user over the unix socket (no TCP password needed).
#
# The scratch database itself is always `pool_dev_scratch`; schema-apply and
# bootstrap both target it, creating it if missing.

PGDSN    ?= user=sid dbname=postgres
SCRATCH  ?= pool_dev_scratch

# conninfo for the scratch database, derived from PGDSN (same user/host, db swapped).
PGDSN_SCRATCH := $(shell echo "$(PGDSN)" | sed 's/dbname=[^ ]*/dbname=$(SCRATCH)/')

PSQL := psql
GO   ?= go

.PHONY: all build vet test schema-apply bootstrap clean

all: build

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

# Integration test; skips cleanly when postgres is unreachable.
test:
	PGDSN="$(PGDSN)" $(GO) test ./...

# Apply schema/0001_init.sql to $(SCRATCH), creating it if missing.
# Idempotent: safe to run repeatedly.
schema-apply:
	@echo "PGDSN=$(PGDSN)"
	$(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN)" -c "select datname from pg_database where datname = '$(SCRATCH)'" | grep -q "$(SCRATCH)" || $(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN)" -c "create database $(SCRATCH)"
	$(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN_SCRATCH)" -c "$$(cat schema/0001_init.sql)"

# Run sql/bootstrap_admin.sql against $(SCRATCH). Idempotent: safe to run twice.
bootstrap:
	$(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN_SCRATCH)" -c "$$(cat sql/bootstrap_admin.sql)"

clean:
	$(GO) clean ./...
