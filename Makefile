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

# conninfo for the scratch database, derived from PGDSN: an existing dbname= token
# is replaced, otherwise dbname=pool_dev_scratch is appended. Fails loudly at
# parse time if the derivation did not land.
PGDSN_SCRATCH := $(shell echo "$(PGDSN)" | sed 's/dbname=[^ ]*/dbname=$(SCRATCH)/; t; s/$$/ dbname=$(SCRATCH)/')

# Does the derived DSN actually name the scratch database?
PGDSN_SCRATCH_OK := $(findstring dbname=$(SCRATCH),$(PGDSN_SCRATCH))
ifeq ($(PGDSN_SCRATCH_OK),)
$(error derived PGDSN_SCRATCH '$(PGDSN_SCRATCH)' lacks 'dbname=$(SCRATCH)'; cannot derive scratch conninfo from PGDSN '$(PGDSN)')
endif

PSQL := psql
GO   ?= go

.PHONY: all build vet test web schema-apply bootstrap clean

all: build

build:
	$(GO) build ./...

# Build the portal SPA into web/build (embedded into the binary via
# web/embed.go). Requires Node.js + npm. The committed web/build keeps
# `make build` working without it.
web:
	cd web && npm ci && npm run build

vet:
	$(GO) vet ./...

# Integration test; skips cleanly when postgres is unreachable.
test:
	PGDSN="$(PGDSN)" $(GO) test ./...

# Apply internal/schema/0001_init.sql (embedded into the coordinator binary
# via go:embed) to $(SCRATCH), creating it if missing.
# Idempotent: safe to run repeatedly.
schema-apply:
	@echo "PGDSN=$(PGDSN)"
	@echo "PGDSN_SCRATCH=$(PGDSN_SCRATCH)"
	$(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN)" -c "select datname from pg_database where datname = '$(SCRATCH)'" | grep -q "$(SCRATCH)" || $(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN)" -c "create database $(SCRATCH)"
	$(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN_SCRATCH)" -c "$$(cat internal/schema/0001_init.sql)"

# Run sql/bootstrap_admin.sql against $(SCRATCH). Idempotent: safe to run twice.
bootstrap:
	$(PSQL) -v ON_ERROR_STOP=1 -q -X -d "$(PGDSN_SCRATCH)" -c "$$(cat sql/bootstrap_admin.sql)"

clean:
	$(GO) clean ./...
