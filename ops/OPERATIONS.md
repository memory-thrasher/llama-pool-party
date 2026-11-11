# Coordinator Runbook

This document provides the operational runbook for the `pool-coordinator`.

## Startup

The coordinator is a postgres-backed service. On startup, it automatically applies the embedded schema to the configured database.

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `COORD_ADDR` | `127.0.0.1:8080` | The listen address (HTTP plane). |
| `COORD_DSN` | (Required) | PostgreSQL connection string (libpq conninfo). |
| `COORD_STATIC_DIR` | `./static` | Static file directory for the embedded portal SPA (fallback when the `go:embed` asset is absent). |
| `COORD_LOG_LEVEL` | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`). |
| `POOL_DATA_DIR` | `./data` | Directory for staging request bodies. |
| `POOL_OFFER_LEASE_MS` | `5000` | Lease duration held by a router after accepting a job. |
| `POOL_MAX_BOUNCES` | `3` | Per-job offer rejection limit before parking. |
| `POOL_HEARTBEAT_MS` | `25000` | SSE heartbeat interval on `/pool/subscribe`. |
| `POOL_CONT_IDLE_MS` | `60000` | Continuity skip-the-line window: a continuation served ahead of queue if the last-serving member has been idle less than this (D-E2). |
| `POOL_CONT_PREFER_MS` | `600000` | Continuity soft-preference window: up to this long after last service the member is preferred (no skip) (D-E2). |
| `POOL_JOB_EXPIRY_MS` | `1800000` | How long a live queued job may wait before it expires (D-G4). |
| `POOL_RUNNING_CAP_MS` | `300000` | Cap on how long a job may stay in `running` before it is terminal-failed (D-G2). |
| `POOL_FAIR_WINDOW_MS` | `604800000` | The D-F2 fairness rolling window (1 week default). |
| `POOL_FAIR_TOPK` | `10` | The D-F3 selection window: the k oldest pending items a user competes in. |
| `POOL_PARK_BACKOFF_MS` | `500` | Base backoff between re-offer attempts for a parked job. |
| `POOL_DEDUPE_WINDOW_MS` | `300000` | The D-G5 intake dedupe window: a byte-identical re-intake within this returns the existing job (200); beyond it a fresh job (201). |
| `POOL_BATCH_WEIGHT` | `0.5` | The D-H1 weight of a batch job's consumed tokens in the fairness ratio (range `(0,1]`). |
| `POOL_BATCH_EXPIRY_MS` | `86400000` | How long a `priority=batch` job may wait before it expires (24h default). |
| `POOL_PORTAL_SESSION_TTL_MS` | `604800000` | The portal cookie session lifetime (7 days default). |

### Startup Banner
The coordinator prints its version, address, data directory, log level, and UTC start time to `stdout` upon successful initialization.

## Health

- **Endpoint**: `GET /healthz`
- **Authentication**: None required.
- **Status**: Returns a successful response if the HTTP server is running and the database is reachable.

A router is considered **CONNECTED** if its `last_seen_at` timestamp is within the last 90 seconds.

## Data Management

### Storage Locations
- **Database (PostgreSQL)**: Stores the job queue (`queue_jobs`), telemetry (`records`), continuity/affinity data (`continuity`), user exclusions (`exclusions`), router/model metadata (`routers`, `router_models`, `class_members`), membership (`users`, `user_tokens`), async batches (`batches`), portal sessions (`portal_sessions`), and per-user settings (`user_settings`).
- **Filesystem (`POOL_DATA_DIR`)**: Stores request bodies in `jobs/` subdirectories (transient; purged on terminal).

## Router (fork) configuration

The pool is a pure additive overlay on `llama-server` router mode. When the
coordinator is not configured the router is behaviorally identical to upstream
router mode (D-A3).

### CLI flags (router mode only)

| Flag | Env override | Description |
|---|---|---|
| `--pool-server URL` | `LLAMA_ARG_POOL_SERVER` | The pool coordinator URL (`http://` or `https://`). When set, the member token env var is required. When unset, the pool is off. |
| `--pool-name NAME` | `LLAMA_ARG_POOL_NAME` | The router's display name (default: hostname, max 64 chars). |

`--models-dir` is required when the pool is on: the stable router id (UUIDv4) is persisted at `<models_dir>/.pool/router-id`.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `LLAMA_POOL_TOKEN` | (Required when pool on) | The member token. **Never logged.** |
| `POOL_BATCH_DIR` | `data/pool-batches` | The router's capture directory for async batch results (D-H1). Relative to the router's working directory. |
| `POOL_BATCH_RESULT_TTL_MS` | `86400000` | How long captured batch results are retained before the sweep removes them (24h default). |
| `LLAMA_POOL_DEBUG_RT` | (unset) | Debug only: when set, the WS-17 cross-template reformat asserts its a->b->a round-trip on every reformat and terminal-fails the job on a broken round trip. |

### Body Lifecycle
Request bodies are staged on intake and are the **ONLY** body storage. 
1. **Intake**: The body is `PUT` to `/pool/jobs/{id}/body`.
2. **Queueing**: The job is staged in the data directory.
3. **Purge**: The body file is deleted immediately upon job completion, failure, or expiration.

### Anti-Memory Guarantee
The coordinator adheres to a strict anti-memory policy. Logs and telemetry records (`records` table) contain only identifiers, tags, counts, and timings. **No prompt or response text is ever stored or logged.**

## Maintenance

### Database Schema
The schema is managed via `make schema-apply`. This is idempotent and safe to run repeatedly. It uses `CREATE IF NOT EXISTS` and `ON CONFLICT DO NOTHING` patterns.

### Bootstrapping
To initialize a new database with seed data (e.g., default size classes), use `make bootstrap`. This is also idempotent.

The bootstrap seeds an `admin` user whose `default` token is the known, published value `sha256('admin')`. **Rotate it immediately after bootstrap**: issue a fresh opaque 256-bit token for the admin in the portal (or revoke `default`). Because the bootstrap upsert restores the known token on re-run, treat the seeded `default` token strictly as a recovery credential, never a standing login.

### Backup
To back up the coordinator state, perform a `pg_dump` of the configured PostgreSQL database. The `POOL_DATA_DIR` is considered transient and should not be backed up.

### Shutdown
The coordinator supports graceful shutdown via `SIGINT` (Ctrl+C) or `SIGTERM`. It will stop the dispatch engine, close all active SSE sessions, and close all response relay pipes before exiting with code 0.

## Log Hygiene
- All operational logs (startup banner, dispatch events) are written to `stdout`.
- Error messages supplied by routers are capped at 200 characters to prevent log pollution.

## Pre-production checklist (rotated-lens final review, WS-19)

The rotated-lens final review (`loop/reports/rotated-lens-final.md`) returned
**SHIP-FOR-GROUP** with no P0/P1. Three operational steps before pointing the
group's real harnesses at the pool (deployment hygiene, no code change):

1. **Terminate TLS at the edge.** D-J1 ("TLS on every pool link") is an
   operational guarantee the code does not enforce: `--pool-server` accepts
   `http://` and `COORD_ADDR` defaults to a bare HTTP listener. Terminate TLS at
   a reverse proxy on the coordinator VM and give members `https://` pool-server
   URLs; do **not** expose the bare HTTP listener (RESID-FINAL-1).
2. **Rotate the bootstrap admin token.** After `make bootstrap`, immediately
   rotate the admin `default` token (see Bootstrapping). A known default token is
   a live admin credential until rotated (RESID-FINAL-2).
3. **Small multi-member soak.** The live slice is a 2-router topology. Run a
   short 3-4 member soak with real GGUFs before the group leans on it - cheap
   insurance, not a blocker.

### The localhost exception (local-only testing and demos)

For local-only stacks (this machine, loopback only), the pool link runs
`http://127.0.0.1` - the sanctioned exception to D-J1: there is no
certificate to terminate against on loopback, and nothing leaves the box.
- The pool link already accepts `http://` `--pool-server` URLs; use loopback
  URLs only (`http://127.0.0.1:PORT`). Never an `http://` address a remote
  host can reach (RESID-FINAL-1).
- The portal's `Secure` session cookie works over loopback http in current
  browsers (verified with chromium 141: the cookie is stored and sent on
  `http://127.0.0.1`).
- `ops/run-demo.sh` brings up exactly this local-only stack (router +
  coordinator + a prepared stock-pi agent dir) and is the demo entry point.

## Local demo (run-demo.sh)

`ops/run-demo.sh` brings up the full local-only pool stack - the fork
router with the pool overlay, the coordinator on a scratch Postgres db, and a
prepared stock-pi agent dir - against a router config you pass in. The
borrowed config is never modified: the script works on an augmented copy in
its own scratch dir, with model files reached through symlinks (the models
mount is read-only).

The demo router is the **Vulkan build** of the fork (`build-vulkan/`, built
with `GGML_VULKAN=ON`; the banked CPU build in `build/` is untouched for the
test suites). It uses the same `VK_DRIVER_FILES` ICD selection as the
production router (`llama.sh`), so `Vulkan0/1/2` mean the same cards:
3090 Ti / 1080 / R9700.

```
# Mode A: coexist with the production router (small model on a spare card)
ops/run-demo.sh --no-autoload \
  --tag Qwen-Drive-1.0-4B-Q5_K_M=pool-image-text-xs \
  --device Qwen-Drive-1.0-4B-Q5_K_M=Vulkan2 --ctx Qwen-Drive-1.0-4B-Q5_K_M=32768 \
  --smoke

# Mode B: production stopped, the full borrowed fleet runs as tailored
# (all three models load on their tuned cards/ctx; tag each as a class)

# Borrow a LIVE config instead of the owned snapshot:
# ops/run-demo.sh --router-config /home/sid/projects/pi/presets.ini ...
```

`ops/demo-preset.ini` is the demo's owned snapshot of the borrowed
projects/pi config as of 2026-11-07 (the 3-model fleet with its tuned
cards/ctx). The owner's live config is re-planned from time to time
(models rotate in and out), and a demo that borrows it live would silently
break the moment the demo model's section is rotated out - the snapshot
keeps Mode A stable. `--router-config` remains for borrowing a live config
when it matches.

**pi context sizing.** pi computes `maxTokens = contextWindow - promptEstimate
- 4096` (a fixed safety margin) and clamps at 1. A class whose advertised ctx
is ~4096 or less therefore can never answer a pi turn (`max_tokens: 1`,
stopReason `length`). The borrowed tuned ctx (32768, KV in RAM via
`cache-ram`) leaves a healthy output budget; that is why the Mode A command
keeps it.

`--tag ALIAS=CLASS` advertises that model under a pool class (the six default
size classes are already seeded in the schema). `--ctx` / `--device` override
a single alias in the augmented copy (Mode A moves a demo model onto a spare
card while production holds the rest; keep the borrowed ctx - the KV cache is
in RAM via `cache-ram`, so ctx size is not VRAM-bound, and pi needs an
advertised ctx well above ~4096 to answer at all). `--no-autoload` starts the router with
nothing loaded - it also suppresses the borrowed config's own
`load-on-startup = 1` lines (otherwise a fleet-preloading config would try to
load everything on cards production still holds), then loads the tagged models
explicitly. `--smoke` runs the scripted end-to-end check (pool link, portal
login + metrics, a stock-pi turn on the class, records / anti-memory
accounting).

Lifecycle (kills by recorded PID only):

```
ops/run-demo.sh --status            # what is up, log paths
ops/run-demo.sh --stop [--drop-db]  # teardown
```

After bring-up the script prints the runbook: point a second terminal at
`PI_CODING_AGENT_DIR=<work>/pi-agent` and run `pi`; the pool classes appear
next to the local models in `/model`, and the portal is at
`http://127.0.0.1:8125/` (login token `admin`).

**The scratch agent dir is mandatory.** `<work>/pi-agent` (auth + model
store generated from the demo catalog, including the class entry with its
advertised ctx) is the only pi dir that can use the pool classes: the
owner's real agent dir holds a store pointing at the production router, so
pi there would not know the class (custom-model-id fallback + 400). The
script's smoke pi turn runs with `PI_CODING_AGENT_DIR=<work>/pi-agent`
for the same reason.

Known limits of the demo: it is a **single-router** stack (the cross-member
path is proven by the live slice, not the demo); the demo db is scratch
(dropping it resets the pool history); and the bootstrap admin token is
`admin` by design for the demo - rotate it per the checklist before any real
use.

## Client keep-alive while queued (2026-11-09)

**Symptom:** pi timed out waiting in the pool queue, even in one-user tests.
**Root cause:** pi's http idle timeout (`httpIdleTimeoutMs`, default **30s**,
undici `bodyTimeout`) kills the request if the server sends no bytes for 30s.
While a job is queued at the coordinator, the origin relay sent the 200 +
headers and then nothing (the coordinator's own 25s relay heartbeats were
dropped by the router) -> any queue wait over ~30s killed the client.

**Fix (server-side, no client config change):** the origin router now emits
an SSE comment `: keepalive` every 15s on streaming requests, from the 200
until the first real data line (then it stops; token flow keeps the client
alive). SSE comments are standard and ignored by pi's stream parser (verified
in pi's dist) and by other OpenAI-compatible clients. Blob (non-streaming)
responses cannot carry keep-alives - such clients still need their own idle
timeout above the worst-case queue wait. The keep-alive is bounded by the job
expiry (30min default).

## Failure logging (2026-11-09)

Every failure code sent to a client is now logged on both sides (message
text is server-generated; request/response content is never logged, D-I1):

**Router (fork)** - grep these in the router log:
- `client error response: status=.. type=.. message=".." [n_prompt_tokens=.. n_ctx=..]`
  - one line for EVERY error response the router sends to a client
    (completion, chat, slots, ...). The size fields appear for
    exceed_context_size errors and say exactly how big the request was vs
    the slot's context.
- `Context size has been exceeded. off = .. n_batch = .. ret = .. n_prompt_tokens = .., n_ctx = ..`
  - the decode-time KV exhaustion itself, now with the token counts
- `pool: client-facing terminal (error|interrupted): ..`
  - pool relays that ended without the child's [DONE] (member died / cap)

**Coordinator** - grep in the coordinator log:
- `http failure: METHOD /path -> STATUS (N ms)`
  - every 4xx/5xx the coordinator sent to any caller (router or portal)

## "Context size has been exceeded." - findings (2026-11-09)

The owner's pi hard-stopped on this with the UI showing ~58% of 262k used.
Findings:
- The string is generated by the **llama.cpp router core**
  (tools/server/server-context.cpp, the decode path: `llama_process` returns
  ret=1 at n_batch=1 = the KV cache cannot take the next token). It is **not
  a pi bug and not a coordinator bug**, and it is a *real* KV exhaustion at
  the moment it fires.
- There are three context-error variants in the server: (1) input larger than
  max context (pre-check, 400 exceed_context_size), (2) request exceeds
  available context (pre-check, 400), (3) decode-time exhaustion (500, the
  one the owner hit). (3) can fire even when the prompt passed the
  pre-checks, because the *usable* KV capacity of the serving slot can be
  smaller than the advertised `n_ctx_slot` (unified KV shared across
  parallel slots, memory-limited allocation) - the catalog and the pool
  class advertisement report the nominal ctx (262144), which clients then
  plan against. A request that fits the advertised number can still exhaust
  the real capacity mid-generation.
- This is a property of the **stock config as well** (the 27B preset:
  ctx 262144 x parallel 4 on a 3090 Ti), not pool-specific. The pool only
  makes it more reachable (class requests may land on the gemma member,
  which is the most memory-constrained).
- With the new logging, the next occurrence is unambiguous: the router log
  will show `n_prompt_tokens` vs `n_ctx` at the failure. If
  n_prompt_tokens < n_ctx, the advertised context is larger than the usable
  capacity (config/metadata mismatch - fix by sizing KV to the ctx, lowering
  ctx-size/parallel, or advertising the true capacity); if pi hard-stopped
  with NO server failure logged at that moment, it is client-side.

**Resolution (owner's production fix, adopted 2026-11-09):** the owner had
already resolved this in production with `parallel = 1` + a large RAM prompt
cache (`cache-ram` + `cache-idle-slots`): one slot on the GPU at a time,
inactive sessions' KV held in system RAM, round-robin across concurrent
sessions. `swap-fleet.ini` (the demo/bring-up fleet) now uses this for the
27B: parallel 1, ctx 262144, cache-ram 18165, cache-idle-slots 1. The
`parallel=4 x 262144` shape is gone from the demo fleet.

**Boot compatibility check (added 2026-11-09):** each model instance now
validates at load that the KV pool ACTUALLY allocated holds every slot's
full advertised context (requested values can differ from actual, per
llama.h). Router log line per model:
- `boot check: KV pool holds N tokens >= S slot(s) x C advertised context = N - compatible`
- `boot check: KV pool holds N tokens, but S slot(s) x C advertised context = M - concurrent sessions can exhaust the pool mid-generation. usable per-slot context with S slot(s) is ~X; lower ctx-size to ~X, lower parallel, or raise the KV pool`
A live 4B test confirmed the arithmetic and surfaced a useful behavior: the
child may itself shrink `n_ctx_slot` to pool/parallel (the check then
reports the shrunk, true numbers as compatible). If a model loads with an
ERR line, set its ctx-size to the suggested ~X (or lower parallel) before
using it. Note the catalog still advertises the model's training context to
clients, so the ERR line - not the catalog - is the source of truth for the
config.

## pi auto-compact on context overflow (2026-11-10)

The owner's plan: advertise the full context size; if the serving model has
less, report the failure and let pi's auto-compact fire. Why it did NOT fire
in the incident, and the fix (verified against the installed pi 1.5.0
bundle):

- pi has two compaction triggers: (1) a pre-response threshold check
  (`estimated context tokens > contextWindow - reserveTokens`), and (2) an
  **overflow-error recovery**: if an assistant message ends with
  `stopReason: "error"` and the errorMessage matches one of pi's ~27
  `OVERFLOW_PATTERNS` (a case-insensitive regex table of provider
  overflow wordings), pi compacts and retries ONCE; if the retry also
  overflows it stops with "Context overflow recovery failed after one
  compact-and-retry attempt". pi's table already contains llama.cpp-specific
  patterns, so our split pre-check message ("request (N tokens) exceeds the
  available context size (M tokens), try increasing it") was already
  recognized.
- The incident's decode-time message "Context size has been exceeded." and
  the nonsplit pre-check ("input (N tokens) is larger than the max context
  size (M tokens). skipping") matched NO pattern -> pi treated it as an
  ordinary error and hard-stopped (the threshold check then saw 58% <
  threshold). That is the whole reason auto-compact never fired.
- Fix (fork, server-context.cpp): the decode-time message is now
  "Context size has been exceeded. The request exceeds the available context
  size." and the nonsplit pre-check reads "input (N tokens) exceeds the
  available context size (M tokens). skipping" - both carry pi's recognized
  phrase "exceeds the available context size". All three variants were
  verified offline against pi's actual pattern table, in the exact form pi
  sees them ("500 <message>", and pool-relayed, where the member's error
  object streams through verbatim). The original decode-time sentence is kept
  as a prefix so existing clients that grep it keep working.
- The decode-time error is now also sent with `ERROR_TYPE_EXCEED_CONTEXT_SIZE`
  (the pre-check variants already were), so the client JSON carries
  `n_prompt_tokens`/`n_ctx` in all three variants.
- Behavior note: a single slot filling its OWN context stops cleanly with
  `finish_reason: length` (the server clamps generation to the slot ctx) -
  it is not an error. The decode-time error fires only when the SHARED KV
  pool cannot take the next token: multiple slots on a pool smaller than
  slots x ctx (the owner's parallel=4 27B topology), or a cross-slot
  allocation. That is exactly what the boot check detects at load.
- Consequence for the pool flow: an overflow on a pool class now makes pi
  compact-and-retry against the same class; with the parallel=1 + RAM-cache
  fleet shape the retry fits (only one slot is on the GPU at a time, and a
  session's KV waits in RAM). No pi extension is needed.

## Coordinator sizing grounding (cloud VM)

`run-demo.sh --smoke` runs a **coordinator usage pass** after the main
checks and reports resource peaks with a workload description. Because a
local serve never touches the coordinator (the designed local path), the
pass drives load the way production will: one long generation occupies the
member's slots, then `$LOAD` (default 6) short requests fire concurrently
and the ones that cannot land locally are queued at the coordinator,
offered, body-pulled and response-relayed. `--load 0` skips the pass.

Report block (printed in the smoke output) + artifact `$WORK/coord-usage.json`:
- workload: request count, in/out tokens, wall time, how many were served
  via the coordinator queue (records delta)
- coordinator: cpu peak / mean (as % of one core), rss peak (MB)
- host: cpu model / core count / RAM (so the numbers carry their context)

How to read it for the cloud VM: the coordinator holds **no weights and no
bodies** - its load scales with queued request rate x member count and
record/metrics volume, not with model size. A single-member demo pass at
cpu peak < ~25% of one core and rss peak < ~64 MB means a 1-vCPU / 1 GB
VM is comfortable headroom for a small group; re-run the pass as members
join (each new member's bring-up can run its own demo first) and let the
numbers, not the guess, set the size. Keep the per-run
`coord-usage.json` files - they are the sizing record.

## Cold start / group bring-up (bring-up.sh)

REQ-3 (owner, 2026-11-08): a **single** script that brings up models + pool
router + coordinator from a **cold** host, with **nothing required to be
running first** - in particular NOT the stock production server. This is the
member-side bring-up for the group deployment: each member runs it on their
machine, and the routers then connect **outbound** to the cloud coordinator.

```
./bring-up.sh                 fleet preset, owner's class mapping, host bind
./bring-up.sh --smoke         + scripted end-to-end check
./bring-up.sh --preset X      a different fleet preset
./bring-up.sh --port 8123 ... any run-demo.sh option passes through
```

It is a thin wrapper over `run-demo.sh` (which already carries the scratch
dir, symlinks, no-download guard, augmented config, and tags). Defaults:
`swap-fleet.ini`, `--host 0.0.0.0`. Class mapping (owner 2026-11-08):
`Qwen-Drive*` -> `pool-image-text-xs` (it is a VLM); all other fleet members
-> `pool-text-md`. It hard-aborts if any preset section's model file is
missing (no downloads). Unlike `swap-to-demo.sh`, it never touches the stock
production router - it assumes the host is cold.

## Demo requirements (running list)

- REQ-1: local demo proves the pool end-to-end on one host (run 6, 8/8 PASS).
- REQ-2: a production -> pool handoff exists and auto-rolls back
  (`swap-to-demo.sh` + `rollback-standalone.sh` + deadman).
- REQ-3: a single cold-start script for models + router + coordinator that
  does **not** require production to be running first (`bring-up.sh`).

## Production -> pool swap (the owner's 27B + gemma test)

`swap-to-demo.sh` replaces the stock production router on :8000 with the pool
demo (fork router + coordinator) running `swap-fleet.ini`
(27B + gemma in pool-text-md; image-capable Qwen-Drive-4B in its own
pool-image-text-xs class - owner decision 2026-11-08), same :8000.
Any failure after the production stop rolls back to the owner's
`projects/pi/llama.sh` automatically.

**Incident of 2026-11-08 01:47 (first attempt):** preflight passed and
production was terminated cleanly, but the demo died at once at its first
psql: `connection to server on socket "/var/run/postgresql/..." failed: No
such file or directory`. Postgres was NOT down - it has been running since
boot (pid 2324, 5+ days uptime at the time of the postmortem). The socket
directory lives on a per-namespace tmpfs: the wrapper was launched via the
monitor daemon from the agent's jailed session, a context in which
`/var/run/postgresql` does not exist (the agent sandbox sees it as
missing; `pg_lsclusters` from there also misreports the clusters as
down). The wrapper was then hard-terminated (a signal no trap can catch) at
the same second the agent session it served was severed - the
session-death cleanup takes the agent's process lineage with it (documented
precedent: 2026-10-05 nebula, "monitored jobs die WITH THE AGENT
SESSION"). The rollback never ran and :8000 was dark until the owner
restarted production by hand. The second attempt (owner-launched from their
own terminal, 17:37) succeeded: result marker SUCCESS, pool connected,
both classes advertised. Lessons baked in since:

- preflight verifies **postgres reachable** before anything is touched -
  only meaningful from a HOST context (the owner's terminal); from a jailed
  context it false-positives "unreachable" (still safe: it aborts before
  any mutation);
- the rollback procedure lives in `rollback-standalone.sh` and runs from
  three places: the direct failure path, the wrapper's EXIT/INT/TERM/HUP
  trap, and a double-detached **deadman** (35 min, recorded pid) that
  covers a hard termination of the wrapper;
- the stock router pid is parsed as field 1 (ps pads the pid column
  variably - the first attempt's `${var%% *}` prefix was fragile);
- the tag heuristic matches token-boundary "4B" only ("A4B" in a MoE name
  is not the 4B class);
- **launch from the owner's own terminal, not the agent's lineage**:

```sh
# owner terminal (host context: sees /var/run/postgresql and can sudo):
psql -d 'user=sid dbname=postgres' -Atc 'select 1'   # must print 1
cd /home/sid/projects/llama-pool/ops
nohup ./swap-to-demo.sh --yes >/dev/null 2>&1 &
# watch: tail -f /home/sid/projects/llama-pool/loop/demo/swap-*.log
# outcome: loop/demo/swap-<ts>.state/result  (SUCCESS | ROLLBACK:<reason>)
```

Expected: ~10-15 min. On success the agent session's next turn runs on the
pool router (same model names, same :8000); on failure the rollback has
already restarted production. Restore stock-only serving per
`RESTORE-8000.md` (stop the demo, then `llama.sh`).

## Swap incidents 2026-11-10 and 2026-11-11 (pool link never established)

Two more swap attempts rolled back, both with `ROLLBACK: pool link not
connected`. Neither is a product defect in the routing path - the local
serve worked in both, the code was proven innocent against a healthy
coordinator, and the 11-08 17:37 run (same router code class) linked in
<5s and served the owner's 3.5h test. What both runs share: the host was
in a bad state in the first ~2 minutes after the production process was
taken down, and the pool link (asynchronous: handshake -> SSE 200 ->
first contact) was checked while that window was still open.

**Run 2026-11-10 21:17** (lasted 14.5s, not 14 minutes - router.log
timestamps are H.M.S.mmm): 4B loaded + boot check PASS in 3.2s; 27B
stalled at Vulkan0 context creation (GPU in a bad state right after the
takedown; production's 27B loaded fine after the rollback); the SSE
subscribe never received its 200 in 13s; the wrapper's check was then a
ONE-SHOT at t~13s - a race by construction.

**Run 2026-11-11 10:08** (the informative one): all three boot checks
printed `compatible` (27B 262144, gemma 262144, 4B 32768 - the data point
we wanted); models loaded in ~30-50s; local serve PASS. But the router
logged `subscribe failed: cannot reach http://127.0.0.1:8125` three times
(57s, 42s, 4.5s attempts) and then `switching to long-poll mode`, and
`last_contact_ms` stayed 0 (NEVER contacted). The coordinator was alive -
it logged every envelope it received - but never sent the subscribe 200.
Key fact about the coordinator: **it has no Postgres connection pool; it
shells out to `psql` for every query** (internal/pooldb). The subscribe
path is 3 such queries before the 200. The host in that window was
saturated: the pi sessions that lost production fell into tight retry
loops (503 no-slot -> retry every ~2s) with 50-130k-token prompts - the
agent's own session is one of them (its growing 67k->75k-token prompts
were visible in the router log; at parallel=1 each retry is a 60-80s
prompt eval that chains the next 503). Under that load the coordinator's
psql-based subscribe path could not complete inside the router's 60s GET
deadline, three times in a row.

Fixes baked into swap-to-demo.sh:

1. the pool-link check is no longer one-shot: it retries for a bounded
   window;
2. the verification order is now **models first, then the link**: the
   model loads (1-3 min) absorb the post-swap storm, and the link window
   (120s) runs on a calm host; the router keeps retrying the link in the
   background the whole time;
3. acceptance is `"connected":true` (SSE) **or `last_contact_ms > 0`** -
   after repeated SSE failures the router runs in long-poll mode where
   `connected` flaps by design (RESID-WS16-1), but any nonzero contact
   proves the link works (both failures showed 0 = never contacted).

Operational rule for the next run: **pause or exit the other pi sessions
before starting the swap, and the agent stays quiet (no tool calls) for
~4 minutes after the owner launches it** - the agent's own retry chain is
the loudest storm source.

Known weakness for production (not a swap blocker): the psql-per-query
coordinator DB access is fragile under host load, and will be MORE
fragile on the cloud (network RTT per spawn). Replacing it with a real
connection pool (database/sql + pgx) is the top post-test coordinator
task.

## Pool-link startup gate (D-K5, 2026-11-11)

Owner decision after the two rolled-back swap runs: **a pool router must
not accept client load before its coordinator connection is
established.** Implemented in the fork (committed):

- `pool_client::established()` latches on the first successful subscribe
  round (SSE or long-poll).
- Router main flow reordered: startup models launch -> pool thread starts
  -> **gate: wait for `established()`** (deadline 5 min,
  `POOL_READY_TIMEOUT_MS` to override) -> HTTP server listens. So
  `:8000` answering already implies the pool link is up.
- Deadline exceeded (coordinator unreachable / host never settles) ->
  the router exits non-zero with `pool: coordinator link not established
  after ... (POOL_READY_TIMEOUT_MS)`. `run-demo.sh` already detects a
  dead router in its wait loop and dies with the router's last log lines;
  the swap wrapper rolls back on that.
- A link that drops AFTER startup never stops the router; the pool thread
  retries with backoff 1s -> 2s -> ... -> **300 s ceiling** (was 60 s).
- Coordinator-side: the subscribe path now logs `pre-200 phase slow: ...`
  when its auth/ownership queries exceed 1 s, and `subscribe: 200 sent`
  on success - so a future link stall is measurable, not inferred.
- Tests: `test_startup_gate_dead_coordinator_fails_router`,
  `test_startup_gate_established_before_accepting_load`
  (tools/server/tests/unit/test_pool_dispatch.py).

This also breaks the feedback loop of the 2026-11-11 incident: while the
gate is open nothing is served, so the retry storm that was saturating
the host (and starving the coordinator's psql-per-query path) cannot form.

### CORRECTED root cause (2026-11-11, after the e2e slice)

The saturation story above was a misdiagnosis built on a false premise.
The real common cause of BOTH rollbacks: **my statusLog middleware
(commit ce533da, 2026-11-10 15:05 - after the only green run, before
both failed runs) wrapped the ResponseWriter in `statusWriter`, which did
not implement `http.Flusher`.** `serveSSE`'s `w.(http.Flusher)` was
therefore nil, the initial flush and every heartbeat flush were no-ops,
and the SSE stream delivered ZERO bytes over the wire (proven with raw
sockets + `ss`: the coordinator sent only the TCP ACK; a minimal Go SSE
server worked, isolating the coordinator code; long-poll worked because
its response completes when the handler returns). The router's SSE
rounds all timed out at 60s; in the 11-10 run the one-shot check hit
that state at t=13s; in the 11-11 run the long-poll fallback also
starved under the post-swap load (its 55s rounds exceeded the router's
60s GET deadline when the psql-per-query path was slow - the saturation
was a real aggravator there, but not the root cause). The 2026-11-11
e2e slice caught it with 0-byte wire evidence.

Fix: `statusWriter` now implements `http.Flusher` (delegates to the
wrapped writer). Regression test: `TestE2E_SubscribeSSE_DeliversBytes`
(a real-connection SSE reader asserting the 200 headers arrive promptly
and a heartbeat byte within 40s - the long-poll-based suites were blind
to this, which is why they stayed green).

The D-K5 gate stands independently: it turns "the link is slow for
whatever reason" from a 13s race into a 5min bounded wait, and it makes
a dead coordinator a loud, fast failure instead of a half-pooled
router.
