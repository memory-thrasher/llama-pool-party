# Pool protocol v1 — message catalog

Protocol version: **1**. This catalog is the human-readable half of the protocol
contract; the machine-readable half is `schemas/` (JSON Schema, draft 2020-12),
with golden examples in `examples/`. Run `python3 validate.py` to check that the
contract is self-consistent. The Go mirror lives in `go/poolproto/`.

Design of record: `../docs/DECISIONS.md` (D-* references below point there).

---

## 1. Envelope and transport

**Every** pool protocol message uses the same envelope, in both directions:

```json
{ "type": "<message_type>", "payload": { ... } }
```

| Direction | Mechanism |
|---|---|
| Router → coordinator | `POST /pool/events` with the envelope as the JSON body. |
| Coordinator → router | SSE `data:` lines on the held-open `GET /pool/subscribe`; each `data:` line is one envelope. |

`type` values (stable, snake_case, closed set for v1):

`handshake`, `capacity`, `work_offer`, `offer_accept`, `offer_reject`,
`body_abort`, `completion`, `error`, `affinity_report`, `exclusion_update`,
`eviction_notice`.

Heartbeats are **not** envelope messages: the coordinator writes SSE comment
lines `: hb <unix_ms>` roughly every 25 s on the open subscribe (so proxies
and clients can tell the link is alive).

### Subscribe flow (coordinator → router)

- The router issues `GET /pool/subscribe` with `Accept: text/event-stream` and
  holds the response open. The coordinator streams envelope events as `data:`
  lines and heartbeat comments `: hb <unix_ms>` ~every 25 s.
- **Long-poll fallback**: with `Accept: application/json`, the same endpoint
  waits for pending work and, after at most 55 s, closes the response with a
  JSON list of pending work-offer envelopes (`[{"type":"work_offer","payload":{...}}, ...]`,
  possibly empty). The router re-issues the GET. This serves routers whose
  HTTP stack cannot hold an SSE stream.
- **Reconnect/backoff**: if the subscribe drops (or a heartbeat goes missing),
  the router reconnects with exponential backoff, capped at 60 s. On
  reconnect the router re-sends `handshake` (the coordinator treats
  handshake + `capacity` as the authoritative membership state).

### Authentication

- Every request in both directions carries `Authorization: Bearer <member_token>`
  — the member's opaque 256-bit user token (issued in the portal, D-J2) — and
  the header `X-Pool-Proto: 1`.
- The router is the initiating client on **every** link (member networking is
  outbound-only, D-A2): it presents the member token on `POST /pool/events`,
  on `GET /pool/subscribe`, and on the body pull `GET /pool/jobs/{job_id}/body`.
  The coordinator never initiates a connection to a router and therefore never
  needs to authenticate *to* one; the router's user-facing token is the only
  credential on the wire.

### Versioning

- Header `X-Pool-Proto: 1` on every request, both directions.
- `handshake.payload.proto` = `1`.
- The coordinator may reject older protocol versions (D-J6); a router facing a
  newer coordinator degrades to graceful offline, never a crash. Bumps are rare.

---

## 2. Message summary

| type | direction | when sent | schema |
|---|---|---|---|
| `handshake` | R→C | first message after `GET /pool/subscribe` connects (and after every reconnect) | `schemas/handshake.json` |
| `capacity` | R→C | any membership/capacity change: model load/unload/exclude, slot count, VRAM | `schemas/capacity.json` |
| `work_offer` | C→R | phase 1 of two-phase dispatch (D-D2): a queued job is offered to a candidate router | `schemas/work_offer.json` |
| `offer_accept` | R→C | router commits to the offered job under a short lease | `schemas/offer_accept.json` |
| `offer_reject` | R→C | router declines the offered job; coordinator moves on **without** transferring the body | `schemas/offer_reject.json` |
| `body_abort` | R→C | after accept, the router aborts the body stream before it completes | `schemas/body_abort.json` |
| `completion` | R→C | a dispatched job finished successfully (feeds D-I3 records, D-E2/D-E3) | `schemas/completion.json` |
| `error` | R→C | a dispatched job failed; `stage` selects hop (pre_token) vs client-visible error (mid_stream) | `schemas/error.json` |
| `affinity_report` | R→C | optional batch of cache-liveness hints (≤16 entries), at the router's cadence | `schemas/affinity_report.json` |
| `exclusion_update` | R→C | owner marked a model excluded/un-excluded (D-F1) | `schemas/exclusion_update.json` |
| `eviction_notice` | R→C | a local model was unloaded by its owner; queued jobs on it are gone | `schemas/eviction_notice.json` |

R = router, C = coordinator. `body_pull` and job intake are HTTP endpoints, not
envelope messages (sections 4–5).

---

## 3. Messages

Field tables below are normative; the schemas are the source of truth for
shapes. `UUID` = lowercase RFC-4122 string; `64-hex` = SHA-256 hex digest.

### 3.1 `handshake` (R→C)

First envelope sent right after `GET /pool/subscribe` connects — and again
after every reconnect, before any other message.

| field | type | notes |
|---|---|---|
| `proto` | int | = 1 in v1; coordinator may reject older (D-J6) |
| `router_id` | UUID | stable UUIDv4 the router generates once and persists |
| `router_name` | string ≤64 | owner-chosen display name |
| `software` | string | e.g. `"llama.cpp b11827-pool"` |
| `models` | array | initial capacity snapshot, same shape as `capacity.payload.models` (§3.2) |

Example: `examples/handshake.json`.

### 3.2 `capacity` (R→C)

On any membership/capacity change: a model loaded, unloaded, or excluded; a
slot-count change; a VRAM change. The coordinator recomputes class
advertisement (D-B7, D-C3) from the latest capacity of connected routers.

| field | type | notes |
|---|---|---|
| `router_id` | UUID | as in `handshake` |
| `models` | array | the router's **full** current model list (not a diff) |
| `updated_at` | RFC 3339 | when the snapshot was taken |

Model entry (shared shape in `handshake` and `capacity`):

| field | type | notes |
|---|---|---|
| `model_id` | string | local model id (llama.cpp model name) |
| `tags` | string[] | pool classes served, e.g. `["pool-text-lg"]` |
| `n_ctx` | int ≥1 | context window the member runs the model with |
| `n_embd` | int ≥1 | embedding dimension (informational) |
| `n_slots` | int ≥1 | parallel slots configured |
| `busy_slots` | int ≥0 | slots currently in use |
| `status` | `"loaded"` \| `"excluded"` | excluded = owner exclusion mark (D-F1) |
| `vram_mb` | int \| null | VRAM in MiB, null if not reportable |

Example: `examples/capacity.json`.

### 3.3 `work_offer` (C→R)

Phase 1 of two-phase dispatch (D-D2). The coordinator offers a queued job to a
candidate router. No prompt/response content is carried here.

| field | type | notes |
|---|---|---|
| `job_id` | UUID | assigned by the coordinator at intake |
| `tag` | string | the class/tag requested, e.g. `pool-text-lg` |
| `est_prompt_tokens` | int ≥0 | estimated prompt size (from intake) |
| `est_ctx_needed` | int ≥0 | context the request is expected to need (drives D-D4 selection) |
| `deadline_ms` | int \| null | service deadline, ms; null if none |
| `priority` | `"live"` \| `"batch"` | batch is always lower priority (D-F4) |
| `source` | `"queue"` \| `"skip"` \| `"pin"` | normal queue / skip-the-line (D-E2) / soft pin (D-E3) |
| `conv_key` | 64-hex \| null | continuity key (D-E1) if known |

Examples: `examples/work_offer.json`, `examples/work_offer_skip.json`,
`examples/work_offer_batch.json`.

### 3.4 `offer_accept` (R→C)

The router commits to the offered job under a short lease. After accept, the
router pulls the body (§5, phase 2).

| field | type | notes |
|---|---|---|
| `job_id` | UUID | the offered job |
| `lease_ms` | int ≥1 | short lease (typical 5000); coordinator may re-offer if the job is not taken within the lease |
| `model_id` | string | which local model will serve |
| `n_ctx` | int ≥1 | the member's real context (used for the `max_tokens` clamp, D-C6) |

Example: `examples/offer_accept.json`.

### 3.5 `offer_reject` (R→C)

The router declines. The coordinator moves the offer to the next candidate
**without** transferring the body (D-D2). Races are normal, not errors (D-D3).

| field | type | notes |
|---|---|---|
| `job_id` | UUID | the offered job |
| `reason` | enum | `model_gone`, `ctx_insufficient`, `no_slot`, `busy`, `evicted`, `other` |
| `detail` | string ≤200 \| null | optional; never prompt/response content (anti-memory §6) |

Examples: `examples/offer_reject.json`, `examples/offer_reject_model_gone.json`.

### 3.6 `body_abort` (R→C)

Sent after `offer_accept` when the router aborts the body stream before it
completes (e.g. the model was unloaded mid-transfer). Aborting the stream =
closing the HTTP stream; this envelope tells the coordinator.

| field | type | notes |
|---|---|---|
| `job_id` | UUID | the aborted job |
| `reason` | enum | `model_gone`, `no_slot`, `evicted`, `other` |

Example: `examples/body_abort.json`.

### 3.7 `completion` (R→C)

A dispatched job finished successfully. Feeds the per-request records (D-I3)
and, via `idle_ms_hint`, the skip-line / soft-pin logic (D-E2, D-E3).
`ok` is always `true` here; failures use the `error` message.

| field | type | notes |
|---|---|---|
| `job_id` | UUID | the completed job |
| `ok` | bool | const `true` |
| `conv_key` | 64-hex \| null | of the served conversation |
| `model_id` | string | the model that actually served |
| `prompt_tokens` | int ≥0 | |
| `response_tokens` | int ≥0 | |
| `cached_tokens` | int \| null | prompt tokens from KV cache, if measurable |
| `idle_ms_hint` | int \| null | how long the member's KV for this conversation is likely warm — drives the skip-line |
| `t_queued_ms` | int ≥0 | intake → dispatch start |
| `t_service_ms` | int ≥0 | dispatch start → end |
| `stop_reason` | string \| null | the engine's stop reason, e.g. `eos`, `length`, `stop` |
| `served_by` | UUID | router_id of the serving router |

Examples: `examples/completion.json`, `examples/completion_cold.json`.

### 3.8 `error` (R→C)

A dispatched job failed. `stage: "pre_token"` enables hop-on-error —
automatic re-dispatch within the same class (D-G1). `stage: "mid_stream"` is a
clean, structured, client-visible error; the harness retries the turn (D-G2).

| field | type | notes |
|---|---|---|
| `job_id` | UUID | the failed job |
| `ok` | bool | const `false` |
| `stage` | enum | `pre_token` (hop eligible) / `mid_stream` |
| `code` | enum | `model_gone`, `overflow`, `crash`, `timeout`, `client_disconnected`, `other` |
| `message` | string 1..200 | short reason; never prompt/response content (§6). `overflow` carries the canonical `exceeds the available context size` wording (D-C5) |
| `prompt_tokens` | int \| null | consumed before failure, if known |
| `response_tokens` | int \| null | partial, if any |
| `conv_key` | 64-hex \| null | |
| `t_queued_ms` | int \| null | |
| `t_service_ms` | int \| null | dispatch start → failure |

Examples: `examples/error.json`, `examples/error_midstream.json`.

### 3.9 `affinity_report` (R→C)

Optional batch of cache-liveness hints — which conversations this router's KV
is still warm for, and how long. The coordinator uses it to prefer warm
members for continuations (D-E2). At most 16 entries per report; routers may
send these at any cadence (including never).

| field | type | notes |
|---|---|---|
| `router_id` | UUID | as in `handshake` |
| `entries` | array ≤16 | each: `conv_key` (64-hex), `idle_ms` (int ≥0), `cached_tokens` (int \| null) |

Example: `examples/affinity_report.json`.

### 3.10 `exclusion_update` (R→C)

D-F1: the owner marked a model excluded (`/models/unload` on the pool member
entry = a mark in the owner's config — not a real unload) or un-excluded
(`/models/load` on an excluded model). The coordinator avoids dispatching this
owner's class requests to an excluded model. Explicit model-id requests
bypass the mark (deliberate choice wins over preference). Local models keep
real load/unload semantics.

| field | type | notes |
|---|---|---|
| `router_id` | UUID | as in `handshake` |
| `model_id` | string | the marked/un-marked model |
| `excluded` | bool | true = now excluded; false = exclusion removed |

Examples: `examples/exclusion_update.json`, `examples/exclusion_update_restore.json`.

### 3.11 `eviction_notice` (R→C)

A local model was **really** unloaded by its owner. Queued/offered jobs that
named it are gone: the coordinator drops them from candidacy (pre-token jobs
may re-dispatch within the class, D-G1) and invalidates skip/pin (D-E4).

| field | type | notes |
|---|---|---|
| `router_id` | UUID | as in `handshake` |
| `model_id` | string | the unloaded model |
| `evicted_jobs` | UUID[] | queued/offered job ids now gone; may be empty |
| `reason` | enum | `owner_unload` / `other` |

Example: `examples/eviction_notice.json`.

---

## 4. Job intake (router → coordinator, client side)

Client-facing endpoint on the router, documented here because it defines what
the coordinator receives. The originating router enqueues a client job whose
local member is busy/absent (D-D1):

1. `POST /pool/jobs`
   - Headers: `Authorization: Bearer <member_token>`, `X-Pool-Proto: 1`,
     `Content-Type: application/json`.
   - Body: the **metadata** JSON of `schemas/job_intake.json`:
     `tag`, `est_prompt_tokens`, `est_ctx_needed`, `priority`
     (`"live"` \| `"batch"`), `conv_key` (64-hex or null), `client_hint`
     (string or null — e.g. harness name; metadata only).
   - Response: `201 Created` with `{"job_id": "<uuid>"}`.

2. `PUT /pool/jobs/{job_id}/body`
   - Headers: `Authorization: Bearer <member_token>`, `X-Pool-Proto: 1`,
     `Content-Type: application/octet-stream`.
   - Body: the raw client request body, **verbatim** (the exact JSON bytes the
     client sent). Streamed; the coordinator stores it while queued (D-I2).

**Design choice (v1):** the body is a follow-up `PUT`, not in-band after a
separator in the intake request. The two-request form keeps the metadata
endpoint a plain small JSON POST, streams arbitrarily large bodies without
framing heuristics, and makes "metadata accepted" a clean checkpoint
(`201` + `job_id`) before any bulk transfer. The coordinator treats the job
as *queued* once the body PUT completes.

Example (metadata only): `examples/job_intake.json`.

The client-facing **async batch** endpoint on the router (and its
coordinator-owned persistent queue, D-H2) is out of scope for protocol v1:
the message names `batch_*` are **reserved for v1.1 (WS-10)**. `priority:
"batch"` and intake already carry the bits it needs (D-F4).

---

## 5. Body pull (coordinator → router, phase 2 of dispatch)

`body_pull` is **not** an envelope message; it is an HTTP endpoint:

`GET /pool/jobs/{job_id}/body` — served by the coordinator after the router's
`offer_accept`. The router streams the original raw client payload (the exact
bytes the client sent) and **may abort at any time** — early abort = close the
stream, then send `body_abort`.

Response headers:

| header | value |
|---|---|
| `Content-Type` | `application/octet-stream` |
| `X-Pool-Conv-Key` | the 64-hex continuity key of the job (if known) |
| `X-Pool-Tag` | the pool tag the job was offered under |

Request headers: `Authorization: Bearer <member_token>`, `X-Pool-Proto: 1`.

Body bytes are the raw JSON request body verbatim — the coordinator is a
store-and-forward here; it never parses or alters the body (D-I4).

---

## 6. Anti-memory rule (normative)

No message may carry prompt or response content. Concretely:

- `offer_reject.detail` and `error.message` are capped (≤200 chars) and must
  be non-informative (e.g. `"model_gone at dispatch"`). They must never
  include body content.
- `completion`/`error` carry token **counts** and timings, never text (D-I1:
  log hashes and token counts, never bodies).
- `job_intake` is metadata only; the body travels exclusively over the
  `PUT .../body` and `GET .../body` endpoints.
- `affinity_report` carries only `conv_key`s (hashes), ids, and counts.

The examples follow this rule (placeholders only) and `validate.py`
enforces a 250-char string cap on example content as a sanity net.

---

## 7. Reserved names

`batch_*` (e.g. `batch_offer`, `batch_completion`) are reserved for the async
batch workstream — **v1.1, WS-10**. Do not use these names in protocol v1.

---

## 8. Versioning policy

- `X-Pool-Proto: 1` on every request (both directions); `handshake.payload.proto = 1`.
- The coordinator may reject older protocol versions (D-J6); parallel support
  of multiple versions is a bump-time decision.
- A router facing a newer coordinator degrades to graceful offline, never a
  crash (D-J6).
- Bumps are rare: new message types or field additions that both sides can
  ignore are preferred; a version bump is for breaking changes.
