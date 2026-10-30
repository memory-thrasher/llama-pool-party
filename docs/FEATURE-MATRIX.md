# Feature matrix - what llama.cpp already has vs. what the pool adds

Base: llama.cpp @ `b11827` (the tag `llama.cpp-pool` is cut from). "Works" = verified in code or in the
llama.cpp/published documentation, not assumed. Companion to `BRIEF.md`, `APPROACH.md`, `DECISIONS.md`.

Status legend ("In llama.cpp today" column):
- **Works - unchanged** - exists in llama.cpp today; the pool leaves it alone.
- **Works - extended** - exists today (including "exists, informational only"); the pool reuses it or adds behavior around it.
- **Absent** - does not exist in llama.cpp; the pool builds it.
- **Works in pi** - works in the pi harness (external to llama.cpp); nothing to build here, it constrains us instead.

Phase: **MVP** = first release, **v2** = deferred, **-** = nothing to build.

---

## 1. Inference and single-model server (foundation)

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| GGUF inference, backends (CPU/CUDA/Metal/...), sampling | Works - unchanged | none | - | - |
| Single-model `llama-server` | Works - unchanged | none (a pool member's models run as ordinary server children) | - | - |
| Client API: OpenAI-compatible chat/completions, streaming, `/props`, `/metrics`, `/slots`, `/tokenize`, `/detokenize`, `/health` | Works - unchanged | clients keep working with zero changes; pool classes ride on top of the same API | - | - |
| Single `--api-key` auth | Works - unchanged | unchanged; pool members and the coordinator use their own link tokens (separate concern) | - | - |

## 2. Router (multi-model server) - the fork's base

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| Router mode: one process supervising several model instances (child processes, one per model, per-GPU) | Works - unchanged | base of the fork | - | - |
| Exact model-id routing: a request for a loaded model id is served by that instance (strict: one name -> one instance, no load balancing) | Works - unchanged | unchanged; **class routing sits in front of it** (tag resolved first, then normal routing) | fork hook | MVP |
| Model lifecycle: load / unload / sleep, presets with auto-load on request | Works - extended | real models keep real semantics; on a **pool member** the same load/unload controls become an **exclusion mark** (no real unload) | fork | MVP |
| Preset change on a running model unloads it | Works - unchanged | documented, unchanged | - | - |
| Live status stream `/models/sse`, `/slots`, `/props`, `/metrics` in router mode | Works - extended | pool status merged into the same streams | fork | MVP |
| Web UI: model manager (load/unload buttons), presets, slots view, live status | Works - extended | pool panels: connection state, queue position, contribution/consumption, GPU + VRAM utilization, offer/withhold per model and per GPU, one-click pause, **queued eviction** (evict as soon as the running operation finishes) | fork (UI) | MVP |
| Model **tags** (`--tags`) | Exists - informational metadata only, not used for routing | **reused as the class mechanism**: tags become requestable | fork | MVP |
| Conversation awareness / request identity | Absent (no conversation id; requests are independent) | request id + full-prompt-hash idempotency dedupe (~5 min window) | fork | MVP |

## 3. Classes and dispatch (the new core)

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| Requesting by class tag (e.g. `pool-text-md`) instead of exact model id | Absent | class resolution: tag -> set of candidate members; membership checked **twice** (at receipt and at dispatch); no membership = canonical "model is not loaded" error | fork | MVP |
| Advertising classes in the model list (`/v1/models`) and `/props` | Absent (tags exist but are not requestable entries) | class entry advertised as `loaded` iff some member has a model loaded under it (slots don't flicker it); **advertised context = min(avg + std, max)** across loaded members, recomputed on membership change | fork (state from coordinator) | MVP |
| Local-first serving: local member with a free slot serves the request | Absent (the router never balances across instances) | free local slot -> serve; otherwise coordinator queue | fork | MVP |
| Never auto-load | (auto-load exists today as opt-in preset behavior) | hard rule: the pool never auto-loads, in any version | fork | MVP |
| Cross-member dispatch: offer -> accept(lease)/reject(reason) -> streamed body pull, bounce before the prompt is transferred | Absent | two-phase dispatch over the member link | fork + coordinator | MVP |
| Large-prompt preference (biggest prompts -> members with most context headroom in the class) | Absent | dispatch preference | coordinator | MVP |
| Per-user priority lists within a class | Absent | honored when multiple GPUs are immediately available | coordinator | MVP |
| Hop-on-error, pre-first-token (member crash / model gone / immediate error -> re-dispatch in same class) | Absent | automatic, per-user toggle | fork + coordinator | MVP |
| Mid-stream failover (carry buffered partial response to a replacement member) | Absent | **deferred** | fork + coordinator | v2 |

## 4. Queues and fairness

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| Admission queue beyond per-model slot queues | Absent (a request for a busy model blocks the caller) | **live queue** at the coordinator (in-memory); the client socket stays held on the router; the client's own timeout is the backstop | coordinator | MVP |
| Async batch endpoint for clients | Absent (no batching endpoint exists anywhere in the code) | client-facing batch API on the router; **persistent** batch queue (survives coordinator restarts) | fork + coordinator | MVP |
| Batches always below live priority; batch consumption counted at 0.5 weight in the fairness ratio | Absent | queue policy | coordinator | MVP |
| Contribution-based priority: `(served + 1) / (consumed + 1)` token ratio, rolling window, pick best user among the top-10 oldest items, per-user FIFO | Absent | queue selection | coordinator | MVP |
| Soft per-user async caps (no hard caps; trusted members) | Absent | soft caps, high defaults | coordinator | MVP |

## 5. Continuity (cache affinity)

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| Any conversation/continuation tracking | Absent (verified: pi sends no conversation id header; the router is per-request stateless) | key = SHA-256 of the first 8 KB of the raw request (stable across turns, breaks on compaction - which is correct) | fork + coordinator | MVP |
| Skip-the-line for continuations | Absent | served ahead of the queue on the last-serving member if idle < 60 s (config) or reported cached | coordinator | MVP (skip) / v2 (authoritative cached-report) |
| Soft pin: next request of a conversation gets queue-priority on the member that just served it (up to 60 s, no slot ever held) | Absent | queue-priority grant, invalidation on eviction/disconnect/error | coordinator | MVP |
| Continuation metrics (warm / cold / denied + reason) | Absent | records | coordinator | MVP |

## 6. Errors and harness compatibility

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| Context-overflow error "...exceeds the available context size..." | Works (native, `tools/server/server-context.cpp`) | one shared constant emitted from all three places: router, coordinator pre-check, serving member | fork + coordinator | MVP |
| "model is not loaded" for an unloaded model | Works (native, `tools/server/server-models.cpp`) | reused as the canonical no-membership class error | fork | MVP |
| pi (verified 1.4.0): reactive compaction on the overflow substring; auto-retry on 5xx / busy-wording; listing gate accepts `loaded` models | Works (in pi, not in llama.cpp) | nothing to build in pi; the **requirement** on any other harness is the same: compact dynamically on overflow | - | - |
| Error taxonomy as control surface (hold = slow 200 on an open socket; give-up = neutral 4xx; client-retry = 5xx/busy wording only when retry is wanted) | Absent as policy | fork's error codes/wording chosen deliberately against pi's retry patterns | fork | MVP |
| `max_tokens` clamped to the selected member's capacity at dispatch | Absent | required because harnesses set `max_tokens` to the advertised window | fork | MVP |
| Chat-template reformatting across members (parse under template A -> canonical messages -> re-serialize under template B) | The jinja engine and per-model templates work; cross-template re-serialization does not exist | new glue on the existing engine; round-trip assert under `LLAMA_POOL_DEBUG_RT`; CI corpus of real conversations (agent turns, tool calls, reasoning blocks) | fork | MVP |

## 7. Coordinator service (entirely new)

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| A control plane of any kind | Absent (the router is single-machine; no outbound control connection exists) | Go service on a small cloud VM: class registry (default size x capability, MoE by total params; admin-curated labels; per-user labels), dispatch engine, live + persistent batch queue, continuity registry, exclusion marks, records, event push, protocol versioning (older versions rejected) | coordinator | MVP |
| Per-user identity | Absent (single shared key, no per-user concept) | opaque 256-bit tokens (shown once, revocable, hashed at rest), portal sessions, **invite-only** (no emails), bootstrap admin via shipped SQL script | coordinator | MVP |
| Records and metrics with **anti-memory rules**: never log bodies; log hashes + token counts; bodies retained only while queued, expunged on completion | Absent | enforced in code, not by convention | coordinator | MVP |
| Portal: users/invites, tokens, user settings, admin mirror of every member's pool state (the moderators' console) | Absent | SvelteKit SPA served by the Go binary; login page carries the accepted trust paragraph (no quality promise, transit + member-side serving stated plainly) | coordinator | MVP |
| Member link mechanics | Absent | plain HTTPS, no websockets: router opens `GET /pool/subscribe`, coordinator holds it open (SSE + ~25 s heartbeats, long-poll fallback); results/capacity/continuity flow back as small POST events; implemented on curl (already a llama.cpp dependency) | both | MVP |

## 8. Shared protocol (entirely new)

| Feature | In llama.cpp today | Pool adds | Where | Phase |
|---|---|---|---|---|
| Router <-> coordinator protocol | Absent | message catalog + JSON schemas: handshake/version, capacity & state, work offer, accept/reject, body transfer, completion/error, affinity report, exclusion update, eviction notice; versioned, controller may reject older; **built first** (P01) - it unblocks fork and coordinator to proceed in parallel | shared | MVP |

---

## At a glance

Counts refer to the rows above (42 total), each row counted once:

| Bucket | Count |
|---|---|
| Works - unchanged (the floor the pool stands on) | 7 |
| Works - extended (existing code the pool reuses; small hooks only) | 6 |
| Works in pi (external; nothing to build, it constrains us) | 1 |
| To be built - MVP | 27 |
| To be built - v2 (deferred) | 1 (plus the cached-report part of the skip-the-line row) |

Split of the 27 MVP rows by primary home: **fork** (class resolution, advertisement, local-first, dispatch client side, hop, batch endpoint, error taxonomy, `max_tokens` clamp, reformatting glue, request-id dedupe, UI panels, link client) / **coordinator** (class registry, dispatch engine, live + batch queue, fairness, continuity registry, records, portal, identity) / **both** (two-phase dispatch, hop, continuity, link mechanics, protocol). Full v2 scope per DECISIONS D-O2: mid-stream handoff, live parameter adjustment, authoritative cache-liveness report, admin-curated shared lists, parallel protocol versions, OpenAI batch schema parity. Cut (D-O3): minute-salt auth, E2E body encryption, router-to-router proxying.

## Verification notes (what "works" rests on)

- Router internals reviewed in code @ b11827: `server_models` name -> one child subprocess; strict name routing, no load balancing; `ensure_model_ready` blocks the HTTP thread; preset change unloads (does not respawn); `/slots`, `/props`, `/metrics`, `/tokenize` proxied in router mode; tags parsed from `--tags` and stored on the model meta (informational); single `--api-key`.
- Native error strings located in code: overflow at `tools/server/server-context.cpp` (`request (%d tokens) exceeds the available context size (%d tokens)`); `model is not loaded` at `tools/server/server-models.cpp`.
- Absence of any batching endpoint confirmed by full-tree grep (no `/v1/batches`).
- pi 1.4.0 behavior verified against its published source: overflow pattern list includes `/exceeds the available context size/i` (matches the native string), retryable patterns (500/502/503/520/524, `server_busy`, `overloaded`, `model is at capacity`, `stream was interrupted`, `timeout`), listing gate selects `loaded`/`sleeping` models, and pi sends no conversation-id header (hence the content-derived continuity key).
