# llama-pool - decision record

Status: **design CONVERGED 2026-10-28**, pending group approval.
Working names: `llama.cpp-pool` (fork), `pool-coordinator` (service).
Workspace: fork worktree at `/home/sid/projects/llama-pool/llama.cpp-pool` (branch `pool/main`, cut from tag `b11827` = commit 970b51ce7); coordinator tree at `/home/sid/projects/llama-pool/pool-coordinator/` (docs under `docs/`).
Siblings in this directory: `BRIEF.md` (group cover), `APPROACH.md` (detailed explainer).
Palace entries (agent memory): 1793172830304 (verified code facts), 1793225681364 (verified pi behavior), 1793229206540 (converged decisions).

ID convention: stable decision IDs (D-*) so they can be referenced in review and in the implementation plan. "MVP" = in the first release; "v2" = deferred; "CUT" = explicitly dropped.

---

## A. Architecture

- **D-A1** Two parts: per-member **router** (the llama.cpp fork, on the member's own machine) and one **coordinator** (small service on a lightweight cloud VM: Go + postgres + web portal).
- **D-A2** Member networking is **outbound-only**: the router initiates the connection to the coordinator. No inbound ports, no port-forwarding, NAT-friendly.
- **D-A3** The fork **adopts llama.cpp's built-in router mode** (multi-model server) as its base. Pool features are a **pure additive overlay**: with no coordinator configured (or connection down), the fork is behaviorally identical to upstream router mode. This is both a compatibility rule and the fork's upstream-mergeability strategy.
- **D-A4** The coordinator is a **separate repo, not C++**: it is a loop of external API calls + DB + web portal, which llama.cpp's own development guidelines exclude from its C++ server code. Go = boring to operate; boring is the goal for the box that holds everyone's tokens.
- **D-A5** One coordinator VM, no HA in v1. All durable state in postgres; restart-safe; in-flight dispatch re-announced on restart.

## B. Classes ("tags")

- **D-B1** Classes reuse llama.cpp's existing model **tags** as the underlying mechanism; a class is a requestable identifier.
- **D-B2** Default classes are **size x capability**: `pool-text-xs/sm/md/lg/xl/2xl`, `pool-image-...`, etc. The `pool-` prefix is reserved.
- **D-B3** Size buckets (defaults, controller-configurable): `xs` <10B < `sm` <25B < `md` <60B < `lg` <250B < `xl` <1.5T < `2xl`. `2xl` is headroom (nothing common today); empty classes cost nothing because they are not advertised (D-B7).
- **D-B4** **MoE** models bucketed by **total** params (VRAM + capability proxy); active params recorded as metadata.
- **D-B5** Size buckets are disjoint; **capability tags overlap freely** (a VLM is `text` and `image`; audio/TTS would add capabilities).
- **D-B6** **Admin labels** (curated in portal, visible to all users) and **user labels** (per user, may overlap with anything) sit on top of defaults, for differing opinions.
- **D-B7** A class with no loaded member is **not advertised at all** (and requests for it get the canonical "model is not loaded" error, D-C7).

## C. Advertisement

- **D-C1** A class is advertised **iff some connected router has a model loaded under it** - slot occupancy does not affect advertisement (a busy class queues; it does not flicker out of the catalog).
- **D-C2** Advertised tag entries use `status: "loaded"` (pi's listing gate accepts loaded; the "sleeping" contract - "could be loaded immediately" - never holds for tags, and non-pi clients may ignore non-loaded entries). No custom status values. Load/unload commands on a tag: real models keep real semantics; pool members map to the exclusion mark (D-F1).
- **D-C3** **Advertised context** = `min(average + std_dev, max)` across loaded members; config option `max`. Recomputed on membership change. Deliberately high by design (see D-C4/D-C5): the overflow path is the pressure valve, not a failure mode.
- **D-C4** Invariant 1: never advertise below what members can serve - protects against harness false-positive "silent overflow" compaction (usage.input > assumed window).
- **D-C5** Invariant 2: **every** overflow rejection (local router, coordinator pre-check, serving worker) carries the substring `exceeds the available context size` - the string llama.cpp already emits natively and pi already matches (`/exceeds the available context size/i`). One constant, three emitters.
- **D-C6** Invariant 3: the router **clamps `max_tokens` to the selected member's capacity at dispatch** (pi sets `max_tokens` = advertised window; without the clamp, high advertisement gets rejected for the wrong reason).
- **D-C7** Class membership is checked **twice**: at request receipt (pre-queue) and at dispatch (post-queue). Canonical no-membership error: `"model is not loaded"` (safe: matches no pi overflow/retry pattern).

## D. Dispatch

- **D-D1** Local member with a **free slot** -> serve locally. Otherwise -> coordinator queue. **Never auto-load** (no version). A loaded-but-busy local member also goes through the coordinator queue (uniform: the own router is just another router; the controller can dispatch back to it when the slot frees).
- **D-D2** **Two-phase dispatch**: (1) offer event `{job_id, tag, est_prompt_tokens, est_ctx_needed, deadline}`; candidate accepts (short lease) or rejects with reason; (2) member pulls the body **streamed** and may abort early. Rejection moves the offer to the next candidate - no full prompt transferred on a bounce.
- **D-D3** The coordinator checks availability (membership + free slot) with the router over the established connection before sending work; on a race (router says "no slot") it moves on.
- **D-D4** Large prompts get preference for members with the most context headroom within the same class.
- **D-D5** Per-user **priority lists** within a class (their own models, or the admin-curated shared list) are honored when multiple GPUs are immediately available.

## E. Continuity (cache affinity)

- **D-E1** Continuity key = **SHA-256 of the first 8 KB of the raw client payload** (messages as sent, before any chat-template transform). Stable across turns (conversations grow at the tail), identical across members by construction, tokenizer-independent, no parsing needed. Breaks on harness compaction (correct: cache is cold then).
- **D-E2** **Skip-the-line**: a continuation is served ahead of the queue on the member that last served the conversation if that member reports it still cached (v2, authoritative) or if idle < **60 s** (config default). Beyond that, up to ~10 min: soft preference, no skip.
- **D-E3** **Soft pin**: when a member with **>1 slot** completes a request successfully, the *next* request of the same conversation gets queue-priority on that member for up to T_pin = 60 s (same knob). **No slot is ever reserved or held**; no free slot at that moment = queue normally; unused pin expires. Two-conversation oscillation falls out naturally; a running task that evicts the other conversation's cache ends its pin.
- **D-E4** **Guards**: skip/pin require the member's last service of that conversation to have completed cleanly. Invalidated by: model/GPU eviction from the pool, member disconnect, failed service.
- **D-E5** Forked conversations sharing a head hash pin to the same member while both stay warm - accepted, and desirable.
- **D-E6** **Metrics**: continuation attempted / skipped / served-warm / served-cold / denied(reason: gone, idle, error, evicted).

## F. Fairness and preference

- **D-F1** **Exclusion marks via the existing load/unload protocol**: `/models/unload` on a pool member = mark in that user's config (not advertised as loaded to them; coordinator avoids it when dispatching *their* class requests). `/models/load` on an excluded model = unmark. **Local** models keep real load/unload. **Explicit model-id requests bypass the mark** (deliberate choice wins over preference). Works because a router is single-user (the owner) - no attribution problem.
- **D-F2** Priority = `(response tokens served by their machines + 1) / (response tokens consumed + 1)`, rolling window default **1 week** (configurable). The +1 keeps new members at a fair baseline and avoids 0/0.
- **D-F3** Contended queue: pick the highest-priority user **among the k oldest pending items** (k default **10**, configurable). A user's items are never reordered relative to each other (per-user FIFO). Serving one request naturally lowers their priority for the rest (denominator grew) - no special logic.
- **D-F4** **Batches always lower priority than live requests.** `batch_received_weight` = **0.5** (config): batch consumption counts discounted in the fairness ratio - a reward for batching.
- **D-F5** Gameability (serve cheap tokens, consume expensive ones) accepted as a known limitation; mitigations = +1 smoothing, class system, trusted-members design. Rationale on record: "if anyone is consuming 4B tokens, they asked for 4B tokens."
- **D-F6** Soft per-user async caps (high defaults); no hard caps.

## G. Requests and failures

- **D-G1** **Hop-on-error, pre-first-token only**: dispatch failure, member crash, model gone, immediate error -> automatic re-dispatch within the same class (local re-dispatch + coordinator re-dispatch), unless disabled by that user (per-user toggle).
- **D-G2** **Post-first-token failure**: clean, structured, client-visible error; the harness retries the turn.
- **D-G3** **v2 (deferred, behind a flag)**: mid-flight handoff - buffer the partial response, replay it to a replacement member in the same class, keep the client stream continuous.
- **D-G4** Live requests: **the client enforces its own timeout** and closes the connection when it gives up (triggering undeliverable-queue removal). The router holds the client connection open while queued. The fork ships with recommended client settings: **longer timeout, no auto-retry** on pool queue errors (a retry goes to the back of the line and is unlikely to work the second time).
- **D-G5** **Idempotency**: router stamps a request id; **full-prompt-hash dedupe** in a ~5 min window prevents double service from client retries (same prompt re-submitted later = legitimate new work).

## H. Async batching

- **D-H1** Client-facing async batch endpoint on the router (simple schema; OpenAI `/v1/batches` parity = v2).
- **D-H2** The batch queue is **coordinator-owned and persistent** (postgres): survives coordinator restarts and member churn. Live-request state lives router-side (the router owns the client socket).

## I. Privacy and trust

- **D-I1** **Never log request or response bodies.** Log body **hashes** and **token counts**; character counts where token counts are not knowable (applies to queueing and records).
- **D-I2** Bodies retained **while queued** (needed to serve) and **expunged when completion returns to the client**. A model going offline must not drop a request.
- **D-I3** Records per request: queue time, service time, completion time, requested class, servicing model, prompt tokens, response tokens, any errors, initiating user.
- **D-I4** Trust model stated plainly: prompts transit the coordinator in flight; the serving member necessarily sees what they serve; the coordinator's operator is a trusted party; **no quality promise** (members offer whatever quantization/context they choose).
- **D-I5** Login-page paragraph (accepted, verbatim):

  > This pool is a volunteer network of personal GPUs, not a service. There is no quality guarantee: models are contributed by members in whatever quantization and context they choose to run, so a "medium" served by one member may differ from one served by another. Your prompts transit the pool coordinator while in flight and are served on another member's machine; the coordinator stores no prompts or responses after your request completes, and logs only metadata (sizes, timings, tags, errors). Use the pool as you would a favor: for work you would be comfortable running on a friend's machine.

## J. Security

- **D-J1** TLS on every pool link. No plaintext.
- **D-J2** **Opaque 256-bit user tokens**: shown once, revocable in the portal, stored only as a hash. Portal sessions via httpOnly cookies. **Minute-salt auth: CUT** (basic tokens over TLS accepted).
- **D-J3** **Invite-only**; membership managed by admins in the portal. **No automatic emails, ever.**
- **D-J4** Bootstrap/recovery: system admins connect directly to the backing postgres and add users; ship a SQL script that adds an `admin/admin` user. Rationale on record: an attacker with enough access to run that script is beyond application-level security anyway.
- **D-J5** **Error taxonomy is a control surface** (pi auto-retries on 500/502/503/520/524, `server_busy`, `overloaded`, `model is at capacity`, `stream was interrupted`, `timeout`, network errors; it compacts on overflow patterns). Fork rule: hold = slow 200 on an open socket; give-up = neutral 4xx wording; client-retry = 5xx/busy wording, only when retry is actually desired.
- **D-J6** **Protocol versioning**: version field in the handshake; the coordinator rejects earlier protocol versions (or supports them in parallel - decided by the implementer at bump time). Bumps are rare. The coordinator advertises its own version; a router facing a newer controller degrades to graceful offline, never a crash.

## K. Router <-> coordinator link

- **D-K1** **Plain HTTPS, no websockets.** The router issues `GET /pool/subscribe`; the coordinator **holds the response open** and writes events (SSE `data:` lines, ~25 s heartbeat comments; fallback: long-poll with ~25 s keepalive + re-issue).
- **D-K2** Router -> coordinator: results, errors, capacity changes, continuity reports, exclusion marks flow back as small **POST events** the moment they happen (the router owns the client socket, so disconnects are known instantly - no polling).
- **D-K3** Implemented with **curl** (already a llama.cpp dependency); cpp-httplib is server-only. No new transport dependency.
- **D-K4** Push, not polling: work offers arrive as events on the open GET. Steady state: a few KB/min per router.

## L. Harness compatibility

- **D-L1** **pi 1.4.0 verified end to end**: model-listing gate, dynamic (reactive) compaction on overflow, model-management controls used for exclusion.
- **D-L2** The one pool-specific requirement for **any** llama.cpp-compatible harness: **dynamic compaction** - on a context-overflow error, compact and retry. Everything else is the standard, unchanged API. (pi is verified, not exclusive.)
- **D-L3** Recommended client settings shipped with the fork: longer timeout for pool classes; no auto-retry on pool queue errors (D-G4).
- **D-L4** pi's listing gate (`modelIsSelectable`): selectable iff status in {`loaded`, `sleeping`} or (`unloaded` + `source: "preset"` + autoload). Tags use `loaded` (D-C2) -> compatible without pi changes.

## M. Chat reformatting

- **D-M1** Tag routing across members with different chat templates requires: parse under template A -> canonical message list -> re-serialize under template B. Built on llama.cpp's existing jinja engine; glue is the new part.
- **D-M2** **Round-trip assert** under debug flag `LLAMA_POOL_DEBUG_RT`: `canonicalize(parse_A(apply_A(M))) == M` (canonicalize = strip whitespace-only differences, normalize tool-call ids) - checked on every reformat in debug mode and as a CI corpus test (agent turns, tool calls, reasoning blocks, multimodal parts). That is the a->b->a identity.

## N. Code boundaries

- **D-N1** **Unchanged** (upstream, byte-identical): inference core (ggml, backends, sampling), single-model server, child-process machinery, entire client-facing API.
- **D-N2** **Additive new files** in the fork: pool client (control channel), class resolution + advertisement merge, async batch endpoint, capacity reporting, continuity hashing, chat-reformatting glue, web-UI pool panels.
- **D-N3** **Small hooks** into existing router code: request path (class intercept before model-name lookup), `/v1/models` merge, `/props` synthesis for classes, load/unload handlers (pool member -> exclusion mark).
- **D-N4** **Separate repo** (`pool-coordinator`): Go service, SvelteKit portal (served as static files), postgres schema, and the protocol contract (JSON schemas + message catalog in a shared directory, mirrored by C++/Go/TS).

## O. Scope

- **D-O1** **MVP**: pool connection (D-K*); class routing/advertisement/two-phase dispatch (D-B*, D-C*, D-D*); continuity hash + skip + soft pin + metrics (D-E1-D-E3, D-E5, D-E6); pre-first-token hop (D-G1); exclusion marks (D-F1); async batch endpoint (D-H*); fairness queue (D-F2-D-F4, D-F6); sharing policy + dashboard extensions (offer/withhold per model and GPU via UI + CLI, one-click pause, queued eviction, GPU/VRAM utilization, pool status); records + anti-memory (D-I*); overflow path with standard message + `max_tokens` clamp (D-C4-D-C6); idempotency (D-G5); portal (users/invites, tokens, user settings, admin mirror); security (D-J*); protocol v1 (D-J6, D-K*).
- **D-O2** **v2 (deferred)**: mid-flight partial-response handoff (D-G3); live parameter adjustment (apply-now / queue-until-idle, worker restart is acceptable); authoritative per-conversation cache-liveness reporting from the worker; admin-curated shared priority lists (followable); parallel protocol-version support; OpenAI `/v1/batches` schema parity.
- **D-O3** **CUT**: minute-salt token auth; end-to-end body encryption (future idea at most - the coordinator never needs to read the body to dispatch); router-to-router proxying; marketplace/pricing.

## P. Verified dependencies (facts the decisions rest on)

Re-verify before implementation; palace entries 1793172830304 and 1793225681364 hold the full detail.

- **V1** pi 1.4.0 has reactive compaction: `pi-ai/dist/utils/overflow.js` `isContextOverflow()` - (a) error message patterns incl. `/exceeds the available context size/i`, which matches llama.cpp's native error at `tools/server/server-context.cpp:3954`; (b) silent overflow: `usage.input + cacheRead > assumed contextWindow`; (c) `length` stop + zero output + input filling the window; plus `isRecoverableLength`. Recovery = one compact-and-retry attempt.
- **V2** pi 1.4.0 sends **no conversation id header** (full-dist grep: none) -> continuity key must be content-derived (D-E1).
- **V3** pi auto-retry patterns (`pi-ai/dist/utils/retry.js`): 429/500/502/503/520/524, `server_busy`, `overloaded`, `model is at capacity`, `stream was interrupted`, `timeout`, network errors -> drives D-J5.
- **V4** pi listing gate in `provider.js` `modelIsSelectable` -> drives D-C2/D-L4.
- **V5** Router internals: `server_models` maps model name -> one child subprocess; strict name routing, no load-balancing between instances; `ensure_model_ready` blocks the HTTP thread; preset change on a running model **unloads** it (does not respawn); `/slots`, `/props`, `/metrics`, `/tokenize` exist and are proxied in router mode; model `tags` exist (informational today); single `--api-key`, no per-user identity.
- **V6** No `/v1/batches` anywhere in upstream (grep-verified) -> the batch endpoint is genuinely new.
- **V7** cpp-httplib is server-only (no WS client); curl is an existing dependency -> D-K1/D-K3.
- **V8** The user-observed "compaction fired erroneously when the model was unavailable" is explained by V1(b): after unload+reload, real usage desyncs from pi's *cached* contextWindow -> false positive. Hence D-C3/D-C4 (advertise high, never below reality).

## Q. Open items

- **Q1** Timeline + who builds what (group's call; matrix is sized for one C++ dev on the fork side, one web dev on the controller side, largely parallel after the protocol lands).
- **Q2** Final names (working: `llama.cpp-pool`, `pool-coordinator`).
- **Q3** Group approval of `BRIEF.md` / `APPROACH.md` (pending).

## Next step (agreed)

Implementation plan: **P01 protocol message catalog + JSON schemas first**, then the vertical slice (pool connection + class resolution + advertisement + local dispatch: **stock pi, zero config changes, sees a class, request routes end to end**), then controller against a fake router, then the rest in matrix order.

---

## Change log

- 2026-10-28 - workspace set up: fork worktree `llama.cpp-pool/` (branch `pool/main` cut from tag `b11827`), coordinator dir `pool-coordinator/` with docs under `docs/`; service working name renamed `pool-controller` -> `pool-coordinator`.
- 2026-10-28 - design converged after three feedback rounds; this record created. All D-* current as written.
