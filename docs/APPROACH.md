# Pooling our GPUs - approach

Detailed write-up of the design, for the curious. Companion to `BRIEF.md`.

## 1. Overview

A volunteer pool of personal GPUs for LLM inference. Each member runs the fork on their own machine; a single small coordinator service matches work to available capacity. The pool is an **additive layer**: clients, harnesses, and local models keep working exactly as they do today, and pool features switch on only when a router is connected to the coordinator.

```
+----------------+          +----------------------------------------------+
| your agent     |          | COORDINATOR (one lightweight cloud VM)       |
| harness        |          | Go service + postgres + web portal           |
| (pi today, or  |          |  - class registry (tags)                     |
| any llama.cpp- |          |  - queue + contribution-based priority       |
| compatible     |          |  - dispatch engine                           |
| harness)       |          |  - records / metrics / admin mirror          |
+-------+--------+          +----------------------+-----------------------+
        | HTTP, unchanged API                       ^
        v                                            | HTTPS, router-initiated
+----------------+         push + events             |
| YOUR ROUTER    |<---------------------------------+
| (llama.cpp     |                                  |
|  fork)         |        +-------------------------+-------------------+
|  - local models|        | MEMBER ROUTERS: other members' machines,    |
|  - web UI      |        | running the same fork, each offering what   |
|  - pool client |        | they chose to offer                          |
+----------------+        +---------------------------------------------+
```

Key properties:

- **Outbound-only networking for members.** The router initiates the connection to the coordinator. No inbound ports, no port-forwarding, works behind NAT.
- **One coordinator, no redundancy** (v1). It holds no unique state that cannot be rebuilt: everything durable lives in postgres.
- **Local-first.** A router with no coordinator connection behaves exactly like an ordinary llama.cpp router.

## 2. The llama.cpp router, and how we adopt it

llama.cpp ships a **router mode** of its server: one process that supervises multiple model instances (each a child process pinned to a chosen GPU), routes incoming requests to the instance named by the request, and manages the model lifecycle (load, unload, sleep, presets, a web UI, a live status stream). It is single-machine: it has no concept of other machines.

We adopt this router as the member-side foundation and extend it:

- **Unchanged:** inference core, sampling, the single-model server, the child-process machinery, and the entire client-facing API (OpenAI-compatible endpoints, props, metrics, slots, streaming, model management).
- **Extended (this fork):** a pool client that speaks to the coordinator; **class routing** (see below) resolved before ordinary model lookup; class advertisement merged into the model list; an async batching endpoint; capacity reporting; continuity hashing; and web-UI panels for pool state.
- **Boundary rule:** when no coordinator is configured (or the connection is down), the fork is behaviorally identical to upstream router mode. Pool code is a pure overlay. This is also what keeps the fork mergeable against upstream, which moves daily.

## 3. Classes ("tags")

A **class** is a requestable model bucket. A request names a class (or a concrete model id); any member with a suitable loaded model can service it.

- Classes reuse llama.cpp's existing model **tags** as the underlying mechanism.
- Default classes are **size x capability**, e.g. `pool-text-medium`, `pool-image-small`.
- **Size buckets** (defaults, controller-configurable; boundaries chosen to separate the model families that actually exist):

  | class | total params |
  |---|---|
  | `xs` | < 10B |
  | `sm` | 10B - 25B |
  | `md` | 25B - 60B |
  | `lg` | 60B - 250B |
  | `xl` | 250B - 1.5T |
  | `2xl` | > 1.5T (headroom; nothing common today) |

- **MoE models** are bucketed by **total** parameters (VRAM footprint and capability proxy); active parameters are recorded as metadata.
- **Capability tags overlap freely** (not just vision): a text model is `text`; a VLM is `text` and `image`; audio/TTS models would add their own capability. Size buckets are disjoint; capability is not.
- **Admin labels** (curated in the portal, visible to all users) and **user labels** (per user, may overlap with anything) are available on top of the defaults, for when people inevitably have different opinions.
- The `pool-` prefix is reserved for default class names.

### Advertisement and advertised context

- A class appears in your model list **iff some connected member has a model loaded under it**. Slot occupancy does not affect advertisement; it affects dispatch (a busy class queues, it does not vanish).
- A class with no members is not advertised at all, and requests for it get the standard "model not loaded" error. Membership is checked twice: at request receipt and again at dispatch.
- **Advertised context** for a class = `min(average + std_dev, max)` across its loaded members (config option: `max`). This is deliberately on the high side, by design:
  - Invariant 1: it must never sit *below* what members can serve (that causes harnesses to compact unnecessarily).
  - Invariant 2: every overflow rejection - local, coordinator pre-check, or the serving member itself - carries the same standard message (`...exceeds the available context size...`), which is the message llama.cpp already emits natively and which pi (and other capable harnesses) recognize as "compact and retry".
  - Invariant 3: the router clamps `max_tokens` to the selected member's capacity at dispatch time (harnesses like pi set `max_tokens` to the advertised window; the clamp makes high advertisement safe).
- The formula is recomputed when membership changes.

## 4. Request lifecycle

### Live request

1. Your harness sends a chat request (model = class or concrete id) to your local router.
2. Router checks: is there a **local** member of that class with a **free slot**? If yes, serve locally. Done.
3. Otherwise the router forwards the request to the coordinator, which queues it (live queue).
4. The coordinator dispatches to a member whose loaded model fits the class, honoring:
   - your **priority list** for that class (which models you prefer),
   - **continuity** (below),
   - **large-prompt preference** (a big prompt is offered first to members with the most context headroom within the class),
   - then **queue priority** (below).
5. Dispatch is **two-phase**: the coordinator sends an *offer* (class, estimated prompt size, context needed, deadline) to a candidate member; the member accepts with a short lease or rejects with a reason (no slot, context too small, model gone). Rejection moves the offer to the next candidate - **no full prompt has been transferred** on a bounce. On accept, the member pulls the body, streamed, and may still abort early.
6. The response streams back through your router to your harness, token by token.
7. On completion the member reports metadata: prompt/response token counts (character counts as fallback), timings, outcome. The coordinator records it and updates the fairness counters.

### Continuation awareness (cache affinity)

Agents rarely make one request - they make long conversations with tool calls in between. Re-prefilling the same context on a different member wastes compute.

- **Key:** SHA-256 of the **first 8 KB of the raw request payload** (the messages as sent by the harness, before any chat-template transformation). A conversation grows at the tail, so the head hash is stable across turns, identical across members by construction, and needs no tokenization. It breaks when the harness compacts - which is correct, because the prompt cache is cold then. Two forked conversations sharing a head hash are pinned to the same member while both stay warm; that is desirable.
- **Skip-the-line:** a new request for a conversation is served ahead of the queue on the member that last served it, if that member reports the conversation still cached, or if it has been idle less than a threshold (default 60 s, configurable). Beyond that, up to ~10 min, the member gets a **soft preference** (no skip).
- **Soft pin:** when a member with **multiple slots** completes a request, the *next* request of the same conversation gets queue-priority on that member for up to the same threshold. No slot is ever reserved or held; if no slot is free when the follow-up arrives, it queues normally. A member can therefore oscillate between two active conversations, keeping both warm; a running task that evicts one conversation's cache simply ends its pin.
- **Guards:** skip/pin require the member's last service of the conversation to have completed cleanly. They are invalidated by: model/GPU eviction from the pool, member disconnect, and failed service.
- **Metrics:** how often continuations do and do not get their member (hit / miss / skipped / denied, with reason) - the payoff of this feature is measured, not assumed.

### Async batching

- Clients (scripts, overnight jobs) submit a batch of jobs to their router's batching endpoint and poll for results.
- Batches live in the coordinator's **persistent** queue (postgres): they survive coordinator restarts and member comings-and-goings.
- **Batches are always lower priority than live requests**, and their token consumption counts with a discount weight (default 0.5) in the fairness ratio - a small reward for choosing batch over real-time.

### Failure handling

- **Before the first token** (dispatch failure, member crash, model gone, immediate error): the coordinator re-dispatches within the same class, automatically, unless the user has disabled hop-on-error.
- **After the first token** (member dies mid-stream): the harness receives a clean, structured error and retries the turn (harnesses with dynamic compaction handle this fine; a retry is a normal event).
- **v2 (deferred):** carry the buffered partial response to a replacement member, so a mid-stream failure resumes the generation on a new model in the same class.

### Context overflow

- If a request is bigger than any member can hold, it is rejected early (pre-check at the router/coordinator, before compute) with the standard overflow message (Invariant 2).
- A harness that compacts dynamically (pi does this natively; any llama.cpp-compatible harness with the same behavior) compacts and retries; the smaller request then fits. The cost is one cheap round trip.

## 5. Fairness

- **Priority** for a user = `(response tokens served by their machines + 1) / (response tokens consumed by them + 1)`, over a rolling window (default 1 week, configurable). The `+1` smoothing keeps new members at a fair baseline and avoids division by zero.
- When the queue is contended, the coordinator looks at the **top k oldest** pending items (default k = 10) and services the one from the highest-priority user. A user's items are never reordered relative to each other.
- Serving one request naturally lowers the user's priority for their remaining items (their denominator just grew) - no special logic needed.
- The ratio is gameable in theory (serve cheap tokens, consume expensive ones); the `+1` smoothing and the class system are the accepted mitigations. Users who consume 4B-class tokens asked for 4B-class tokens.
- **Exclusion:** a user can mark a specific model as "not for me" using the ordinary model load/unload controls their harness already exposes (in pi: select it in the model manager). For pool members this is a preference mark - the model stops being advertised as loaded to that user and is avoided when dispatching that user's class requests. Loading it again unmarks it. Local models keep their real load/unload semantics. Explicit model-id requests always bypass the mark.

## 6. Sharing policy and dashboard

- Each member chooses, per model and per GPU, what is offered to the pool and when, via the router's web UI and CLI. No automatic model swapping.
- One click (or CLI) **pauses the whole router** - pull every GPU out for gaming, back in later. No penalty beyond missed contribution.
- Members can evict an individual model or GPU from the pool; eviction can be **queued** (happens as soon as the running operation finishes), without interrupting ongoing work on other devices.
- The coordinator is notified of evictions and breaks the affected continuity pins immediately.
- The router's web UI (the one llama.cpp already ships) is extended with: pool connection state, queue position, contribution/consumption, GPU and VRAM utilization, and the eviction controls above.
- The coordinator's portal mirrors all of it **for all users** (the moderators' console).

## 7. Privacy and trust

- **Anti-memory rules:** the coordinator never logs request or response bodies. It logs body **hashes** and token counts (character counts where token counts are not knowable). Bodies are retained **while queued** (they must be, to be served) and are expunged when completion is returned.
- Records per request: queue time, service time, completion time, requested class, servicing model, prompt tokens, response tokens, any errors, initiating user.
- **Trust model, stated plainly:** prompts transit the coordinator in flight, and are served on the machine of the member who services them. The coordinator's operator is a trusted party. The serving member necessarily sees what they serve. There is no quality promise. The login page carries a short paragraph to this effect.
- (Future, not planned: the coordinator never needs to *read* a prompt to dispatch - it routes by class - so end-to-end encryption of the body to the serving member is theoretically possible later.)

## 8. Security

- TLS on every pool link. No plaintext.
- **Tokens:** opaque 256-bit user tokens (shown once, revocable in the portal), stored only as a hash. The coordinator's first admin is bootstrapped by a shipped SQL script - an attacker with enough access to run it is beyond application-level security anyway.
- **Invite-only.** No emails, ever.
- **Error taxonomy is a control surface:** harnesses like pi auto-retry on 5xx and "busy"-style errors, and compact on overflow-style errors. The fork's error codes and wording are chosen deliberately to get hold/retry/compact semantics right for each situation.
- **Protocol versioning:** the router<->coordinator protocol carries a version; the coordinator rejects older versions (or supports them in parallel, decided at bump time). Version bumps are rare. The coordinator advertises its own version.

## 9. The router<->coordinator link

Plain HTTPS, no websockets:

- The router opens a `GET /pool/subscribe`; the coordinator **holds the response open** and writes events (server-sent events, with ~25 s heartbeat comments; long-poll with re-issue as the fallback for hostile networks). Work offers arrive as events on this connection - push, not polling.
- Everything the router learns (results, errors, capacity changes, continuity reports, exclusion marks) goes back as small POST events, the moment they happen.
- Steady-state traffic is a few KB per minute per router.
- The router re-checks class membership at dispatch time, because the world changes between offer and send.

## 10. Harness compatibility

- **pi** is verified end to end: model listing, class selection, streaming, dynamic compaction on overflow errors, and the model-management controls used for exclusion.
- **Any harness that speaks the llama.cpp server API** should work, with one requirement: **dynamic compaction** - on a context-overflow error, compact and retry. That is the only pool-specific behavior a harness must have; everything else is the standard, unchanged API.
- The fork ships with recommended client settings: longer request timeouts for pool classes, and no auto-retry on pool queue errors (a retry would just go to the back of the line).

## 11. Code boundaries (what is new, what is old)

| Layer | Status |
|---|---|
| Inference core (ggml, backends, sampling) | **Unchanged**, upstream |
| Single-model server and child-process machinery | **Unchanged**, upstream |
| Client-facing API (OpenAI-compatible, props, metrics, streaming) | **Unchanged**; clients keep working |
| Router: pool client (control channel, offers, events) | **New files**, additive |
| Router: class resolution (before model lookup), class advertisement in the model list, `/props` synthesis for classes | **New files + small hooks** in the router's request path |
| Router: async batching endpoint | **New**, additive |
| Router: capacity reporting, continuity hashing | **New**, additive |
| Router: chat-template reformatting glue (parse under template A, re-serialize under template B) with round-trip assert under a debug flag (`canonicalize(parse_A(apply_A(M))) == M`) | **New**, additive; built on the existing jinja engine |
| Router: web-UI pool panels and eviction controls | **Extension** of the existing UI |
| Coordinator service (Go) | **New repo** (`pool-coordinator`), standalone |
| Portal (SvelteKit SPA, served by the Go binary) | **New**, in the coordinator repo |
| Postgres schema (users, routers, models, classes, queue, records, affinity, tokens) | **New**, in the coordinator repo |
| Protocol contract (JSON schemas, message catalog, version) | **New**, shared directory mirrored by C++ (router), Go (coordinator), TS (portal) |

Why the coordinator is a separate repo and not C++: it is a loop of external API calls with a database and a web portal - exactly the kind of work llama.cpp's own development guidelines exclude from its C++ server code. A Go binary + postgres is boring to operate on a lightweight VM, and boring is what we want from the thing that holds everyone's tokens.

## 12. Testing (as implemented)

- **Live acceptance slice** (the MVP acceptance test, scripted and re-runnable): stock pi, zero configuration changes, pointed at a member router, sees pool classes in `--list-models`, a local request is served locally (no coordinator round trip), a cross-member request is served by the other member (record `ok=true`, `served_by != origin`), an oversized prompt compacts dynamically and succeeds, and a concrete (non-pool) model request never touches the pool. Plus: no leftover bodies, and an anti-memory canary absent from all logs and the data dir. 14 checks, green at every banked HEAD.
- Upstream's server test suite (pytest) runs as the regression net for "the unchanged flow stays unchanged"; the pool unit suites (dispatch, classes, link, status) extend it.
- Coordinator tested against a **fake router** (scripted offers, bounces, disconnects, mid-stream deaths) and a **fake coordinator** on the fork side; Go e2e suites cover intake, dedupe, fairness, batches, continuity, restart, records, and the anti-memory canary.
- Template reformatting: CI corpus of real Qwen3/Llama-3.1/Mistral template texts with agent turns, tool calls (incl. parallel), reasoning blocks, and multimodal parts, a per-template round-trip pass report, and the pinned cross-template multimodal limitation. 104 assertions.
- The pool web panels have browser-level Playwright E2E (connected / not-configured / degraded states) on top of the component (vitest) and Storybook coverage.

## 13. Operations

- Coordinator: one VM, one Go binary + postgres. Restart-safe: durable state in postgres; in-flight dispatch re-announced on restart (offered/running jobs terminal-fail as `coordinator_restart`, pending jobs re-register).
- Router offline = ordinary router; coordinator down = pool dark, local service unaffected.
- Moderators: portal for membership/labels/priorities; postgres direct access for bootstrap and recovery.
- Full runbook (all configuration knobs on both sides, body lifecycle, backup, log hygiene): `ops/OPERATIONS.md` at the workspace root.

## 14. Scope

**MVP:** pool connection; class routing, advertisement, and two-phase dispatch; async batching endpoint; continuity (hash, skip, soft pin, metrics); pre-first-token hop; exclusion marks; sharing policy and dashboard extensions; portal (users, tokens, settings, admin mirror); fairness queue; records and anti-memory; overflow path with standard message and `max_tokens` clamp.

**v2 (deferred):** mid-stream failover with partial-response carryover; live parameter adjustment (apply-now / queue-until-idle); authoritative per-conversation cache-liveness reporting; admin-curated shared priority lists; parallel protocol-version support.

**Cut:** minute-salt token auth (plain tokens over TLS); end-to-end body encryption (future at most); router-to-router proxying; marketplace or pricing of any kind.

## 15. Risks

| Risk | Mitigation |
|---|---|
| Mid-stream failure semantics are the hardest part of the design | v1 limits hops to pre-first-token; clean structured errors; harness retry is the recovery path |
| Upstream llama.cpp moves daily; fork drifts | Thin fork: additive pool files, small hooks only, upstream test suite as gate, pool-off = byte-identical behavior |
| Abuse: a greedy user hogs free GPUs | Trusted-members design (invite-only); batches discounted and deprioritized; soft caps configurable |
| Trust: the coordinator's operator is a trusted party | Stated plainly on the login page; anti-memory rules enforced in code; metadata-only records |
| Harnesses without dynamic compaction hit overflow errors they cannot recover | Documented requirement; the standard overflow message is emitted early and cheaply so the failure is obvious, not silent |
