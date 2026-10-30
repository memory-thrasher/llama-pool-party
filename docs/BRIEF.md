# Pooling our GPUs - brief

One-page summary for group review and approval. A more detailed write-up is attached (`APPROACH.md`).

## What it is

- A volunteer pool of personal GPUs for LLM inference.
- Built on **llama.cpp**, the open-source engine for running GGUF models locally, as a public fork.
- Two parts:
  - a **router**, the fork, running on each member's own machine; and
  - a **coordinator**, a small service on a lightweight cloud VM that keeps the pool in sync.
- Invite-only. Membership and settings are managed by moderators in a small web portal. No emails, nothing to maintain.

## What it feels like (your workflow)

- Your agent harness keeps talking to your own machine. You point it at your local router as you do today - no new client, no new API.
  - **pi** works out of the box (verified end to end).
  - Any other harness that speaks the llama.cpp server API should work, with one requirement: it must **compact dynamically** - i.e. when the server reports that the prompt does not fit, the harness compacts its context and retries.
- Your model picker shows **model classes** alongside your local models, e.g. `pool-text-medium`, `pool-image-small`, `pool-text-large`.
- Pick a class and your agent runs. If your machine can serve it, it does. Otherwise the request is quietly served by a member who can - same streaming, same tool calls, same everything, from your harness's point of view.
- You decide what to offer: which models, which GPUs, and when. One click (or one CLI call) pulls a GPU out of the pool - gaming, rendering, your life - and puts it back when you are done.
- Queue priority is earned, not paid: your rank scales with the ratio of response tokens your machines have served the pool to response tokens you have consumed, over a rolling week.

## Behind the scenes (high level)

- Your harness sends a chat request to your local router.
- If a local model fits the requested class and has a free slot, it is served locally. Done.
- Otherwise the router forwards the request to the coordinator, which queues it.
- The coordinator dispatches it to a member whose loaded model fits the class and has a free slot. The response streams back through your router to your harness, token by token.
- **Continuation awareness:** follow-up turns of an ongoing conversation are steered back to the member that served the previous turn (a short-lived pin, no reserved slots), so that member's prompt cache pays off.
- **Failover:** if a serving member fails before the first token, the coordinator re-dispatches within the same class. Mid-stream, you get a clean error and your harness retries the turn.
- **Oversized context:** if your conversation is bigger than any member can hold, the pool returns a standard overflow error. A harness that compacts dynamically compacts and retries, and the request succeeds.

## What we commit about your data

- The coordinator never stores prompts or responses after a request completes (bodies are held only while queued).
- It logs metadata only: sizes, timings, requested class, servicing model, errors, user.
- Honest trust model: your prompt transits the coordinator while in flight, and it is served on the machine of the member who services it. No quality promise - models are offered in whatever quantization and context the member chooses.
- Use the pool as you would a favor: for work you would be comfortable running on a friend's machine.

## Scope (proposed MVP)

- **Router:** pool connection; class routing and advertisement; async batching endpoint; sharing policy; dashboard extensions (pool status, GPU utilization, eviction).
- **Coordinator:** portal (invite-only users, tokens, settings); class registry with default size x capability buckets; queue with contribution-based priority; dispatch; metrics plus an "all users" admin mirror.
- **Deferred to v2:** mid-stream failover with partial-response carryover; live parameter adjustment; authoritative cache-liveness reporting.

## Ask of this group

- Approval of the design and MVP scope (details in `APPROACH.md`).
