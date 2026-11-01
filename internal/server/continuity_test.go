package server

// Continuity e2e tests (WS-08, D-E2/D-E3/D-E4/D-E6): skip-the-line, soft
// preference, the last_ok/gone guards, and the D-E6 outcome metrics on the
// records row.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// nextOffer returns the next work_offer the router receives, skipping
// non-offer envelopes (mirrored completion/error).
func nextOffer(t *testing.T, fr *FakeRouter, ctx context.Context, timeout time.Duration) poolproto.WorkOffer {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ev, err := fr.NextEvent(ctx, deadline.Sub(time.Now()))
		must(t, err)
		if ev.Type != poolproto.TypeWorkOffer {
			continue
		}
		return offerOf(t, ev)
	}
}

// waitRecord polls until the job's records row exists.
func waitRecord(t *testing.T, env *e2eEnv, ctx context.Context, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		n, err := env.db.Count(ctx, "select count(*) from records where job_id = "+pooldb.UUID(jobID))
		if err == nil && n >= 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("waitRecord: no records row for job %s in %s", jobID, timeout)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// contRecord returns (cont_outcome, cont_reason) from the job's records row
// (empty string = null).
func contRecord(t *testing.T, env *e2eEnv, ctx context.Context, jobID string) (string, string) {
	t.Helper()
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select coalesce(cont_outcome, ''), coalesce(cont_reason, '') from records where job_id = %s",
		pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows for job %s = %d, want 1", jobID, len(rows))
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 2 {
		t.Fatalf("bad cont record row: %s", rows[0])
	}
	return f[0], f[1]
}

// continuityRow returns (last_router_id, last_ok) for the conversation.
func continuityRow(t *testing.T, env *e2eEnv, ctx context.Context, convKey string) (string, string) {
	t.Helper()
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select coalesce(last_router_id::text, ''), last_ok from continuity where conv_key = %s",
		pooldb.S(convKey)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("continuity rows for %s = %d, want 1", convKey, len(rows))
	}
	f := strings.Split(rows[0], ",")
	return f[0], f[1]
}

// waitContinuity polls until the conversation's continuity row exists (the
// dispatcher writes it right after the records row on completion) and returns
// (last_router_id, last_ok).
func waitContinuity(t *testing.T, env *e2eEnv, ctx context.Context, convKey string, timeout time.Duration) (string, string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		rows, err := env.db.Query(ctx, fmt.Sprintf(
			"select coalesce(last_router_id::text, ''), last_ok from continuity where conv_key = %s",
			pooldb.S(convKey)))
		if err == nil && len(rows) == 1 {
			f := strings.Split(rows[0], ",")
			return f[0], f[1]
		}
		select {
		case <-deadline:
			t.Fatalf("waitContinuity: no continuity row for %s in %s", convKey, timeout)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// waitContinuityRouter polls until the hint's last_router_id becomes wantRouter
// and returns (last_router_id, last_ok).
func waitContinuityRouter(t *testing.T, env *e2eEnv, ctx context.Context, convKey, wantRouter string, timeout time.Duration) (string, string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		rows, err := env.db.Query(ctx, fmt.Sprintf(
			"select coalesce(last_router_id::text, ''), last_ok from continuity where conv_key = %s",
			pooldb.S(convKey)))
		if err == nil && len(rows) == 1 {
			f := strings.Split(rows[0], ",")
			if f[0] == wantRouter {
				return f[0], f[1]
			}
		}
		select {
		case <-deadline:
			t.Fatalf("waitContinuityRouter: hint for %s never reached %s", convKey, wantRouter)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// scriptedAccept accepts the offers for the job ids Allow() was called with
// and rejects every other offer with no_slot.
type scriptedAccept struct {
	mu     sync.Mutex
	accept map[string]bool
}

func newScriptedAccept() *scriptedAccept {
	return &scriptedAccept{accept: map[string]bool{}}
}

func (p *scriptedAccept) Allow(jobID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.accept[jobID] = true
}

func (p *scriptedAccept) Decide(of poolproto.WorkOffer) OfferDecision {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.accept[of.JobID] {
		return OfferDecision{Accept: true}
	}
	return OfferDecision{Accept: false, Reason: "no_slot"}
}

// TestE2E_Continuity_SkipTheLine: frA (1 slot) serves turn 1 of conversation
// K; an older pending job P is queued (body never staged: un-offerable); a
// second turn of K arrives. The warm continuation must be offered to frA
// before P's turn, served by frA, with cont_outcome skip/warm (D-E2).
func TestE2E_Continuity_SkipTheLine(t *testing.T) {
	env := newE2E(t)

	// frA: 1 slot, free. frB: loaded but fully busy -> not a candidate.
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 2, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	ndjson := []string{`{"choices":[{"delta":{"content":"ws08"}}]}`, "data: [DONE]"}
	body := []byte(`WS08-BODY-` + strings.Repeat("x", 2048))

	// Turn 1: served by frA, leaves the continuity hint.
	job1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job1 {
		t.Fatalf("turn-1 offer for job %s, want %s", of.JobID, job1)
	}
	must(t, frA.AcceptAndServe(ctx, job1, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, job1, 15*time.Second)
	if router, ok := waitContinuity(t, env, ctx, convKey, 15*time.Second); router != frA.ID() || ok != "t" {
		t.Fatalf("continuity after turn 1 = (%s,%s), want (%s,t)", router, ok, frA.ID())
	}

	// P: older pending job, never offerable (body never staged).
	pJobID, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)

	// The continuation of K arrives and must jump the queue.
	contJobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != contJobID {
		t.Fatalf("offer for job %s, want the warm continuation %s (ahead of %s)",
			of.JobID, contJobID, pJobID)
	}
	if of.Source != poolproto.SourceSkip {
		t.Fatalf("offer source = %q, want %q", of.Source, poolproto.SourceSkip)
	}
	must(t, frA.AcceptAndServe(ctx, contJobID, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, contJobID, 15*time.Second)

	outcome, reason := contRecord(t, env, ctx, contJobID)
	if outcome != "skip" && outcome != "warm" {
		t.Fatalf("cont_outcome = %q, want skip or warm", outcome)
	}
	if reason != "" {
		t.Fatalf("cont_reason = %q, want null", reason)
	}
	st, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(pJobID))
	must(t, err)
	if st != "pending" {
		t.Fatalf("job %s status = %q, want pending (never offered)", pJobID, st)
	}
	if router, ok := continuityRow(t, env, ctx, convKey); router != frA.ID() || ok != "t" {
		t.Fatalf("continuity after continuation = (%s,%s), want (%s,t)", router, ok, frA.ID())
	}
}

// TestE2E_Continuity_IdleDenied: same setup, but the hint is backdated past
// the skip window (IDLE_MS+1s): no queue priority (the older job is offered
// first) and the continuation is served cold with cont_reason 'idle' (D-E2,
// D-E6).
func TestE2E_Continuity_IdleDenied(t *testing.T) {
	env := newE2E(t)

	// frA: largest ctx, scripted accept (turns and P only). frB: accepts all.
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	sA := newScriptedAccept()
	frA.SetPolicy(sA)
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	ndjson := []string{`{"choices":[{"delta":{"content":"ws08"}}]}`, "data: [DONE]"}
	body := []byte(`WS08-BODY-` + strings.Repeat("y", 2048))

	// Turn 1: served by frA.
	job1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	sA.Allow(job1)
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job1 {
		t.Fatalf("turn-1 offer for job %s, want %s", of.JobID, job1)
	}
	must(t, frA.AcceptAndServe(ctx, job1, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, job1, 15*time.Second)
	waitContinuity(t, env, ctx, convKey, 15*time.Second)

	// Backdate the hint: IDLE_MS + 1s (past the skip window, inside the
	// soft-preference window).
	must(t, env.db.Exec(ctx, fmt.Sprintf(`
update continuity set last_served_at = now() - '%d milliseconds'::interval
where conv_key = %s`, config.DefaultContIdleMS+1000, pooldb.S(convKey))))

	// P: older pending job. It is offered first (no skip priority now).
	jobP, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	sA.Allow(jobP)
	must(t, frA.StageBody(ctx, jobP, body))
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobP {
		t.Fatalf("first offer after backdate for job %s, want older job %s", of.JobID, jobP)
	}
	must(t, frA.AcceptAndServe(ctx, jobP, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobP, 15*time.Second)

	// The continuation: no priority, hinted router fronted (soft pin) but
	// scripted to reject -> served by frB: cold, reason idle.
	contJobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	of = nextOffer(t, frA, ctx, 15*time.Second) // frA is offered first: fronting held
	if of.JobID != contJobID {
		t.Fatalf("continuation offer for job %s, want %s", of.JobID, contJobID)
	}
	if of.Source != poolproto.SourcePin {
		t.Fatalf("offer source = %q, want %q", of.Source, poolproto.SourcePin)
	}
	must(t, frA.DecideAndRespond(ctx, of, len(body), convKey, nil)) // scripted no_slot
	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != contJobID {
		t.Fatalf("bounce offer for job %s, want %s", of.JobID, contJobID)
	}
	must(t, frB.AcceptAndServe(ctx, contJobID, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, contJobID, 15*time.Second)

	outcome, reason := contRecord(t, env, ctx, contJobID)
	if outcome != "denied" && outcome != "cold" {
		t.Fatalf("cont_outcome = %q, want denied or cold", outcome)
	}
	if reason != "idle" {
		t.Fatalf("cont_reason = %q, want idle", reason)
	}
	router, err := env.db.FirstString(ctx,
		"select router_id::text from records where job_id = "+pooldb.UUID(contJobID))
	must(t, err)
	if router != frB.ID() {
		t.Fatalf("continuation served by %s, want %s (the hint was not honored)", router, frB.ID())
	}
}

// TestE2E_Continuity_FailedNoPin: the first service of conversation K fails
// (mid_stream error) -> the hint is marked last_ok=false (D-E4) -> the next
// same-conversation job gets no hint: normal queue, normal candidates (first
// offer by ctx order, not fronted), cont_outcome denied / reason error.
func TestE2E_Continuity_FailedNoPin(t *testing.T) {
	env := newE2E(t)

	// frA: largest ctx, starts busy (not a candidate for turn 1); freed by a
	// capacity event before job 2. frB: small ctx, serves turn 1.
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 1, poolproto.ModelLoaded)
	sA := newScriptedAccept()
	frA.SetPolicy(sA)
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	body := []byte(`WS08-BODY-` + strings.Repeat("z", 2048))

	// Turn 1: frB is the only candidate; it accepts, pulls the body, then the
	// service fails mid-stream.
	job1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	of := nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != job1 {
		t.Fatalf("turn-1 offer for job %s, want %s", of.JobID, job1)
	}
	must(t, frB.AcceptAndHold(ctx, job1))
	must(t, frB.PostError(ctx, job1, poolproto.StageMidStream, poolproto.ErrorOther,
		"fake mid-stream failure (ws08 test)"))
	waitRecord(t, env, ctx, job1, 15*time.Second)

	router, okStr := continuityRow(t, env, ctx, convKey)
	if router != frB.ID() || okStr != "f" {
		t.Fatalf("continuity after failed service = (%s,%s), want (%s,f)", router, okStr, frB.ID())
	}

	// Free frA's slot: it becomes the normal-order first candidate (largest
	// ctx), so job 2's first offer proves fronting did NOT happen.
	must(t, frA.PostCapacity(ctx, []poolproto.ModelEntry{{
		ModelID: "md-a", Tags: []string{"pool-text-md"}, NCtx: 131072,
		NSlots: 1, BusySlots: 0, Status: poolproto.ModelLoaded,
	}}))
	frA.SetBusy(0)

	job2, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	sA.Allow(job2)
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job2 {
		t.Fatalf("job-2 offer for job %s, want %s", of.JobID, job2)
	}
	if of.Source != poolproto.SourceQueue {
		t.Fatalf("job-2 offer source = %q, want %q (no hint after a failed service)",
			of.Source, poolproto.SourceQueue)
	}
	ndjson := []string{`{"choices":[{"delta":{"content":"ws08"}}]}`, "data: [DONE]"}
	must(t, frA.AcceptAndServe(ctx, job2, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, job2, 15*time.Second)

	outcome, reason := contRecord(t, env, ctx, job2)
	if outcome != "denied" || reason != "error" {
		t.Fatalf("job-2 (cont_outcome,cont_reason) = (%q,%q), want (denied,error)", outcome, reason)
	}
	router, okStr = waitContinuityRouter(t, env, ctx, convKey, frA.ID(), 15*time.Second)
	if okStr != "t" {
		t.Fatalf("continuity after job 2 = (%s,%s), want (%s,t) - hint restored by a clean service",
			router, okStr, frA.ID())
	}
}

// TestE2E_Continuity_GoneRouter: the hint points at a router that is unloaded
// (not connected with a loaded model) -> the warm pick's skip is denied
// reason=gone; the job still completes via the other member; no hang (D-E4).
func TestE2E_Continuity_GoneRouter(t *testing.T) {
	env := newE2E(t)

	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	ndjson := []string{`{"choices":[{"delta":{"content":"ws08"}}]}`, "data: [DONE]"}
	body := []byte(`WS08-BODY-` + strings.Repeat("q", 2048))

	// Turn 1: served by frA (largest ctx) -> hint points at frA.
	job1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job1 {
		t.Fatalf("turn-1 offer for job %s, want %s", of.JobID, job1)
	}
	must(t, frA.AcceptAndServe(ctx, job1, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, job1, 15*time.Second)
	waitContinuity(t, env, ctx, convKey, 15*time.Second)

	// frA unloads its model: no longer a candidate.
	must(t, frA.PostCapacity(ctx, nil))

	// The continuation: warm hint, but the hinted member is gone.
	contJobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != contJobID {
		t.Fatalf("continuation offer for job %s, want %s", of.JobID, contJobID)
	}
	must(t, frB.AcceptAndServe(ctx, contJobID, len(body), convKey, ndjson))
	waitRecord(t, env, ctx, contJobID, 15*time.Second)

	outcome, reason := contRecord(t, env, ctx, contJobID)
	if outcome != "denied" || reason != "gone" {
		t.Fatalf("(cont_outcome,cont_reason) = (%q,%q), want (denied,gone)", outcome, reason)
	}
	st, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(contJobID))
	must(t, err)
	if st != "done" {
		t.Fatalf("continuation status = %q, want done", st)
	}
}
