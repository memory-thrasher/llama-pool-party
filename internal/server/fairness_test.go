package server

// Fairness queue e2e tests (WS-11, D-F2/D-F3) plus the parked-job
// hardening (DISP-RESIDUAL-1): the served/consumed ratio orders users,
// the top-k window bounds the pick, per-user FIFO is preserved, a
// no_slot-exhausted park backs off, and a pre_token-overflow-exhausted
// job terminal-fails with the D-C5 overflow wording.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// TestE2E_Fairness_BaselineOldestFirst: two fresh users (ratio 1.0 each),
// one pending job each: the D-F2 tie is broken by the oldest job (D-F3).
func TestE2E_Fairness_BaselineOldestFirst(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	_, tokA := setupAuthUser(t, env, "fair-base-a@example.com", "Fair Base A", "member", "active")
	_, tokB := setupAuthUser(t, env, "fair-base-b@example.com", "Fair Base B", "member", "active")

	frA := NewFakeRouter(env.baseURL, tokA, "fair-base-a", "fair-base-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, tokB, "fair-base-b", "fair-base-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(ctx))

	body := []byte(`WS11-BASELINE-BODY-` + strings.Repeat("a", 1024))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws11"}}]}`, "data: [DONE]"}

	jobA, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, jobA, body))
	jobB, err := frB.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frB.StageBody(ctx, jobB, body))

	// Both ratios are the 1.0 baseline: the older job is served first.
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobA {
		t.Fatalf("first offer for job %s, want the older job %s (ratio 1.0 tie)", of.JobID, jobA)
	}
	must(t, frA.AcceptAndServe(ctx, jobA, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA, 15*time.Second)

	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobB {
		t.Fatalf("second offer for job %s, want %s", of.JobID, jobB)
	}
	must(t, frA.AcceptAndServe(ctx, jobB, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobB, 15*time.Second)
}

// TestE2E_Fairness_DeprioritizeConsumer: A's request is served by B's
// machine (A consumed, B served) -> A's ratio drops below B's -> B's NEWER
// pending job is served before A's OLDER one (D-F2).
func TestE2E_Fairness_DeprioritizeConsumer(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	_, tokA := setupAuthUser(t, env, "fair-dep-a@example.com", "Fair Dep A", "member", "active")
	_, tokB := setupAuthUser(t, env, "fair-dep-b@example.com", "Fair Dep B", "member", "active")

	// frA: busy (not a candidate). frB: serves everything.
	frA := NewFakeRouter(env.baseURL, tokA, "fair-dep-a", "fair-dep-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, tokB, "fair-dep-b", "fair-dep-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(ctx))

	body := []byte(`WS11-DEP-BODY-` + strings.Repeat("b", 1024))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws11"}}]}`, "data: [DONE]"}

	// A consumes: B's router serves A's request (A's machine is busy).
	jobA1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobA1 {
		t.Fatalf("step-1 offer for job %s, want %s", of.JobID, jobA1)
	}
	must(t, frB.AcceptAndServe(ctx, jobA1, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA1, 15*time.Second)

	// Now A (older pending job) and B (newer pending job) both wait.
	jobA2, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	jobB, err := frB.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	// Stage both bodies together so the first pick sees both jobs.
	must(t, frA.StageBody(ctx, jobA2, body))
	must(t, frB.StageBody(ctx, jobB, body))

	// A's ratio is below the baseline (it consumed from B's machine) and B's
	// is above it (it served A): B's newer job jumps ahead of A's older one.
	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobB {
		t.Fatalf("first offer for job %s, want %s (A's consumption must deprioritize A)", of.JobID, jobB)
	}
	must(t, frB.AcceptAndServe(ctx, jobB, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobB, 15*time.Second)

	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobA2 {
		t.Fatalf("second offer for job %s, want %s", of.JobID, jobA2)
	}
	must(t, frB.AcceptAndServe(ctx, jobA2, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA2, 15*time.Second)
}

// TestE2E_Fairness_PerUserFIFO: one user with two pending jobs: the older
// one is always offered first (the D-F2 ratio is per-user, never per-job).
func TestE2E_Fairness_PerUserFIFO(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "fair-fifo", "fair-fifo")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(ctx))

	body := []byte(`WS11-FIFO-BODY-` + strings.Repeat("f", 1024))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws11"}}]}`, "data: [DONE]"}

	job1, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, job1, body))
	job2, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, job2, body))

	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job1 {
		t.Fatalf("first offer for job %s, want the older job %s (per-user FIFO)", of.JobID, job1)
	}
	must(t, frA.AcceptAndServe(ctx, job1, len(body), "", ndjson))
	waitRecord(t, env, ctx, job1, 15*time.Second)

	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job2 {
		t.Fatalf("second offer for job %s, want %s", of.JobID, job2)
	}
	must(t, frA.AcceptAndServe(ctx, job2, len(body), "", ndjson))
	waitRecord(t, env, ctx, job2, 15*time.Second)
}

// TestE2E_Fairness_TopKWindow: POOL_FAIR_TOPK=2, three pending users: the
// pick is bounded to the k oldest jobs, so the heaviest (oldest) user is
// served LAST even though it has been waiting longest, and the heavy
// server (user C, outside the first window) cannot pull its job in early.
func TestE2E_Fairness_TopKWindow(t *testing.T) {
	env := newE2EWith(t, e2eOpts{fairTopK: 2})
	ctx := context.Background()
	userA, tokA := setupAuthUser(t, env, "fair-topk-a@example.com", "Fair Topk A", "member", "active")
	_, tokB := setupAuthUser(t, env, "fair-topk-b@example.com", "Fair Topk B", "member", "active")
	_, tokC := setupAuthUser(t, env, "fair-topk-c@example.com", "Fair Topk C", "member", "active")

	frA := NewFakeRouter(env.baseURL, tokA, "fair-topk-a", "fair-topk-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, tokB, "fair-topk-b", "fair-topk-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 65536, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))
	frC := NewFakeRouter(env.baseURL, tokC, "fair-topk-c", "fair-topk-c")
	frC.LoadModel("md-c", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frC.Connect(ctx))

	// A consumed heavily (from C's machine): A's ratio is near zero, C's is
	// huge. Within the window, the baseline users beat A.
	must(t, env.db.Exec(ctx, fmt.Sprintf(`
insert into records (job_id, user_id, router_id, response_tokens, ok)
values (gen_random_uuid(), %s, %s, 100000, true)`,
		pooldb.UUID(userA), pooldb.UUID(frC.ID()))))

	body := []byte(`WS11-TOPK-BODY-` + strings.Repeat("t", 1024))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws11"}}]}`, "data: [DONE]"}

	jobA, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	jobB, err := frB.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	jobC, err := frC.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	// Stage all bodies together so the first pick sees the full window.
	must(t, frA.StageBody(ctx, jobA, body))
	must(t, frB.StageBody(ctx, jobB, body))
	must(t, frC.StageBody(ctx, jobC, body))

	// frA (largest ctx) is the first candidate for every job: the offer
	// sequence on frA is the service order.
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobB {
		t.Fatalf("first offer for job %s, want %s (window = the 2 oldest, A's ratio is ~0)", of.JobID, jobB)
	}
	must(t, frA.AcceptAndServe(ctx, jobB, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobB, 15*time.Second)

	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobC {
		t.Fatalf("second offer for job %s, want %s (C's ratio is highest now that it is in the window)", of.JobID, jobC)
	}
	must(t, frA.AcceptAndServe(ctx, jobC, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobC, 15*time.Second)

	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobA {
		t.Fatalf("third offer for job %s, want %s (heaviest user served last)", of.JobID, jobA)
	}
	must(t, frA.AcceptAndServe(ctx, jobA, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA, 15*time.Second)
}

// TestE2E_Fairness_WindowKnob: POOL_FAIR_WINDOW_MS=2000; A's heavy
// consumption is OUTSIDE the window -> its ratio is back to the 1.0
// baseline -> no deprioritize, the older job (A's) is served first.
func TestE2E_Fairness_WindowKnob(t *testing.T) {
	env := newE2EWith(t, e2eOpts{fairWindowMS: 2000})
	ctx := context.Background()
	userA, tokA := setupAuthUser(t, env, "fair-win-a@example.com", "Fair Win A", "member", "active")
	_, tokB := setupAuthUser(t, env, "fair-win-b@example.com", "Fair Win B", "member", "active")

	frA := NewFakeRouter(env.baseURL, tokA, "fair-win-a", "fair-win-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, tokB, "fair-win-b", "fair-win-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))

	// A consumed heavily 10 minutes ago: outside the 2s window.
	must(t, env.db.Exec(ctx, fmt.Sprintf(`
insert into records (job_id, user_id, router_id, response_tokens, ok, created_at)
values (gen_random_uuid(), %s, %s, 100000, true, now() - interval '10 minutes')`,
		pooldb.UUID(userA), pooldb.UUID(frB.ID()))))

	body := []byte(`WS11-WIN-BODY-` + strings.Repeat("w", 1024))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws11"}}]}`, "data: [DONE]"}

	jobA, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	jobB, err := frB.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	// Stage both bodies together so the first pick sees both jobs.
	must(t, frA.StageBody(ctx, jobA, body))
	must(t, frB.StageBody(ctx, jobB, body))

	// A's window is empty: the 1.0 tie serves A's older job first.
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobA {
		t.Fatalf("first offer for job %s, want %s (consumption outside the window must not deprioritize)",
			of.JobID, jobA)
	}
	must(t, frA.AcceptAndServe(ctx, jobA, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA, 15*time.Second)
}

// TestE2E_Fairness_ParkBackoff: a no_slot-exhausted park re-offers with a
// backoff (POOL_PARK_BACKOFF_MS=3000 here): the offers are spaced >= the
// backoff, and the job stays pending (retry, not a terminal).
func TestE2E_Fairness_ParkBackoff(t *testing.T) {
	env := newE2EWith(t, e2eOpts{parkBackoffMS: 3000})
	ctx := context.Background()

	// The only candidate rejects every offer (no free slot).
	frA := NewFakeRouter(env.baseURL, env.token, "fair-back", "fair-back")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(ctx))

	body := []byte(`WS11-BACKOFF-BODY-` + strings.Repeat("k", 512))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)

	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobID {
		t.Fatalf("first offer for job %s, want %s", of.JobID, jobID)
	}
	t1 := time.Now()
	must(t, frA.DecideAndRespond(ctx, of, len(body), "", nil)) // scripted no_slot

	// The exhaustion parks the job (pending), not a terminal.
	deadline := time.After(10 * time.Second)
	st := ""
	for {
		var err2 error
		st, err2 = env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobID))
		if err2 == nil && st == "pending" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("job did not return to pending after the no_slot exhaustion (status=%q)", st)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// The re-offer respects the backoff.
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobID {
		t.Fatalf("re-offer for job %s, want %s", of.JobID, jobID)
	}
	if got := time.Since(t1); got < 3000*time.Millisecond {
		t.Fatalf("parked job re-offered after %v, want >= 3s (POOL_PARK_BACKOFF_MS)", got)
	}
}

// TestE2E_Fairness_OverflowTerminal: every candidate rejects with a
// pre_token overflow -> the job terminal-fails immediately (no retry loop):
// the origin gets an error with the D-C5 substring, the job is failed with
// code pre_token, and the body is purged (DISP-RESIDUAL-1).
func TestE2E_Fairness_OverflowTerminal(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "fair-ov-a", "fair-ov-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, env.token, "fair-ov-b", "fair-ov-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))

	body := []byte(`WS11-OVERFLOW-BODY-` + strings.Repeat("v", 2048))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	// frA (largest ctx) is offered first; it overflows.
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobID {
		t.Fatalf("first offer for job %s, want %s", of.JobID, jobID)
	}
	must(t, frA.AcceptAndError(ctx, jobID, poolproto.StagePreToken, poolproto.ErrorOverflow,
		"request exceeds the available context size (fake child)"))

	// frB overflows too: the candidate loop is exhausted by overflow.
	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobID {
		t.Fatalf("second offer for job %s, want %s", of.JobID, jobID)
	}
	must(t, frB.AcceptAndError(ctx, jobID, poolproto.StagePreToken, poolproto.ErrorOverflow,
		"request exceeds the available context size (fake child)"))

	// The origin's subscribe gets the terminal error (D-C5 substring).
	ev, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	for ev.Type != poolproto.TypeError {
		ev, err = frA.NextEvent(ctx, 15*time.Second)
		must(t, err)
	}
	var rep poolproto.ErrorReport
	must(t, json.Unmarshal(ev.Payload, &rep))
	if rep.JobID != jobID {
		t.Fatalf("terminal for job %s, want %s", rep.JobID, jobID)
	}
	if rep.Code != "pre_token" {
		t.Fatalf("terminal code = %q, want pre_token", rep.Code)
	}
	if !strings.Contains(rep.Message, "exceeds the available context size") {
		t.Fatalf("terminal message %q lacks the D-C5 substring", rep.Message)
	}

	// The origin's held relay closes on the terminal.
	select {
	case <-rgA.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the origin's relay did not close after the terminal")
	}

	// Job failed; the terminal records row carries pre_token.
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select ok, error_code from records where job_id = %s and error_code = 'pre_token'",
		pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("terminal records rows = %d, want 1", len(rows))
	}
	if f := strings.Split(rows[0], ","); f[0] != "f" {
		t.Fatalf("terminal records row = %s, want ok=f", rows[0])
	}
	st, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobID))
	must(t, err)
	if st != "failed" {
		t.Fatalf("status = %q, want failed", st)
	}

	// No retry loop: the job is never offered again.
	if ev, err := frA.NextEvent(ctx, 2*time.Second); err == nil && ev.Type == poolproto.TypeWorkOffer {
		t.Fatalf("job was re-offered after the overflow terminal: %+v", ev)
	}

	// Body purged: no file left in the data dir.
	entries, err := os.ReadDir(env.dataDir + "/jobs")
	must(t, err)
	if len(entries) != 0 {
		t.Fatalf("body file not purged: %v", entries)
	}
}
