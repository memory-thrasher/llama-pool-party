package server

// WS-10: async batch (D-H1/D-H2) on the coordinator side.
//
//   - live-before-batch: a pending live job is always offered before any
//     batch job, even an older one;
//   - batch weight: a user who consumed N tokens via batch is deprioritized
//     only POOL_BATCH_WEIGHT (0.5) as much as one who consumed N live;
//   - startup reload (D-H2): pending jobs resume the offer loop after the
//     in-memory table is lost (simulated restart); offered/running jobs
//     terminal-fail with coordinator_restart;
//   - batch expiry: POOL_BATCH_EXPIRY_MS caps batch jobs; live keeps the 30m.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// TestWS10_LiveBeforeBatch: a batch job submitted BEFORE a live job (older)
// is still offered after it (D-H1).
func TestWS10_LiveBeforeBatch(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "ws10-lba", "ws10-lba")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(ctx))

	body := []byte(`WS10-LBA-BODY-` + strings.Repeat("b", 1024))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws10"}}]}`, "data: [DONE]"}

	// The batch job is strictly older than the live job.
	jobBatch, err := frA.IntakeNoBodyBatch(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, jobBatch, body))
	time.Sleep(100 * time.Millisecond)
	jobLive, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, jobLive, body))

	// The live job is offered first, despite the older batch job.
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobLive {
		t.Fatalf("first offer for job %s, want the live job %s (batch is older but lower priority)", of.JobID, jobLive)
	}
	if of.Priority != poolproto.PriorityLive {
		t.Fatalf("first offer priority = %q, want live", of.Priority)
	}
	must(t, frA.AcceptAndServe(ctx, jobLive, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobLive, 15*time.Second)

	// Then the batch job, carrying its priority on the offer.
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobBatch {
		t.Fatalf("second offer for job %s, want the batch job %s", of.JobID, jobBatch)
	}
	if of.Priority != poolproto.PriorityBatch {
		t.Fatalf("second offer priority = %q, want batch", of.Priority)
	}
	must(t, frA.AcceptAndServe(ctx, jobBatch, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobBatch, 15*time.Second)
}

// TestWS10_BatchWeight: A consumes N tokens live, C consumes N tokens via
// batch (same size, same server). C's new job must be offered before A's
// older one: batch consumption counts at POOL_BATCH_WEIGHT (0.5) in the D-F2
// ratio, so C is deprioritized only half as much (D-H1).
func TestWS10_BatchWeight(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	_, tokA := setupAuthUser(t, env, "ws10-wt-a@example.com", "WS10 Weight A", "member", "active")
	_, tokC := setupAuthUser(t, env, "ws10-wt-c@example.com", "WS10 Weight C", "member", "active")

	// frA and frC are busy (their machines cannot serve); frB serves all.
	frA := NewFakeRouter(env.baseURL, tokA, "ws10-wt-a", "ws10-wt-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 1, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))
	frC := NewFakeRouter(env.baseURL, tokC, "ws10-wt-c", "ws10-wt-c")
	frC.LoadModel("md-c", []string{"pool-text-md"}, 65536, 1, 1, poolproto.ModelLoaded)
	must(t, frC.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, env.token, "ws10-wt-b", "ws10-wt-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(ctx))

	// 10 response tokens per serve: A consumes 10 (live), C consumes 10 (batch).
	ndjson := make([]string, 0, 10)
	for i := 0; i < 9; i++ {
		ndjson = append(ndjson, `{"choices":[{"delta":{"content":"ws10"}}]}`)
	}
	ndjson = append(ndjson, "data: [DONE]")
	body := []byte(`WS10-WT-BODY-` + strings.Repeat("w", 512))

	// A consumes N live: frB serves A's request (A's machine is busy).
	jobA1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobA1 {
		t.Fatalf("consumption-1 offer for job %s, want %s", of.JobID, jobA1)
	}
	must(t, frB.AcceptAndServe(ctx, jobA1, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA1, 15*time.Second)

	// C consumes N via batch: frB serves C's batch request.
	jobC1, err := frC.IntakeNoBodyBatch(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frC.StageBody(ctx, jobC1, body))
	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobC1 {
		t.Fatalf("consumption-2 offer for job %s, want %s", of.JobID, jobC1)
	}
	must(t, frB.AcceptAndServe(ctx, jobC1, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobC1, 15*time.Second)

	// Comparison: A's older job vs C's newer job. Both machines busy, so the
	// pick is purely the fairness ratio. C: (0+1)/(10*0.5+1) = 1/6 > A:
	// (0+1)/(10+1) = 1/11.
	jobA2, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, jobA2, body))
	jobC2, err := frC.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frC.StageBody(ctx, jobC2, body))

	// C's job (batch consumer) is offered first: half the deprioritize.
	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobC2 {
		t.Fatalf("comparison offer for job %s, want the batch consumer's job %s (0.5 weight)", of.JobID, jobC2)
	}
	must(t, frB.AcceptAndServe(ctx, jobC2, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobC2, 15*time.Second)

	of = nextOffer(t, frB, ctx, 15*time.Second)
	if of.JobID != jobA2 {
		t.Fatalf("second comparison offer for job %s, want %s", of.JobID, jobA2)
	}
	must(t, frB.AcceptAndServe(ctx, jobA2, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobA2, 15*time.Second)
}

// TestWS10_Reload_PendingReOffer: a parked (pending) job survives the
// simulated restart - after the in-memory table is dropped, Reload brings it
// back and the offer loop re-offers it (D-H2).
func TestWS10_Reload_PendingReOffer(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "ws10-rl-a", "ws10-rl-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"}) // park the job
	must(t, frA.Connect(ctx))

	body := []byte(`WS10-RL-BODY-` + strings.Repeat("r", 512))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws10"}}]}`, "data: [DONE]"}
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)

	// The first offer is rejected: the job parks (pending, backoff armed).
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobID {
		t.Fatalf("first offer for job %s, want %s", of.JobID, jobID)
	}
	must(t, frA.DecideAndRespond(ctx, of, len(body), "", nil))
	st, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobID))
	must(t, err)
	if st != "pending" {
		t.Fatalf("job status = %q, want pending (parked)", st)
	}

	// Simulated restart: the in-memory table is lost, Reload rebuilds it from
	// queue_jobs (the body is still on disk).
	disp := env.srv.Dispatcher()
	must(t, disp.DropMemory())
	resumed, failed, err := disp.Reload()
	must(t, err)
	if resumed != 1 || failed != 0 {
		t.Fatalf("reload = (%d resumed, %d failed), want (1, 0)", resumed, failed)
	}

	// The re-offer arrives and the job completes (the policy now accepts).
	frA.SetPolicy(AcceptPolicy{})
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != jobID {
		t.Fatalf("re-offer for job %s, want the reloaded job %s", of.JobID, jobID)
	}
	must(t, frA.AcceptAndServe(ctx, jobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobID, 15*time.Second)
}

// TestWS10_Reload_InFlightFails: jobs offered or running at the simulated
// restart terminal-fail with coordinator_restart, mirrored to the origin,
// bodies purged, records rows written (D-H2).
func TestWS10_Reload_InFlightFails(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "ws10-rf-a", "ws10-rf-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(ctx))

	body := []byte(`WS10-RF-BODY-` + strings.Repeat("f", 512))

	// job1: accepted and body pulled -> running at restart.
	job1, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job1 {
		t.Fatalf("first offer for job %s, want %s", of.JobID, job1)
	}
	must(t, frA.AcceptAndHold(ctx, job1))
	// job2: offered, no answer -> offered at restart.
	job2, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of = nextOffer(t, frA, ctx, 15*time.Second)
	if of.JobID != job2 {
		t.Fatalf("second offer for job %s, want %s", of.JobID, job2)
	}
	// job2's offer is left unanswered (it stays offered).

	// Simulated restart: both in-flight jobs are terminal now.
	disp := env.srv.Dispatcher()
	must(t, disp.DropMemory())
	resumed, failed, err := disp.Reload()
	must(t, err)
	if resumed != 0 || failed != 2 {
		t.Fatalf("reload = (%d resumed, %d failed), want (0, 2)", resumed, failed)
	}

	// The origin's subscribe gets the coordinator_restart terminal for both.
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		ev, err := frA.NextEvent(ctx, 15*time.Second)
		must(t, err)
		if ev.Type != poolproto.TypeError {
			t.Fatalf("envelope type = %s, want error", ev.Type)
		}
		var rep poolproto.ErrorReport
		must(t, json.Unmarshal(ev.Payload, &rep))
		if rep.Code != "coordinator_restart" {
			t.Fatalf("terminal code = %q, want coordinator_restart", rep.Code)
		}
		if rep.JobID != job1 && rep.JobID != job2 {
			t.Fatalf("terminal for job %s, want %s or %s", rep.JobID, job1, job2)
		}
		if !strings.Contains(rep.Message, "coordinator restarted") {
			t.Fatalf("terminal message %q lacks the restart explanation", rep.Message)
		}
		seen[rep.JobID] = true
	}
	if !seen[job1] || !seen[job2] {
		t.Fatalf("terminals seen: %v, want both %s and %s", seen, job1, job2)
	}

	// Both rows failed, both records rows carry the code, both bodies purged.
	for _, jobID := range []string{job1, job2} {
		st, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobID))
		must(t, err)
		if st != "failed" {
			t.Fatalf("job %s status = %q, want failed", jobID, st)
		}
		rows, err := env.db.Query(ctx, fmt.Sprintf(
			"select ok, error_code from records where job_id = %s", pooldb.UUID(jobID)))
		must(t, err)
		if len(rows) != 1 {
			t.Fatalf("job %s records rows = %d, want 1", jobID, len(rows))
		}
		if f := strings.Split(rows[0], ","); f[0] != "f" || f[1] != "coordinator_restart" {
			t.Fatalf("job %s records row = %s, want f,coordinator_restart", jobID, rows[0])
		}
	}
}

// TestWS10_BatchExpiry: POOL_BATCH_EXPIRY_MS (2s here) caps batch jobs; a
// live job of the same age (30m cap) stays pending (D-H1). The only member is
// fully busy, so both jobs simply park - the expiry, not the offers, decides.
func TestWS10_BatchExpiry(t *testing.T) {
	env := newE2EWith(t, e2eOpts{batchExpiryMS: 2000})
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "ws10-exp", "ws10-exp")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded) // fully busy
	must(t, frA.Connect(ctx))

	body := []byte(`WS10-EXP-BODY-` + strings.Repeat("e", 512))
	jobBatch, err := frA.IntakeNoBodyBatch(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, jobBatch, body))
	time.Sleep(100 * time.Millisecond)
	jobLive, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, frA.StageBody(ctx, jobLive, body))

	// The batch job expires at ~2s.
	deadline := time.After(20 * time.Second)
	for {
		st, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobBatch))
		if err == nil && st == "failed" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("batch job did not expire (status=%q)", st)
		case <-time.After(200 * time.Millisecond):
		}
	}
	// The same-age live job is still pending (its cap is 30m).
	stLive, err := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobLive))
	must(t, err)
	if stLive != "pending" {
		t.Fatalf("live job of the same age expired (status=%q, want pending)", stLive)
	}
	// The batch terminal is the expiry terminal (ok=false, code other).
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select ok, error_code from records where job_id = %s", pooldb.UUID(jobBatch)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("batch records rows = %d, want 1", len(rows))
	}
	if f := strings.Split(rows[0], ","); f[0] != "f" || f[1] != poolproto.ErrorOther {
		t.Fatalf("batch records row = %s, want f,%s", rows[0], poolproto.ErrorOther)
	}
}
