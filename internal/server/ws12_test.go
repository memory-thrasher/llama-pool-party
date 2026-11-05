package server

// WS-12: exclusion marks (D-F1) and intake idempotency (D-G5).
//
// Exclusion: the owner's /models/unload on a pool member is a mark, not a real
// unload; the coordinator avoids that owner's class requests to the marked
// model. The mark is per (owner, router, model) - it never leaks to another
// owner.
//
// Idempotency: a re-intake with the same (user_id, request_id) within the
// dedupe window returns the existing job (200); beyond the window the id is
// released and the prompt is fresh work (201). The scope is per-user.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// ws12NDJSON is the minimal stream a fake serving router pushes.
var ws12NDJSON = []string{`{"choices":[{"delta":{"content":"ok"}}]}`, "data: [DONE]"}

// TestWS12_Exclusion_MarkAndUnmark: user A marks model md-a excluded; A's
// class job is no longer offered md-a (the other member, md-b, serves);
// unmarking offers md-a again (it has the most context, so it fronts). D-F1.
//
// A dedicated origin router (frO, no loaded models) intakes the jobs so the
// serving routers frA/frB only ever see work_offer events (the origin is the
// one that receives completions, which would otherwise pollute a serving
// router's event channel).
func TestWS12_Exclusion_MarkAndUnmark(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	// A single owner (A) with two serving members plus one intake-only origin.
	// md-a has the most context, so it is the first candidate while unmarked.
	_, tokenA := setupAuthUser(t, env, "ws12-excl-a@example.com", "WS12 Excl A", "member", "active")

	frO := NewFakeRouter(env.baseURL, tokenA, "ws12-excl-o", "fake-excl-o")
	must(t, frO.Connect(ctx))

	frA := NewFakeRouter(env.baseURL, tokenA, "ws12-excl-a", "fake-excl-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))

	frB := NewFakeRouter(env.baseURL, tokenA, "ws12-excl-b", "fake-excl-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 8192, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))

	// A marks md-a excluded.
	must(t, frA.PostExclusion(ctx, "md-a", true))

	// A's class job: md-a is excluded for A, so frB (md-b) is the only
	// candidate and serves.
	body := []byte(`WS12-EXCL-BODY-` + strings.Repeat("x", 512))
	jobID, err := frO.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 2048, nil, body)
	must(t, err)
	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("fr-b offer for %s, want %s", of.JobID, jobID)
	}
	must(t, frB.DecideAndRespond(ctx, offerOf(t, evB), len(body), "", ws12NDJSON))
	waitRecord(t, env, ctx, jobID, 15*time.Second)
	router, model := servedBy(t, env, ctx, jobID)
	if router != frB.ID() || model != "md-b" {
		t.Fatalf("excluded job served by (%s,%s), want (%s,md-b)", router, model, frB.ID())
	}

	// A unmarks md-a.
	must(t, frA.PostExclusion(ctx, "md-a", false))

	// A's second job: md-a is a candidate again (largest ctx -> first) and serves.
	body2 := []byte(`WS12-EXCL-BODY-2-` + strings.Repeat("y", 512))
	job2, err := frO.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 2048, nil, body2)
	must(t, err)
	evA, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evA); of.JobID != job2 {
		t.Fatalf("fr-a offer for %s, want %s", of.JobID, job2)
	}
	must(t, frA.DecideAndRespond(ctx, offerOf(t, evA), len(body2), "", ws12NDJSON))
	waitRecord(t, env, ctx, job2, 15*time.Second)
	router2, model2 := servedBy(t, env, ctx, job2)
	if router2 != frA.ID() || model2 != "md-a" {
		t.Fatalf("unmarked job served by (%s,%s), want (%s,md-a)", router2, model2, frA.ID())
	}
}

// TestWS12_Exclusion_OwnerScoped: A's exclusion of md-a does not affect B's
// dispatch to md-a (D-F1 is per-owner).
func TestWS12_Exclusion_OwnerScoped(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, tokenA := setupAuthUser(t, env, "ws12-scope-a@example.com", "WS12 Scope A", "member", "active")
	_, tokenB := setupAuthUser(t, env, "ws12-scope-b@example.com", "WS12 Scope B", "member", "active")

	frA := NewFakeRouter(env.baseURL, tokenA, "ws12-scope-a", "fake-scope-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))

	frB := NewFakeRouter(env.baseURL, tokenB, "ws12-scope-b", "fake-scope-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 8192, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))

	// A marks md-a excluded.
	must(t, frA.PostExclusion(ctx, "md-a", true))

	// B's class job (B intakes, so B is the origin and receives the completion;
	// frA only sees the work_offer). md-a is NOT excluded for B, so frA (md-a,
	// largest ctx) serves. If A's mark leaked to B, B's job would go to frB.
	body := []byte(`WS12-SCOPE-BODY-` + strings.Repeat("x", 512))
	jobID, err := frB.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 2048, nil, body)
	must(t, err)
	evA, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evA); of.JobID != jobID {
		t.Fatalf("fr-a offer for %s, want %s", of.JobID, jobID)
	}
	must(t, frA.DecideAndRespond(ctx, offerOf(t, evA), len(body), "", ws12NDJSON))
	waitRecord(t, env, ctx, jobID, 15*time.Second)
	router, model := servedBy(t, env, ctx, jobID)
	if router != frA.ID() || model != "md-a" {
		t.Fatalf("B's job served by (%s,%s), want (%s,md-a) - A's exclusion leaked to B",
			router, model, frA.ID())
	}
}

// TestWS12_Dedupe_WithinWindow: intake id R -> 201 job1; re-intake the same
// (user, R) within the window -> 200 + job1; exactly one queue_jobs row.
func TestWS12_Dedupe_WithinWindow(t *testing.T) {
	env := newE2EWith(t, e2eOpts{dedupeWindowMS: 300000})
	ctx := context.Background()
	_, tokenA := setupAuthUser(t, env, "ws12-dedup-a@example.com", "WS12 Dedup A", "member", "active")
	frA := NewFakeRouter(env.baseURL, tokenA, "ws12-dedup-a", "fake-dedup-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))

	reqID := pooldb.NewUUID()
	job1, status1, err := frA.IntakeMeta(ctx, reqID, "pool-text-md", 128, 2048, nil)
	must(t, err)
	if status1 != http.StatusCreated {
		t.Fatalf("first intake status = %d, want 201", status1)
	}
	job2, status2, err := frA.IntakeMeta(ctx, reqID, "pool-text-md", 128, 2048, nil)
	must(t, err)
	if status2 != http.StatusOK {
		t.Fatalf("re-intake status = %d, want 200 (in-window dedupe)", status2)
	}
	if job1 != job2 {
		t.Fatalf("dedupe returned job %s, want %s", job2, job1)
	}
	n, err := env.db.Count(ctx, "select count(*) from queue_jobs where request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if n != 1 {
		t.Fatalf("queue_jobs rows for request_id = %d, want 1", n)
	}
}

// TestWS12_Dedupe_WindowExpiry: an existing job older than the window releases
// its id on re-intake; a fresh job is created (201) and the old row's
// request_id is NULL.
func TestWS12_Dedupe_WindowExpiry(t *testing.T) {
	env := newE2EWith(t, e2eOpts{dedupeWindowMS: 300000})
	ctx := context.Background()
	userID, tokenA := setupAuthUser(t, env, "ws12-win-a@example.com", "WS12 Win A", "member", "active")
	frA := NewFakeRouter(env.baseURL, tokenA, "ws12-win-a", "fake-win-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))

	reqID := pooldb.NewUUID()
	// Seed a terminal job with request_id R, created 6 minutes ago (beyond the
	// 5-minute window).
	oldJobID := pooldb.NewUUID()
	must(t, env.db.Exec(ctx, fmt.Sprintf(`
insert into queue_jobs (id, user_id, class_id, priority, est_prompt_tokens, est_ctx_needed,
                       status, request_id, originating_router_id, created_at)
values (%s, %s, 'pool-text-md', 'live', 128, 2048, 'done', %s, %s, now() - interval '6 minutes')`,
		pooldb.UUID(oldJobID), pooldb.UUID(userID), pooldb.UUID(reqID), pooldb.UUID(frA.ID()))))

	// Intake the same R: the id is released and a fresh job is created (201).
	newJob, status, err := frA.IntakeMeta(ctx, reqID, "pool-text-md", 128, 2048, nil)
	must(t, err)
	if status != http.StatusCreated {
		t.Fatalf("expired re-intake status = %d, want 201 (window elapsed)", status)
	}
	if newJob == oldJobID {
		t.Fatal("window-expiry re-intake returned the old job id")
	}
	// The old row's request_id is released (NULL); the new row carries it.
	oldRID, err := env.db.FirstString(ctx,
		"select coalesce(request_id::text, '') from queue_jobs where id = "+pooldb.UUID(oldJobID))
	must(t, err)
	if oldRID != "" {
		t.Fatalf("old row's request_id = %s, want null (released)", oldRID)
	}
	newRID, err := env.db.FirstString(ctx,
		"select request_id::text from queue_jobs where id = "+pooldb.UUID(newJob))
	must(t, err)
	if newRID != reqID {
		t.Fatalf("new row's request_id = %s, want %s", newRID, reqID)
	}
	// Only the new row now matches the request_id (the old row is released).
	n, err := env.db.Count(ctx, "select count(*) from queue_jobs where request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if n != 1 {
		t.Fatalf("queue_jobs rows for request_id = %d, want 1 (old released)", n)
	}
}

// TestWS12_Dedupe_CrossUser: user A's intake of R does not capture user B's
// identical prompt; B gets its own fresh job (201).
func TestWS12_Dedupe_CrossUser(t *testing.T) {
	env := newE2EWith(t, e2eOpts{dedupeWindowMS: 300000})
	ctx := context.Background()
	_, tokenA := setupAuthUser(t, env, "ws12-xuser-a@example.com", "WS12 XUser A", "member", "active")
	_, tokenB := setupAuthUser(t, env, "ws12-xuser-b@example.com", "WS12 XUser B", "member", "active")

	frA := NewFakeRouter(env.baseURL, tokenA, "ws12-xuser-a", "fake-xuser-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))

	frB := NewFakeRouter(env.baseURL, tokenB, "ws12-xuser-b", "fake-xuser-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 8192, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))

	reqID := pooldb.NewUUID()
	// User A intakes R -> 201 (A's job).
	jobA, statusA, err := frA.IntakeMeta(ctx, reqID, "pool-text-md", 128, 2048, nil)
	must(t, err)
	if statusA != http.StatusCreated {
		t.Fatalf("A intake status = %d, want 201", statusA)
	}
	// User B intakes the SAME R -> 201 (B's own job, not A's).
	jobB, statusB, err := frB.IntakeMeta(ctx, reqID, "pool-text-md", 128, 2048, nil)
	must(t, err)
	if statusB != http.StatusCreated {
		t.Fatalf("B intake status = %d, want 201 (cross-user must not dedupe)", statusB)
	}
	if jobA == jobB {
		t.Fatal("cross-user dedupe captured the other user's job")
	}
	// Two rows: one per user.
	n, err := env.db.Count(ctx, "select count(*) from queue_jobs where request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if n != 2 {
		t.Fatalf("queue_jobs rows for request_id = %d, want 2 (one per user)", n)
	}
}

// TestWS12_Dedupe_ConcurrentIdentical: two byte-identical intakes of the same
// user racing at the same instant collapse onto ONE job. The partial unique
// index (user_id, request_id) lets exactly one insert win; the loser catches
// the conflict and returns the winner's job with 200. Pinned: both responses
// 2xx, both job ids equal, exactly one queue_jobs row for the request_id.
func TestWS12_Dedupe_ConcurrentIdentical(t *testing.T) {
	env := newE2EWith(t, e2eOpts{dedupeWindowMS: 300000})
	ctx := context.Background()
	_, tokenA := setupAuthUser(t, env, "ws12-race-a@example.com", "WS12 Race A", "member", "active")
	frA := NewFakeRouter(env.baseURL, tokenA, "ws12-race-a", "fake-race-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))

	reqID := pooldb.NewUUID()
	type result struct {
		jobID  string
		status int
		err    error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			jobID, status, err := frA.IntakeMeta(ctx, reqID, "pool-text-md", 128, 2048, nil)
			results <- result{jobID: jobID, status: status, err: err}
		}()
	}

	jobs := make([]string, 2)
	for i := 0; i < 2; i++ {
		r := <-results
		must(t, r.err)
		if r.status != http.StatusCreated && r.status != http.StatusOK {
			t.Fatalf("intake %d status = %d, want 2xx (201 or 200)", i, r.status)
		}
		jobs[i] = r.jobID
	}
	if jobs[0] != jobs[1] {
		t.Fatalf("concurrent identical intakes returned different jobs: %s vs %s", jobs[0], jobs[1])
	}
	n, err := env.db.Count(ctx, "select count(*) from queue_jobs where request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if n != 1 {
		t.Fatalf("queue_jobs rows for request_id = %d, want 1 (the race collapsed to one job)", n)
	}
}

// servedBy returns the (router_id, model_id) from a job's records row.
func servedBy(t *testing.T, env *e2eEnv, ctx context.Context, jobID string) (router, model string) {
	t.Helper()
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select coalesce(router_id::text, ''), coalesce(model_id, '') from records where job_id = %s",
		pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows for job %s = %d, want 1", jobID, len(rows))
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 2 {
		t.Fatalf("bad servedBy row: %s", rows[0])
	}
	return f[0], f[1]
}
