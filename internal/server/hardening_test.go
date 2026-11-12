package server

// Hop hardening e2e tests (WS-09, the two MEDIUMs from the WS-07 two-family
// review): the serving router drops mid-push (the running cap must make the
// job terminal, the origin must get the terminal error, the relay pipe must
// not leak), the origin goes away mid-push (push abort), and the body gate
// (no offer until the body is staged).

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

// TestE2E_ServingDropsMidPush: frB accepts and body-pulls (the job is
// running), then dies with a hard RST mid-push. No completion ever comes;
// the running cap (2s in this test) must fail the job, mirror the terminal
// error to the origin's subscribe stream, and drop the relay pipe (no leak);
// a new job for the class is then served normally.
func TestE2E_ServingDropsMidPush(t *testing.T) {
	env := newE2EWith(t, e2eOpts{runningCapMS: 2000})

	// frA: origin, busy (no free slot), rejects.
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(context.Background()))

	// frB: free, serves.
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	body := []byte(`WS09-DIE-BODY-` + strings.Repeat("x", 2048))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	// frB is offered, accepts, body-pulls (running), then dies mid-push.
	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("offer for job %s, want %s", of.JobID, jobID)
	}
	ndjson := []string{
		`{"choices":[{"delta":{"content":"ws09-die-1"}}]}`,
		`{"choices":[{"delta":{"content":"ws09-die-2"}}]}`,
	}
	must(t, frB.AcceptAndDieMidPush(ctx, jobID, len(body), convKey, ndjson))

	// The first line relayed: the push was in flight when the router died.
	select {
	case <-rgA.Lines():
	case <-time.After(10 * time.Second):
		t.Fatal("the push never started")
	}

	// The origin's held relay closes once the cap fails the job (it was
	// open while the push was stuck - the M1 symptom).
	select {
	case <-rgA.Done():
	case <-time.After(20 * time.Second):
		t.Fatal("the origin's relay did not close after the running cap")
	}

	// The push read actually errored (a mid-body drop, not a clean end).
	if !strings.Contains(env.logs.String(), "push read error") {
		t.Fatalf("no push read error in the logs: %s", truncate([]byte(env.logs.String()), 2000))
	}

	// The origin's subscribe got the terminal error envelope.
	evA, err := frA.NextEvent(ctx, 20*time.Second)
	must(t, err)
	if evA.Type != poolproto.TypeError {
		t.Fatalf("envelope type = %s, want error", evA.Type)
	}
	var rep poolproto.ErrorReport
	must(t, json.Unmarshal(evA.Payload, &rep))
	if rep.JobID != jobID || rep.Code != poolproto.ErrorOther {
		t.Fatalf("bad error report: %+v", rep)
	}

	// Records row: terminal, not ok.
	waitRecord(t, env, ctx, jobID, 20*time.Second)
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select ok, error_code from records where job_id = %s", pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows = %d, want 1", len(rows))
	}
	r := strings.Split(rows[0], ",")
	if r[0] != "f" || r[1] != poolproto.ErrorOther {
		t.Fatalf("records row = %s, want f,%s", rows[0], poolproto.ErrorOther)
	}

	// Leak assertion: the relay pipe map no longer holds the job.
	env.srv.pipesMu.Lock()
	_, held := env.srv.pipes[jobID]
	n := len(env.srv.pipes)
	env.srv.pipesMu.Unlock()
	if held || n != 0 {
		t.Fatalf("relay pipe map still holds the job (held=%v, entries=%d)", held, n)
	}

	// A new job for the class is served normally (the pool recovered).
	body2 := []byte(`WS09-DIE-BODY-2-` + strings.Repeat("y", 1024))
	job2, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body2)
	must(t, err)
	rgB, err := frA.OpenResponseGet(ctx, job2)
	must(t, err)
	evB2, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB2); of.JobID != job2 {
		t.Fatalf("new-job offer for %s, want %s", of.JobID, job2)
	}
	must(t, frB.AcceptAndServe(ctx, job2, len(body2), "", ndjson))
	waitRecord(t, env, ctx, job2, 15*time.Second)
	rows, err = env.db.Query(ctx, fmt.Sprintf(
		"select ok from records where job_id = %s", pooldb.UUID(job2)))
	must(t, err)
	if len(rows) != 1 || rows[0] != "t" {
		t.Fatalf("new-job records = %v, want t", rows)
	}
	select {
	case <-rgB.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("new-job relay did not close")
	}
}

// TestE2E_PushAbort_OriginGone: frB is pushing (paced, multi-line) when the
// origin frA closes its held GET mid-stream: the job fails
// client_disconnected, frB's push is observed as aborted, and nothing hangs.
func TestE2E_PushAbort_OriginGone(t *testing.T) {
	env := newE2E(t)

	// frA: origin, busy (no free slot).
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(context.Background()))

	// frB: free, serves with a paced push.
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	body := []byte(`WS09-ABORT-BODY-` + strings.Repeat("z", 1024))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("offer for job %s, want %s", of.JobID, jobID)
	}
	lines := []string{
		`{"choices":[{"delta":{"content":"ws09-abort-1"}}]}`,
		`{"choices":[{"delta":{"content":"ws09-abort-2"}}]}`,
		`{"choices":[{"delta":{"content":"ws09-abort-3"}}]}`,
		`{"choices":[{"delta":{"content":"ws09-abort-4"}}]}`,
		`{"choices":[{"delta":{"content":"ws09-abort-5"}}]}`,
		`data: [DONE]`,
	}
	pushDone := make(chan struct{}, 1)
	go func() {
		defer close(pushDone)
		_, _ = frB.AcceptAndServePaced(ctx, jobID, len(body), "", lines, 500*time.Millisecond)
	}()

	// The push is in flight (the first line relayed); the origin gives up.
	select {
	case <-rgA.Lines():
	case <-time.After(15 * time.Second):
		t.Fatal("the push never started")
	}
	rgA.Close()

	// frB's push aborts (bounded: no hang).
	select {
	case <-pushDone:
	case <-time.After(15 * time.Second):
		t.Fatal("the push did not abort after the origin went away")
	}

	// The job failed with client_disconnected.
	waitRecord(t, env, ctx, jobID, 15*time.Second)
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select ok, error_code from records where job_id = %s", pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows = %d, want 1", len(rows))
	}
	r := strings.Split(rows[0], ",")
	if r[0] != "f" || r[1] != poolproto.ErrorClientDisconnected {
		t.Fatalf("records row = %s, want f,%s", rows[0], poolproto.ErrorClientDisconnected)
	}

	// The origin's subscribe got the terminal error envelope.
	evA, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if evA.Type != poolproto.TypeError {
		t.Fatalf("envelope type = %s, want error", evA.Type)
	}
	var rep poolproto.ErrorReport
	must(t, json.Unmarshal(evA.Payload, &rep))
	if rep.JobID != jobID || rep.Code != poolproto.ErrorClientDisconnected {
		t.Fatalf("bad error report: %+v", rep)
	}
}

// TestE2E_BodyGate: a job whose body was never staged is not offered (the
// catalog: "the coordinator treats the job as queued when the body PUT
// completes"); once staged, the offer lands and the job completes.
func TestE2E_BodyGate(t *testing.T) {
	env := newE2E(t)

	// frA: origin, busy (not a candidate).
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	must(t, frA.Connect(context.Background()))

	// frB: the only candidate.
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	jobID, err := frA.IntakeNoBody(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)

	// Several sweep ticks pass: no offer may reach the router.
	if ev, err := frB.NextEvent(ctx, 3*time.Second); err == nil {
		if of := offerOf(t, ev); of.JobID == jobID {
			t.Fatalf("offer sent before the body was staged: %+v", of)
		}
	}

	// Now stage the body: the offer lands and the job completes.
	body := []byte(`WS09-GATE-BODY-` + strings.Repeat("g", 1024))
	must(t, frA.StageBody(ctx, jobID, body))
	evB, err := frB.NextEvent(ctx, 10*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("offer for job %s, want %s", of.JobID, jobID)
	}
	ndjson := []string{`{"choices":[{"delta":{"content":"ws09-gate"}}]}`, "data: [DONE]"}
	must(t, frB.AcceptAndServe(ctx, jobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobID, 15*time.Second)
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select ok from records where job_id = %s", pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 || rows[0] != "t" {
		t.Fatalf("records = %v, want t", rows)
	}
}

// TestE2E_RunningCapPromptScaled: a job with a large estimated prompt must not
// trip the running cap during prompt eval (no lines come out then). The base
// cap alone (300ms here) would fail it mid-eval - the KI-1 incident (a
// 171k-token prompt eval took 560s against the 5m base cap).
func TestE2E_RunningCapPromptScaled(t *testing.T) {
	env := newE2EWith(t, e2eOpts{runningCapMS: 300})

	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(context.Background()))

	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	body := []byte(`WS09-SCALE-BODY-` + strings.Repeat("x", 2048))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 100000, 4096, &convKey, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("offer for job %s, want %s", of.JobID, jobID)
	}
	// accept + body pull, then hold: a slow prompt eval (no lines)
	must(t, frB.AcceptAndHold(ctx, jobID))

	// cap = 300ms base + 100000 x 4ms = ~6.9 min: the job must be well past
	// the base cap and still running
	time.Sleep(1500 * time.Millisecond)
	if evA, err := frA.NextEvent(ctx, 500*time.Millisecond); err == nil {
		t.Fatalf("unexpected envelope while the job should still be running: %+v", evA)
	}
	rgA.Close()
}

// TestE2E_RunningCapActivityExtends: while the serving router streams, the
// relayed lines extend the D-G2 deadline (KI-1 amendment), so a long
// generation is never mistaken for a dead push. The base cap alone (300ms
// here) would fail the job mid-stream and cut the relay.
func TestE2E_RunningCapActivityExtends(t *testing.T) {
	env := newE2EWith(t, e2eOpts{runningCapMS: 300})

	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(context.Background()))

	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32)
	body := []byte(`WS09-PACED-BODY-` + strings.Repeat("x", 2048))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 0, 4096, &convKey, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("offer for job %s, want %s", of.JobID, jobID)
	}
	ndjson := []string{
		`{"choices":[{"delta":{"content":"pace-1"}}]}`,
		`{"choices":[{"delta":{"content":"pace-2"}}]}`,
		`{"choices":[{"delta":{"content":"pace-3"}}]}`,
		`{"choices":[{"delta":{"content":"pace-4"}}]}`,
		`{"choices":[{"delta":{"content":"pace-5"}}]}`,
	}
	// 5 lines at 200ms gaps = ~1s of streaming, crossing the 300ms base cap
	// several times
	status, err := frB.AcceptAndServePaced(ctx, jobID, len(body), convKey, ndjson, 200*time.Millisecond)
	must(t, err)
	if status != 200 {
		t.Fatalf("push status = %d, want 200", status)
	}
	// all 5 lines reached the origin (no early cap hit)
	for i := 1; i <= len(ndjson); i++ {
		select {
		case line := <-rgA.Lines():
			if !strings.Contains(line, fmt.Sprintf("pace-%d", i)) {
				t.Fatalf("relay line %d = %q", i, line)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("relay line %d never arrived", i)
		}
	}
	rgA.Close()
}
