package server

// WS-15: records completeness audit + anti-memory canary (D-I1/D-I2/D-I3).
//
// Part 1: one records row per job, written at the terminal - every terminal
// path (ok, pre_token overflow, park expiry, running cap, client
// disconnect, model_gone, pre-offer expiry, coordinator restart, batch
// ok/fail) leaves exactly ONE row with the right ok/error_code/priority/
// cont_* and non-negative timings; the deduped re-intake (WS-12) gets no
// service and no row.
//
// Part 2: a unique 40-char canary in the prompt body is absent from the
// coordinator logs, every text/jsonb/json column of the durable tables, and
// the data dir; the staged body file is gone after the ok AND the fail
// terminal.

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// assertOneRecord asserts the D-I3 invariant for one terminal: exactly ONE
// records row for the job, with the expected ok/error_code/priority/cont_*
// and non-negative timings.
func assertOneRecord(t *testing.T, env *e2eEnv, ctx context.Context, jobID string,
	wantOK, wantCode, wantPriority, wantCont, wantContReason string) {
	t.Helper()
	rows, err := env.db.Query(ctx, fmt.Sprintf(`
select ok, coalesce(error_code, ''), priority, coalesce(cont_outcome, ''), coalesce(cont_reason, ''),
       coalesce(t_queued_ms, -1), coalesce(t_service_ms, -1)
from records where job_id = %s`, pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows for job %s = %d, want 1: %v", jobID, len(rows), rows)
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 7 {
		t.Fatalf("bad record row: %s", rows[0])
	}
	if f[0] != wantOK {
		t.Fatalf("ok = %s, want %s (row %s)", f[0], wantOK, rows[0])
	}
	if f[1] != wantCode {
		t.Fatalf("error_code = %q, want %q (row %s)", f[1], wantCode, rows[0])
	}
	if f[2] != wantPriority {
		t.Fatalf("priority = %q, want %q (row %s)", f[2], wantPriority, rows[0])
	}
	if f[3] != wantCont {
		t.Fatalf("cont_outcome = %q, want %q (row %s)", f[3], wantCont, rows[0])
	}
	if f[4] != wantContReason {
		t.Fatalf("cont_reason = %q, want %q (row %s)", f[4], wantContReason, rows[0])
	}
	tq, _ := strconv.Atoi(f[5])
	ts, _ := strconv.Atoi(f[6])
	if tq < 0 || ts < 0 {
		t.Fatalf("negative timings: t_queued_ms=%d t_service_ms=%d (row %s)", tq, ts, rows[0])
	}
}

// waitFailed polls until the job's terminal records row exists.
func waitFailed(t *testing.T, env *e2eEnv, ctx context.Context, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		n, err := env.db.Count(ctx, "select count(*) from records where job_id = "+pooldb.UUID(jobID))
		if err == nil && n >= 1 {
			return
		}
		select {
		case <-deadline:
			st, _ := env.db.FirstString(ctx, "select status from queue_jobs where id = "+pooldb.UUID(jobID))
			t.Fatalf("no terminal records row for job %s in %s (status=%q)", jobID, timeout, st)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ws15Router is a free member that accepts and serves.
func ws15Router(t *testing.T, env *e2eEnv, name string) *FakeRouter {
	t.Helper()
	fr := NewFakeRouter(env.baseURL, env.token, name, name)
	fr.LoadModel("md-"+name, []string{"pool-text-md"}, 32768, 2, 0, poolproto.ModelLoaded)
	fr.SetPolicy(AcceptPolicy{})
	must(t, fr.Connect(context.Background()))
	return fr
}

// TestWS15_Records_OKComplete: the ok terminal leaves one row (ok, no error
// code, priority live, not a continuation, non-negative timings).
func TestWS15_Records_OKComplete(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-ok")

	body := []byte(`WS15-OK-BODY-` + strings.Repeat("x", 512))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws15"}}]}`, "data: [DONE]"}
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, fr, ctx, 15*time.Second)
	must(t, fr.AcceptAndServe(ctx, of.JobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobID, 15*time.Second)
	assertOneRecord(t, env, ctx, jobID, "t", "", "live", "", "")
}

// TestWS15_Records_PreTokenOverflow: two pre_token overflow bounces plus the
// exhaustion terminal leave exactly ONE row (the audit fix: a hop writes no
// row; the terminal carries the code).
func TestWS15_Records_PreTokenOverflow(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	frA := NewFakeRouter(env.baseURL, env.token, "ws15-ov-a", "ws15-ov-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(ctx))
	frB := NewFakeRouter(env.baseURL, env.token, "ws15-ov-b", "ws15-ov-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(ctx))

	body := []byte(`WS15-OV-BODY-` + strings.Repeat("v", 2048))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	// Both candidates overflow: the pass is exhausted by pre_token.
	nextOffer(t, frA, ctx, 15*time.Second)
	must(t, frA.AcceptAndError(ctx, jobID, poolproto.StagePreToken, poolproto.ErrorOverflow, "fake overflow a"))
	nextOffer(t, frB, ctx, 15*time.Second)
	must(t, frB.AcceptAndError(ctx, jobID, poolproto.StagePreToken, poolproto.ErrorOverflow, "fake overflow b"))

	waitFailed(t, env, ctx, jobID, 15*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", "pre_token", "live", "", "")
	select {
	case <-rgA.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the origin's relay did not close after the terminal")
	}
}

// TestWS15_Records_ParkExpiry: a job parked on rejections expires (D-D2 MVP
// cap); one row, code other.
func TestWS15_Records_ParkExpiry(t *testing.T) {
	env := newE2EWith(t, e2eOpts{jobExpiryMS: 2000, parkBackoffMS: 200})
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-park")
	fr.SetPolicy(RejectPolicy{Reason: "no_slot"})

	body := []byte(`WS15-PARK-BODY-` + strings.Repeat("p", 512))
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, fr, ctx, 15*time.Second)
	must(t, fr.DecideAndRespond(ctx, of, len(body), "", nil)) // reject: park

	waitFailed(t, env, ctx, jobID, 20*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", poolproto.ErrorOther, "live", "", "")
}

// TestWS15_Records_RunningCap: a running job that never completes hits the
// cap (D-G2); one row, code other.
func TestWS15_Records_RunningCap(t *testing.T) {
	env := newE2EWith(t, e2eOpts{runningCapMS: 1500})
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-cap")

	body := []byte(`WS15-CAP-BODY-` + strings.Repeat("c", 512))
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, fr, ctx, 15*time.Second)
	must(t, fr.AcceptAndHold(ctx, of.JobID)) // running, no completion

	waitFailed(t, env, ctx, jobID, 20*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", poolproto.ErrorOther, "live", "", "")
}

// TestWS15_Records_ClientDisconnected: the origin closing its held GET fails
// the running job (D-G4); one row, code client_disconnected.
func TestWS15_Records_ClientDisconnected(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-disc")

	body := []byte(`WS15-DISC-BODY-` + strings.Repeat("d", 512))
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	rgA, err := fr.OpenResponseGet(ctx, jobID)
	must(t, err)
	of := nextOffer(t, fr, ctx, 15*time.Second)
	must(t, fr.AcceptAndHold(ctx, of.JobID))
	time.Sleep(300 * time.Millisecond) // let the body pull settle
	rgA.Close()

	waitFailed(t, env, ctx, jobID, 15*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", poolproto.ErrorClientDisconnected, "live", "", "")
}

// TestWS15_Records_ModelGone: a class nobody loaded fails with model_gone
// (D-C7); one row.
func TestWS15_Records_ModelGone(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-mg")

	body := []byte(`WS15-MG-BODY-` + strings.Repeat("m", 512))
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-xl", 128, 4096, nil, body)
	must(t, err)

	waitFailed(t, env, ctx, jobID, 20*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", poolproto.ErrorModelGone, "live", "", "")
}

// TestWS15_Records_PreOfferExpiry: the only member is disconnected, so the
// job is never offered; it expires before its first offer; one row.
func TestWS15_Records_PreOfferExpiry(t *testing.T) {
	env := newE2EWith(t, e2eOpts{jobExpiryMS: 2000})
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-pre")
	fr.Disconnect()

	body := []byte(`WS15-PRE-BODY-` + strings.Repeat("n", 512))
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)

	waitFailed(t, env, ctx, jobID, 20*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", poolproto.ErrorOther, "live", "", "")
}

// TestWS15_Records_DuplicateNoSecondService: an in-window re-intake with the
// same request_id returns the existing job (WS-12) and gets no second
// service: one queue row and one records row in total.
func TestWS15_Records_DuplicateNoSecondService(t *testing.T) {
	env := newE2EWith(t, e2eOpts{dedupeWindowMS: 300000})
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-dup")

	body := []byte(`WS15-DUP-BODY-` + strings.Repeat("u", 512))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws15"}}]}`, "data: [DONE]"}
	reqID := pooldb.NewUUID()
	job1, st1, err := fr.IntakeMeta(ctx, reqID, "pool-text-md", 128, 4096, nil)
	must(t, err)
	if st1 != http.StatusCreated {
		t.Fatalf("first intake status = %d, want 201", st1)
	}
	must(t, fr.StageBody(ctx, job1, body))
	of := nextOffer(t, fr, ctx, 15*time.Second)
	must(t, fr.AcceptAndServe(ctx, of.JobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, job1, 15*time.Second)

	// The retry: 200 + the same job.
	job2, st2, err := fr.IntakeMeta(ctx, reqID, "pool-text-md", 128, 4096, nil)
	must(t, err)
	if st2 != http.StatusOK {
		t.Fatalf("re-intake status = %d, want 200 (in-window dedupe)", st2)
	}
	if job2 != job1 {
		t.Fatalf("dedupe returned job %s, want %s", job2, job1)
	}

	n, err := env.db.Count(ctx, "select count(*) from queue_jobs where request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if n != 1 {
		t.Fatalf("queue_jobs rows for request_id = %d, want 1", n)
	}
	nRec, err := env.db.Count(ctx,
		"select count(*) from records rec join queue_jobs q on q.id = rec.job_id where q.request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if nRec != 1 {
		t.Fatalf("records rows for the request = %d, want 1 (the duplicate was not served)", nRec)
	}
	assertOneRecord(t, env, ctx, job1, "t", "", "live", "", "")
}

// TestWS15_Records_CoordinatorRestart: offered and running jobs at the
// simulated restart terminal-fail with coordinator_restart (D-H2); one row
// each.
func TestWS15_Records_CoordinatorRestart(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-re")

	body := []byte(`WS15-RE-BODY-` + strings.Repeat("e", 512))
	job1, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, fr, ctx, 15*time.Second)
	must(t, fr.AcceptAndHold(ctx, of.JobID)) // running at restart
	job2, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of = nextOffer(t, fr, ctx, 15*time.Second)
	// job2's offer is left unanswered (offered at restart).

	disp := env.srv.Dispatcher()
	must(t, disp.DropMemory())
	resumed, failed, err := disp.Reload()
	must(t, err)
	if resumed != 0 || failed != 2 {
		t.Fatalf("reload = (%d resumed, %d failed), want (0, 2)", resumed, failed)
	}

	waitFailed(t, env, ctx, job1, 15*time.Second)
	waitFailed(t, env, ctx, job2, 15*time.Second)
	assertOneRecord(t, env, ctx, job1, "f", "coordinator_restart", "live", "", "")
	assertOneRecord(t, env, ctx, job2, "f", "coordinator_restart", "live", "", "")
}

// TestWS15_Records_BatchOK: a batch job that completes carries priority
// 'batch' on its single row.
func TestWS15_Records_BatchOK(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-bok")

	body := []byte(`WS15-BOK-BODY-` + strings.Repeat("b", 512))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws15"}}]}`, "data: [DONE]"}
	jobID, err := fr.IntakeNoBodyBatch(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, fr.StageBody(ctx, jobID, body))
	of := nextOffer(t, fr, ctx, 15*time.Second)
	if of.Priority != poolproto.PriorityBatch {
		t.Fatalf("offer priority = %q, want batch", of.Priority)
	}
	must(t, fr.AcceptAndServe(ctx, of.JobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobID, 15*time.Second)
	assertOneRecord(t, env, ctx, jobID, "t", "", "batch", "", "")
}

// TestWS15_Records_BatchExpiry: a batch job the pool cannot serve expires
// (POOL_BATCH_EXPIRY_MS); one row, priority 'batch', code other.
func TestWS15_Records_BatchExpiry(t *testing.T) {
	env := newE2EWith(t, e2eOpts{batchExpiryMS: 2000})
	ctx := context.Background()
	fr := NewFakeRouter(env.baseURL, env.token, "ws15-bex", "ws15-bex")
	fr.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded) // fully busy
	must(t, fr.Connect(ctx))

	body := []byte(`WS15-BEX-BODY-` + strings.Repeat("x", 512))
	jobID, err := fr.IntakeNoBodyBatch(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil)
	must(t, err)
	must(t, fr.StageBody(ctx, jobID, body))

	waitFailed(t, env, ctx, jobID, 20*time.Second)
	assertOneRecord(t, env, ctx, jobID, "f", poolproto.ErrorOther, "batch", "", "")
}

// --- Part 2: anti-memory canary (D-I1/D-I2) ---------------------------------

// makeCanary40 returns a unique 40-char canary: fixed prefix + 35 random
// alphanumerics (no SQL metachars, not pure hex).
func makeCanary40(t *testing.T) string {
	t.Helper()
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
	b := make([]byte, 35)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("canary: %v", err)
	}
	for i, c := range b {
		b[i] = alphabet[int(c)%len(alphabet)]
	}
	return "WS15C" + string(b)
}

// assertCanaryAbsentInDB walks every text/jsonb/json column of every table in
// the public schema and fails if the canary appears in any of them (D-I1: no
// request content in the store). The table set is walked from
// information_schema, not a hard-coded list: a new durable table cannot
// silently drop out of the check.
func assertCanaryAbsentInDB(t *testing.T, env *e2eEnv, ctx context.Context, canary string) {
	t.Helper()
	cols, err := env.db.Query(ctx, `
select c.table_name || '.' || c.column_name
from information_schema.columns c
join pg_tables p on p.schemaname = c.table_schema and p.tablename = c.table_name
where c.table_schema = 'public'
  and c.data_type in ('text','jsonb','json','character varying')
order by 1`)
	must(t, err)
	if len(cols) == 0 {
		t.Fatal("column walk found no columns (schema change?)")
	}
	for _, col := range cols {
		tab, colName, _ := strings.Cut(col, ".")
		n, err := env.db.Count(ctx, fmt.Sprintf(
			"select count(*) from %s where position(%s in %s::text) > 0", tab, pooldb.S(canary), colName))
		must(t, err)
		if n > 0 {
			t.Fatalf("canary found in %s (%d rows)", col, n)
		}
	}
}

// assertCanaryAbsentInDir fails if any file under dir contains the canary.
func assertCanaryAbsentInDir(t *testing.T, dir, canary string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if bytes.Contains(b, []byte(canary)) {
			t.Errorf("canary found in data dir file %s", path)
		}
		return nil
	})
	must(t, err)
}

// assertBodyPurged asserts the staged body file for the job is gone (D-I2).
func assertBodyPurged(t *testing.T, dataDir, jobID string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dataDir, "jobs", jobID)); !os.IsNotExist(err) {
		t.Fatalf("staged body for job %s still present after terminal (D-I2): %v", jobID, err)
	}
}

// TestWS15_AntiMemory_Canary: a unique 40-char canary in the prompt body of
// a normal pool request is absent, after the ok terminal, from the
// coordinator logs, every durable text/jsonb/json column, and the data dir;
// the staged body is gone after the ok AND after the fail terminal (D-I2).
func TestWS15_AntiMemory_Canary(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()
	fr := ws15Router(t, env, "ws15-can")

	// ok terminal: the canary rides in the prompt body.
	canary := makeCanary40(t)
	body := []byte(`{"canary":"` + canary + `","pad":"` + strings.Repeat("q", 4096) + `"}`)
	jobID, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, fr, ctx, 15*time.Second)
	ndjson := []string{`{"choices":[{"delta":{"content":"ws15"}}]}`, "data: [DONE]"}
	must(t, fr.AcceptAndServe(ctx, of.JobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobID, 15*time.Second)

	// (a) the captured coordinator logs.
	if strings.Contains(env.logs.String(), canary) {
		t.Fatalf("canary found in coordinator logs (D-I1):\n%s", truncate([]byte(env.logs.String()), 4000))
	}
	// (b) every text/jsonb/json column of the durable tables.
	assertCanaryAbsentInDB(t, env, ctx, canary)
	// (c) the data dir (walk all files).
	assertCanaryAbsentInDir(t, env.dataDir, canary)
	// D-I2: the ok terminal purged the staged body.
	assertBodyPurged(t, env.dataDir, jobID)

	// fail terminal (model_gone): a fresh canary; the staged body is purged
	// and the canary stays out of the logs.
	canary2 := makeCanary40(t)
	body2 := []byte(`{"canary":"` + canary2 + `"}`)
	job2, err := fr.Intake(ctx, pooldb.NewUUID(), "pool-text-xl", 128, 4096, nil, body2)
	must(t, err)
	waitFailed(t, env, ctx, job2, 20*time.Second)
	assertBodyPurged(t, env.dataDir, job2)
	if strings.Contains(env.logs.String(), canary2) {
		t.Fatalf("canary found in coordinator logs after the fail terminal (D-I1)")
	}
}
