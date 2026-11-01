package server

// End-to-end tests for the coordinator protocol plane (WS-06). Each test runs a
// scratch postgres DB plus a live HTTP server, driven end to end by
// NewFakeRouter: connect+handshake, job intake, cross-member dispatch,
// live response relay, and the intake idempotency key.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/pooldb"
	"pool-coordinator/internal/schema"
	"pool-coordinator/protocol/go/poolproto"
)

// e2eEnv is a scratch postgres DB plus a running coordinator server.
type e2eEnv struct {
	srv     *Server
	db      *pooldb.DB
	dataDir string
	logs    *bytes.Buffer
	baseURL string
	token   string
	drop    func()
}

// e2eOpts tunes the test coordinator (zero = the config defaults).
type e2eOpts struct {
	contIdleMS   int
	jobExpiryMS  int
	runningCapMS int
}

func newE2E(t *testing.T) *e2eEnv {
	return newE2EWith(t, e2eOpts{})
}

func newE2EWith(t *testing.T, opts e2eOpts) *e2eEnv {
	t.Helper()
	ctx := context.Background()
	pgDSN := os.Getenv("PGDSN")
	if pgDSN == "" {
		pgDSN = "user=sid dbname=postgres"
	}
	if out, err := exec.Command("psql", "-X", "-q", "-d", pgDSN, "-c", "select 1").CombinedOutput(); err != nil {
		t.Skipf("postgres unreachable: %v\n%s", err, out)
	}
	name := "pool_ws06_e2e_" + randHex(5)
	baseDSN := strings.Replace(pgDSN, "dbname=postgres", "dbname="+name, 1)
	psqlMust(t, "drop database if exists "+name, pgDSN)
	psqlMust(t, "create database "+name, pgDSN)
	db := pooldb.New(baseDSN)
	if err := schema.Apply(ctx, baseDSN); err != nil {
		t.Fatalf("schema apply: %v", err)
	}
	bpath, err := bootstrapSQLPath()
	if err != nil {
		t.Fatalf("bootstrap path: %v", err)
	}
	if err := schema.ApplyFile(ctx, baseDSN, bpath); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	dataDir := t.TempDir()
	logs := &bytes.Buffer{}
	cfg := config.Config{Addr: "127.0.0.1:0", DSN: baseDSN, DataDir: dataDir, LogLevel: "info",
		ContIdleMS: opts.contIdleMS, JobExpiryMS: opts.jobExpiryMS, RunningCapMS: opts.runningCapMS}
	srv := New(cfg, db, dataDir, logs)
	srv.Dispatcher().SetLogger(srv.logf)
	srv.Dispatcher().Start(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go http.Serve(ln, srv.Handler())

	env := &e2eEnv{srv: srv, db: db, dataDir: dataDir, logs: logs,
		baseURL: "http://" + ln.Addr().String(), token: "admin",
		drop: func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
			psqlMust(t, "drop database "+name, pgDSN)
		}}
	t.Cleanup(env.drop)
	return env
}

func psqlMust(t *testing.T, query, conn string) {
	t.Helper()
	out, err := exec.Command("psql", "-X", "-q", "-d", conn, "-c", query).CombinedOutput()
	if err != nil {
		t.Fatalf("psql %q: %v\n%s", query, err, out)
	}
}

func bootstrapSQLPath() (string, error) {
	for _, c := range []string{"sql/bootstrap_admin.sql", "../sql/bootstrap_admin.sql", "../../sql/bootstrap_admin.sql"} {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("bootstrap_admin.sql not found")
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// countingReject is a fixed "no_slot" policy that counts the calls.
type countingReject struct{ calls int }

func (p *countingReject) Decide(offer poolproto.WorkOffer) OfferDecision {
	p.calls++
	return OfferDecision{Accept: false, Reason: "no_slot"}
}

// offerOf unmarshals a work_offer envelope payload.
func offerOf(t *testing.T, ev poolproto.Event) poolproto.WorkOffer {
	t.Helper()
	var of poolproto.WorkOffer
	if err := json.Unmarshal(ev.Payload, &of); err != nil {
		t.Fatalf("unmarshal work_offer: %v", err)
	}
	return of
}

// TestE2E_HappyPath_CrossMember: intake+body by a busy router, the free
// member is offered, accepts, pulls the body, streams the response, and the
// originator's held-open GET receives the exact NDJSON; records row written;
// body purged on terminal.
func TestE2E_HappyPath_CrossMember(t *testing.T) {
	env := newE2E(t)

	// frA: loaded but busy (no free slot), rejects; cannot be a candidate.
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(context.Background()))

	// frB: loaded, free, accepts.
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	convKey := hexToken(32) // 64-hex continuity key (D-E1)
	body := []byte(`WS06-BODY-MARKER-` + strings.Repeat("x", 4096))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)

	// The originator holds a GET .../response (D-D6).
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	// The free member is offered the job.
	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("fr-b offer for job %s, want %s", of.JobID, jobID)
	}

	ndjson := []string{
		`{"choices":[{"delta":{"content":"WS06-RESP-MARKER-1"}}]}`,
		`{"choices":[{"delta":{"content":"WS06-RESP-MARKER-2"}}]}`,
		"data: [DONE]",
	}
	must(t, frB.AcceptAndServe(ctx, jobID, len(body), convKey, ndjson))

	// Exact relay to the originator, in order (bounded: a relay regression
	// must fail, not hang).
	got := []string{}
	rgDeadline := time.After(15 * time.Second)
	for len(got) < len(ndjson) {
		select {
		case line := <-rgA.Lines():
			got = append(got, line)
		case <-rgDeadline:
			t.Fatalf("relay timeout: got %d lines, want %d", len(got), len(ndjson))
		}
	}
	if diff := slices.Compare(got, ndjson); diff != 0 {
		t.Fatalf("response relay mismatch: %v", diff)
	}
	select {
	case err := <-rgA.Done():
		if err != nil {
			t.Fatalf("response get err: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("response get did not close")
	}

	// The originator's subscribe sees the mirrored completion envelope.
	evA, err := frA.NextEvent(ctx, 5*time.Second)
	must(t, err)
	if evA.Type != poolproto.TypeCompletion {
		t.Fatalf("mirrored envelope type = %s, want completion", evA.Type)
	}

	// Records row: ok, model, token counts, serving router.
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select job_id, ok, error_code, model_id, prompt_tokens, response_tokens, cached_tokens, router_id from records where job_id = %s",
		pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows = %d, want 1", len(rows))
	}
	r := strings.Split(rows[0], ",")
	if r[1] != "t" || r[3] != "md-b" || r[4] != "128" || r[5] != "3" || r[7] != frB.ID() {
		t.Fatalf("bad records row: %s", rows[0])
	}

	// Continuity hint recorded on completion (D-E2): same conv key, served by frB.
	if router, ok := waitContinuity(t, env, ctx, convKey, 15*time.Second); router != frB.ID() || ok != "t" {
		t.Fatalf("continuity rows = (%s,%s), want (%s,t)", router, ok, frB.ID())
	}

	// Continuation after the cross-member service (D-E2): a second turn of the
	// same conversation is again offered to frB (the last server), served by
	// it, and leaves the continuity row last_ok=true.
	body2 := []byte(`WS06-BODY-MARKER-2-` + strings.Repeat("w", 2048))
	job2, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body2)
	must(t, err)
	evB2, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if evB2.Type != poolproto.TypeWorkOffer {
		t.Fatalf("continuation event type = %s, want work_offer", evB2.Type)
	}
	if of := offerOf(t, evB2); of.JobID != job2 {
		t.Fatalf("continuation offer for job %s, want %s", of.JobID, job2)
	}
	must(t, frB.AcceptAndServe(ctx, job2, len(body2), convKey, ndjson))
	waitRecord(t, env, ctx, job2, 15*time.Second)
	// Deterministic: the continuation is warm (skip priority) and the hinted
	// router frB is the only candidate and serves, so 'skip' stands.
	outcome, reason := contRecord(t, env, ctx, job2)
	if outcome != "skip" {
		t.Fatalf("continuation cont_outcome = %q, want skip", outcome)
	}
	if reason != "" {
		t.Fatalf("continuation cont_reason = %q, want null", reason)
	}
	if router, ok := waitContinuity(t, env, ctx, convKey, 15*time.Second); router != frB.ID() || ok != "t" {
		t.Fatalf("continuity after continuation = (%s,%s), want (%s,t)", router, ok, frB.ID())
	}

	// Body staged on disk, purged on terminal (D-I2).
	entries, err := os.ReadDir(env.dataDir + "/jobs")
	must(t, err)
	if len(entries) != 0 {
		t.Fatalf("body file not purged: %v", entries)
	}
}

// TestE2E_Bounce_SecondAccepts: the first candidate (largest ctx) rejects
// with no_slot; the coordinator parks it and the second candidate accepts and
// completes.
func TestE2E_Bounce_SecondAccepts(t *testing.T) {
	env := newE2E(t)

	// frA: free, rejects. Largest ctx -> first candidate.
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 262144, 1, 0, poolproto.ModelLoaded)
	cr := &countingReject{}
	frA.SetPolicy(cr)
	must(t, frA.Connect(context.Background()))

	// frB: free, accepts.
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	body := []byte(`WS06-BODY-MARKER-B-` + strings.Repeat("y", 1024))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)

	// frA is offered first; its installed policy (countingReject) decides and
	// the decision is executed through the policy mechanism (not by hand).
	evA, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evA); of.JobID != jobID {
		t.Fatalf("fr-a offer for job %s, want %s", of.JobID, jobID)
	}
	must(t, frA.DecideAndRespond(ctx, offerOf(t, evA), len(body), "", nil))

	// frB is offered next and its AcceptPolicy decides+executes the serve.
	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("fr-b offer for job %s, want %s", of.JobID, jobID)
	}
	ndjson := []string{
		`{"choices":[{"delta":{"content":"WS06-B2-1"}}]}`,
		"data: [DONE]",
	}
	must(t, frB.DecideAndRespond(ctx, offerOf(t, evB), len(body), "", ndjson))

	if cr.calls != 1 {
		t.Fatalf("fr-a decide calls = %d, want 1", cr.calls)
	}

	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select router_id, ok from records where job_id = %s", pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 || rows[0] != frB.ID()+",t" {
		t.Fatalf("records rows = %v, want %s,t", rows, frB.ID())
	}
}

// TestE2E_IntakeIdempotency_RequestID: the same request_id twice resolves to the
// same job id and a single queue_jobs row (D-G5).
func TestE2E_IntakeIdempotency_RequestID(t *testing.T) {
	env := newE2E(t)
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(context.Background()))

	ctx := context.Background()
	reqID := pooldb.NewUUID()
	body := []byte(`WS06-INTAKE-BODY-` + strings.Repeat("z", 512))
	job1, err := frA.Intake(ctx, reqID, "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	job2, err := frA.Intake(ctx, reqID, "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	if job1 != job2 {
		t.Fatalf("idempotent intake diverged: %s vs %s", job1, job2)
	}
	n, err := env.db.Count(ctx, "select count(*) from queue_jobs where request_id = "+pooldb.UUID(reqID))
	must(t, err)
	if n != 1 {
		t.Fatalf("queue_jobs rows for request_id = %d, want 1", n)
	}
}

// TestE2E_NoMembers_ModelGone: intake for a class nobody has loaded must
// fail with model_gone (D-C7), mirrored to the originator with a records row.
func TestE2E_NoMembers_ModelGone(t *testing.T) {
	env := newE2E(t)
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(context.Background()))

	ctx := context.Background()
	body := []byte(`WS06-MG-BODY-` + strings.Repeat("m", 512))
	// pool-text-xl: nobody has a model with this tag.
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-xl", 128, 4096, nil, body)
	must(t, err)

	// The originator's subscribe sees the error envelope (no relay for a
	// failed job).
	ev, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if ev.Type != poolproto.TypeError {
		t.Fatalf("envelope type = %s, want error", ev.Type)
	}
	var rep poolproto.ErrorReport
	if err := json.Unmarshal(ev.Payload, &rep); err != nil || rep.Code != "model_gone" || rep.JobID != jobID {
		t.Fatalf("bad error report: %v (%v)", rep, err)
	}

	// Records row: not ok, error_code = model_gone, router is null.
	rows, err := env.db.Query(ctx, fmt.Sprintf(
		"select ok, error_code, router_id from records where job_id = %s", pooldb.UUID(jobID)))
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("records rows = %d, want 1", len(rows))
	}
	r := strings.Split(rows[0], ",")
	if r[0] != "f" || r[1] != "model_gone" || r[2] != "" {
		t.Fatalf("bad records row: %s", rows[0])
	}

	// Body staged on intake, purged on terminal.
	entries, err := os.ReadDir(env.dataDir + "/jobs")
	must(t, err)
	if len(entries) != 0 {
		t.Fatalf("body file not purged: %v", entries)
	}
}

// TestE2E_Classes_AdvertisedCtx: advertised_ctx = min(round(avg + pop_stddev),
// max) across loaded members (D-C1/D-C2). 32768 + 131072 -> avg 81920, pop
// stddev 49152 -> 131072, capped at max 131072.
func TestE2E_Classes_AdvertisedCtx(t *testing.T) {
	env := newE2E(t)
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	must(t, frB.Connect(context.Background()))

	ctx := context.Background()
	res, err := frA.doGet(ctx, "/pool/classes")
	must(t, err)
	defer res.Body.Close()
	var envelope struct {
		Protocol int              `json:"protocol"`
		Classes  []map[string]any `json:"classes"`
	}
	must(t, json.NewDecoder(res.Body).Decode(&envelope))
	var got map[string]any
	for _, c := range envelope.Classes {
		if c["tag"] == "pool-text-md" {
			got = c
		}
	}
	if got == nil {
		t.Fatalf("pool-text-md not advertised: %v", envelope.Classes)
	}
	if got["member_count"].(float64) != 2 || got["min_ctx"].(float64) != 32768 || got["max_ctx"].(float64) != 131072 {
		t.Fatalf("bad class row: %v", got)
	}
	if got["advertised_ctx"].(float64) != 131072 {
		t.Fatalf("advertised_ctx = %v, want 131072", got["advertised_ctx"])
	}
}

// TestE2E_AntiMemory_BodyNeverLogged: request/response bodies must never reach
// the coordinator's logs or disk beyond the staged file (D-I1/D-I3). A
// recognizable marker in the body must be absent from the log buffer.
func TestE2E_AntiMemory_BodyNeverLogged(t *testing.T) {
	env := newE2E(t)
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(context.Background()))

	ctx := context.Background()
	marker := `WS06-ANTIMEM-` + strings.Repeat("Q", 64)
	body := []byte(marker + strings.Repeat("q", 4096))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)
	ev, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	nndjson := []string{`{"choices":[{"delta":{"content":"ok"}}]}`, "data: [DONE]"}
	must(t, frA.DecideAndRespond(ctx, offerOf(t, ev), len(body), "", nndjson))
	select {
	case <-rgA.Done(): // wait for the full relay to drain
	case <-time.After(10 * time.Second):
		t.Fatal("response get did not close")
	}

	if strings.Contains(env.logs.String(), "WS06-ANTIMEM") {
		t.Fatalf("request body marker found in coordinator logs (anti-memory violation):\n%s",
			truncate([]byte(env.logs.String()), 4000))
	}
	entries, err := os.ReadDir(env.dataDir + "/jobs")
	must(t, err)
	if len(entries) != 0 {
		t.Fatalf("body file not purged: %v", entries)
	}
}

// TestE2E_ClientDisconnect_FailsJob: the originator closing its held GET
// (D-D6) must fail the running job with client_disconnected.
func TestE2E_ClientDisconnect_FailsJob(t *testing.T) {
	env := newE2E(t)
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(context.Background()))

	ctx := context.Background()
	body := []byte(`WS06-DISC-BODY-` + strings.Repeat("y", 1024))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)
	ev, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	// Serve but hold: no completion, the stream stays open.
	must(t, frA.AcceptAndHold(ctx, offerOf(t, ev).JobID))
	time.Sleep(300 * time.Millisecond) // let the body pull settle
	rgA.Close()

	deadline := time.After(15 * time.Second)
	for {
		rows, err := env.db.Query(ctx, fmt.Sprintf(
			"select ok, error_code from records where job_id = %s", pooldb.UUID(jobID)))
		if err == nil && len(rows) == 1 {
			r := strings.Split(rows[0], ",")
			if r[0] != "f" || r[1] != "client_disconnected" {
				t.Fatalf("records row = %s, want f,client_disconnected", rows[0])
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("job did not fail with client_disconnected in time")
		case <-time.After(200 * time.Millisecond):
		}
	}
}
