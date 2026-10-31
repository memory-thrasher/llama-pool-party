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

func newE2E(t *testing.T) *e2eEnv {
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
	cfg := config.Config{Addr: "127.0.0.1:0", DSN: baseDSN, DataDir: dataDir, LogLevel: "info"}
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
	body := []byte(`WS06-BODY-MARKER-` + strings.Repeat("x", 4096))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
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
	must(t, frB.AcceptAndServe(ctx, jobID, len(body), "", ndjson))

	// Exact relay to the originator, in order.
	got := []string{}
	for line := range rgA.Lines() {
		got = append(got, line)
		if line == "data: [DONE]" {
			break
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

	// frA is offered first and rejects.
	evA, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evA); of.JobID != jobID {
		t.Fatalf("fr-a offer for job %s, want %s", of.JobID, jobID)
	}
	dec := cr.Decide(offerOf(t, evA))
	if dec.Accept {
		t.Fatal("fr-a should reject")
	}
	must(t, frA.postEvent(ctx, poolproto.TypeOfferReject,
		&poolproto.OfferReject{JobID: jobID, Reason: dec.Reason}))

	// frB is offered next and accepts.
	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("fr-b offer for job %s, want %s", of.JobID, jobID)
	}
	ndjson := []string{
		`{"choices":[{"delta":{"content":"WS06-B2-1"}}]}`,
		"data: [DONE]",
	}
	must(t, frB.AcceptAndServe(ctx, jobID, len(body), "", ndjson))

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
