package server

// WS-15: sharing-policy metrics (D-O1). GET /portal/metrics (cookie auth,
// a member sees only their own data) and GET /api/admin/metrics (the /api
// gate: pool-wide + per-user + per-router). The rolling window is
// POOL_FAIR_WINDOW_MS, so the displayed numbers match the D-F2 fairness
// ratio.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// seedMetricUser creates an active member user and returns (user id, token).
func seedMetricUser(t *testing.T, env *e2eEnv, email, name string) (string, string) {
	t.Helper()
	return setupAuthUser(t, env, email, name, "member", "active")
}

// seedRouter inserts a router row owned by userID.
func seedRouter(t *testing.T, env *e2eEnv, userID, name string) string {
	t.Helper()
	id := pooldb.NewUUID()
	must(t, env.db.Exec(context.Background(), fmt.Sprintf(
		"insert into routers (id, owner_id, router_name) values (%s, %s, %s)",
		pooldb.UUID(id), pooldb.UUID(userID), pooldb.S(name))))
	return id
}

// seedRecord inserts one records row (aggregates only; no content).
func seedRecord(t *testing.T, env *e2eEnv, userID, classID, routerID string, ok bool, rt, tq, ts int, cont string) {
	t.Helper()
	contSQL := "NULL"
	if cont != "" {
		contSQL = pooldb.S(cont)
	}
	routerSQL := pooldb.UUID(routerID)
	must(t, env.db.Exec(context.Background(), fmt.Sprintf(`
insert into records (job_id, user_id, class_id, model_id, router_id, priority,
                     prompt_tokens, response_tokens, cached_tokens, t_queued_ms, t_service_ms,
                     ok, error_code, stop_reason, cont_outcome, cont_reason, created_at)
values (%s, %s, %s, 'md-x', %s, 'live', 5, %d, 0, %d, %d, %t, %s, %s, %s, NULL, now())`,
		pooldb.UUID(pooldb.NewUUID()), pooldb.UUID(userID), pooldb.S(classID), routerSQL,
		rt, tq, ts, ok, pooldb.S("ok"), pooldb.S("stop"), contSQL)))
}

// metricClose is a float compare with a 1e-9 tolerance.
func metricClose(t *testing.T, got, want float64, what string) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// TestMetrics_Portal_MemberOwn: a member's /portal/metrics reflects only
// their own rows and routers, and the response carries no other user's
// identity (the member-cannot-see-other-users assertion).
func TestMetrics_Portal_MemberOwn(t *testing.T) {
	env := newE2E(t)

	ua, tokenA := seedMetricUser(t, env, "ws15-met-a@example.com", "WS15 Met A")
	ub, tokenB := seedMetricUser(t, env, "ws15-met-b@example.com", "WS15 Met B")
	ra := seedRouter(t, env, ua, "ws15-met-ra")
	rb := seedRouter(t, env, ub, "ws15-met-rb")
	_ = tokenB

	// A: 3 ok + 1 fail; one ok row served by A's own router (ra); the
	// continuity outcomes cover warm/skip/cold; classes md(2)/lg/sm.
	seedRecord(t, env, ua, "pool-text-md", ra, true, 10, 100, 500, "warm")
	seedRecord(t, env, ua, "pool-text-lg", "", true, 20, 300, 900, "skip")
	seedRecord(t, env, ua, "pool-text-md", "", false, 0, 200, 0, "")
	seedRecord(t, env, ua, "pool-text-sm", "", true, 30, 150, 700, "cold")
	// B: one ok row served by B's router, with a distinctive token count.
	seedRecord(t, env, ub, "pool-text-2xl", rb, true, 999, 40, 80, "")

	// A logs in (cookie) and reads their metrics.
	_, cookieA := portalLoginMust(t, env.baseURL, tokenA)
	resp, raw := portalDo(t, "GET", env.baseURL+"/portal/metrics", cookieA, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /portal/metrics = %d: %s", resp.StatusCode, raw)
	}

	var got struct {
		WindowMS int `json:"window_ms"`
		metricsSummary
	}
	must(t, json.Unmarshal([]byte(raw), &got))

	if got.RequestsMade != 4 {
		t.Fatalf("requests_made = %d, want 4 (only A's rows)", got.RequestsMade)
	}
	if got.RequestsServedByMe != 1 || got.TokensServed != 10 {
		t.Fatalf("served = (%d, %d), want (1, 10) (A's router only)", got.RequestsServedByMe, got.TokensServed)
	}
	if got.TokensConsumed != 60 {
		t.Fatalf("tokens_consumed = %d, want 60", got.TokensConsumed)
	}
	metricClose(t, got.Ratio, 11.0/61.0, "ratio (D-F2: (served+1)/(consumed+1))")
	metricClose(t, got.OKRate, 3.0/4.0, "ok_rate")
	if got.QueueTimeMS.P50 == nil || got.QueueTimeMS.P95 == nil {
		t.Fatalf("queue_time_ms null: %+v", got.QueueTimeMS)
	}
	metricClose(t, *got.QueueTimeMS.P50, 150, "queue p50")
	metricClose(t, *got.QueueTimeMS.P95, 285, "queue p95")
	metricClose(t, *got.ServiceTimeMS.P50, 700, "service p50")
	metricClose(t, *got.ServiceTimeMS.P95, 880, "service p95")
	if got.Continuity.Warm != 1 || got.Continuity.Skip != 1 || got.Continuity.Cold != 1 {
		t.Fatalf("continuity = %+v, want warm=1 skip=1 cold=1", got.Continuity)
	}
	if len(got.TopClasses) != 3 || got.TopClasses[0].ClassID != "pool-text-md" ||
		got.TopClasses[0].Requests != 2 {
		t.Fatalf("top_classes = %+v, want pool-text-md first with 2", got.TopClasses)
	}

	// The member sees ONLY their own data: no trace of user B in the body.
	for _, marker := range []string{ub, "ws15-met-b@example.com", "WS15 Met B", rb} {
		if strings.Contains(raw, marker) {
			t.Fatalf("member metrics response contains other user's data (%s)", marker)
		}
	}
}

// TestMetrics_Admin: the /api gate serves the pool-wide aggregates plus the
// per-user and per-router tables; a non-admin member gets 403.
func TestMetrics_Admin(t *testing.T) {
	env := newE2E(t)

	ua, tokenA := seedMetricUser(t, env, "ws15-adm-a@example.com", "WS15 Adm A")
	ub, tokenB := seedMetricUser(t, env, "ws15-adm-b@example.com", "WS15 Adm B")
	ra := seedRouter(t, env, ua, "ws15-adm-ra")
	rb := seedRouter(t, env, ub, "ws15-adm-rb")
	_ = tokenB

	// A: one ok (served by their own router ra) + one fail. B: one ok
	// served by A's router (asymmetric: B consumes a lot, serves nothing).
	seedRecord(t, env, ua, "pool-text-md", ra, true, 10, 100, 500, "")
	seedRecord(t, env, ua, "pool-text-md", "", false, 0, 200, 0, "")
	seedRecord(t, env, ub, "pool-text-2xl", ra, true, 999, 40, 80, "")

	// Non-admin member: 403.
	req, _ := http.NewRequest("GET", env.baseURL+"/api/admin/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("member GET /api/admin/metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member GET /api/admin/metrics = %d, want 403", resp.StatusCode)
	}

	// No token: 401.
	resp, err = http.Get(env.baseURL + "/api/admin/metrics")
	if err != nil {
		t.Fatalf("unauthenticated GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /api/admin/metrics = %d, want 401", resp.StatusCode)
	}

	// Admin: pool + users + routers.
	req, _ = http.NewRequest("GET", env.baseURL+"/api/admin/metrics", nil)
	req.Header.Set("Authorization", "Bearer admin")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("admin GET /api/admin/metrics: %v", err)
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin GET /api/admin/metrics = %d: %s", resp.StatusCode, raw)
	}

	var got adminMetricsResp
	must(t, json.Unmarshal([]byte(raw), &got))
	if got.Pool.RequestsMade != 3 || got.Pool.TokensServed != 1009 || got.Pool.TokensConsumed != 1009 {
		t.Fatalf("pool = %+v, want made=3 served=1009 consumed=1009", got.Pool)
	}
	// Pool-wide, served always equals consumed: the ratio is 1.0 by
	// construction (it is a per-user quantity).
	metricClose(t, got.Pool.Ratio, 1.0, "pool ratio")
	metricClose(t, got.Pool.OKRate, 2.0/3.0, "pool ok_rate")

	byID := map[string]adminUserMetrics{}
	for _, u := range got.Users {
		byID[u.UserID] = u
	}
	a, okA := byID[ua]
	b, okB := byID[ub]
	if !okA || !okB {
		t.Fatalf("per-user table missing users: %+v", got.Users)
	}
	if a.Served != 1009 || a.Consumed != 10 || a.OKRate != 0.5 {
		t.Fatalf("user A row = %+v, want served=1009 consumed=10 ok_rate=0.5", a)
	}
	metricClose(t, a.Ratio, 1010.0/11.0, "user A ratio")
	if b.Served != 0 || b.Consumed != 999 || b.OKRate != 1.0 {
		t.Fatalf("user B row = %+v, want served=0 consumed=999 ok_rate=1", b)
	}
	metricClose(t, b.Ratio, 1.0/1000.0, "user B ratio")

	byRouter := map[string]adminRouterMetrics{}
	for _, r := range got.Routers {
		byRouter[r.RouterID] = r
	}
	raRow, okRA := byRouter[ra]
	rbRow, okRB := byRouter[rb]
	if !okRA || !okRB {
		t.Fatalf("per-router table missing routers: %+v", got.Routers)
	}
	if raRow.Owner != ua || raRow.JobsServed != 2 || raRow.TokensServed != 1009 {
		t.Fatalf("router ra row = %+v, want owner=A jobs=2 tokens=1009", raRow)
	}
	if rbRow.Owner != ub || rbRow.JobsServed != 0 || rbRow.TokensServed != 0 {
		t.Fatalf("router rb row = %+v, want owner=B jobs=0 tokens=0", rbRow)
	}
}

// TestMetrics_Window: the rolling window is POOL_FAIR_WINDOW_MS; an older
// row (2h back, window 1min here) is excluded.
func TestMetrics_Window(t *testing.T) {
	env := newE2EWith(t, e2eOpts{fairWindowMS: 60000})
	ctx := context.Background()

	ua, tokenA := seedMetricUser(t, env, "ws15-win-a@example.com", "WS15 Win A")
	ra := seedRouter(t, env, ua, "ws15-win-ra")

	seedRecord(t, env, ua, "pool-text-md", ra, true, 10, 100, 500, "")
	// A row two hours old: outside the 1-minute window.
	must(t, env.db.Exec(ctx, fmt.Sprintf(`
insert into records (job_id, user_id, class_id, router_id, priority, prompt_tokens,
                     response_tokens, t_queued_ms, t_service_ms, ok, created_at)
values (%s, %s, 'pool-text-md', %s, 'live', 5, 77, 100, 500, true, now() - interval '2 hours')`,
		pooldb.UUID(pooldb.NewUUID()), pooldb.UUID(ua), pooldb.UUID(ra))))

	_, cookieA := portalLoginMust(t, env.baseURL, tokenA)
	resp, raw := portalDo(t, "GET", env.baseURL+"/portal/metrics", cookieA, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /portal/metrics = %d: %s", resp.StatusCode, raw)
	}
	var got struct {
		WindowMS int `json:"window_ms"`
		metricsSummary
	}
	must(t, json.Unmarshal([]byte(raw), &got))
	if got.RequestsMade != 1 {
		t.Fatalf("requests_made = %d, want 1 (the 2h-old row is outside the window)", got.RequestsMade)
	}
	if got.TokensConsumed != 10 {
		t.Fatalf("tokens_consumed = %d, want 10", got.TokensConsumed)
	}
}

// TestMetrics_E2E_Wiring: a real pool job (fake router end to end) shows up
// in the member's metrics after the ok terminal.
func TestMetrics_E2E_Wiring(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, tokenA := seedMetricUser(t, env, "ws15-e2e-a@example.com", "WS15 E2E A")
	frA := NewFakeRouter(env.baseURL, tokenA, "ws15-e2e-a", "ws15-e2e-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 2, 0, poolproto.ModelLoaded)
	frA.SetPolicy(AcceptPolicy{})
	must(t, frA.Connect(ctx))

	body := []byte(`WS15-E2E-BODY-` + strings.Repeat("w", 512))
	ndjson := []string{`{"choices":[{"delta":{"content":"ws15"}}]}`, "data: [DONE]"}
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, nil, body)
	must(t, err)
	of := nextOffer(t, frA, ctx, 15*time.Second)
	must(t, frA.AcceptAndServe(ctx, of.JobID, len(body), "", ndjson))
	waitRecord(t, env, ctx, jobID, 15*time.Second)

	_, cookieA := portalLoginMust(t, env.baseURL, tokenA)
	resp, raw := portalDo(t, "GET", env.baseURL+"/portal/metrics", cookieA, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /portal/metrics = %d: %s", resp.StatusCode, raw)
	}
	var got struct {
		metricsSummary
	}
	must(t, json.Unmarshal([]byte(raw), &got))
	if got.RequestsMade != 1 || got.OKRate != 1.0 {
		t.Fatalf("after one ok job: requests_made=%d ok_rate=%v, want 1 and 1.0", got.RequestsMade, got.OKRate)
	}
	// The fake router reports 2 response tokens, served by the member's router.
	if got.TokensConsumed != 2 || got.TokensServed != 2 {
		t.Fatalf("tokens = (consumed %d, served %d), want (2, 2)", got.TokensConsumed, got.TokensServed)
	}
}

// portalLoginMust is portalLogin with a fatal on a non-200.
func portalLoginMust(t *testing.T, baseURL, token string) (int, string) {
	t.Helper()
	status, cookie := portalLogin(t, baseURL, token)
	if status != http.StatusOK || cookie == "" {
		t.Fatalf("portal login = %d (cookie %q)", status, cookie)
	}
	return status, cookie
}
