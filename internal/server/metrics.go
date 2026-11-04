// Sharing-policy metrics (WS-15, D-O1 "sharing policy data"): read-only
// aggregates over the records table, in the D-F2 rolling window
// (POOL_FAIR_WINDOW_MS) so the numbers match the fairness ratio.
//
// GET /portal/metrics (cookie auth): a member sees only their own data.
// GET /api/admin/metrics (the /api token gate): pool-wide plus the per-user
// and per-router tables. Aggregates only: no request content, no
// per-conversation detail, no user input in the queries (closed sets).
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/pooldb"
)

type metricsTimePct struct {
	P50 *float64 `json:"p50"`
	P95 *float64 `json:"p95"`
}

type metricsContCounts struct {
	Warm int `json:"warm"`
	Skip int `json:"skip"`
	Cold int `json:"cold"`
}

type metricsClass struct {
	ClassID  string `json:"class_id"`
	Requests int    `json:"requests"`
}

// metricsSummary is the D-O1 aggregate set. For the member, the served side
// counts the routers they own; for the admin pool view it counts all
// routers.
type metricsSummary struct {
	RequestsMade       int               `json:"requests_made"`
	RequestsServedByMe int               `json:"requests_served_by_me"`
	TokensConsumed     int64             `json:"tokens_consumed"`
	TokensServed       int64             `json:"tokens_served"`
	Ratio              float64           `json:"ratio"`
	OKRate             float64           `json:"ok_rate"`
	QueueTimeMS        metricsTimePct    `json:"queue_time_ms"`
	ServiceTimeMS      metricsTimePct    `json:"service_time_ms"`
	Continuity         metricsContCounts `json:"continuity"`
	TopClasses         []metricsClass    `json:"top_classes"`
}

type adminUserMetrics struct {
	UserID      string  `json:"user_id"`
	DisplayName string  `json:"display_name"`
	Served      int64   `json:"served"`
	Consumed    int64   `json:"consumed"`
	Ratio       float64 `json:"ratio"`
	OKRate      float64 `json:"ok_rate"`
}

type adminRouterMetrics struct {
	RouterID     string `json:"router_id"`
	Owner        string `json:"owner"`
	OwnerName    string `json:"owner_name"`
	LastSeenAt   string `json:"last_seen_at,omitempty"`
	JobsServed   int    `json:"jobs_served"`
	TokensServed int64  `json:"tokens_served"`
}

type adminMetricsResp struct {
	WindowMS int                  `json:"window_ms"`
	Pool     metricsSummary       `json:"pool"`
	Users    []adminUserMetrics   `json:"users"`
	Routers  []adminRouterMetrics `json:"routers"`
}

// metricsWindowMS is the D-F2 rolling window in ms.
func (s *Server) metricsWindowMS() int {
	ms := s.cfg.FairWindowMS
	if ms <= 0 {
		ms = config.DefaultFairWindowMS
	}
	return ms
}

// batchWeightMS renders the D-H1 batch consumption weight for the consumed
// sum (a closed constant; POOL_BATCH_WEIGHT).
func (s *Server) batchWeightSQL() string {
	w := s.cfg.BatchWeight
	if w <= 0 {
		w = config.DefaultBatchWeight
	}
	return strconv.FormatFloat(w, 'f', -1, 64)
}

// fairRatioValue is the D-F2 ratio (served+1)/(consumed+1): the 1.0 baseline
// for an empty window.
func fairRatioValue(served, consumed int64) float64 {
	return (float64(served) + 1) / (float64(consumed) + 1)
}

// metricsSummary computes the aggregate set for one user (their own numbers)
// or the whole pool when userID is empty.
func (s *Server) metricsSummary(ctx context.Context, userID string) (metricsSummary, error) {
	cutoff := fmt.Sprintf("now() - interval '%d milliseconds'", s.metricsWindowMS())
	mine, served := "true", "true"
	if userID != "" {
		mine = fmt.Sprintf("rec.user_id = %s", pooldb.UUID(userID))
		served = fmt.Sprintf("rec.router_id in (select id from routers where owner_id = %s)", pooldb.UUID(userID))
	}

	sum := metricsSummary{TopClasses: []metricsClass{}}
	head, err := s.db.Query(ctx, fmt.Sprintf(`
select
  coalesce(count(*) filter (where %s), 0),
  coalesce(count(*) filter (where %s and rec.ok), 0),
  coalesce(count(*) filter (where rec.ok and %s), 0),
  coalesce(sum(rec.response_tokens) filter (where rec.ok and %s), 0),
  coalesce(sum(rec.response_tokens * (case when rec.priority = 'batch' then %s else 1 end)) filter (where %s and rec.ok), 0)
from records rec
where rec.created_at >= %s`,
		mine, mine, served, served, s.batchWeightSQL(), mine, cutoff))
	if err != nil {
		return sum, err
	}
	if len(head) != 1 || len(strings.Split(head[0], ",")) < 5 {
		return sum, fmt.Errorf("metrics: bad headline row %q", head)
	}
	hf := strings.Split(head[0], ",")
	sum.RequestsMade, _ = strconv.Atoi(hf[0])
	okCount, _ := strconv.Atoi(hf[1])
	sum.RequestsServedByMe, _ = strconv.Atoi(hf[2])
	sum.TokensServed, _ = strconv.ParseInt(hf[3], 10, 64)
	consumed, _ := strconv.ParseFloat(hf[4], 64) // the batch weight makes it a float
	sum.TokensConsumed = int64(consumed)
	sum.Ratio = fairRatioValue(sum.TokensServed, sum.TokensConsumed)
	if sum.RequestsMade > 0 {
		sum.OKRate = float64(okCount) / float64(sum.RequestsMade)
	}

	// p50/p95 over the ok rows: the router-reported timings are the ones
	// that are actually queue/service times (the fail path writes
	// created-to-terminal, which is not a service time).
	pct, err := s.db.Query(ctx, fmt.Sprintf(`
select
  percentile_cont(0.5) within group (order by rec.t_queued_ms) filter (where %s and rec.ok and rec.t_queued_ms is not null),
  percentile_cont(0.95) within group (order by rec.t_queued_ms) filter (where %s and rec.ok and rec.t_queued_ms is not null),
  percentile_cont(0.5) within group (order by rec.t_service_ms) filter (where %s and rec.ok and rec.t_service_ms is not null),
  percentile_cont(0.95) within group (order by rec.t_service_ms) filter (where %s and rec.ok and rec.t_service_ms is not null)
from records rec
where rec.created_at >= %s`,
		mine, mine, mine, mine, cutoff))
	if err != nil {
		return sum, err
	}
	if len(pct) == 1 {
		pf := strings.Split(pct[0], ",")
		sum.QueueTimeMS = metricsTimePct{P50: pctFloat(pf[0]), P95: pctFloat(pf[1])}
		sum.ServiceTimeMS = metricsTimePct{P50: pctFloat(pf[2]), P95: pctFloat(pf[3])}
	}

	cont, err := s.db.Query(ctx, fmt.Sprintf(`
select rec.cont_outcome, count(*)
from records rec
where %s and rec.cont_outcome in ('warm','skip','cold') and rec.created_at >= %s
group by rec.cont_outcome`, mine, cutoff))
	if err != nil {
		return sum, err
	}
	for _, row := range cont {
		cf := strings.Split(row, ",")
		if len(cf) < 2 {
			continue
		}
		n, _ := strconv.Atoi(cf[1])
		switch cf[0] {
		case "warm":
			sum.Continuity.Warm = n
		case "skip":
			sum.Continuity.Skip = n
		case "cold":
			sum.Continuity.Cold = n
		}
	}

	cls, err := s.db.Query(ctx, fmt.Sprintf(`
select rec.class_id, count(*)
from records rec
where %s and rec.class_id is not null and rec.created_at >= %s
group by rec.class_id
order by count(*) desc, rec.class_id asc
limit 5`, mine, cutoff))
	if err != nil {
		return sum, err
	}
	for _, row := range cls {
		cf := strings.Split(row, ",")
		if len(cf) < 2 {
			continue
		}
		n, _ := strconv.Atoi(cf[1])
		sum.TopClasses = append(sum.TopClasses, metricsClass{ClassID: cf[0], Requests: n})
	}
	return sum, nil
}

func pctFloat(cell string) *float64 {
	if cell == "" {
		return nil
	}
	f, err := strconv.ParseFloat(cell, 64)
	if err != nil {
		return nil
	}
	return &f
}

// handlePortalMetrics serves the member's own sharing-policy metrics
// (cookie auth; their own rows and routers only).
func (s *Server) handlePortalMetrics(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return
	}
	sum, err := s.metricsSummary(r.Context(), userID)
	if err != nil {
		s.logf("metrics: member summary: %v", err)
		http.Error(w, "failed to compute metrics", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	_ = enc.Encode(struct {
		WindowMS int `json:"window_ms"`
		metricsSummary
	}{s.metricsWindowMS(), sum})
}

// handleAdminMetrics serves the pool-wide metrics plus the per-user and
// per-router tables (the D-O1 "all users" admin mirror; existing /api gate).
func (s *Server) handleAdminMetrics(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	ctx := r.Context()
	pool, err := s.metricsSummary(ctx, "")
	if err != nil {
		s.logf("metrics: pool summary: %v", err)
		http.Error(w, "failed to compute metrics", http.StatusInternalServerError)
		return
	}
	users, err := s.metricsUsers(ctx)
	if err != nil {
		s.logf("metrics: per-user table: %v", err)
		http.Error(w, "failed to compute metrics", http.StatusInternalServerError)
		return
	}
	routers, err := s.metricsRouters(ctx)
	if err != nil {
		s.logf("metrics: per-router table: %v", err)
		http.Error(w, "failed to compute metrics", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, adminMetricsResp{WindowMS: s.metricsWindowMS(), Pool: pool,
		Users: users, Routers: routers})
}

// metricsUsers is the per-user table: served/consumed token sums (the D-F2
// inputs), the ratio, and the ok rate.
func (s *Server) metricsUsers(ctx context.Context) ([]adminUserMetrics, error) {
	cutoff := fmt.Sprintf("now() - interval '%d milliseconds'", s.metricsWindowMS())
	w := s.batchWeightSQL()
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
select u.id::text, u.display_name,
  coalesce((select sum(rec.response_tokens) from records rec
            join routers rt on rt.id = rec.router_id
            where rt.owner_id = u.id and rec.ok and rec.created_at >= %s), 0),
  coalesce((select sum(rec.response_tokens * (case when rec.priority = 'batch' then %s else 1 end)) from records rec
            where rec.user_id = u.id and rec.ok and rec.created_at >= %s), 0),
  coalesce((select count(*) from records rec
            where rec.user_id = u.id and rec.created_at >= %s), 0),
  coalesce((select count(*) from records rec
            where rec.user_id = u.id and rec.ok and rec.created_at >= %s), 0)
from users u
order by u.created_at asc`, cutoff, w, cutoff, cutoff, cutoff))
	if err != nil {
		return nil, err
	}
	out := []adminUserMetrics{}
	for _, row := range rows {
		f := strings.Split(row, ",")
		if len(f) < 6 {
			continue
		}
		served, _ := strconv.ParseInt(f[2], 10, 64)
		consumedF, _ := strconv.ParseFloat(f[3], 64) // the batch weight makes it a float
		consumed := int64(consumedF)
		requests, _ := strconv.Atoi(f[4])
		okCount, _ := strconv.Atoi(f[5])
		u := adminUserMetrics{UserID: f[0], DisplayName: f[1], Served: served, Consumed: consumed,
			Ratio: fairRatioValue(served, consumed)}
		if requests > 0 {
			u.OKRate = float64(okCount) / float64(requests)
		}
		out = append(out, u)
	}
	return out, nil
}

// metricsRouters is the per-router table: owner, last_seen, and the served
// side within the window.
func (s *Server) metricsRouters(ctx context.Context) ([]adminRouterMetrics, error) {
	cutoff := fmt.Sprintf("now() - interval '%d milliseconds'", s.metricsWindowMS())
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
select r.id::text, coalesce(u.id::text, ''), u.display_name, coalesce(r.last_seen_at::text, ''),
  coalesce((select count(*) from records rec
            where rec.router_id = r.id and rec.ok and rec.created_at >= %s), 0),
  coalesce((select sum(rec.response_tokens) from records rec
            where rec.router_id = r.id and rec.ok and rec.created_at >= %s), 0)
from routers r
left join users u on u.id = r.owner_id
order by r.created_at asc`, cutoff, cutoff))
	if err != nil {
		return nil, err
	}
	out := []adminRouterMetrics{}
	for _, row := range rows {
		f := strings.Split(row, ",")
		if len(f) < 6 {
			continue
		}
		jobs, _ := strconv.Atoi(f[4])
		tokens, _ := strconv.ParseInt(f[5], 10, 64)
		out = append(out, adminRouterMetrics{RouterID: f[0], Owner: f[1], OwnerName: f[2],
			LastSeenAt: f[3], JobsServed: jobs, TokensServed: tokens})
	}
	return out, nil
}
