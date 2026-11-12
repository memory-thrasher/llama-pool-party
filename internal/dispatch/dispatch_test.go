package dispatch

import (
	"testing"
	"time"

	"pool-coordinator/protocol/go/poolproto"
)

// TestPickFair_WarmHint pins the D-E2 skip-the-line pick ordering: a
// warm-continuation job is picked before older plain jobs; within a group
// the per-user FIFO (users by created_at) applies; parked jobs (NextOfferAt
// in the future) are not eligible in either group.
func TestPickFair_WarmHint(t *testing.T) {
	now := time.Now()
	user := now.Add(-time.Hour)
	mk := func(id string, created, userCreated, next time.Time) *jobState {
		return &jobState{JobID: id, Status: stPending, CreatedAt: created,
			UserCreatedAt: userCreated, NextOfferAt: next}
	}
	isWarm := func(j *jobState) bool { return j.JobID == "warm" }
	cases := []struct {
		name string
		jobs map[string]*jobState
		warm func(*jobState) bool
		want string // "" = no eligible job
	}{
		{
			name: "warm new job beats older plain job",
			jobs: map[string]*jobState{
				"old":  mk("old", now.Add(-3*time.Second), user, time.Time{}),
				"warm": mk("warm", now.Add(-1*time.Second), user, time.Time{}),
			},
			warm: isWarm,
			want: "warm",
		},
		{
			name: "oldest warm job wins among warm jobs",
			jobs: map[string]*jobState{
				"w1": mk("w1", now.Add(-3*time.Second), user, time.Time{}),
				"w2": mk("w2", now.Add(-1*time.Second), user, time.Time{}),
			},
			warm: func(*jobState) bool { return true },
			want: "w1",
		},
		{
			name: "no warm: oldest plain job wins",
			jobs: map[string]*jobState{
				"a": mk("a", now.Add(-3*time.Second), user, time.Time{}),
				"b": mk("b", now.Add(-1*time.Second), user, time.Time{}),
			},
			warm: func(*jobState) bool { return false },
			want: "a",
		},
		{
			name: "parked warm is not eligible yet",
			jobs: map[string]*jobState{
				"warm":  mk("warm", now.Add(-3*time.Second), user, now.Add(time.Second)),
				"plain": mk("plain", now.Add(-2*time.Second), user, time.Time{}),
			},
			warm: isWarm,
			want: "plain",
		},
		{
			name: "warm job parked in the past is eligible",
			jobs: map[string]*jobState{
				"warm":  mk("warm", now.Add(-3*time.Second), user, now.Add(-time.Second)),
				"plain": mk("plain", now.Add(-2*time.Second), user, time.Time{}),
			},
			warm: isWarm,
			want: "warm",
		},
		{
			name: "non-pending jobs are ignored",
			jobs: map[string]*jobState{
				"running": {JobID: "running", Status: stRunning, CreatedAt: now.Add(-10 * time.Second), UserCreatedAt: user},
				"warm":    mk("warm", now.Add(-1*time.Second), user, time.Time{}),
			},
			warm: isWarm,
			want: "warm",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pickFair(tc.jobs, now, tc.warm, 10, func([]string) map[string]float64 { return map[string]float64{} })
			if (got == nil) != (tc.want == "") {
				t.Fatalf("pickFair = %v, want %q", got, tc.want)
			}
			if got != nil && got.JobID != tc.want {
				t.Fatalf("pickFair = %q, want %q", got.JobID, tc.want)
			}
		})
	}
}

// TestPickFair_LiveBeforeBatch pins the D-H1 priority grouping: a pending
// live job is always offered before any batch job (even an older one); the
// batch group is picked only when no live job is pending, and the per-user
// FIFO inside a group is unchanged.
func TestPickFair_LiveBeforeBatch(t *testing.T) {
	now := time.Now()
	user := now.Add(-time.Hour)
	mk := func(id, priority string, created time.Time) *jobState {
		return &jobState{JobID: id, UserID: "u", Priority: priority, Status: stPending,
			CreatedAt: created, UserCreatedAt: user}
	}
	cases := []struct {
		name string
		jobs map[string]*jobState
		want string
	}{
		{
			name: "older batch job loses to a newer live job",
			jobs: map[string]*jobState{
				"batch": mk("batch", poolproto.PriorityBatch, now.Add(-5*time.Second)),
				"live":  mk("live", poolproto.PriorityLive, now.Add(-1*time.Second)),
			},
			want: "live",
		},
		{
			name: "no live job: the batch job is picked",
			jobs: map[string]*jobState{
				"batch": mk("batch", poolproto.PriorityBatch, now.Add(-5*time.Second)),
			},
			want: "batch",
		},
		{
			name: "batch group keeps its own FIFO",
			jobs: map[string]*jobState{
				"old": mk("old", poolproto.PriorityBatch, now.Add(-5*time.Second)),
				"new": mk("new", poolproto.PriorityBatch, now.Add(-1*time.Second)),
			},
			want: "old",
		},
		{
			name: "live group keeps its own FIFO",
			jobs: map[string]*jobState{
				"l1": mk("l1", poolproto.PriorityLive, now.Add(-5*time.Second)),
				"l2": mk("l2", poolproto.PriorityLive, now.Add(-1*time.Second)),
			},
			want: "l1",
		},
		{
			name: "parked live job: the batch job still waits behind the unparked live job",
			jobs: map[string]*jobState{
				"batch":  mk("batch", poolproto.PriorityBatch, now.Add(-5*time.Second)),
				"parked": {JobID: "parked", UserID: "u", Priority: poolproto.PriorityLive, Status: stPending, CreatedAt: now.Add(-4 * time.Second), UserCreatedAt: user, NextOfferAt: now.Add(time.Second)},
				"live":   mk("live", poolproto.PriorityLive, now.Add(-1*time.Second)),
			},
			want: "live",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pickFair(tc.jobs, now, func(*jobState) bool { return false }, 10,
				func([]string) map[string]float64 { return map[string]float64{} })
			if got == nil {
				t.Fatalf("pickFair = nil, want %q", tc.want)
			}
			if got.JobID != tc.want {
				t.Fatalf("pickFair = %q, want %q", got.JobID, tc.want)
			}
		})
	}
}

// TestPickFair_Fairness pins the D-F2/D-F3 non-warm pick: among the k oldest
// eligible jobs the highest-ratio user is served, at their oldest job in the
// window; a ratio 1.0 tie serves the oldest job (per-user FIFO).
func TestPickFair_Fairness(t *testing.T) {
	now := time.Now()
	mk := func(id, user string, created time.Time) *jobState {
		return &jobState{JobID: id, UserID: user, Status: stPending, CreatedAt: created}
	}
	cases := []struct {
		name   string
		jobs   map[string]*jobState
		topK   int
		ratios map[string]float64
		want   string
	}{
		{
			name: "ratio 1.0 tie -> oldest job first",
			jobs: map[string]*jobState{
				"a": mk("a", "uA", now.Add(-3*time.Second)),
				"b": mk("b", "uB", now.Add(-1*time.Second)),
			},
			ratios: map[string]float64{"uA": 1.0, "uB": 1.0},
			want:   "a",
		},
		{
			name: "higher ratio user beats the older job",
			jobs: map[string]*jobState{
				"old": mk("old", "uLow", now.Add(-3*time.Second)),
				"new": mk("new", "uHigh", now.Add(-1*time.Second)),
			},
			ratios: map[string]float64{"uLow": 0.001, "uHigh": 100},
			want:   "new",
		},
		{
			name: "per-user FIFO: the user's oldest job is served",
			jobs: map[string]*jobState{
				"old": mk("old", "u", now.Add(-3*time.Second)),
				"new": mk("new", "u", now.Add(-1*time.Second)),
			},
			ratios: map[string]float64{},
			want:   "old",
		},
		{
			name: "top-k window: users outside the window are not picked",
			jobs: map[string]*jobState{
				"heaviest": mk("heaviest", "uH", now.Add(-5*time.Second)),
				"b":        mk("b", "uB", now.Add(-3*time.Second)),
				"c":        mk("c", "uC", now.Add(-1*time.Second)),
			},
			topK:   2,
			ratios: map[string]float64{"uH": 0.001, "uB": 1.0, "uC": 1.0},
			want:   "b",
		},
		{
			name: "parked jobs are not eligible in the window",
			jobs: map[string]*jobState{
				"parked": {JobID: "parked", UserID: "uH", Status: stPending, CreatedAt: now.Add(-5 * time.Second), NextOfferAt: now.Add(time.Second)},
				"b":      mk("b", "uB", now.Add(-3*time.Second)),
				"c":      mk("c", "uC", now.Add(-1*time.Second)),
			},
			topK:   2,
			ratios: map[string]float64{},
			want:   "b",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ratios := tc.ratios
			if ratios == nil {
				ratios = map[string]float64{}
			}
			topK := tc.topK
			if topK <= 0 {
				topK = 10
			}
			got := pickFair(tc.jobs, now, func(*jobState) bool { return false }, topK,
				func([]string) map[string]float64 { return ratios })
			if got == nil {
				t.Fatalf("pickFair = nil, want %q", tc.want)
			}
			if got.JobID != tc.want {
				t.Fatalf("pickFair = %q, want %q", got.JobID, tc.want)
			}
		})
	}
}

// TestRunningCapFor pins the D-G2 amendment (2026-11-12, KI-1): the running
// cap scales with the estimated prompt - a large prompt takes minutes to
// evaluate and produces no response lines meanwhile (incident: 185981 tokens
// in 560s against the 5m base cap) - and the scale is capped at the largest
// advertised context so image-heavy bodies (est = raw bytes/4) cannot
// inflate it without bound.
func TestRunningCapFor(t *testing.T) {
	d := &Dispatcher{runningCap: 5 * time.Minute}
	base := 5 * time.Minute
	cases := []struct {
		name string
		est  int
		want time.Duration
	}{
		{"no estimate: base only", 0, base},
		{"small prompt: base + 20s", 5000, base + 20*time.Second},
		{"incident prompt: 171861 tokens", 171861, base + 171861*runningPerTokenMS*time.Millisecond},
		{"image-heavy body capped at the largest advertised ctx", 10_000_000, base + runningMaxScaleTokens*runningPerTokenMS*time.Millisecond},
	}
	for _, c := range cases {
		j := &jobState{EstPromptTokens: c.est}
		if got := d.runningCapFor(j); got != c.want {
			t.Errorf("%s: runningCapFor = %s, want %s", c.name, got, c.want)
		}
	}
}
