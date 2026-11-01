package dispatch

import (
	"testing"
	"time"
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
			got := pickFair(tc.jobs, now, tc.warm)
			if (got == nil) != (tc.want == "") {
				t.Fatalf("pickFair = %v, want %q", got, tc.want)
			}
			if got != nil && got.JobID != tc.want {
				t.Fatalf("pickFair = %q, want %q", got.JobID, tc.want)
			}
		})
	}
}
