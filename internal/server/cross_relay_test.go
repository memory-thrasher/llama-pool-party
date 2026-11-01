package server

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// Cross-member relay regression: origin rejects (no_slot), the next member
// accepts and serves a large (120-line) response that must relay fully back
// to the origin. Guards the response-push reply ordering (see handlePutResponse).
func TestCrossRelay_BigResponse(t *testing.T) {
	env := newE2E(t)
	// frA: free slot in the coordinator's view but scripted to reject with
	// no_slot (the D-D3 race). Largest ctx -> offered first; its rejection
	// reason is asserted (review M2).
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 131072, 1, 0, poolproto.ModelLoaded)
	cr := &countingReject{}
	frA.SetPolicy(cr)
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 32768, 2, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))
	ctx := context.Background()
	convKey := hexToken(32)
	body := []byte(`BIG-BODY-` + strings.Repeat("x", 4096))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)

	// frA is offered first and its installed policy rejects no_slot.
	evA, err := frA.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evA); of.JobID != jobID {
		t.Fatalf("fr-a offer for job %s, want %s", of.JobID, jobID)
	}
	must(t, frA.DecideAndRespond(ctx, offerOf(t, evA), len(body), convKey, nil))
	// the rejection event reached the coordinator carrying reason no_slot
	deadline := time.After(5 * time.Second)
	for !strings.Contains(env.logs.String(), "offer_reject") || !strings.Contains(env.logs.String(), "(no_slot)") {
		select {
		case <-deadline:
			t.Fatalf("fr-a rejection reason not recorded: %s", truncate([]byte(env.logs.String()), 2000))
		case <-time.After(100 * time.Millisecond):
		}
	}
	if cr.calls != 1 {
		t.Fatalf("fr-a decide calls = %d, want 1", cr.calls)
	}

	evB, err := frB.NextEvent(ctx, 15*time.Second)
	must(t, err)
	if of := offerOf(t, evB); of.JobID != jobID {
		t.Fatalf("offer for job %s, want %s", of.JobID, jobID)
	}
	ndjson := make([]string, 0, 121)
	for i := 0; i < 120; i++ {
		ndjson = append(ndjson, `{"choices":[{"delta":{"content":"line `+strconv.Itoa(i)+` xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}]}`)
	}
	ndjson = append(ndjson, `{"choices":[{"delta":{"content":""}}],"usage":{"prompt_tokens":33,"completion_tokens":120}}`)
	must(t, frB.AcceptAndServe(ctx, jobID, len(body), convKey, ndjson))
	got := []string{}
	rgDeadline := time.After(20 * time.Second)
	for len(got) < len(ndjson) {
		select {
		case line := <-rgA.Lines():
			got = append(got, line)
		case <-rgDeadline:
			t.Fatalf("relay timeout: got %d lines, want %d", len(got), len(ndjson))
		}
	}
	t.Logf("relayed %d lines OK", len(got))
}
