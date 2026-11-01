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
	frA := NewFakeRouter(env.baseURL, env.token, "fr-a", "fake-a")
	frA.LoadModel("md-a", []string{"pool-text-md"}, 32768, 1, 1, poolproto.ModelLoaded)
	frA.SetPolicy(RejectPolicy{Reason: "no_slot"})
	must(t, frA.Connect(context.Background()))
	frB := NewFakeRouter(env.baseURL, env.token, "fr-b", "fake-b")
	frB.LoadModel("md-b", []string{"pool-text-md"}, 131072, 2, 0, poolproto.ModelLoaded)
	frB.SetPolicy(AcceptPolicy{})
	must(t, frB.Connect(context.Background()))
	ctx := context.Background()
	convKey := hexToken(32)
	body := []byte(`BIG-BODY-` + strings.Repeat("x", 4096))
	jobID, err := frA.Intake(ctx, pooldb.NewUUID(), "pool-text-md", 128, 4096, &convKey, body)
	must(t, err)
	rgA, err := frA.OpenResponseGet(ctx, jobID)
	must(t, err)
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
