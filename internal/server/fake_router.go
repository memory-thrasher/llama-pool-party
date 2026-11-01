package server

// FakeRouter is a test double for a pool router: it speaks the
// coordinator's HTTP plane (POST /pool/events, GET /pool/subscribe,
// POST/PUT/GET /pool/jobs) and lets tests drive the full WS-06 loop
// end to end: connect with models, subscribe (SSE), intake a job, pull
// the body, stream a response, complete, and read the client-held
// response GET (D-D6).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"pool-coordinator/protocol/go/poolproto"
)

// OfferDecision is a router's verdict on a work_offer.
type OfferDecision struct {
	Accept bool
	Reason string
}

// OfferPolicy decides whether to accept a work_offer.
type OfferPolicy interface {
	Decide(offer poolproto.WorkOffer) OfferDecision
}

// AcceptPolicy accepts every offer.
type AcceptPolicy struct{}

func (AcceptPolicy) Decide(offer poolproto.WorkOffer) OfferDecision { return OfferDecision{Accept: true} }

// RejectPolicy rejects every offer with a fixed reason.
type RejectPolicy struct{ Reason string }

func (p RejectPolicy) Decide(offer poolproto.WorkOffer) OfferDecision {
	return OfferDecision{Accept: false, Reason: p.Reason}
}

// CountingPolicy wraps a policy and counts the calls.
type CountingPolicy struct {
	Base  OfferPolicy
	Calls  int
	mu     sync.Mutex
}

func (p *CountingPolicy) Decide(offer poolproto.WorkOffer) OfferDecision {
	p.mu.Lock()
	p.Calls++
	p.mu.Unlock()
	return p.Base.Decide(offer)
}

// FakeRouter speaks the coordinator HTTP plane as a router client.
type FakeRouter struct {
	baseURL string
	token   string
	id      string
	name    string

	mu     sync.Mutex
	models []poolproto.ModelEntry
	policy OfferPolicy
	client *http.Client

	// events is the router's event stream (from the subscribe SSE).
	events chan poolproto.Event

	// the subscribe link, so Disconnect can drop it (a silent disconnect)
	subCancel context.CancelFunc
	subBody   io.ReadCloser

	closed bool
}

// NewFakeRouter creates a fake router speaking baseURL with the given token.
// router_id must be a UUID (the coordinator validates it); when id is not one,
// a random UUID is generated and id is used as the router name instead.
func NewFakeRouter(baseURL, token, id, name string) *FakeRouter {
	rid, rname := id, name
	if !uuidRE.MatchString(id) {
		rid, rname = randUUID(), id
	}
	return &FakeRouter{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		id:      rid,
		name:    rname,
		policy:  AcceptPolicy{},
		client:  &http.Client{},
		events:  make(chan poolproto.Event, 64),
	}
}

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func randUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (fr *FakeRouter) ID() string       { return fr.id }
func (fr *FakeRouter) Name() string     { return fr.name }

// SetPolicy installs the router's offer policy.
func (fr *FakeRouter) SetPolicy(p OfferPolicy) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.policy = p
}

// Policy returns the router's current offer policy.
func (fr *FakeRouter) Policy() OfferPolicy {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return fr.policy
}

// LoadModel registers a model as loaded on this router.
func (fr *FakeRouter) LoadModel(modelID string, tags []string, nCtx, nSlots, busySlots int, status string) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.models = append(fr.models, poolproto.ModelEntry{
		ModelID:    modelID,
		Tags:      tags,
		NCtx:      nCtx,
		NSlots:    nSlots,
		BusySlots: busySlots,
		Status:    status,
	})
}

// Connect performs the handshake (POST /pool/events) and opens the
// subscribe stream (GET /pool/subscribe, SSE). The SSE reader feeds
// fr.events.
func (fr *FakeRouter) Connect(ctx context.Context) error {
	fr.mu.Lock()
	models := fr.models
	fr.mu.Unlock()

	if err := fr.postEvent(ctx, poolproto.TypeHandshake, &poolproto.Handshake{
		Proto:      1,
		RouterID:   fr.id,
		RouterName:  fr.name,
		Software: "fake-router/1.0",
		Models:    models,
	}); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}

	subCtx, subCancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(subCtx, http.MethodGet, fr.baseURL+"/pool/subscribe", nil)
	if err != nil {
		subCancel()
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+fr.token)
	req.Header.Set("X-Pool-Router-Id", fr.id)
	resp, err := fr.client.Do(req)
	if err != nil {
		subCancel()
		return err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		subCancel()
		return fmt.Errorf("subscribe: status %d", resp.StatusCode)
	}

	fr.mu.Lock()
	fr.closed = false
	fr.subCancel = subCancel
	fr.subBody = resp.Body
	fr.mu.Unlock()
	go fr.readEvents(resp.Body, subCtx)
	return nil
}

// Disconnect drops the subscribe link: the router goes away silently (no
// eviction event), so the coordinator stops counting it as connected while
// its router_models rows stay. Test hook for the parked-member scenarios.
func (fr *FakeRouter) Disconnect() {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if fr.subCancel != nil {
		fr.subCancel()
		fr.subCancel = nil
	}
	if fr.subBody != nil {
		fr.subBody.Close()
		fr.subBody = nil
	}
}

// postEvent posts a typed event envelope to POST /pool/events.
func (fr *FakeRouter) postEvent(ctx context.Context, typ string, payload any) error {
	body, err := json.Marshal(poolproto.Event{Type: typ, Payload: jsonBytes(payload)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fr.baseURL+"/pool/events",
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+fr.token)
	resp, err := fr.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: status %d: %s", typ, resp.StatusCode, truncate(b, 200))
	}
	return nil
}

// readEvents parses the SSE stream and feeds fr.events.
func (fr *FakeRouter) readEvents(r io.Reader, ctx context.Context) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			var ev poolproto.Event
			if err := json.Unmarshal([]byte(line[len("data: "):]), &ev); err == nil {
				select {
				case fr.events <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// NextEvent blocks until the next event arrives or the timeout expires.
func (fr *FakeRouter) NextEvent(ctx context.Context, timeout time.Duration) (poolproto.Event, error) {
	select {
	case ev := <-fr.events:
		return ev, nil
	case <-time.After(timeout):
		return poolproto.Event{}, fmt.Errorf("next event timeout")
	case <-ctx.Done():
		return poolproto.Event{}, ctx.Err()
	}
}

// Intake posts a job intake (POST /pool/jobs) with the router id
// header, then stages the body (PUT /pool/jobs/{id}/body). Returns
// the coordinator-assigned job id.
func (fr *FakeRouter) Intake(ctx context.Context, reqID, tag string, estPrompt, estCtx int, convKey *string, body []byte) (string, error) {
	jobID, err := fr.IntakeNoBody(ctx, reqID, tag, estPrompt, estCtx, convKey)
	if err != nil {
		return "", err
	}
	if err := fr.StageBody(ctx, jobID, body); err != nil {
		return "", err
	}
	return jobID, nil
}

// IntakeNoBody posts only the metadata intake (POST /pool/jobs); the body is
// never staged, so the job is not offerable (dispatch body gate) until
// StageBody is called. Test hook for queue-ordering tests.
func (fr *FakeRouter) IntakeNoBody(ctx context.Context, reqID, tag string, estPrompt, estCtx int, convKey *string) (string, error) {
	ji := poolproto.JobIntake{
		RequestID:      reqID,
		Tag:            tag,
		EstPromptTokens: estPrompt,
		EstCtxNeeded:   estCtx,
		Priority:       poolproto.PriorityLive,
	}
	if convKey != nil {
		ji.ConvKey = convKey
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fr.baseURL+"/pool/jobs",
		bytes.NewReader(jsonBytes(ji)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+fr.token)
	req.Header.Set("X-Pool-Router-Id", fr.id)
	resp, err := fr.client.Do(req)
	if err != nil {
		return "", err
	}
	var bodyResp struct {
		JobID string `json:"job_id"`
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	_ = json.Unmarshal(raw, &bodyResp)
	if bodyResp.JobID == "" {
		return "", fmt.Errorf("intake: empty job id: status %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	return bodyResp.JobID, nil
}

// StageBody PUTs the job body (D-D2: the coordinator treats the job as
// queued when the body PUT completes).
func (fr *FakeRouter) StageBody(ctx context.Context, jobID string, body []byte) error {
	breq, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s/pool/jobs/%s/body", fr.baseURL, jobID), bytes.NewReader(body))
	if err != nil {
		return err
	}
	breq.Header.Set("Content-Type", "application/octet-stream")
	breq.Header.Set("Authorization", "Bearer "+fr.token)
	breq.Header.Set("X-Pool-Router-Id", fr.id)
	bresp, err := fr.client.Do(breq)
	if err != nil {
		return err
	}
	defer bresp.Body.Close()
	if bresp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(bresp.Body)
		return fmt.Errorf("stage body: status %d: %s", bresp.StatusCode, truncate(b, 200))
	}
	return nil
}

// SetBusy updates the local busy_slots of the first loaded model, so the
// fake's local view matches the coordinator's after a PostCapacity.
func (fr *FakeRouter) SetBusy(busySlots int) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	for i, m := range fr.models {
		if m.Status == poolproto.ModelLoaded {
			fr.models[i].BusySlots = busySlots
			break
		}
	}
}

// PostError posts an error report (catalog 3.8): stage pre_token hops to the
// next candidate (D-G1); mid_stream fails the job.
func (fr *FakeRouter) PostError(ctx context.Context, jobID, stage, code, message string) error {
	return fr.postEvent(ctx, poolproto.TypeError, &poolproto.ErrorReport{
		JobID: jobID, OK: false, Stage: stage, Code: code, Message: message,
	})
}

// PostCapacity reports a capacity change (catalog 3.2): the coordinator
// replaces the router's model list (e.g. a slot freed, a model unloaded).
func (fr *FakeRouter) PostCapacity(ctx context.Context, models []poolproto.ModelEntry) error {
	return fr.postEvent(ctx, poolproto.TypeCapacity, &poolproto.Capacity{
		RouterID: fr.id, Models: models, UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

// OpenResponseGet opens the client-held GET /pool/jobs/{id}/response and
// returns a ResponseGet that streams the relayed NDJSON lines. The request
// runs on a derived context so Close() can disconnect it.
func (fr *FakeRouter) OpenResponseGet(ctx context.Context, jobID string) (*ResponseGet, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/pool/jobs/%s/response", fr.baseURL, jobID), nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+fr.token)
	req.Header.Set("X-Pool-Router-Id", fr.id)
	resp, err := fr.client.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("open response: status %d", resp.StatusCode)
	}

	g := &ResponseGet{
		lines:  make(chan string, 16),
		done:   make(chan error, 1),
		ctx:    ctx,
		cancel: cancel,
		body:   resp.Body,
	}
	go g.readLoop(resp.Body)
	return g, nil
}

// DecideAndRespond runs the installed offer policy against a work_offer and
// executes the decision (accept+serve, or reject). This wires the scripted
// policy to the event stream so counting policies observe real router traffic
// (the reviewer's "nominal policy" point).
func (fr *FakeRouter) DecideAndRespond(ctx context.Context, offer poolproto.WorkOffer, bodyBytes int, convKey string, ndjson []string) error {
	fr.mu.Lock()
	policy := fr.policy
	fr.mu.Unlock()
	dec := policy.Decide(offer)
	if dec.Accept {
		return fr.AcceptAndServe(ctx, offer.JobID, bodyBytes, convKey, ndjson)
	}
	return fr.postEvent(ctx, poolproto.TypeOfferReject,
		&poolproto.OfferReject{JobID: offer.JobID, Reason: dec.Reason})
}

// AcceptAndServe runs the serving-router flow for a job: accept
// the offer, pull the body, stream the NDJSON response, post
// completion.
func (fr *FakeRouter) AcceptAndServe(ctx context.Context, jobID string, bodyBytes int, convKey string, ndjson []string) error {
	modelID, nCtx, err := fr.currentModel(jobID)
	if err != nil {
		return err
	}
	if err := fr.postEvent(ctx, poolproto.TypeOfferAccept, &poolproto.OfferAccept{
		JobID:    jobID,
		LeaseMS:   5000,
		ModelID:   modelID,
		NCtx:     nCtx,
	}); err != nil {
		return fmt.Errorf("offer_accept: %w", err)
	}

	// Pull the body (D-D2 phase 2).
	br, err := fr.doGet(ctx, fmt.Sprintf("/pool/jobs/%s/body", jobID))
	if err != nil {
		return err
	}
	n, err := io.Copy(io.Discard, br.Body)
	br.Body.Close()
	if err != nil {
		return fmt.Errorf("pull body: %v", err)
	}
	if n != int64(bodyBytes) {
		return fmt.Errorf("pull body: got %d bytes, want %d", n, bodyBytes)
	}

	// Stream the NDJSON response (PUT /pool/jobs/{id}/response).
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s/pool/jobs/%s/response", fr.baseURL, jobID),
		bytes.NewReader([]byte(strings.Join(ndjson, "\n")+"\n")))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "Bearer "+fr.token)
	req.Header.Set("X-Pool-Router-Id", fr.id)
	resp, err := fr.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put response: status %d: %s", resp.StatusCode, truncate(b, 200))
	}

	// Report completion (D-D6).
	return fr.postEvent(ctx, poolproto.TypeCompletion, &poolproto.Completion{
		JobID:        jobID,
		OK:          true,
		ConvKey:      &convKey,
		ModelID:       modelID,
		PromptTokens: 128,
		ResponseTokens: len(ndjson),
		CachedTokens:  intPtr(0),
		IdleMSHint:   intPtr(0),
		TQueuedMS:     1,
		TServiceMS:    2,
		StopReason:   strPtr("finish_reason"),
		ServedBy:    fr.id,
	})
}

// AcceptAndDieMidPush runs the serving flow through the body pull (the job
// is running now), pushes only a few lines, then drops the connection hard
// (RST) mid-push: the serving router dies with the job still running (the
// M1 scenario the running cap must cover).
func (fr *FakeRouter) AcceptAndDieMidPush(ctx context.Context, jobID string, bodyBytes int, convKey string, lines []string) error {
	modelID, nCtx, err := fr.currentModel(jobID)
	if err != nil {
		return err
	}
	if err := fr.postEvent(ctx, poolproto.TypeOfferAccept, &poolproto.OfferAccept{
		JobID:   jobID,
		LeaseMS: 5000,
		ModelID: modelID,
		NCtx:    nCtx,
	}); err != nil {
		return fmt.Errorf("offer_accept: %w", err)
	}
	fr.pullBody(ctx, jobID)

	// raw socket: advertise the full body, deliver only the first line, wait
	// for the coordinator to relay it and block on the next read, then RST on
	// close: the push read errors mid-body (a real mid-push drop) and the
	// relay pipe stays open until the running cap fails the job.
	host, portStr, err := net.SplitHostPort(strings.TrimPrefix(fr.baseURL, "http://"))
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, portStr), 5*time.Second)
	if err != nil {
		return err
	}
	first := lines[0] + "\n"
	rest := ""
	if len(lines) > 1 {
		rest = strings.Join(lines[1:], "\n") + "\n"
	}
	full := first + rest
	fmt.Fprintf(conn, "PUT /pool/jobs/%s/response HTTP/1.1\r\n", jobID)
	fmt.Fprintf(conn, "Host: %s:%d\r\n", host, port)
	if _, err := conn.Write([]byte("Authorization: Bearer " + fr.token + "\r\n")); err != nil {
		conn.Close()
		return err
	}
	if _, err := conn.Write([]byte("X-Pool-Router-Id: " + fr.id + "\r\n")); err != nil {
		conn.Close()
		return err
	}
	if _, err := conn.Write([]byte("Content-Type: application/x-ndjson\r\n")); err != nil {
		conn.Close()
		return err
	}
	fmt.Fprintf(conn, "Content-Length: %d\r\n\r\n", len(full))
	if _, err := conn.Write([]byte(first)); err != nil {
		conn.Close()
		return err
	}
	time.Sleep(500 * time.Millisecond) // the coordinator relays the line and blocks on the next read
	type lingerer interface{ SetLinger(int) error }
	if l, ok := conn.(lingerer); ok {
		_ = l.SetLinger(0) // RST on close, not a clean FIN
	}
	conn.Close()
	return nil
}

// AcceptAndError accepts the offer and then posts the error without serving:
// a pre_token bounce (D-G1) or a scripted failure, driven by the test.
func (fr *FakeRouter) AcceptAndError(ctx context.Context, jobID, stage, code, message string) error {
	modelID, nCtx, err := fr.currentModel(jobID)
	if err != nil {
		return err
	}
	if err := fr.postEvent(ctx, poolproto.TypeOfferAccept, &poolproto.OfferAccept{
		JobID:   jobID,
		LeaseMS: 5000,
		ModelID: modelID,
		NCtx:    nCtx,
	}); err != nil {
		return fmt.Errorf("offer_accept: %w", err)
	}
	return fr.postEvent(ctx, poolproto.TypeError, &poolproto.ErrorReport{
		JobID: jobID, OK: false, Stage: stage, Code: code, Message: message,
	})
}

// pacedReader emits one line per Read with a fixed gap: the PUT body
// trickles so a test can act (close the origin) while the push is in flight.
type pacedReader struct {
	lines []string
	gap   time.Duration
	i     int
}

func (r *pacedReader) Read(p []byte) (int, error) {
	if r.i >= len(r.lines) {
		return 0, io.EOF
	}
	if r.gap > 0 {
		time.Sleep(r.gap)
	}
	line := r.lines[r.i] + "\n"
	r.i++
	return copy(p, []byte(line)), nil
}

// AcceptAndServePaced runs the serving flow with a paced push (gap between
// lines). It posts no completion: the test drives the outcome (the origin
// going away mid-push aborts the PUT). status != 200 or err report the
// abort.
func (fr *FakeRouter) AcceptAndServePaced(ctx context.Context, jobID string, bodyBytes int, convKey string, lines []string, gap time.Duration) (int, error) {
	modelID, nCtx, err := fr.currentModel(jobID)
	if err != nil {
		return 0, err
	}
	if err := fr.postEvent(ctx, poolproto.TypeOfferAccept, &poolproto.OfferAccept{
		JobID:   jobID,
		LeaseMS: 5000,
		ModelID: modelID,
		NCtx:    nCtx,
	}); err != nil {
		return 0, fmt.Errorf("offer_accept: %w", err)
	}
	fr.pullBody(ctx, jobID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s/pool/jobs/%s/response", fr.baseURL, jobID), &pacedReader{lines: lines, gap: gap})
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "Bearer "+fr.token)
	req.Header.Set("X-Pool-Router-Id", fr.id)
	resp, err := fr.client.Do(req)
	if err != nil {
		return 0, err // the push aborted: the coordinator closed the stream
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("push aborted: status %d", resp.StatusCode)
	}
	return http.StatusOK, nil
}

// AcceptAndHold accepts the offer and pulls the body but streams no response
// and posts no completion - the job stays running until the originator
// disconnects (client_disconnected) or the lease expires.
func (fr *FakeRouter) AcceptAndHold(ctx context.Context, jobID string) error {
	modelID, nCtx, err := fr.currentModel(jobID)
	if err != nil {
		return err
	}
	if err := fr.postEvent(ctx, poolproto.TypeOfferAccept, &poolproto.OfferAccept{
		JobID:   jobID,
		LeaseMS: 5000,
		ModelID: modelID,
		NCtx:    nCtx,
	}); err != nil {
		return fmt.Errorf("offer_accept: %w", err)
	}
	fr.pullBody(ctx, jobID)
	return nil
}

// StreamResponse streams the NDJSON response with a fixed gap between
// lines and returns when the server's PUT /response handler
// returns (either the stream completed or the client
// disconnected).
func (fr *FakeRouter) StreamResponse(ctx context.Context, jobID string, ndjson []string, gap time.Duration) error {
	modelID, nCtx, err := fr.currentModel(jobID)
	if err != nil {
		return err
	}
	if err := fr.postEvent(ctx, poolproto.TypeOfferAccept, &poolproto.OfferAccept{
		JobID:    jobID,
		LeaseMS:  5000,
		ModelID:   modelID,
		NCtx:     nCtx,
	}); err != nil {
		return fmt.Errorf("offer_accept: %w", err)
	}
	// Pull the body first (D-D2) so the job is running.
	fr.pullBody(ctx, jobID)

	body := strings.Join(ndjson, "\n") + "\n"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut,
			fmt.Sprintf("%s/pool/jobs/%s/response", fr.baseURL, jobID),
			strings.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("Authorization", "Bearer "+fr.token)
		req.Header.Set("X-Pool-Router-Id", fr.id)
		resp, err := fr.client.Do(req)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		return fmt.Errorf("stream response: status %d", resp.StatusCode)
	}
}

func (fr *FakeRouter) pullBody(ctx context.Context, jobID string) {
	br, err := fr.doGet(ctx, fmt.Sprintf("/pool/jobs/%s/body", jobID))
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, br.Body)
	br.Body.Close()
}

// currentModel returns the first loaded, free model on this router.
func (fr *FakeRouter) currentModel(jobID string) (string, int, error) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	for _, m := range fr.models {
		if m.Status == poolproto.ModelLoaded && m.BusySlots < m.NSlots {
			return m.ModelID, m.NCtx, nil
		}
	}
	return "", 0, fmt.Errorf("no free loaded model for job %s", jobID)
}

// doGet performs an authenticated GET for the client-held router.
func (fr *FakeRouter) doGet(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fr.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+fr.token)
	req.Header.Set("X-Pool-Router-Id", fr.id)
	resp, err := fr.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get %s: status %d: %s", path, resp.StatusCode, truncate(b, 200))
	}
	return resp, nil
}

// ResponseGet is a held-open client GET .../response stream (D-D6).
type ResponseGet struct {
	lines  chan string
	done   chan error
	ctx    context.Context
	cancel context.CancelFunc
	body   io.ReadCloser
	mu     sync.Mutex
	closed bool
}

// Lines receives relayed NDJSON lines.
func (g *ResponseGet) Lines() <-chan string { return g.lines }

// Done receives nil when the stream closed cleanly (server- or
// client-side).
func (g *ResponseGet) Done() <-chan error { return g.done }

// Close gives up on the GET (D-D6 client disconnect): cancels the request
// context and closes the body so the server sees the disconnect and fails
// the job (client_disconnected).
func (g *ResponseGet) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		if g.cancel != nil {
			g.cancel()
		}
		if g.body != nil {
			g.body.Close()
		}
	}
}

// readLoop parses the SSE stream: "data: <ndjson line>" lines are
// relayed; ": hb <ms>" heartbeats and blank lines are skipped. Exits on
// client Close() or when the server closes the stream.
func (g *ResponseGet) readLoop(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		select {
		case <-g.ctx.Done():
			return
		default:
		}
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			select {
			case g.lines <- line[len("data: "):]:
			case <-g.ctx.Done():
				return
			}
		}
		// heartbeat ": hb ..." and blank lines are skipped
	}
	// Server closed the stream (job ended): report clean close.
	select {
	case g.done <- nil:
	default:
	}
}

// Close shuts the fake router down.
func (fr *FakeRouter) Close() {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if !fr.closed {
		fr.closed = true
	}
}

// --- test helpers -------------------------------------------------------

func jsonBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func truncate(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// hexToken returns a random lowercase hex string of the given byte
// length (conv keys are 64-hex in the database).
func hexToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}


func intPtr(v int) *int { return &v }
func strPtr(v string) *string { return &v }
