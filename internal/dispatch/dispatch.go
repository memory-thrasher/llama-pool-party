// Package dispatch implements the coordinator's job dispatch engine (WS-06).
//
// A single dispatcher goroutine owns the in-memory job state machine (mirroring the
// queue_jobs rows) and drives the offer loop: candidate selection (D-D1), two
// phase offers over the subscribe stream (D-D2), lease re-offers (D-D2), the
// POOL_MAX_BOUNCES pressure valve (D-G1), the 30m job expiry, and the
// live response-relay terminal events (D-D6). Durable state stays in postgres;
// the dispatcher only mirrors rows into memory to make decisions.
package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// RouterHub is implemented by the HTTP server. It must be safe for
// concurrent use; the dispatcher calls it from its single goroutine, but
// the subscribe/mirror calls land on the server's own handlers.
type RouterHub interface {
	// Connected reports whether routerID holds an open /pool/subscribe.
	Connected(routerID string) bool
	// SendEvent queues one envelope (the marshalled Event JSON) for the
	// router's subscribe stream; false when the router has no open connection
	// (the candidate is skipped) or its buffer is full.
	SendEvent(routerID string, data []byte) bool
	// MirrorTerminal mirrors a terminal envelope (completion/error) onto the
	// originating router's subscribe stream AND closes its held-open
	// GET /pool/jobs/{id}/response pipe, so the client's stream closes on
	// failure (D-D6 catalog §6b.4).
	MirrorTerminal(routerID, jobID string, data []byte)
}

// Purger deletes the staged body file for a job (D-I2: purge on
// completion/failure/expiry).
type Purger func(ctx context.Context, jobID string) error

// In-memory job statuses (mirror queue_jobs.status).
const (
	stPending  = "pending"
	stOffered  = "offered"
	stRunning = "running"
	stFailed  = "failed"
	stDone    = "done"
)

type jobState struct {
	JobID string
	UserID string
	Tag string
	Priority string
	EstPromptTokens int
	EstCtxNeeded int
	ConvKey string // "" = null

	OriginRouter string

	Status string

	Bounces int
	NextOfferAt time.Time // zero = eligible now

	CreatedAt time.Time
	UserCreatedAt time.Time

	CandidateIdx int
	CurrentCandidate string // router offered to, awaiting decision
	CandidateModel string
	ServingRouterID string

	// passCands caches the candidate list for the current offer pass. It is
	// captured once when the pass starts (CandidateIdx 0) and reused as the
	// index advances on rejections, so the index stays meaningful even though
	// per-router connectivity (and thus the live selectCandidates result)
	// changes mid-pass. Reset to nil by parkJob to start a fresh pass.
	passCands []candidate

	BodyPulled bool
	LeaseExpiresAt time.Time // zero = no lease armed
	AcceptedAt time.Time // zero = offered but not accepted (gates body pull/response push)
	TerminalAt time.Time // zero = not terminal yet (memory GC uses this)

	completion poolproto.Completion // stashed from the last completion event
}

type opKind uint8

const (
	opRegister opKind = iota
	opAccept
	opReject
	opAbort
	opError
	opCompletion
	opBodyPulled
	opFailClientDisconnected
	opAffinity
	opInternalReject
)

type dispatchOp struct {
	kind opKind
	jobID string
	leaseMS int
	reason string
	stage string
	code string
	message string
	pt, rt *int
	tq, ts *int
	routerID string
	modelID string
	convKey string
	entries []poolproto.AffinityEntry
	completion poolproto.Completion
	errOut chan error
}

// Dispatcher is the WS-06 dispatch engine. Construct with New, start with
// Start, stop with Stop. All mutation happens on its single goroutine.
type Dispatcher struct {
	db *pooldb.DB
	hub RouterHub
	maxBounces int
	leaseDefaultMS int
	jobExpiry time.Duration
	parkDelay time.Duration
	purge Purger
	bodyReady func(jobID string) bool // e2e fix: gates offerNext on the staged body

	log func(format string, args ...any)

	jobs map[string]*jobState
	in chan dispatchOp

	wg sync.WaitGroup
	stopChan chan struct{}
	doneChan chan struct{}
}

// Opts tunes the dispatcher; zero values take the config defaults.
type Opts struct {
	MaxBounces int
	LeaseDefaultMS int
	JobExpiry time.Duration // 0 = 30m
	ParkDelay time.Duration // 0 = 5s
	// e2e fix: BodyReady reports whether the origin has staged the job's body
	// file. A job is only offerable once the body is staged (catalog: "the
	// coordinator treats the job as *queued* when the body PUT completes").
	BodyReady func(jobID string) bool
}

func (o Opts) withDefaults() Opts {
	if o.MaxBounces <= 0 {
		o.MaxBounces = config.DefaultMaxBounces
	}
	if o.LeaseDefaultMS <= 0 {
		o.LeaseDefaultMS = config.DefaultOfferLeaseMS
	}
	if o.JobExpiry <= 0 {
		o.JobExpiry = 30 * time.Minute
	}
	if o.ParkDelay <= 0 {
		o.ParkDelay = 5 * time.Second
	}
	return o
}

// New returns a Dispatcher. hub must be safe for concurrent use.
func New(db *pooldb.DB, hub RouterHub, opts Opts, purge Purger) *Dispatcher {
	opts = opts.withDefaults()
	return &Dispatcher{
		db: db, hub: hub,
		maxBounces: opts.MaxBounces,
		leaseDefaultMS: opts.LeaseDefaultMS,
		jobExpiry: opts.JobExpiry,
		parkDelay: opts.ParkDelay,
		purge: purge,
		bodyReady: opts.BodyReady,
		jobs: make(map[string]*jobState),
		in: make(chan dispatchOp, 64),
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
}

// Start runs the dispatcher goroutine until Stop is called.
func (d *Dispatcher) Start(ctx context.Context) {
	d.wg.Add(1)
	go d.run(ctx)
}

// Stop signals the dispatcher to exit and waits for it.
func (d *Dispatcher) Stop(ctx context.Context) error {
	close(d.stopChan)
	select {
	case <-d.doneChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetLogger installs the dispatcher's log sink (called once by the server).
func (d *Dispatcher) SetLogger(fn func(string, ...any)) {
	d.log = fn
}

func (d *Dispatcher) logf(format string, args ...any) {
	if d.log != nil {
		d.log(format, args...)
	}
}

func (d *Dispatcher) run(ctx context.Context) {
	defer close(d.doneChan)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopChan:
			return
		case <-ticker.C:
			d.sweep()
		case op := <-d.in:
			d.applyOp(op)
		}
	}
}

func (d *Dispatcher) enqueue(op dispatchOp) error {
	done := make(chan error, 1)
	op.errOut = done
	select {
	case d.in <- op:
	case <-d.stopChan:
		return fmt.Errorf("dispatch stopped")
	}
	select {
	case err := <-done:
		return err
	case <-d.stopChan:
		return fmt.Errorf("dispatch stopped")
	}
}

// Register mirrors a freshly inserted queue_jobs row into memory. Called by
// the intake handler right after the insert.
func (d *Dispatcher) Register(jobID string) error {
	return d.enqueue(dispatchOp{kind: opRegister, jobID: jobID})
}

// OfferAccepted handles an offer_accept event (D-D2): the current candidate
// commits under a short lease; the coordinator may re-offer when the lease
// expires without a body pull.
func (d *Dispatcher) OfferAccepted(jobID string, leaseMS int) error {
	return d.enqueue(dispatchOp{kind: opAccept, jobID: jobID, leaseMS: leaseMS})
}

// OfferRejected handles an offer_reject event (D-D2): drop this candidate,
// offer the next.
func (d *Dispatcher) OfferRejected(jobID, reason string) error {
	return d.enqueue(dispatchOp{kind: opReject, jobID: jobID, reason: reason})
}

// BodyAborted handles a body_abort event: same as a reject (re-offer).
func (d *Dispatcher) BodyAborted(jobID, reason string) error {
	return d.enqueue(dispatchOp{kind: opAbort, jobID: jobID, reason: reason})
}

// ErrorReport handles an error event (D-G1/D-G2): pre_token re-offers within
// the class; mid_stream fails the job and mirrors it to the originator.
func (d *Dispatcher) ErrorReport(jobID, stage, code, message string, pt, rt, tq, ts *int) error {
	return d.enqueue(dispatchOp{kind: opError, jobID: jobID, stage: stage, code: code, message: message, pt: pt, rt: rt, tq: tq, ts: ts})
}

// JobCompleted handles a completion event (D-I3/D-E2): records row, body
// purge, continuity upsert, mark done, mirror to the originator.
func (d *Dispatcher) JobCompleted(jobID string, comp poolproto.Completion) error {
	return d.enqueue(dispatchOp{kind: opCompletion, jobID: jobID, completion: comp})
}

// BodyPulled records that the serving router pulled the body (D-D2 phase 2):
// the lease is cleared and the job becomes running.
func (d *Dispatcher) BodyPulled(jobID string) error {
	return d.enqueue(dispatchOp{kind: opBodyPulled, jobID: jobID})
}

// AffinityReport handles an affinity_report event (D-E1/D-E2 MVP): per-entry
// continuity hints.
func (d *Dispatcher) AffinityReport(routerID string, entries []poolproto.AffinityEntry) error {
	return d.enqueue(dispatchOp{kind: opAffinity, jobID: "", routerID: routerID, entries: entries})
}

// InternalReject cancels a pending offer from the given router (eviction D-D6):
// the candidate index advances and the job re-offers, exactly like a bounce.
func (d *Dispatcher) InternalReject(jobID, routerID, modelID, reason string) error {
	return d.enqueue(dispatchOp{kind: opInternalReject, jobID: jobID, routerID: routerID, modelID: modelID, reason: reason})
}

// FailClientDisconnected marks the job failed (D-G4): the originator gave
// up on its held-open GET /pool/jobs/{id}/response mid-push.
func (d *Dispatcher) FailClientDisconnected(jobID string) error {
	return d.enqueue(dispatchOp{kind: opFailClientDisconnected, jobID: jobID})
}

// sweep is called every second: lease expiry, job expiry, parked retries, and the
// fair offer pass.
func (d *Dispatcher) sweep() {
	now := time.Now()

	// Lease expiry: the body pull has not started within the lease; the
	// candidate never committed, so move to the next one (D-D2).
	for _, j := range d.jobs {
		if j.Status == stOffered && !j.BodyPulled && !j.LeaseExpiresAt.IsZero() && now.After(j.LeaseExpiresAt) {
			d.logf("dispatch: lease expired for job %s (candidate %s); re-offering", j.JobID, short(j.CurrentCandidate))
			j.CandidateIdx++
			d.offerNext(j)
		}
	}

	// Job expiry (MVP constant): pending/offered jobs older than the cap fail.
	for _, j := range d.jobs {
		if (j.Status == stPending || j.Status == stOffered) && now.Sub(j.CreatedAt) > d.jobExpiry {
			d.failJob(j, poolproto.ErrorOther, fmt.Sprintf("job expired after %s (D-D2 MVP)", d.jobExpiry),
				0, 0, int(now.Sub(j.CreatedAt).Milliseconds()), 0)
		}
	}

	// Parked jobs retry after the park delay.
	for _, j := range d.jobs {
		if j.Status == stPending && !j.NextOfferAt.IsZero() && now.After(j.NextOfferAt) {
			d.logf("dispatch: parked job %s retry (candidate idx %d)", j.JobID, j.CandidateIdx)
			d.offerNext(j)
		}
	}

	// Memory GC: drop terminal job state after 10 min (DB rows remain for
	// the portal; the map otherwise grows for the coordinator's lifetime).
	for id, j := range d.jobs {
		if !j.TerminalAt.IsZero() && now.Sub(j.TerminalAt) > 10*time.Minute {
			delete(d.jobs, id)
		}
	}

	// Fair offer pass: one job at a time, per-user FIFO with users ordered by
	// created_at (WS-11 fairness is explicitly out of scope).
	var pick *jobState
	var pickKey time.Time
	for _, j := range d.jobs {
		if j.Status != stPending {
			continue
		}
		t := j.NextOfferAt
		if !t.IsZero() && now.Before(t) {
			continue
		}
		if pick == nil || t.Before(pickKey) || (t.Equal(pickKey) && beforeInLine(j, pick)) {
			pick, pickKey = j, t
		}
	}
	if pick != nil {
		d.offerNext(pick)
	}
}

// beforeInLine orders two pending jobs per-user FIFO, users by created_at.
func beforeInLine(a, b *jobState) bool {
	if !a.UserCreatedAt.Equal(b.UserCreatedAt) {
		return a.UserCreatedAt.Before(b.UserCreatedAt)
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

// candidate is one dispatch candidate for a job.
type candidate struct {
	routerID, routerName, modelID string
	nCtx, nSlots, busySlots int
}

// selectCandidates returns the ordered, connected, de-duplicated candidates for j:
// loaded models on connected routers of the class with a free slot and enough
// context (D-D1/D-D3/D-D4). Ordered by context headroom (n_ctx desc, then
// router name for determinism). Excluded models (for this user, D-F1) are
// skipped.
func (d *Dispatcher) selectCandidates(j *jobState) []candidate {
	rows, err := d.db.Query(context.Background(), fmt.Sprintf(`
select rm.router_id, r.router_name, rm.model_id, rm.n_ctx, rm.n_slots, rm.busy_slots
from class_members cm
join router_models rm on rm.router_id = cm.router_id and rm.model_id = cm.model_id
join routers r on r.id = cm.router_id and r.deleted_at is null
where cm.class_id = %s
  and rm.status = 'loaded'
  and rm.busy_slots < rm.n_slots
  and rm.n_ctx >= %s
  and not exists (
    select 1 from exclusions e
    where e.owner_id = %s and e.router_id = rm.router_id and e.model_id = rm.model_id and e.excluded
  )
order by rm.n_ctx desc, r.router_name asc`, pooldb.S(j.Tag), pooldb.I64(int64(j.EstCtxNeeded)), pooldb.UUID(j.UserID)))
	if err != nil {
		d.logf("dispatch: candidate query for job %s: %v", j.JobID, err)
		return nil
	}
	var out []candidate
	seen := map[string]bool{}
	for _, row := range rows {
		f := strings.Split(row, ",")
		if len(f) < 6 {
			continue
		}
		c := candidate{
			routerID: f[0], routerName: f[1], modelID: f[2],
			nCtx: atoiCell(f[3]), nSlots: atoiCell(f[4]), busySlots: atoiCell(f[5]),
		}
		if !d.hub.Connected(c.routerID) {
			continue
		}
		if seen[c.routerID] {
			continue
		}
		seen[c.routerID] = true
		out = append(out, c)
	}
	return out
}

// hasMembers reports whether the class has any loaded member on a connected
// router at all (D-C1). A class with no member fails with model_gone.
func (d *Dispatcher) hasMembers(j *jobState) bool {
	rows, err := d.db.Query(context.Background(), fmt.Sprintf(`
select rm.router_id
from class_members cm
join router_models rm on rm.router_id = cm.router_id and rm.model_id = cm.model_id
join routers r on r.id = cm.router_id and r.deleted_at is null
where cm.class_id = %s and rm.status = 'loaded'
order by rm.router_id`, pooldb.S(j.Tag)))
	if err != nil {
		d.logf("dispatch: member query for job %s: %v", j.JobID, err)
		return false
	}
	// A loaded member means "may return": connectivity is enforced in the
	// offer loop (unreachable candidates are skipped), so a loaded-but-
	// disconnected member parks the job for retry instead of failing it
	// with model_gone (review M: reconnects are normal).
	return len(rows) > 0
}

// offerNext offers the job at j.CandidateIdx and persists the offered state.
// Exhaustion parks the job (retry after the park delay); a class with no
// members at all fails it with model_gone (D-C7).
func (d *Dispatcher) offerNext(j *jobState) {
	// e2e fix: hold the offer until the origin's body PUT has landed. Without
	// this gate the 1s sweep can offer (and get the offer accepted) before the
	// body is staged; the PUT then 409s ("job no longer pending") and the
	// acceptor's body pull 404s. The job stays pending and is re-picked on the
	// next sweep tick.
	if d.bodyReady != nil && !d.bodyReady(j.JobID) {
		return
	}
	// e2e fix: cache the candidate list for the pass instead of re-selecting on
	// every rejection. The list is ordered by context headroom then router name;
	// re-selecting per rejection changes its size/order as routers' live
	// connectivity fluctuates (a long-poll router disconnects each time it
	// receives an offer), which left the index pointing past the end of a
	// shorter list so the next member was silently skipped (cross-member
	// never dispatched). Capturing once keeps the index stable for the pass.
	if j.passCands == nil {
		j.passCands = d.selectCandidates(j)
	}
	cands := j.passCands
	for j.CandidateIdx < len(cands) {
		c := cands[j.CandidateIdx]
		j.CurrentCandidate = c.routerID
		j.CandidateModel = c.modelID
		j.ServingRouterID = c.routerID
		j.Status = stOffered
		j.LeaseExpiresAt = time.Time{}
		j.NextOfferAt = time.Time{}
		j.AcceptedAt = time.Time{} // fresh offer: not accepted yet
		env := poolproto.Event{Type: poolproto.TypeWorkOffer, Payload: mustJSON(poolproto.WorkOffer{
			JobID: j.JobID, Tag: j.Tag,
			EstPromptTokens: j.EstPromptTokens, EstCtxNeeded: j.EstCtxNeeded,
			DeadlineMS: nil, Priority: j.Priority,
			Source: poolproto.SourceQueue, ConvKey: convKeyPtr(j.ConvKey),
		})}
		data, err := json.Marshal(env)
		if err != nil {
			d.logf("dispatch: marshal work_offer job %s: %v", j.JobID, err)
			j.CandidateIdx++
			continue
		}
		if !d.hub.SendEvent(c.routerID, data) {
			d.logf("dispatch: candidate %s not reachable; trying next", short(c.routerID))
			j.CandidateIdx++
			continue
		}
		d.persistStatus(j, stOffered)
		d.logf("dispatch: offered job %s to %s (model %s)", j.JobID, short(c.routerID), c.modelID)
		return
	}
	// Exhausted every candidate (rejections or unreachable): park and retry.
	j.CurrentCandidate = ""
	if d.hasMembers(j) {
		d.parkJob(j, fmt.Sprintf("candidate loop exhausted (%d bounces)", j.Bounces))
		return
	}
	d.failJob(j, poolproto.ErrorModelGone,
		"no loaded member for class "+j.Tag+" (D-C7)", 0, 0, int(time.Since(j.CreatedAt).Milliseconds()), 0)
}

// parkJob leaves the job pending; the sweep retries it after the park delay.
func (d *Dispatcher) parkJob(j *jobState, why string) {
	j.Status = stPending
	j.CurrentCandidate = ""
	j.ServingRouterID = ""
	j.AcceptedAt = time.Time{}
	// A fresh offer pass starts from the first candidate; in-pass rejections
	// are already counted in Bounces (review H1: a stale index wedged the job
	// until expiry). Drop the cached list so the next pass re-selects.
	j.CandidateIdx = 0
	j.passCands = nil
	j.LeaseExpiresAt = time.Time{}
	j.NextOfferAt = time.Now().Add(d.parkDelay)
	d.persistStatus(j, stPending)
	d.logf("dispatch: job %s parked: %s", j.JobID, why)
}

func (d *Dispatcher) applyOp(op dispatchOp) {
	switch op.kind {
	case opRegister:
		d.register(op.jobID, op.errOut)
	case opAccept:
		d.accept(op, op.errOut)
	case opReject:
		d.rejectOrAbort(op, op.errOut, false)
	case opAbort:
		d.rejectOrAbort(op, op.errOut, true)
	case opError:
		d.errorReport(op, op.errOut)
	case opCompletion:
		d.complete(op, op.errOut)
	case opBodyPulled:
		d.bodyPulled(op, op.errOut)
	case opFailClientDisconnected:
		d.failClientDisconnected(op, op.errOut)
	case opAffinity:
		d.affinity(op, op.errOut)
	case opInternalReject:
		d.internalReject(op, op.errOut)
	}
}

func (d *Dispatcher) ack(errOut chan error, err error) {
	if errOut != nil {
		errOut <- err
	}
}

func (d *Dispatcher) register(jobID string, errOut chan error) {
	defer d.ack(errOut, nil)
	rows, err := d.db.Query(context.Background(), fmt.Sprintf(`
select user_id, class_id, priority, est_prompt_tokens, est_ctx_needed, conv_key,
       status, originating_router_id, serving_router_id, created_at
from queue_jobs where id = %s`, pooldb.UUID(jobID)))
	if err != nil {
		d.ack(errOut, fmt.Errorf("dispatch register %s: %w", jobID, err))
		return
	}
	if len(rows) == 0 {
		d.ack(errOut, fmt.Errorf("dispatch: job %s not found", jobID))
		return
	}
	r := rows[0]
	f := strings.Split(r, ",")
	if len(f) < 10 {
		d.ack(errOut, fmt.Errorf("dispatch: bad job row %s: %q", jobID, r))
		return
	}
	created, perr := parseTS(f[9])
	if perr != nil {
		d.ack(errOut, fmt.Errorf("dispatch: parse created_at for job %s: %w", jobID, perr))
		return
	}
	d.jobs[jobID] = &jobState{
		JobID: jobID, UserID: f[0], Tag: f[1], Priority: f[2],
		EstPromptTokens: atoiCell(f[3]), EstCtxNeeded: atoiCell(f[4]),
		ConvKey: f[5], Status: f[6],
		OriginRouter: f[7],
		CreatedAt: created, UserCreatedAt: created,
	}
	if err := d.fixUserCreatedAt(d.jobs[jobID]); err != nil {
		d.logf("dispatch: fix user created_at for job %s: %v", jobID, err)
	}
	d.logf("dispatch: registered job %s (tag %s, est_ctx %d)", jobID, d.jobs[jobID].Tag, d.jobs[jobID].EstCtxNeeded)
}

func (d *Dispatcher) fixUserCreatedAt(j *jobState) error {
	uc, err := d.db.Query(context.Background(), fmt.Sprintf(
		"select created_at from users where id = %s", pooldb.UUID(j.UserID)))
	if err != nil {
		return err
	}
	if len(uc) == 0 {
		return fmt.Errorf("user %s missing", j.UserID)
	}
	s, perr := parseTS(uc[0])
	if perr != nil {
		return perr
	}
	j.UserCreatedAt = s
	return nil
}

func (d *Dispatcher) accept(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	if j == nil || j.Status != stOffered || j.BodyPulled || j.CurrentCandidate == "" {
		d.logf("dispatch: stale offer_accept for job %s", op.jobID)
		return
	}
	lease := op.leaseMS
	if lease <= 0 {
		lease = d.leaseDefaultMS
	}
	j.LeaseExpiresAt = time.Now().Add(time.Duration(lease) * time.Millisecond)
	j.AcceptedAt = time.Now() // body pull + response push now allowed (review H2)
	d.persistStatus(j, stOffered)
	d.logf("dispatch: job %s accepted by %s; lease %dms", j.JobID, short(j.CurrentCandidate), lease)
}

func (d *Dispatcher) rejectOrAbort(op dispatchOp, errOut chan error, isAbort bool) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	kind := "offer_reject"
	if isAbort {
		kind = "body_abort"
	}
	if j == nil || j.Status != stOffered || j.CurrentCandidate == "" {
		d.logf("dispatch: stale %s for job %s", kind, op.jobID)
		return
	}
	j.Bounces++
	if j.Bounces > d.maxBounces {
		d.parkJob(j, fmt.Sprintf("%s: bounce limit %d (D-G1)", op.reason, d.maxBounces))
		return
	}
	d.logf("dispatch: job %s %s by %s (%s); %d/%d bounces",
		j.JobID, kind, short(j.CurrentCandidate), op.reason, j.Bounces, d.maxBounces)
	j.CandidateIdx++
	d.offerNext(j)
}

func (d *Dispatcher) errorReport(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	if j == nil {
		return
	}
	switch op.stage {
	case poolproto.StagePreToken:
		// Hop-on-error (D-G1): record, then re-offer within the class.
		if j.Status != stOffered && j.Status != stRunning {
			d.logf("dispatch: stale pre_token error for job %s", op.jobID)
			return
		}
		j.Bounces++
		if j.Bounces > d.maxBounces {
			d.parkJob(j, fmt.Sprintf("pre_token error: bounce limit %d (D-G1)", d.maxBounces))
			return
		}
		j.CandidateIdx++ // skip the router that errored (offered or running)
		if j.Status == stRunning {
			// Body was already pulled: reset the round, the next candidate
			// re-pulls the staged body (review M: pre_token on running jobs
			// was silently skipped).
			j.BodyPulled = false
		}
		d.records(j, false, op.code,
			firstIntPtr(op.pt), firstIntPtr(op.rt),
			firstIntPtr(op.tq), firstIntPtr(op.ts))
		d.logf("dispatch: job %s pre-token %s; re-offering (%d/%d bounces)", j.JobID, op.code, j.Bounces, d.maxBounces)
		d.offerNext(j)
	case poolproto.StageMidStream:
		d.failJob(j, op.code, op.message,
			firstIntPtr(op.pt), firstIntPtr(op.rt),
			firstIntPtr(op.tq), firstIntPtr(op.ts))
	}
}

func (d *Dispatcher) complete(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	if j == nil || (j.Status != stOffered && j.Status != stRunning) {
		d.logf("dispatch: stale completion for job %s", op.jobID)
		return
	}
	comp := op.completion
	if comp.ServedBy != "" && j.ServingRouterID != "" && comp.ServedBy != j.ServingRouterID {
		d.logf("dispatch: completion for job %s from non-serving router %s", op.jobID, short(comp.ServedBy))
		return
	}
	pt := comp.PromptTokens
	rt := comp.ResponseTokens
	tq := comp.TQueuedMS
	ts := comp.TServiceMS
	j.Status = stDone
	j.BodyPulled = true
	j.CurrentCandidate = ""
	j.NextOfferAt = time.Time{}
	j.TerminalAt = time.Now()
	d.persistStatus(j, stDone)

	d.recordsDone(j, pt, rt, comp.CachedTokens, tq, ts, comp.StopReason)
	d.purgeBody(j)
	d.upsertContinuity(j, comp)
	if j.OriginRouter != "" {
		env := poolproto.Event{Type: poolproto.TypeCompletion, Payload: mustJSON(comp)}
		data, _ := json.Marshal(env)
		d.hub.MirrorTerminal(j.OriginRouter, j.JobID, data)
	}
	d.logf("dispatch: job %s completed by %s (ptokens=%d rtokens=%d, %.1fs)",
		j.JobID, short(j.ServingRouterID), pt, rt, float64(ts)/1000.0)
}

// recordsDone inserts the D-I3 telemetry row for a successful job.
func (d *Dispatcher) recordsDone(j *jobState, pt, rt int, cached *int, tq, ts int, stopReason *string) {
	stopReasonSQL := pooldb.S("")
	if stopReason != nil {
		stopReasonSQL = pooldb.S(*stopReason)
	}
	stmt := fmt.Sprintf(`
insert into records (job_id, user_id, class_id, model_id, router_id, priority,
                   prompt_tokens, response_tokens, cached_tokens,
                   t_queued_ms, t_service_ms, ok, error_code, stop_reason, created_at)
values (%s, %s, %s, %s, %s, %s, %d, %d, %s, %d, %d, %t, NULL, %s, now())`,
		pooldb.UUID(j.JobID), pooldb.UUID(j.UserID), pooldb.S(j.Tag), pooldb.S(j.CandidateModel),
		pooldb.UUID(j.ServingRouterID), pooldb.S(j.Priority), pt, rt,
		nullInt(cached), tq, ts, true, stopReasonSQL)
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: records insert job %s: %v", j.JobID, err)
	}
}

// records inserts the D-I3 telemetry row for a failed job.
func (d *Dispatcher) records(j *jobState, ok bool, errorCode string, pt, rt, tq, ts int) {
	stmt := fmt.Sprintf(`
insert into records (job_id, user_id, class_id, model_id, router_id, priority,
                   prompt_tokens, response_tokens, cached_tokens,
                   t_queued_ms, t_service_ms, ok, error_code, stop_reason, created_at)
values (%s, %s, %s, %s, %s, %s, %d, %d, NULL, %d, %d, %t, %s, NULL, now())`,
		pooldb.UUID(j.JobID), pooldb.UUID(j.UserID), pooldb.S(j.Tag), pooldb.S(j.CandidateModel),
		pooldb.UUID(j.ServingRouterID), pooldb.S(j.Priority), pt, rt, tq, ts, ok, pooldb.S(errorCode))
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: records insert job %s: %v", j.JobID, err)
	}
}

// upsertContinuity records the D-I3/D-E2 continuity hint on completion.
func (d *Dispatcher) upsertContinuity(j *jobState, comp poolproto.Completion) {
	convKey := ""
	if comp.ConvKey != nil {
		convKey = *comp.ConvKey
	}
	if convKey == "" {
		return
	}
	idle := nullInt(comp.IdleMSHint)
	stmt := fmt.Sprintf(`
insert into continuity (conv_key, last_router_id, last_model_id, last_served_at, last_ok, idle_hint_ms)
values (%s, %s, %s, now(), true, %s)
on conflict (conv_key) do update
  set last_router_id = excluded.last_router_id,
      last_model_id = excluded.last_model_id,
      last_served_at = excluded.last_served_at,
      last_ok = true,
      idle_hint_ms = excluded.idle_hint_ms`,
		pooldb.S(convKey), pooldb.UUID(j.ServingRouterID), pooldb.S(j.CandidateModel), idle)
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: continuity upsert %s: %v", convKey, err)
	}
}

// failJob marks the job failed, writes the records row, purges the body (D-I2),
// and mirrors a mid_stream error envelope to the originating router (D-D6).
func (d *Dispatcher) failJob(j *jobState, code, message string, pt, rt, tq, ts int) {
	if j.Status == stFailed || j.Status == stDone {
		return
	}
	j.Status = stFailed
	j.CurrentCandidate = ""
	j.ServingRouterID = ""
	j.AcceptedAt = time.Time{}
	j.TerminalAt = time.Now()
	j.NextOfferAt = time.Time{}
	d.persistStatus(j, stFailed)
	d.records(j, false, code, pt, rt, tq, ts)
	d.purgeBody(j)
	if j.OriginRouter != "" {
		env := poolproto.Event{Type: poolproto.TypeError, Payload: mustJSON(poolproto.ErrorReport{
			JobID: j.JobID, OK: false, Stage: poolproto.StageMidStream, Code: code,
			Message: message,
			PromptTokens: ptrInt(pt), ResponseTokens: ptrInt(rt),
			ConvKey: convKeyPtr(j.ConvKey), TQueuedMS: ptrInt(tq), TServiceMS: ptrInt(ts),
		})}
		data, _ := json.Marshal(env)
		d.hub.MirrorTerminal(j.OriginRouter, j.JobID, data)
	}
	// Router-supplied text: bound it before it reaches the log (a rogue member
	// must not inject arbitrary log lines; the catalog caps messages at 200).
	d.logf("dispatch: job %s failed %s (%s)", j.JobID, code, capMsg(message))
}

func capMsg(s string) string {
	r := []rune(s)
	if len(r) > 200 {
		return string(r[:200]) + "..."
	}
	return s
}

func (d *Dispatcher) purgeBody(j *jobState) {
	if d.purge != nil {
		if err := d.purge(context.Background(), j.JobID); err != nil {
			d.logf("dispatch: purge body job %s: %v", j.JobID, err)
		}
	}
}

func (d *Dispatcher) bodyPulled(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	if j == nil || j.BodyPulled {
		return
	}
	if j.Status == stOffered {
		j.BodyPulled = true
		j.Status = stRunning
		j.LeaseExpiresAt = time.Time{}
		d.persistStatus(j, stRunning)
		d.logf("dispatch: job %s body pulled by %s; running", j.JobID, short(j.ServingRouterID))
	} else if j.Status == stFailed || j.Status == stDone {
		d.logf("dispatch: stale body_pulled for job %s", op.jobID)
	}
}

func (d *Dispatcher) failClientDisconnected(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	if j == nil || j.Status == stFailed || j.Status == stDone {
		return
	}
	d.failJob(j, poolproto.ErrorClientDisconnected, "originator disconnected mid-response (D-G4)",
		0, 0, int(time.Since(j.CreatedAt).Milliseconds()), 0)
}

func (d *Dispatcher) affinity(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	for _, e := range op.entries {
		stmt := fmt.Sprintf(`
insert into continuity (conv_key, last_router_id, idle_hint_ms)
values (%s, %s, %d)
on conflict (conv_key) do update
  set last_router_id = excluded.last_router_id,
      idle_hint_ms = excluded.idle_hint_ms`,
			pooldb.S(e.ConvKey), pooldb.UUID(op.routerID), e.IdleMS)
		if err := d.db.Exec(context.Background(), stmt); err != nil {
			d.logf("dispatch: affinity upsert conv %s: %v", e.ConvKey, err)
		}
	}
}

func (d *Dispatcher) internalReject(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	j := d.jobs[op.jobID]
	if j == nil || (j.Status != stOffered && j.Status != stRunning) {
		return
	}
	if j.CurrentCandidate != op.routerID && j.ServingRouterID != op.routerID {
		return
	}
	if j.CandidateModel != "" && j.CandidateModel != op.modelID {
		return
	}
	j.Bounces++
	if j.Bounces > d.maxBounces {
		d.parkJob(j, fmt.Sprintf("eviction: bounce limit %d (D-G1)", d.maxBounces))
		return
	}
	if j.Status == stOffered {
		j.CandidateIdx++
	}
	d.logf("dispatch: job %s internal reject (evicted model %s on %s); %d bounces",
		j.JobID, op.modelID, short(op.routerID), j.Bounces)
	if j.Status == stOffered {
		d.offerNext(j)
	}
}

// persistStatus writes the in-memory status back to queue_jobs.
// serving_router_id is kept while set (offered and running both carry it;
// park/fail clear j.ServingRouterID first so terminal/pending rows go NULL).
func (d *Dispatcher) persistStatus(j *jobState, status string) {
	serving := "NULL"
	if j.ServingRouterID != "" {
		serving = pooldb.UUID(j.ServingRouterID)
	}
	accepted := "NULL"
	if !j.AcceptedAt.IsZero() {
		accepted = "now()"
	}
	stmt := fmt.Sprintf(`
update queue_jobs set status = %s, serving_router_id = %s, accepted_at = %s, updated_at = now()
where id = %s`, pooldb.S(status), serving, accepted, pooldb.UUID(j.JobID))
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: persistStatus %s job %s: %v", status, j.JobID, err)
	}
}

func convKeyPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullInt(n *int) string {
	if n == nil {
		return "NULL"
	}
	return pooldb.I64(int64(*n))
}

func ptrInt(n int) *int { return &n }

func firstIntPtr(p *int) int {
	if p != nil {
		return *p
	}
	return 0
}

func atoiCell(cell string) int {
	var n int
	fmt.Sscanf(cell, "%d", &n)
	return n
}

func parseTS(cell string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999Z07",
		time.RFC3339Nano,
	}
	var lastErr error
	for _, l := range layouts {
		if t, err := time.Parse(l, cell); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	return time.Time{}, fmt.Errorf("parse timestamptz %q: %w", cell, lastErr)
}

func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("dispatch: marshal %T: %v", v, err))
	}
	return b
}
