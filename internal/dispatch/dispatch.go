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
	"sort"
	"strconv"
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
	stPending = "pending"
	stOffered = "offered"
	stRunning = "running"
	stFailed  = "failed"
	stDone    = "done"
)

// codePreToken is the error/records code for the overflow terminal
// (DISP-RESIDUAL-1): the prompt exceeded every candidate's context.
const codePreToken = "pre_token"

// codeCoordinatorRestart is the error/records code for a job that was offered
// or running when the coordinator restarted (D-H2): its lease and relay are
// dead, so the job terminal-fails instead of resuming.
const codeCoordinatorRestart = "coordinator_restart"

type jobState struct {
	JobID           string
	UserID          string
	Tag             string
	Priority        string
	EstPromptTokens int
	EstCtxNeeded    int
	ConvKey         string // "" = null

	OriginRouter string

	Status string

	Bounces     int
	ParkCount   int       // consecutive parks; grows the re-offer backoff (DISP-RESIDUAL-1)
	NextOfferAt time.Time // zero = eligible now

	CreatedAt     time.Time
	UserCreatedAt time.Time

	CandidateIdx     int
	CurrentCandidate string // router offered to, awaiting decision
	CandidateModel   string
	ServingRouterID  string

	// passCands caches the candidate list for the current offer pass. It is
	// captured once when the pass starts (CandidateIdx 0) and reused as the
	// index advances on rejections, so the index stays meaningful even though
	// per-router connectivity (and thus the live selectCandidates result)
	// changes mid-pass. Reset to nil by parkJob to start a fresh pass.
	passCands []candidate

	// passBounces/passOverflow count the rejections of the current offer pass
	// (since the last park): every candidate rejecting with a pre_token
	// overflow makes the exhaustion terminal (DISP-RESIDUAL-1).
	passBounces  int
	passOverflow int

	// Continuity (D-E2/D-E3): Cont is the conversation's hint snapshot captured
	// at register (nil = first turn of the conversation); contPriority/contEvicted
	// feed the decision; contOutcome/contReason is the D-E6 outcome, decided at
	// the first offer and finalized at completion.
	Cont         *contHint
	contPriority bool
	contEvicted  bool
	contOutcome  string // '', 'skip', 'warm', 'cold', 'denied'
	contReason   string // '', 'idle', 'gone', 'error', 'evicted'
	contDecided  bool

	BodyPulled     bool
	LeaseExpiresAt time.Time // zero = no lease armed
	AcceptedAt     time.Time // zero = offered but not accepted (gates body pull/response push)
	RunningAt      time.Time // zero = not running yet (the running cap's timestamp, D-G2)
	TerminalAt     time.Time // zero = not terminal yet (memory GC uses this)

	completion poolproto.Completion // stashed from the last completion event
}

// contHint is the continuity snapshot for a job's conversation (D-E2):
// the member that last served it, when, and whether that service completed
// cleanly.
type contHint struct {
	routerID string
	modelID  string
	servedAt time.Time
	lastOK   bool
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
	opContEvicted
	opRestartFail
	opDropMemory
)

type dispatchOp struct {
	kind       opKind
	jobID      string
	leaseMS    int
	reason     string
	stage      string
	code       string
	message    string
	pt, rt     *int
	tq, ts     *int
	routerID   string
	modelID    string
	convKey    string
	entries    []poolproto.AffinityEntry
	completion poolproto.Completion
	errOut     chan error
}

// Dispatcher is the WS-06 dispatch engine. Construct with New, start with
// Start, stop with Stop. All mutation happens on its single goroutine.
type Dispatcher struct {
	db             *pooldb.DB
	hub            RouterHub
	maxBounces     int
	leaseDefaultMS int
	jobExpiry      time.Duration
	runningCap     time.Duration // POOL_RUNNING_CAP_MS (D-G2 running cap)
	parkBackoff    time.Duration // POOL_PARK_BACKOFF_MS base (DISP-RESIDUAL-1)
	fairWindowMS   int           // POOL_FAIR_WINDOW_MS (D-F2 rolling window)
	fairTopK       int           // POOL_FAIR_TOPK (D-F3 selection window)
	batchWeight    float64       // POOL_BATCH_WEIGHT (D-H1 batch consumption weight)
	batchExpiry    time.Duration // POOL_BATCH_EXPIRY_MS (D-H1 batch job expiry)
	contIdleMS     time.Duration // POOL_CONT_IDLE_MS (D-E2 skip window)
	contPreferMS   time.Duration // POOL_CONT_PREFER_MS (D-E2 soft preference)
	purge          Purger
	bodyReady      func(jobID string) bool // e2e fix: gates offerNext on the staged body

	log func(format string, args ...any)
	housekeep func(ctx context.Context) // per-tick housekeeping (portal sessions)

	jobs map[string]*jobState
	in   chan dispatchOp

	wg       sync.WaitGroup
	stopChan chan struct{}
	doneChan chan struct{}
}

// Opts tunes the dispatcher; zero values take the config defaults.
type Opts struct {
	MaxBounces     int
	LeaseDefaultMS int
	// e2e fix: BodyReady reports whether the origin has staged the job's body
	// file. A job is only offerable once the body is staged (catalog: "the
	// coordinator treats the job as *queued* when the body PUT completes").
	BodyReady func(jobID string) bool
	// ContIdleMS is the skip-the-line window in ms (POOL_CONT_IDLE_MS, D-E2);
	// 0 takes the default (60s).
	ContIdleMS int
	// ContPreferMS is the soft-preference window in ms (POOL_CONT_PREFER_MS,
	// D-E2/D-E3); 0 takes the default (600s).
	ContPreferMS int
	// JobExpiryMS caps pending/offered jobs in ms (POOL_JOB_EXPIRY_MS);
	// 0 takes the default (30m).
	JobExpiryMS int
	// RunningCapMS caps running jobs in ms (POOL_RUNNING_CAP_MS, D-G2);
	// 0 takes the default (5m).
	RunningCapMS int
	// FairWindowMS is the D-F2 rolling window in ms (POOL_FAIR_WINDOW_MS);
	// 0 takes the default (7 days).
	FairWindowMS int
	// FairTopK is the D-F3 selection window (POOL_FAIR_TOPK): the k oldest
	// pending jobs the fairness pick considers; 0 takes the default (10).
	FairTopK int
	// BatchWeight is the D-F2 weight of a batch job's consumed tokens
	// (POOL_BATCH_WEIGHT); 0 takes the default (0.5).
	BatchWeight float64
	// BatchExpiryMS caps pending/offered batch jobs in ms (POOL_BATCH_EXPIRY_MS);
	// 0 takes the default (24h).
	BatchExpiryMS int
	// ParkBackoffMS is the base parked-job re-offer backoff in ms
	// (POOL_PARK_BACKOFF_MS); 0 takes the default (500ms).
	ParkBackoffMS int
	// Housekeep runs once per sweep tick (WS-14: expired portal sessions).
	Housekeep func(ctx context.Context)
}

func (o Opts) withDefaults() Opts {
	if o.MaxBounces <= 0 {
		o.MaxBounces = config.DefaultMaxBounces
	}
	if o.LeaseDefaultMS <= 0 {
		o.LeaseDefaultMS = config.DefaultOfferLeaseMS
	}
	if o.ContIdleMS <= 0 {
		o.ContIdleMS = config.DefaultContIdleMS
	}
	if o.ContPreferMS <= 0 {
		o.ContPreferMS = config.DefaultContPreferMS
	}
	if o.JobExpiryMS <= 0 {
		o.JobExpiryMS = config.DefaultJobExpiryMS
	}
	if o.RunningCapMS <= 0 {
		o.RunningCapMS = config.DefaultRunningCapMS
	}
	if o.FairWindowMS <= 0 {
		o.FairWindowMS = config.DefaultFairWindowMS
	}
	if o.FairTopK <= 0 {
		o.FairTopK = config.DefaultFairTopK
	}
	if o.ParkBackoffMS <= 0 {
		o.ParkBackoffMS = config.DefaultParkBackoffMS
	}
	if o.BatchWeight <= 0 {
		o.BatchWeight = config.DefaultBatchWeight
	}
	if o.BatchExpiryMS <= 0 {
		o.BatchExpiryMS = config.DefaultBatchExpiryMS
	}
	return o
}

// New returns a Dispatcher. hub must be safe for concurrent use.
func New(db *pooldb.DB, hub RouterHub, opts Opts, purge Purger) *Dispatcher {
	opts = opts.withDefaults()
	return &Dispatcher{
		db: db, hub: hub,
		maxBounces:     opts.MaxBounces,
		leaseDefaultMS: opts.LeaseDefaultMS,
		jobExpiry:      time.Duration(opts.JobExpiryMS) * time.Millisecond,
		runningCap:     time.Duration(opts.RunningCapMS) * time.Millisecond,
		parkBackoff:    time.Duration(opts.ParkBackoffMS) * time.Millisecond,
		fairWindowMS:   opts.FairWindowMS,
		fairTopK:       opts.FairTopK,
		batchWeight:    opts.BatchWeight,
		batchExpiry:    time.Duration(opts.BatchExpiryMS) * time.Millisecond,
		contIdleMS:     time.Duration(opts.ContIdleMS) * time.Millisecond, contPreferMS: time.Duration(opts.ContPreferMS) * time.Millisecond,
		purge:     purge, bodyReady: opts.BodyReady, housekeep: opts.Housekeep,
		jobs:      make(map[string]*jobState),
		in:        make(chan dispatchOp, 64),
		stopChan:  make(chan struct{}),
		doneChan:  make(chan struct{}),
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

// Reload rebuilds the in-memory job table from queue_jobs (D-H2). Called once
// at startup, after the dispatcher goroutine is running: pending jobs resume
// the offer loop (their bodies are on disk; the BodyReady gate already handles
// a body not staged yet), and jobs that were offered or running when the
// process died terminal-fail with coordinator_restart (their lease and relay
// are dead). Returns (resumed, failed, error).
func (d *Dispatcher) Reload() (int, int, error) {
	ctx := context.Background()
	pending, err := d.db.Query(ctx, "select id from queue_jobs where status = 'pending' order by created_at")
	if err != nil {
		return 0, 0, fmt.Errorf("dispatch reload: %w", err)
	}
	dead, err := d.db.Query(ctx, "select id from queue_jobs where status in ('offered', 'running') order by created_at")
	if err != nil {
		return 0, 0, fmt.Errorf("dispatch reload: %w", err)
	}
	resumed, failed := 0, 0
	for _, row := range pending {
		if err := d.enqueue(dispatchOp{kind: opRegister, jobID: strings.Split(row, ",")[0]}); err != nil {
			return resumed, failed, fmt.Errorf("dispatch reload register %s: %w", row, err)
		}
		resumed++
	}
	for _, row := range dead {
		if err := d.enqueue(dispatchOp{kind: opRestartFail, jobID: strings.Split(row, ",")[0]}); err != nil {
			return resumed, failed, fmt.Errorf("dispatch reload restart-fail %s: %w", row, err)
		}
		failed++
	}
	d.logf("dispatch: reload: %d pending job(s) resumed, %d in-flight job(s) failed coordinator_restart (D-H2)", resumed, failed)
	return resumed, failed, nil
}

// DropMemory clears the in-memory job table. Test seam for D-H2: it simulates
// the process restart, after which Reload must rebuild the table from
// queue_jobs. Not part of the service lifecycle.
func (d *Dispatcher) DropMemory() error {
	return d.enqueue(dispatchOp{kind: opDropMemory})
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

// HintEvicted marks the pending jobs whose continuity hint names routerID
// (D-E4, D-E6): a capacity/unload event dropped the hinted member, so their
// skip is denied with reason=evicted instead of gone.
func (d *Dispatcher) HintEvicted(routerID string) error {
	return d.enqueue(dispatchOp{kind: opContEvicted, routerID: routerID})
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

	// Job expiry: pending/offered jobs older than the cap fail. Batch jobs get
	// the longer POOL_BATCH_EXPIRY_MS (D-H1); live keeps POOL_JOB_EXPIRY_MS.
	for _, j := range d.jobs {
		if j.Status != stPending && j.Status != stOffered {
			continue
		}
		expiry := d.jobExpiry
		if jobPriority(j) == poolproto.PriorityBatch {
			expiry = d.batchExpiry
		}
		if now.Sub(j.CreatedAt) > expiry {
			d.failJob(j, poolproto.ErrorOther, fmt.Sprintf("job expired after %s (D-D2 MVP)", expiry),
				0, 0, int(now.Sub(j.CreatedAt).Milliseconds()), 0)
		}
	}

	// Running cap (D-G2): a running job older than the cap fails with a clean
	// terminal error. This is the backstop for a serving router that drops
	// mid-push (the push read errors, the job has no completion) or streams
	// forever: failJob mirrors the error to the origin and closes its relay
	// pipe, so the client stream ends structured instead of hanging.
	for _, j := range d.jobs {
		if j.Status == stRunning && !j.RunningAt.IsZero() && now.Sub(j.RunningAt) > d.runningCap {
			d.logf("dispatch: running cap hit for job %s (running since body pull)", j.JobID)
			d.failJob(j, poolproto.ErrorOther, fmt.Sprintf("service timeout after %s (D-G2)", d.runningCap),
				0, 0, int(now.Sub(j.CreatedAt).Milliseconds()), 0)
		}
	}

	// Parked jobs retry after their backoff.
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

	// Fair offer pass: one job at a time. A warm continuation (D-E2 skip-
	// the-line) is picked before the fairness window; the fairness pick
	// (D-F2/D-F3) serves the highest-priority user among the k oldest
	// eligible jobs, at their oldest job (per-user FIFO).
	pick := d.pickFair(now)
	if pick != nil {
		if d.isWarmHint(pick, now) {
			pick.contPriority = true
		}
		d.offerNext(pick)
	}

	// Housekeeping: expired portal session rows (WS-14, D-A5 durable state).
	if d.housekeep != nil {
		d.housekeep(context.Background())
	}
}

// pickFair returns the pending job to offer next (or nil). Batches are always
// lower priority than live requests (D-H1): a pending live job is always
// offered before any batch job. Inside each priority group the pick is the
// same: warm-continuation jobs (isWarm) before the rest (D-E2 skip-the-line),
// and for the rest the D-F3 fairness pick - among the topK oldest eligible
// jobs, the user with the highest D-F2 ratio is served, at their oldest job
// in the window (per-user FIFO - a user's jobs never reorder relative to each
// other). Jobs parked until NextOfferAt are not eligible in either group.
func pickFair(jobs map[string]*jobState, now time.Time, isWarm func(*jobState) bool, topK int, ratios func([]string) map[string]float64) *jobState {
	if p := pickPriority(jobs, now, isWarm, topK, ratios, poolproto.PriorityLive); p != nil {
		return p
	}
	return pickPriority(jobs, now, isWarm, topK, ratios, poolproto.PriorityBatch)
}

// pickPriority is the offer pick restricted to one priority group.
func pickPriority(jobs map[string]*jobState, now time.Time, isWarm func(*jobState) bool, topK int, ratios func([]string) map[string]float64, priority string) *jobState {
	var warmPick *jobState
	var eligible []*jobState
	for _, j := range jobs {
		if j.Status != stPending || jobPriority(j) != priority {
			continue
		}
		t := j.NextOfferAt
		if !t.IsZero() && now.Before(t) {
			continue
		}
		if isWarm(j) {
			if warmPick == nil || t.Before(warmPick.NextOfferAt) ||
				(t.Equal(warmPick.NextOfferAt) && beforeInLine(j, warmPick)) {
				warmPick = j
			}
			continue
		}
		eligible = append(eligible, j)
	}
	if warmPick != nil {
		return warmPick
	}
	if len(eligible) == 0 {
		return nil
	}
	// The fairness window: the k oldest eligible jobs (D-F3).
	sort.Slice(eligible, func(i, j int) bool {
		if !eligible[i].CreatedAt.Equal(eligible[j].CreatedAt) {
			return eligible[i].CreatedAt.Before(eligible[j].CreatedAt)
		}
		return eligible[i].JobID < eligible[j].JobID
	})
	if topK > 0 && len(eligible) > topK {
		eligible = eligible[:topK]
	}
	// Each user's oldest job in the window, in first-seen (oldest) order.
	oldest := make(map[string]*jobState, len(eligible))
	order := make([]string, 0, len(eligible))
	for _, j := range eligible {
		if _, ok := oldest[j.UserID]; !ok {
			oldest[j.UserID] = j
			order = append(order, j.UserID)
		}
	}
	r := ratios(order)
	// Highest ratio wins; tie -> the user with the oldest job in the window.
	best := oldest[order[0]]
	for _, u := range order[1:] {
		if ru, rb := fairRatio(r, u), fairRatio(r, best.UserID); ru > rb ||
			(ru == rb && oldest[u].CreatedAt.Before(best.CreatedAt)) {
			best = oldest[u]
		}
	}
	return best
}

// jobPriority is the offer-priority group of a job: a batch job (the D-H1
// lower group) when queue_jobs.priority = 'batch', live otherwise (a missing
// priority counts as live; the intake requires live|batch).
func jobPriority(j *jobState) string {
	if j.Priority == poolproto.PriorityBatch {
		return poolproto.PriorityBatch
	}
	return poolproto.PriorityLive
}

// fairRatio looks up a user's D-F2 priority; absent = the (0+1)/(0+1) = 1.0
// baseline (new member, or an empty window).
func fairRatio(r map[string]float64, user string) float64 {
	if v, ok := r[user]; ok {
		return v
	}
	return 1.0
}

// pickFair runs the dispatcher's per-tick offer pick.
func (d *Dispatcher) pickFair(now time.Time) *jobState {
	return pickFair(d.jobs, now, func(j *jobState) bool { return d.isWarmHint(j, now) },
		d.fairTopK, d.fairRatios)
}

// fairRatios computes the D-F2 priority ratio (served + 1) / (consumed + 1)
// for the given users, once per offer pass (two aggregate queries). served =
// response tokens the user's routers served (any consumer); consumed =
// response tokens the user consumed (from any router); both ok, both within
// the rolling window. A user with no in-window records gets the 1.0
// baseline (absent from the map).
func (d *Dispatcher) fairRatios(users []string) map[string]float64 {
	ratios := make(map[string]float64, len(users))
	for _, u := range users {
		ratios[u] = 1.0
	}
	if len(users) == 0 {
		return ratios
	}
	ids := make([]string, 0, len(users))
	for _, u := range users {
		if q := pooldb.UUID(u); q != "NULL" {
			ids = append(ids, q)
		}
	}
	if len(ids) == 0 {
		return ratios
	}
	in := strings.Join(ids, ", ")
	cutoff := fmt.Sprintf("now() - interval '%d milliseconds'", d.fairWindowMS)
	served := d.fairSum(cutoff, in, `left join routers r on r.owner_id = u.id
left join records rec on rec.router_id = r.id and rec.ok and rec.created_at >= `, `rec.response_tokens`)
	consumed := d.fairSum(cutoff, in, `left join records rec on rec.user_id = u.id and rec.ok and rec.created_at >= `,
		batchConsumedExpr(d.batchWeight))
	for _, u := range users {
		ratios[u] = (served[u] + 1) / (consumed[u] + 1)
	}
	return ratios
}

// batchConsumedExpr renders the D-F2 consumed-side sum with the batch discount
// (D-H1): a batch job's response tokens count at POOL_BATCH_WEIGHT, a live
// job's at 1 (serving a batch job still counts in full on the served side).
func batchConsumedExpr(weight float64) string {
	return fmt.Sprintf("rec.response_tokens * (case when rec.priority = 'batch' then %s else 1 end)",
		strconv.FormatFloat(weight, 'f', -1, 64))
}

// fairSum runs one D-F2 aggregate (the join clause selects the served or
// consumed side, expr is the summed column) and returns user_id -> sum
// (missing = 0).
func (d *Dispatcher) fairSum(cutoff, in, join, expr string) map[string]float64 {
	out := map[string]float64{}
	query := fmt.Sprintf(`
select u.id::text, coalesce(sum(%s), 0)
from users u
%s %s
where u.id in (%s)
group by u.id`, expr, join, cutoff, in)
	rows, err := d.db.Query(context.Background(), query)
	if err != nil {
		d.logf("dispatch: fairness ratio query: %v", err)
		return out
	}
	for _, row := range rows {
		f := strings.Split(row, ",")
		if len(f) < 2 {
			continue
		}
		out[f[0]] = atofCell(f[1])
	}
	return out
}

// isWarmHint reports whether j's continuation hint grants skip-the-line
// priority (D-E2): the last service of the conversation completed cleanly and
// the member is still plausibly cached (idle < POOL_CONT_IDLE_MS).
func (d *Dispatcher) isWarmHint(j *jobState, now time.Time) bool {
	if j.Cont == nil || !j.Cont.lastOK {
		return false
	}
	return now.Sub(j.Cont.servedAt) < d.contIdleMS
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
	nCtx, nSlots, busySlots       int
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

// contHintActive reports whether j's hint is usable at all (D-E2/D-E3): the
// last service completed cleanly, a member is recorded, and the hint has not
// fully expired (age < POOL_CONT_PREFER_MS).
func (d *Dispatcher) contHintActive(j *jobState) bool {
	return j.Cont != nil && j.Cont.lastOK && j.Cont.routerID != "" &&
		time.Since(j.Cont.servedAt) < d.contPreferMS
}

// frontContHint moves the hinted router to the front of the pass's candidate
// list (D-E2/D-E3): the selectCandidates order (context headroom) still
// applies behind it. A hint whose member is not a valid candidate
// (disconnected, evicted, unloaded, or with no free slot) is left in place
// and the normal list is offered (D-E3: no slot is ever reserved).
func (d *Dispatcher) frontContHint(j *jobState) {
	if !d.contHintActive(j) {
		return
	}
	cands := j.passCands
	for i, c := range cands {
		if c.routerID != j.Cont.routerID {
			continue
		}
		if i > 0 {
			j.passCands = append([]candidate{c}, append(cands[:i], cands[i+1:]...)...)
		}
		return
	}
}

// decideCont records the D-E6 continuity outcome at the first offer:
//
//	'skip'   warm hint: queue priority granted AND the hinted router is offered first
//	'warm'   hint honored without priority (soft pin); finalized to 'cold'
//	         at completion when another member actually serves
//	'cold'   continuation attempted (hint existed) but the hinted router is
//	         not offered first (and skip/warm jobs finalize here too when
//	         another member serves)
//	'denied' guard failed (D-E4): reason 'error' (last service failed),
//	         'evicted', 'gone' (member not a valid candidate), 'idle'
//	         (the skip window had passed)
//
// 'gone' vs 'evicted': a SILENT disconnect (the router's link just goes
// away, no event) records 'gone'; 'evicted' is reserved for the explicit
// capacity/unload event (HintEvicted). The decision at the first offer can
// only tell them apart by that event bit; the terminal decision
// (decideContTerminal) has no such bit and uses 'gone'.
//
// A job with no hint row (first turn) is not a continuation: the outcome
// stays null. One log line per decision, ids/counts only (anti-memory).
func (d *Dispatcher) decideCont(j *jobState) {
	if j.contDecided {
		return
	}
	j.contDecided = true
	if j.Cont == nil {
		return
	}
	if !j.Cont.lastOK {
		d.setContDecision(j, "denied", "error") // D-E4: failed service invalidates the hint
		return
	}
	if !d.contHintActive(j) {
		if j.Cont.routerID == "" {
			d.setContDecision(j, "denied", "gone") // no member recorded for the conversation
		}
		// else: hint fully expired - not a continuation, outcome stays null
		return
	}
	hintFirst := len(j.passCands) > 0 && j.passCands[0].routerID == j.Cont.routerID
	if j.contPriority {
		if hintFirst {
			d.setContDecision(j, "skip", "")
		} else if j.contEvicted {
			d.setContDecision(j, "denied", "evicted")
		} else {
			d.setContDecision(j, "denied", "gone")
		}
		return
	}
	// No queue priority: the hint only fronts the candidate list (D-E3).
	// The 'idle' reason marks that the skip window had passed (D-E6).
	reason := ""
	if time.Since(j.Cont.servedAt) >= d.contIdleMS {
		reason = "idle"
	}
	if hintFirst {
		d.setContDecision(j, "warm", reason)
	} else {
		d.setContDecision(j, "cold", reason)
	}
}

// setContDecision stores the D-E6 outcome and logs the decision (ids only).
func (d *Dispatcher) setContDecision(j *jobState, outcome, reason string) {
	j.contOutcome, j.contReason = outcome, reason
	if reason == "" {
		reason = "-"
	}
	d.logf("dispatch: continuity job %s outcome=%s reason=%s (hint %s)",
		j.JobID, outcome, reason, short(j.Cont.routerID))
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
		d.frontContHint(j) // D-E2/D-E3: hinted router to the front of the list
	}
	// Catalog 3.3: the offer that fronts the hinted router is marked "skip"
	// (skip-the-line pick) or "pin" (soft preference); the rest stay "queue".
	contSource := poolproto.SourceQueue
	if d.contHintActive(j) {
		if j.contPriority {
			contSource = poolproto.SourceSkip
		} else {
			contSource = poolproto.SourcePin
		}
	}
	cands := j.passCands
	for j.CandidateIdx < len(cands) {
		c := cands[j.CandidateIdx]
		source := poolproto.SourceQueue
		if contSource != poolproto.SourceQueue && c.routerID == j.Cont.routerID {
			source = contSource
		}
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
			Source: source, ConvKey: convKeyPtr(j.ConvKey),
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
		// D-E6: record the continuity outcome now that the first offer is
		// actually delivered. A job that stays parked (never a first offer)
		// keeps its decision for the terminal moment (decideContTerminal),
		// which measures the skip window at terminal time.
		if !j.contDecided {
			d.decideCont(j)
		}
		d.logf("dispatch: offered job %s to %s (model %s)", j.JobID, short(c.routerID), c.modelID)
		return
	}
	// Exhausted every candidate (rejections or unreachable).
	j.CurrentCandidate = ""
	if d.hasMembers(j) {
		// DISP-RESIDUAL-1: exhaustion by pre_token overflow (every candidate
		// said the prompt is too big) is terminal - the ctx never grows, so
		// retrying is useless. The message carries the D-C5 overflow substring
		// (pi matches it, compacts, and retries the request smaller).
		if j.overflowExhausted() {
			d.failOverflow(j)
			return
		}
		d.parkJob(j, fmt.Sprintf("candidate loop exhausted (%d bounces)", j.Bounces))
		return
	}
	d.failJob(j, poolproto.ErrorModelGone,
		"no loaded member for class "+j.Tag+" (D-C7)", 0, 0, int(time.Since(j.CreatedAt).Milliseconds()), 0)
}

// overflowExhausted reports whether the current pass was rejected entirely
// by pre_token overflows (DISP-RESIDUAL-1): retrying cannot help, the job is
// terminal.
func (j *jobState) overflowExhausted() bool {
	return j.passBounces > 0 && j.passOverflow == j.passBounces
}

// failOverflow terminal-fails an overflow-exhausted job (DISP-RESIDUAL-1): the
// records row and the mirrored origin envelope carry code pre_token and the
// D-C5 overflow substring; the body is purged.
func (d *Dispatcher) failOverflow(j *jobState) {
	d.failJob(j, codePreToken,
		fmt.Sprintf("request exceeds the available context size on every candidate of class %s (D-C5)", j.Tag),
		0, 0, int(time.Since(j.CreatedAt).Milliseconds()), 0)
}

// parkJob leaves the job pending; the sweep re-offers it after the backoff
// (DISP-RESIDUAL-1: POOL_PARK_BACKOFF_MS doubling per consecutive park, so an
// exhausted candidate loop stops tight-retrying until the job expiry).
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
	j.passBounces = 0
	j.passOverflow = 0
	j.LeaseExpiresAt = time.Time{}
	j.NextOfferAt = time.Now().Add(d.parkDelay(j.ParkCount))
	j.ParkCount++
	d.persistStatus(j, stPending)
	d.logf("dispatch: job %s parked: %s (backoff %s, %d consecutive)",
		j.JobID, why, d.parkDelay(j.ParkCount-1), j.ParkCount)
}

// parkDelay is the backoff before re-offering the n-th consecutive park:
// POOL_PARK_BACKOFF_MS doubling, capped at 10x the base (~5s at the default).
func (d *Dispatcher) parkDelay(n int) time.Duration {
	cap := d.parkBackoff * 10
	delay := d.parkBackoff
	for i := 0; i < n && delay < cap; i++ {
		delay *= 2
	}
	if delay > cap {
		delay = cap
	}
	return delay
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
	case opContEvicted:
		d.contEvicted(op, op.errOut)
	case opRestartFail:
		d.restartFail(op.jobID, op.errOut)
	case opDropMemory:
		d.jobs = make(map[string]*jobState)
		d.ack(op.errOut, nil)
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
		CreatedAt:    created, UserCreatedAt: created,
	}
	if err := d.fixUserCreatedAt(d.jobs[jobID]); err != nil {
		d.logf("dispatch: fix user created_at for job %s: %v", jobID, err)
	}
	d.loadContHint(d.jobs[jobID])
	d.logf("dispatch: registered job %s (tag %s, est_ctx %d)", jobID, d.jobs[jobID].Tag, d.jobs[jobID].EstCtxNeeded)
}

// loadContHint snapshots the job's conversation continuity row (D-E2) at
// register; j.Cont stays nil when the conversation has no row yet (first
// turn, not a continuation).
func (d *Dispatcher) loadContHint(j *jobState) {
	if j.ConvKey == "" {
		return
	}
	rows, err := d.db.Query(context.Background(), fmt.Sprintf(`
select last_router_id, last_model_id, last_served_at, last_ok
from continuity where conv_key = %s`, pooldb.S(j.ConvKey)))
	if err != nil {
		d.logf("dispatch: continuity lookup job %s: %v", j.JobID, err)
		return
	}
	if len(rows) == 0 {
		return
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 4 {
		return
	}
	servedAt, perr := parseTS(f[2])
	if perr != nil {
		d.logf("dispatch: continuity lookup job %s: parse last_served_at: %v", j.JobID, perr)
		return
	}
	j.Cont = &contHint{routerID: f[0], modelID: f[1], servedAt: servedAt, lastOK: f[3] == "t"}
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
	j.ParkCount = 0           // the streak of consecutive parks is broken
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
	j.passBounces++
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
		j.passBounces++
		if op.code == poolproto.ErrorOverflow {
			j.passOverflow++
		}
		if j.Bounces > d.maxBounces {
			// Same as the exhausted loop: an all-overflow pass is terminal even
			// when the pass ends at the bounce limit (more candidates than the
			// limit).
			if j.overflowExhausted() {
				d.failOverflow(j)
				return
			}
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
	// D-E6 finalization: 'skip' (queue priority, hinted router offered first)
	// and 'warm' (soft pin) stand only when the hinted member actually served;
	// the hinted router bounced and another member served = the cache did not
	// pay off, the outcome finalizes 'cold'.
	if (j.contOutcome == "warm" || j.contOutcome == "skip") && j.Cont != nil && j.Cont.routerID != "" &&
		j.ServingRouterID != j.Cont.routerID {
		j.contOutcome = "cold"
	}
	stopReasonSQL := pooldb.S("")
	if stopReason != nil {
		stopReasonSQL = pooldb.S(*stopReason)
	}
	stmt := fmt.Sprintf(`
insert into records (job_id, user_id, class_id, model_id, router_id, priority,
                   prompt_tokens, response_tokens, cached_tokens,
                   t_queued_ms, t_service_ms, ok, error_code, stop_reason,
                   cont_outcome, cont_reason, created_at)
values (%s, %s, %s, %s, %s, %s, %d, %d, %s, %d, %d, %t, NULL, %s, %s, %s, now())`,
		pooldb.UUID(j.JobID), pooldb.UUID(j.UserID), pooldb.S(j.Tag), pooldb.S(j.CandidateModel),
		pooldb.UUID(j.ServingRouterID), pooldb.S(j.Priority), pt, rt,
		nullInt(cached), tq, ts, true, stopReasonSQL,
		pooldb.S(j.contOutcome), pooldb.S(j.contReason))
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: records insert job %s: %v", j.JobID, err)
	}
}

// records inserts the D-I3 telemetry row for a failed job.
func (d *Dispatcher) records(j *jobState, ok bool, errorCode string, pt, rt, tq, ts int) {
	stmt := fmt.Sprintf(`
insert into records (job_id, user_id, class_id, model_id, router_id, priority,
                   prompt_tokens, response_tokens, cached_tokens,
                   t_queued_ms, t_service_ms, ok, error_code, stop_reason,
                   cont_outcome, cont_reason, created_at)
values (%s, %s, %s, %s, %s, %s, %d, %d, NULL, %d, %d, %t, %s, NULL, %s, %s, now())`,
		pooldb.UUID(j.JobID), pooldb.UUID(j.UserID), pooldb.S(j.Tag), pooldb.S(j.CandidateModel),
		pooldb.UUID(j.ServingRouterID), pooldb.S(j.Priority), pt, rt, tq, ts, ok, pooldb.S(errorCode),
		pooldb.S(j.contOutcome), pooldb.S(j.contReason))
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: records insert job %s: %v", j.JobID, err)
	}
}

// upsertContinuityFailed marks the conversation's hint as failed (D-E4):
// the next same-conversation request gets no skip or pin (denied, reason
// =error). A job that never got a serving router keeps the previous hint's
// member on record (coalesce).
func (d *Dispatcher) upsertContinuityFailed(j *jobState) {
	if j.ConvKey == "" {
		return
	}
	stmt := fmt.Sprintf(`
insert into continuity (conv_key, last_router_id, last_model_id, last_served_at, last_ok)
values (%s, %s, %s, now(), false)
on conflict (conv_key) do update
  set last_router_id = coalesce(excluded.last_router_id, continuity.last_router_id),
      last_model_id = coalesce(excluded.last_model_id, continuity.last_model_id),
      last_served_at = excluded.last_served_at,
      last_ok = false`,
		pooldb.S(j.ConvKey), pooldb.UUID(j.ServingRouterID), pooldb.S(j.CandidateModel))
	if err := d.db.Exec(context.Background(), stmt); err != nil {
		d.logf("dispatch: continuity fail-mark %s: %v", j.ConvKey, err)
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

// decideContTerminal records the D-E6 outcome for a continuation that went
// terminal BEFORE its first offer (parked until expiry, failed pre-offer): the
// attempted continuation is counted as 'denied'. Reason precedence (WS-09
// brief, order matters): 'idle' - the hint's skip window had already passed
// by terminal time, so the cache could not have paid off; else 'gone' - the
// hinted router is no longer a valid candidate (silently disconnected, or no
// loaded member of the class); else 'error' - the last service of the
// conversation failed (the hint is invalid, D-E4). A hint that is still warm
// on a valid member from a clean service explains the terminal state
// nothing: the outcome stays null.
func (d *Dispatcher) decideContTerminal(j *jobState) {
	if j.Cont == nil || j.contDecided {
		return
	}
	j.contDecided = true
	if time.Since(j.Cont.servedAt) >= d.contIdleMS {
		d.setContDecision(j, "denied", "idle")
		return
	}
	if !d.hintStillCandidate(j) {
		d.setContDecision(j, "denied", "gone")
		return
	}
	if !j.Cont.lastOK {
		d.setContDecision(j, "denied", "error")
	}
}

// hintStillCandidate reports whether the hinted router could still be offered
// the job: connected, with a loaded member of the class (D-D3).
func (d *Dispatcher) hintStillCandidate(j *jobState) bool {
	if !d.hub.Connected(j.Cont.routerID) {
		return false
	}
	rows, err := d.db.Query(context.Background(), fmt.Sprintf(`
select 1
from class_members cm
join router_models rm on rm.router_id = cm.router_id and rm.model_id = cm.model_id
where cm.router_id = %s and cm.class_id = %s and rm.status = 'loaded'
limit 1`, pooldb.UUID(j.Cont.routerID), pooldb.S(j.Tag)))
	if err != nil {
		d.logf("dispatch: hint candidate check job %s: %v", j.JobID, err)
		return false
	}
	return len(rows) > 0
}

// failJob marks the job failed, writes the records row, purges the body (D-I2),
// and mirrors a mid_stream error envelope to the originating router (D-D6).
func (d *Dispatcher) failJob(j *jobState, code, message string, pt, rt, tq, ts int) {
	if j.Status == stFailed || j.Status == stDone {
		return
	}
	// D-E6: a continuation whose hint never got a first offer is decided here
	// (denied/idle|gone|error) so the attempt counts.
	d.decideContTerminal(j)
	// D-E4: a failed service invalidates the conversation's hint; run this
	// while j.ServingRouterID is still set (cleared below).
	d.upsertContinuityFailed(j)
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
			Message:      message,
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
		j.RunningAt = time.Now() // the running cap runs from the body pull (D-G2)
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
	j.passBounces++
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

// restartFail terminal-fails a job that was offered or running when the
// coordinator restarted (D-H2): its lease and relay are dead. The records row,
// the origin mirror, and the body purge run exactly as for any other terminal
// (code coordinator_restart).
func (d *Dispatcher) restartFail(jobID string, errOut chan error) {
	defer d.ack(errOut, nil)
	rows, err := d.db.Query(context.Background(), fmt.Sprintf(`
select user_id, class_id, priority, status, conv_key, originating_router_id, serving_router_id, created_at
from queue_jobs where id = %s`, pooldb.UUID(jobID)))
	if err != nil {
		d.ack(errOut, fmt.Errorf("dispatch restart-fail %s: %w", jobID, err))
		return
	}
	if len(rows) == 0 {
		d.ack(errOut, nil) // raced: another terminal landed first
		return
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 8 {
		d.ack(errOut, fmt.Errorf("dispatch: bad job row %s: %q", jobID, rows[0]))
		return
	}
	created, perr := parseTS(f[7])
	if perr != nil {
		d.ack(errOut, fmt.Errorf("dispatch restart-fail %s: %w", jobID, perr))
		return
	}
	j := &jobState{JobID: jobID, UserID: f[0], Tag: f[1], Priority: f[2],
		ConvKey: f[4], OriginRouter: f[5], ServingRouterID: f[6],
		Status: f[3], CreatedAt: created, UserCreatedAt: created}
	d.jobs[jobID] = j
	d.failJob(j, codeCoordinatorRestart,
		"coordinator restarted while the job was in flight; its lease and relay are dead (D-H2)",
		0, 0, int(time.Since(created).Milliseconds()), 0)
}

func (d *Dispatcher) contEvicted(op dispatchOp, errOut chan error) {
	defer d.ack(errOut, nil)
	marked := 0
	for _, j := range d.jobs {
		if j.Status != stPending || j.Cont == nil || j.Cont.routerID != op.routerID {
			continue
		}
		j.contEvicted = true
		marked++
	}
	if marked > 0 {
		d.logf("dispatch: eviction marked on %d pending hint job(s) for %s", marked, short(op.routerID))
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

func atofCell(cell string) float64 {
	var n float64
	fmt.Sscanf(cell, "%g", &n)
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
