// Package server implements the coordinator's HTTP plane (WS-06): the
// protocol endpoints (POST /pool/events, GET /pool/subscribe with SSE and
// long-poll modes, job intake, body pull, the D-D6 live response relay
// GET/PUT .../response, and GET /pool/classes) and GET /healthz. It is
// stdlib-only and is backed by the dispatch engine (internal/dispatch) and
// postgres (internal/pooldb).
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/dispatch"
	"pool-coordinator/internal/pooldb"
)

// Server is the HTTP plane. It owns the subscribe sessions, the response-relay
// pipes, and the dispatch engine. Start it with Run.
type Server struct {
	cfg config.Config
	db  *pooldb.DB
	disp *dispatch.Dispatcher
	logs io.Writer

	dataDir string

	// mu guards subs.
	mu  sync.Mutex
	subs map[string]*session

	// pipesMu guards pipes.
	pipesMu sync.Mutex
	pipes   map[string]*relayPipe
}

// New returns a Server. dataDir is where request bodies are staged while
// queued (POOL_DATA_DIR, default ./data). logs receives the startup banner
// and one-line operational messages (ids, tags, counts, timings - never body
// content, D-I1).
func New(cfg config.Config, db *pooldb.DB, dataDir string, logs io.Writer) *Server {
	if cfg.HeartbeatMS <= 0 {
		cfg.HeartbeatMS = config.DefaultHeartbeatMS
	}
	s := &Server{
		cfg: cfg, db: db, logs: logs,
		dataDir: dataDir,
		subs: make(map[string]*session),
		pipes: make(map[string]*relayPipe),
	}
	opts := dispatch.Opts{MaxBounces: cfg.MaxBounces, LeaseDefaultMS: cfg.OfferLeaseMS}
	s.disp = dispatch.New(db, s, opts, s.purgeBody)
	return s
}

// Dispatcher exposes the dispatch engine (used by tests).
func (s *Server) Dispatcher() *dispatch.Dispatcher { return s.disp }

// Handler returns the HTTP handler (all routes).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pool/events", s.handleEvents)
	mux.HandleFunc("GET /pool/subscribe", s.handleSubscribe)
	mux.HandleFunc("POST /pool/jobs", s.handleIntake)
	mux.HandleFunc("PUT /pool/jobs/{id}/body", s.handlePutBody)
	mux.HandleFunc("GET /pool/jobs/{id}/body", s.handleGetBody)
	mux.HandleFunc("GET /pool/jobs/{id}/response", s.handleGetResponse)
	mux.HandleFunc("PUT /pool/jobs/{id}/response", s.handlePutResponse)
	mux.HandleFunc("GET /pool/classes", s.handleClasses)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

// Run serves the HTTP plane on addr until ctx is cancelled. The dispatcher
// starts here and stops on shutdown.
func (s *Server) Run(ctx context.Context, addr string) error {
	s.disp.SetLogger(s.logf)
	s.disp.Start(ctx)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server listen %s: %w", addr, err)
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:          s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops the dispatcher and the server.
func (s *Server) Shutdown(ctx context.Context) error {
	// Close all subscribe sessions (unblock their writers) and all response
	// pipes (unblock any in-flight PUT).
	s.mu.Lock()
	for _, ses := range s.subs {
		ses.close()
	}
	s.mu.Unlock()
	s.pipesMu.Lock()
	for _, p := range s.pipes {
		p.close()
	}
	s.pipesMu.Unlock()
	return s.disp.Stop(ctx)
}

// logf writes one line to the server's log sink.
func (s *Server) logf(format string, args ...any) {
	if s.logs == nil {
		return
	}
	line := fmt.Sprintf(format, args...)
	s.logs.Write([]byte(line + "\n"))
}

// purgeBody implements dispatch.Purger (D-I2): the body file is the only body
// storage, and it is deleted on completion/failure/expiry.
func (s *Server) purgeBody(ctx context.Context, jobID string) error {
	return os.Remove(filepath.Join(s.dataDir, "jobs", jobID))
}

// --- auth helpers ---------------------------------------------------------

// short truncates a uuid for one-line log messages.
func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// authUser authenticates the bearer token: it must match a non-revoked row of
// user_tokens (hash compare, revoked_at is null, D-J2). The token
// identifies the USER; the returned user id scopes every operation.
func (s *Server) authUser(r *http.Request) (string, error) {
	token := r.Header.Get("Authorization")
	token = strings.TrimPrefix(token, "Bearer ")
	tokenHash := sha256hex(token)
	q := fmt.Sprintf("select user_id from user_tokens where token_hash = %s and revoked_at is null",
		pooldb.S(tokenHash))
	userID, err := s.db.FirstString(r.Context(), q)
	if err != nil || userID == "" {
		return "", fmt.Errorf("auth: bad or revoked token")
	}
	return userID, nil
}

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// routerFromHeader reads and validates the X-Pool-Router-Id header and
// requires the router row to exist (the router identified itself via
// handshake/capacity earlier).
func (s *Server) routerFromHeader(r *http.Request) (string, error) {
	rid := r.Header.Get("X-Pool-Router-Id")
	if q := pooldb.UUID(rid); q == "NULL" {
		return "", fmt.Errorf("invalid router id")
	}
	n, err := s.db.Count(r.Context(), fmt.Sprintf("select count(*) from routers where id = %s", pooldb.UUID(rid)))
	if err != nil || n == 0 {
		return "", fmt.Errorf("unknown router %s", short(rid))
	}
	return rid, nil
}

// routerMustOwn requires the router row to belong to the token's user
// (the router<->user binding lives in routers.owner_id, D-H2).
func (s *Server) routerMustOwn(w http.ResponseWriter, r *http.Request, userID, routerID string) {
	n, err := s.db.FirstBool(r.Context(), fmt.Sprintf(
		"select 1 from routers where id = %s and owner_id = %s", pooldb.UUID(routerID), pooldb.UUID(userID)))
	if err != nil || !n {
		http.Error(w, "router does not belong to token's owner", http.StatusForbidden)
		return
	}
}

// maxSessionBuf caps the per-session envelope deque.
const maxSessionBuf = 4096

// session is one open /pool/subscribe for a router: a deque of pending
// envelope payloads ("data: ...\n\n" lines) plus a notify signal so the SSE
// writer wakes promptly on new envelopes (not just on the heartbeat tick).
// The long-poll reader waits on the notify signal with a timeout. A second
// connection replaces the first (MVP).
type session struct {
	routerID   string
	mu         sync.Mutex
	items      []string
	notify     chan struct{} // buffered 1; signaled on push
	closed     chan struct{}
	closeOnce   sync.Once
}

func (s *Server) registerSession(routerID string) *session {
	s.mu.Lock()
	if old := s.subs[routerID]; old != nil {
		// Replace: the old connection's serve loop wakes on closed and returns.
		old.close()
	}
	ses := &session{routerID: routerID, notify: make(chan struct{}, 1), closed: make(chan struct{})}
	s.subs[routerID] = ses
	s.mu.Unlock()
	return ses
}

func (ses *session) close() {
	ses.closeOnce.Do(func() { close(ses.closed) })
}

// unregister removes the session for routerID if it is still the active
// one, and closes it (the serve loop wakes and returns).
func (s *Server) unregisterSession(routerID string, ses *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subs[routerID] == ses {
		delete(s.subs, routerID)
	}
	ses.close()
}

// push queues one envelope payload for the router's subscribe stream and
// signals the waiters (dropped when the buffer is full).
func (ses *session) push(payload []byte) bool {
	ses.mu.Lock()
	if len(ses.items) >= maxSessionBuf {
		ses.mu.Unlock()
		return false
	}
	ses.items = append(ses.items, string(payload))
	ses.mu.Unlock()
	select {
	case ses.notify <- struct{}{}:
	default:
	}
	return true
}

// pop returns the oldest envelope payload, or ok=false when empty.
func (ses *session) pop() (payload string, ok bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if len(ses.items) == 0 {
		return "", false
	}
	it := ses.items[0]
	ses.items = ses.items[1:]
	return it, true
}

// drainAll returns all currently pending envelope payloads (in order).
func (ses *session) drainAll() []string {
	var out []string
	for {
		payload, ok := ses.pop()
		if !ok {
			break
		}
		out = append(out, payload)
	}
	return out
}

// Connected implements dispatch.RouterHub.
func (s *Server) Connected(routerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.subs[routerID]
	return ok
}

// SendEvent implements dispatch.RouterHub: queue one envelope for the
// router's subscribe stream (non-blocking; false when no open session or
// a full buffer). data is the marshaled envelope JSON; it is wrapped as
// an SSE line ("data: <json>\n\n") - serveSSE writes the deque verbatim
// and the long-poll reader strips the prefix (envelopeOf).
func (s *Server) SendEvent(routerID string, data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ses, ok := s.subs[routerID]
	if !ok {
		return false
	}
	return ses.push(append(append(append([]byte("data: "), data...), '\n'), '\n'))
}

// MirrorTerminal implements dispatch.RouterHub: mirror a terminal envelope
// (completion/error) to the originating router's subscribe stream and
// close its held-open GET /pool/jobs/{id}/response pipe (D-D6 §6b.4).
func (s *Server) MirrorTerminal(routerID, jobID string, data []byte) {
	s.SendEvent(routerID, data)
	s.closePipe(jobID)
}

// relayPipe is the in-memory relay channel for GET/PUT .../response (D-D6):
// the coordinator never stores the response bytes (D-I4), so a pipe is just a
// buffered channel of NDJSON lines with a closed signal; bounded (a stuck reader
// unblocks after 30s).
//
// lines is NEVER closed (a send evaluated on a closed channel panics, review
// H3). close() only flips the closed signal; the reader drains whatever is
// still buffered after observing it.
type relayPipe struct {
	lines     chan string
	closed    chan struct{}
	closeOnce sync.Once
}

func (p *relayPipe) send(line string) bool {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case p.lines <- line:
		return true
	case <-p.closed:
		return false
	case <-timer.C:
		return false
	}
}

// close signals end-of-stream. Lines already in flight or buffered are still
// delivered: the reader observes closed and then drains the channel in order.
func (p *relayPipe) close() {
	p.closeOnce.Do(func() { close(p.closed) })
}

// closePipe closes the response-relay pipe for jobID and drops it from the
// map (no-op when absent; idempotent with removePipe).
func (s *Server) closePipe(jobID string) {
	s.pipesMu.Lock()
	defer s.pipesMu.Unlock()
	if p, ok := s.pipes[jobID]; ok {
		p.close()
		delete(s.pipes, jobID)
	}
}

// getPipe returns the response-relay pipe for jobID, creating it on first
// use (buffered; the coordinator never stores the bytes, D-I4/D-I4-ext).
func (s *Server) getPipe(jobID string) *relayPipe {
	s.pipesMu.Lock()
	defer s.pipesMu.Unlock()
	if p, ok := s.pipes[jobID]; ok {
		return p
	}
	p := &relayPipe{lines: make(chan string, 1024), closed: make(chan struct{})}
	s.pipes[jobID] = p
	return p
}

// removePipe drops the pipe for jobID from the map (terminal reached).
func (s *Server) removePipe(jobID string) {
	s.pipesMu.Lock()
	defer s.pipesMu.Unlock()
	delete(s.pipes, jobID)
}

// jobRow loads a queue_jobs row by id; ok=false when the job is unknown.
type jobRow struct {
	ID string
	UserID string
	Tag string
	Status string
	RequestID string
	OriginRouter string
	ServingRouter string
	AcceptedAt string // "" = not accepted (gates body pull / response push)
	ConvKey string
	CreatedAt string
}

func (s *Server) jobRow(ctx context.Context, jobID string) (jobRow, bool) {
	var r jobRow
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
select id, user_id, class_id, status, request_id, originating_router_id, serving_router_id,
       coalesce(accepted_at::text, ''), conv_key, created_at
from queue_jobs where id = %s`, pooldb.UUID(jobID)))
	if err != nil || len(rows) == 0 {
		return r, false
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 10 {
		return r, false
	}
	r = jobRow{ID: f[0], UserID: f[1], Tag: f[2], Status: f[3], RequestID: f[4],
		OriginRouter: f[5], ServingRouter: f[6], AcceptedAt: f[7], ConvKey: f[8], CreatedAt: f[9]}
	return r, true
}

// writeJSON is the standard envelope of helpers for small JSON responses.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
