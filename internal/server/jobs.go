package server

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// Job status values mirror queue_jobs.status; the DB column is the source of
// truth. (The dispatch engine has its own in-memory copies.)
const (
	jobStatusPending  = "pending"
	jobStatusOffered = "offered"
	jobStatusRunning = "running"
	jobStatusFailed = "failed"
	jobStatusDone   = "done"
)

// handleIntake implements POST /pool/jobs (catalog §4): metadata-only job
// intake. The raw client body follows over PUT .../body. The originating
// router is identified by the X-Pool-Router-Id header (additive contract
// clarification, WS-06); it must exist and belong to the token's owner.
// Idempotency (D-G5): a re-intake with the same (user_id, request_id) within
// the dedupe window (POOL_DEDUPE_WINDOW_MS) returns the existing job_id with
// 200; a fresh prompt, or one older than the window, is a new job (201).
// The scope is per-user: an identical prompt from another user is fresh work.
func (s *Server) handleIntake(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	routerID, err := s.routerFromHeader(r)
	if err != nil {
		http.Error(w, "bad router id", http.StatusBadRequest)
		return
	}
	s.routerMustOwn(w, r, userID, routerID)

	var ji poolproto.JobIntake
	if err := json.NewDecoder(r.Body).Decode(&ji); err != nil {
		http.Error(w, "bad intake body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateJobIntake(ji); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Insert the job; a same-(user_id, request_id) conflict resolves below.
	jobID := pooldb.NewUUID()
	if err := s.insertJob(r.Context(), userID, routerID, ji, jobID); err != nil {
		s.logf("intake: insert job: %v", err)
		http.Error(w, "intake failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	inserted, _ := s.db.FirstBool(r.Context(),
		"select 1 from queue_jobs where id = "+pooldb.UUID(jobID))
	if inserted {
		// Fresh job: hand it to the dispatcher.
		if err := s.disp.Register(jobID); err != nil {
			s.logf("intake: register job %s: %v", jobID, err)
		}
		s.logf("intake: user %s router %s tag %s -> job %s (request_id %s)",
			short(userID), short(routerID), ji.Tag, jobID, short(ji.RequestID))
		writeJSON(w, http.StatusCreated, map[string]string{"job_id": jobID})
		return
	}

	// Conflict: a job with the same (user, request_id) already exists. The
	// dedupe window (POOL_DEDUPE_WINDOW_MS) decides: within it this is a retry
	// of an accepted request (return the existing job, 200); beyond it the id
	// is released and the prompt is new work (fresh insert, 201).
	existingID, ageMS, ok := s.dedupeExisting(r.Context(), userID, ji.RequestID)
	if !ok {
		http.Error(w, "intake failed: no job", http.StatusInternalServerError)
		return
	}
	if ageMS < int64(s.cfg.DedupeWindowMS) {
		s.logf("intake: dedupe user %s request_id %s -> job %s (age %dms < %dms)",
			short(userID), short(ji.RequestID), existingID, ageMS, s.cfg.DedupeWindowMS)
		writeJSON(w, http.StatusOK, map[string]string{"job_id": existingID})
		return
	}

	// Beyond the window: release the id (guarded so a just-young row is never
	// released) and re-intake the prompt as fresh work. The released row keeps
	// its history; its records row is untouched.
	_ = s.db.Exec(r.Context(), fmt.Sprintf(`
update queue_jobs set request_id = null where id = %s and user_id = %s
  and created_at < now() - interval '1 millisecond' * %d`,
		pooldb.UUID(existingID), pooldb.UUID(userID), s.cfg.DedupeWindowMS))
	newJobID := pooldb.NewUUID()
	if err := s.insertJob(r.Context(), userID, routerID, ji, newJobID); err != nil {
		s.logf("intake: re-insert job: %v", err)
		http.Error(w, "intake failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	reinserted, _ := s.db.FirstBool(r.Context(),
		"select 1 from queue_jobs where id = "+pooldb.UUID(newJobID))
	if !reinserted {
		// Raced: another intake grabbed the id between the release and insert;
		// return the winner.
		if rid, _, ok2 := s.dedupeExisting(r.Context(), userID, ji.RequestID); ok2 {
			s.logf("intake: re-insert raced, returning existing job %s", rid)
			writeJSON(w, http.StatusOK, map[string]string{"job_id": rid})
			return
		}
		http.Error(w, "intake failed: no job", http.StatusInternalServerError)
		return
	}
	if err := s.disp.Register(newJobID); err != nil {
		s.logf("intake: register job %s: %v", newJobID, err)
	}
	s.logf("intake: window expired, re-intake user %s request_id %s -> job %s (age %dms)",
		short(userID), short(ji.RequestID), newJobID, ageMS)
	writeJSON(w, http.StatusCreated, map[string]string{"job_id": newJobID})
}

// insertJob stages one queue_jobs row (metadata only; the body follows over
// PUT .../body). A same-(user_id, request_id) conflict is a no-op (D-G5).
func (s *Server) insertJob(ctx context.Context, userID, routerID string, ji poolproto.JobIntake, jobID string) error {
	return s.db.Exec(ctx, fmt.Sprintf(`
insert into queue_jobs (id, user_id, class_id, priority, est_prompt_tokens, est_ctx_needed,
                       conv_key, status, request_id, originating_router_id, created_at,
                       origin_template_hash)
values (%s, %s, %s, %s, %d, %d, %s, 'pending', %s, %s, now(), %s)
on conflict (user_id, request_id) where request_id is not null do nothing`,
		pooldb.UUID(jobID), pooldb.UUID(userID), pooldb.S(ji.Tag), pooldb.S(ji.Priority),
		ji.EstPromptTokens, ji.EstCtxNeeded, optStr(ji.ConvKey),
		pooldb.UUID(ji.RequestID), pooldb.UUID(routerID), optStr(ji.OriginTemplateHash)))
}

// dedupeExisting returns the existing job id for (userID, requestID) and its
// age in ms; ok=false when there is no such job.
func (s *Server) dedupeExisting(ctx context.Context, userID, requestID string) (string, int64, bool) {
	rows, err := s.db.Query(ctx, fmt.Sprintf(
		"select id, extract(epoch from (now() - created_at)) * 1000 from queue_jobs where user_id = %s and request_id = %s",
		pooldb.UUID(userID), pooldb.UUID(requestID)))
	if err != nil || len(rows) == 0 {
		return "", 0, false
	}
	f := strings.Split(rows[0], ",")
	if len(f) < 2 {
		return "", 0, false
	}
	age, err := strconv.ParseFloat(f[1], 64)
	if err != nil {
		return "", 0, false
	}
	return f[0], int64(age), true
}

// validateJobIntake enforces the catalog §4 constraints (metadata only; D-G5 id
// key, D-C3 class tag, D-C6 priority).
func validateJobIntake(ji poolproto.JobIntake) error {
	if pooldb.UUID(ji.RequestID) == "NULL" {
		return fmt.Errorf("intake: request_id is required and must be a uuid")
	}
	if !isSafeTag(ji.Tag) {
		return fmt.Errorf("intake: invalid tag %q", ji.Tag)
	}
	if ji.Priority != poolproto.PriorityLive && ji.Priority != poolproto.PriorityBatch {
		return fmt.Errorf("intake: priority must be live or batch")
	}
	if ji.EstPromptTokens < 0 || ji.EstCtxNeeded < 0 {
		return fmt.Errorf("intake: negative estimate")
	}
	if ji.ConvKey != nil && (*ji.ConvKey != "" && !isHex64(*ji.ConvKey)) {
		return fmt.Errorf("intake: conv_key must be 64 hex or null")
	}
	if ji.ClientHint != nil && len(*ji.ClientHint) > 64 {
		return fmt.Errorf("intake: client_hint too long")
	}
	if ji.OriginTemplateHash != nil && *ji.OriginTemplateHash != "" && !isHex64(*ji.OriginTemplateHash) {
		return fmt.Errorf("intake: origin_template_hash must be 64 hex or null")
	}
	return nil
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, ch := range s {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
			return false
		}
	}
	return true
}

// handlePutBody implements PUT /pool/jobs/{id}/body (catalog §5): the originating
// router streams the raw client payload verbatim; the coordinator stores it as the job's
// only body storage (D-I2). Atomic: write .part, rename.
func (s *Server) handlePutBody(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	routerID, err := s.routerFromHeader(r)
	if err != nil {
		http.Error(w, "bad router id", http.StatusBadRequest)
		return
	}
	s.routerMustOwn(w, r, userID, routerID)

	jobID := r.PathValue("id")
	job, ok := s.jobRow(r.Context(), jobID)
	if !ok {
		http.Error(w, "unknown job", http.StatusNotFound)
		return
	}
	if job.OriginRouter != routerID {
		http.Error(w, "this router did not originate the job", http.StatusForbidden)
		return
	}
	if job.UserID != userID {
		http.Error(w, "job does not belong to token's user", http.StatusForbidden)
		return
	}
	if job.Status != jobStatusPending {
		// Terminal/offered jobs must not stage bodies: the purge already ran
		// (or the job moved on) and the file would be an unpurged orphan
		// (review M, D-I2).
		http.Error(w, "job no longer pending: "+job.Status, http.StatusConflict)
		return
	}

	jobsDir := filepath.Join(s.dataDir, "jobs")
	if err := os.MkdirAll(jobsDir, 0o755); err != nil {
		http.Error(w, "body dir: "+err.Error(), http.StatusInternalServerError)
		return
	}
	partPath := filepath.Join(jobsDir, jobID+".part")
	finalPath := filepath.Join(jobsDir, jobID)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(partPath, body, 0o600); err != nil {
		os.Remove(partPath)
		http.Error(w, "write body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		os.Remove(partPath)
		http.Error(w, "rename body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// The job is fully queued now (status stays pending; body presence = queued).
	s.logf("body: job %s queued (%d bytes)", jobID, len(body))
	writeJSON(w, http.StatusOK, map[string]string{"status": "queued"})
}

// handleGetBody implements GET /pool/jobs/{id}/body (catalog §5): the serving
// router pulls the stored body after offer_accept. 404 if the job is unknown,
// expired, or not served by this router (D-D2 phase 2).
func (s *Server) handleGetBody(w http.ResponseWriter, r *http.Request) {
	_, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	routerID, err := s.routerFromHeader(r)
	if err != nil {
		http.Error(w, "bad router id", http.StatusBadRequest)
		return
	}

	jobID := r.PathValue("id")
	job, ok := s.jobRow(r.Context(), jobID)
	if !ok {
		http.Error(w, "unknown job", http.StatusNotFound)
		return
	}
	if job.Status != jobStatusOffered && job.Status != jobStatusRunning {
		http.Error(w, "job not serving (expired or completed)", http.StatusNotFound)
		return
	}
	if job.ServingRouter != routerID {
		http.Error(w, "this router is not serving the job", http.StatusNotFound)
		return
	}
	if job.AcceptedAt == "" {
		// Offered but not accepted: reading the body would be a cross-tenant
		// prompt disclosure (review H2).
		http.Error(w, "offer not accepted", http.StatusNotFound)
		return
	}

	path := filepath.Join(s.dataDir, "jobs", jobID)
	f, err := os.Open(path)
	if err != nil {
		// Missing file: the job is gone (purged on completion/failure/expiry).
		http.Error(w, "body not staged (expired or completed)", http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		http.Error(w, "stat body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if job.ConvKey != "" {
		w.Header().Set("X-Pool-Conv-Key", job.ConvKey)
	}
	w.Header().Set("X-Pool-Tag", job.Tag)
	w.Header().Set("Content-Length", fmt.Sprint(info.Size()))
	w.WriteHeader(http.StatusOK)
	n, err := io.Copy(w, f)
	s.logf("body: job %s served %d bytes to %s", jobID, n, short(routerID))
	if err == nil && n == info.Size() {
		// The client read the full body: the job is running now (D-D2).
		_ = s.disp.BodyPulled(jobID)
	}
}

// handlePutResponse implements PUT /pool/jobs/{id}/response (catalog §6b):
// the serving router pushes the NDJSON response stream; the coordinator relays
// each line in-memory to the originating router's held-open GET .../response.
// It never stores or logs the bytes (D-I4).
func (s *Server) handlePutResponse(w http.ResponseWriter, r *http.Request) {
	_, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	routerID, err := s.routerFromHeader(r)
	if err != nil {
		http.Error(w, "bad router id", http.StatusBadRequest)
		return
	}

	jobID := r.PathValue("id")
	job, ok := s.jobRow(r.Context(), jobID)
	if !ok {
		http.Error(w, "unknown job", http.StatusNotFound)
		return
	}
	if job.ServingRouter != routerID {
		http.Error(w, "this router is not serving the job", http.StatusNotFound)
		return
	}
	if job.AcceptedAt == "" {
		// Pushing a response without accepting would inject bytes into another
		// user's client stream (review H2).
		http.Error(w, "offer not accepted", http.StatusNotFound)
		return
	}
	if job.Status != jobStatusOffered && job.Status != jobStatusRunning {
		http.Error(w, "job not serving", http.StatusNotFound)
		return
	}

	// e2e fix: the final response goes out only after the pushed body has been
	// fully received. The serving router's http client (cpp-httplib) adds
	// "Expect: 100-continue" to every request body of >= 1024 bytes, and if
	// the server's first reply is a final status instead of "100 Continue" it
	// withholds the body (waiting for a response body that never ends) while
	// the coordinator waits for the request body: a deadlock until the router's
	// read timeout, observed as "push read error: unexpected EOF" + "(no
	// response)". Reading the body first makes the server emit 100-continue and
	// the lines still relay to the origin as they arrive.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Connection", "close")
	flusher, _ := w.(http.Flusher)
	pipe := s.getPipe(jobID)
	count := 0
	scanner := newLineScanner(r.Body)
	for scanner.Scan() {
		line := scanner.Text()
		count++
		// response activity extends the D-G2 deadline (amended 2026-11-12,
		// KI-1): the job is alive while it streams, whatever the turn length
		_ = s.disp.ExtendRunning(jobID)
		if !pipe.send(line) {
			// The originating side is gone: abort this push (the router sees
			// its PUT stream close) and fail the job (D-G4).
			s.closePipe(jobID)
			_ = s.disp.FailClientDisconnected(jobID)
			s.logf("relay: job %s push aborted (%d lines relayed)", jobID, count)
			return
		}
	}
	if err := scanner.Err(); err != nil {
		s.logf("relay: job %s push read error: %v", jobID, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}
	// The push ended: terminal for the origin's stream.
	pipe.close()
	s.removePipe(jobID)
	s.logf("relay: job %s push ended (%d lines relayed)", jobID, count)
}

// handleGetResponse implements GET /pool/jobs/{id}/response (catalog §6b):
// the originating router holds this open; the coordinator streams the NDJSON
// lines verbatim (one SSE data: line per response line), heartbeats while
// queued, and closes when the push ends or the job fails/expires.
func (s *Server) handleGetResponse(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	routerID, err := s.routerFromHeader(r)
	if err != nil {
		http.Error(w, "bad router id", http.StatusBadRequest)
		return
	}

	jobID := r.PathValue("id")
	job, ok := s.jobRow(r.Context(), jobID)
	if !ok {
		http.Error(w, "unknown job", http.StatusNotFound)
		return
	}
	if job.OriginRouter != routerID {
		http.Error(w, "this router did not originate the job", http.StatusForbidden)
		return
	}
	if job.UserID != userID {
		http.Error(w, "job does not belong to token's user", http.StatusForbidden)
		return
	}

	// A terminal job (failed/expired/completed) closes the stream cleanly.
	if job.Status == jobStatusFailed || job.Status == jobStatusDone {
		s.logf("relay: job %s already terminal; clean close for %s", jobID, short(routerID))
		writeJSON(w, http.StatusOK, map[string]string{"status": "terminal"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	pipe := s.getPipe(jobID)
	hb := time.NewTicker(time.Duration(s.cfg.HeartbeatMS) * time.Millisecond)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			// Client gave up: abort the in-flight push, fail the job.
			s.closePipe(jobID)
			_ = s.disp.FailClientDisconnected(jobID)
			s.logf("relay: job %s originator gave up; failed client_disconnected", jobID)
			return
		case line := <-pipe.lines:
			fmt.Fprintf(w, "data: %s\n\n", line)
			if flusher != nil {
				flusher.Flush()
			}
		case <-pipe.closed:
			// End of stream (push finished or job terminal): drain the lines
			// still buffered, in order, then close.
			for {
				select {
				case line := <-pipe.lines:
					fmt.Fprintf(w, "data: %s\n\n", line)
					if flusher != nil {
						flusher.Flush()
					}
				default:
					s.logf("relay: job %s stream drained for %s", jobID, short(routerID))
					return
				}
			}
		case <-hb.C:
			fmt.Fprintf(w, ": hb %d\n\n", timeNowUnixMs())
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// handleClasses implements GET /pool/classes (catalog §6): the cross-member class
// advertisement state, computed from class_members + router_models (D-C1, D-C3):
// per class with >= 1 member: member_count, min_ctx, max_ctx,
// advertised_ctx = min(round(avg + pop_std), max).
func (s *Server) handleClasses(w http.ResponseWriter, r *http.Request) {
	_, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	rows, err := s.db.Query(r.Context(), `
select cm.class_id, rm.n_ctx
from class_members cm
join router_models rm on rm.router_id = cm.router_id and rm.model_id = cm.model_id
join routers r on r.id = cm.router_id and r.deleted_at is null
where rm.status = 'loaded'
order by cm.class_id, rm.n_ctx`)
	if err != nil {
		http.Error(w, "classes query: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Aggregate per class.
	type agg struct {
		count, min, max, sum, sumsq int
	}
	classes := map[string]*agg{}
	var order []string
	for _, row := range rows {
		f := strings.Split(row, ",")
		if len(f) < 2 {
			continue
		}
		tag := f[0]
		var n int
		if _, err := fmt.Sscanf(f[1], "%d", &n); err != nil {
			continue
		}
		a, ok := classes[tag]
		if !ok {
			a = &agg{}
			classes[tag] = a
			order = append(order, tag)
		}
		a.count++
		a.sum += n
		a.sumsq += n * n
		if a.count == 1 || n < a.min {
			a.min = n
		}
		if n > a.max {
			a.max = n
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	out := []map[string]any{}
	for _, tag := range order {
		a := classes[tag]
		avg := float64(a.sum) / float64(a.count)
		variance := float64(a.sumsq)/float64(a.count) - avg*avg
		if variance < 0 {
			variance = 0
		}
		stddev := math.Sqrt(variance)
		advertised := int(math.Round(avg + stddev))
		if advertised > a.max {
			advertised = a.max
		}
		out = append(out, map[string]any{
			"tag": tag, "member_count": a.count,
			"min_ctx": a.min, "max_ctx": a.max, "advertised_ctx": advertised,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"protocol": 1, "classes": out})
}

// handleHealthz implements GET /healthz (no auth).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// optStr renders a *string as a SQL literal, or NULL.
func optStr(s *string) string {
	if s == nil || *s == "" {
		return "NULL"
	}
	return pooldb.S(*s)
}

func timeNowUnixMs() int64 {
	return time.Now().UnixMilli()
}

func newLineScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 4<<20)
	return sc
}

