package server

import (
	"bufio"
	"encoding/json"
	"math"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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
// Idempotency: a repeat request_id returns the existing job_id (D-G5).
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

	jobID := pooldb.NewUUID()
	inserted := true
	err = s.db.Exec(r.Context(), fmt.Sprintf(`
insert into queue_jobs (id, user_id, class_id, priority, est_prompt_tokens, est_ctx_needed,
                       conv_key, status, request_id, originating_router_id, created_at)
values (%s, %s, %s, %s, %d, %d, %s, 'pending', %s, %s, now())
on conflict (request_id) where request_id is not null do nothing`,
		pooldb.UUID(jobID), pooldb.UUID(userID), pooldb.S(ji.Tag), pooldb.S(ji.Priority),
		ji.EstPromptTokens, ji.EstCtxNeeded, optStr(ji.ConvKey),
		pooldb.UUID(ji.RequestID), pooldb.UUID(routerID)))
	if err != nil {
		s.logf("intake: insert job: %v", err)
		http.Error(w, "intake failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Idempotency: was it a conflict (existing job_id) or a fresh insert?
	existing, _ := s.db.FirstString(r.Context(), fmt.Sprintf(
		"select id from queue_jobs where id = %s", pooldb.UUID(jobID)))
	if existing == "" {
		existing, _ = s.db.FirstString(r.Context(), fmt.Sprintf(
			"select id from queue_jobs where request_id = %s", pooldb.UUID(ji.RequestID)))
		if existing == "" {
			http.Error(w, "intake failed: no job", http.StatusInternalServerError)
			return
		}
		jobID = existing
		inserted = false
	}
	if inserted {
		// Fresh job: hand it to the dispatcher.
		if err := s.disp.Register(jobID); err != nil {
			s.logf("intake: register job %s: %v", jobID, err)
		}
	}
	s.logf("intake: user %s router %s tag %s -> job %s (request_id %s)",
		short(userID), short(routerID), ji.Tag, jobID, short(ji.RequestID))
	writeJSON(w, http.StatusCreated, map[string]string{"job_id": jobID})
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
		http.Error(w, "write body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(partPath, finalPath); err != nil {
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
	if job.Status != jobStatusOffered && job.Status != jobStatusRunning {
		http.Error(w, "job not serving", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	pipe := s.getPipe(jobID)
	count := 0
	scanner := newLineScanner(r.Body)
	for scanner.Scan() {
		line := scanner.Text()
		count++
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
		case line, ok := <-pipe.lines:
			if !ok {
				// The push ended (pipe closed): every relayed line was
				// drained in order already.
				s.logf("relay: job %s stream drained for %s", jobID, short(routerID))
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", line)
			if flusher != nil {
				flusher.Flush()
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
		n, err := fmt.Sscanf(f[1], "%d", new(int))
		if err != nil {
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
		stddev := mathSqrt(variance)
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

func mathSqrt(v float64) float64 {
	// Newton's method for float64; avoids importing math for one call.
	if v <= 0 {
		return 0
	}
	x := v
	for i := 0; i < 60; i++ {
		y := x - (x*x-v)/(2*x)
		if y == x {
			return y
		}
		x = y
	}
	return x
}
