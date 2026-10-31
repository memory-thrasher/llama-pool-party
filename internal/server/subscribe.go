package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// handleSubscribe implements GET /pool/subscribe (catalog §1): the
// router holds this open and receives work_offer envelopes plus the
// mirrored completion/error envelopes (D-D6). Two modes, selected by
// Accept:
//
//   - text/event-stream: SSE. One writer loop drains the router's session
//     on new-envelope notifications and on the heartbeat tick; a second
//     connection replaces the first (MVP).
//   - application/json: long-poll. Wait up to 55 s for pending events,
//     respond with a JSON array of work_offer envelopes (possibly empty);
//     heartbeat comments keep proxies from timing out the link.
func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
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

	switch r.Header.Get("Accept") {
	case "text/event-stream":
		s.serveSSE(w, r, routerID)
	case "application/json":
		s.serveLongPoll(w, r, routerID)
	default:
		http.Error(w, "unsupported accept (want text/event-stream or application/json)", http.StatusNotAcceptable)
	}
}

// serveSSE is the SSE mode: drain the router's session deque on new-envelope
// notifications (prompt) and on heartbeat ticks; write each payload as a
// "data: ..." line.
func (s *Server) serveSSE(w http.ResponseWriter, r *http.Request, routerID string) {
	ses := s.registerSession(routerID)
	defer s.unregisterSession(routerID, ses)

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	hb := time.NewTicker(time.Duration(s.cfg.HeartbeatMS) * time.Millisecond)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ses.closed:
			return
		case <-ses.notify:
			for _, payload := range ses.drainAll() {
				w.Write([]byte(payload))
				if flusher != nil {
					flusher.Flush()
				}
			}
		case <-hb.C:
			fmt.Fprintf(w, ": hb %d\n\n", time.Now().UnixMilli())
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// serveLongPoll is the long-poll mode: wait up to 55 s for pending envelopes
// and respond with a JSON array (possibly empty). The router re-issues the
// GET after each response.
func (s *Server) serveLongPoll(w http.ResponseWriter, r *http.Request, routerID string) {
	ses := s.registerSession(routerID)
	defer s.unregisterSession(routerID, ses)

	w.Header().Set("Content-Type", "application/json")
	const timeout = 55 * time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Non-nil: the catalog promises a (possibly empty) JSON list.
	envelopes := []map[string]any{}
	select {
	case <-ses.notify:
	case <-timer.C:
	}
	for _, payload := range ses.drainAll() {
		if len(envelopes) >= 16 {
			break
		}
		envelopes = append(envelopes, envelopeOf(payload))
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(envelopes)
}

// envelopeOf parses one "data: <json>" envelope payload into a map for the
// long-poll JSON array. Unparseable lines become a minimal envelope.
func envelopeOf(payload string) map[string]any {
	line := payload
	if strings.HasPrefix(line, "data: ") {
		line = line[len("data: "):]
	}
	var ev map[string]any
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		return map[string]any{"type": "raw", "payload": strings.TrimSpace(line)}
	}
	return ev
}
