package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// handleEvents implements POST /pool/events (catalog §1, §2): one envelope per
// POST. The closed type set is enforced by poolproto.Event.Decode.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var ev poolproto.Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, "bad envelope: "+err.Error(), http.StatusBadRequest)
		return
	}
	typed, err := ev.Decode()
	if err != nil {
		http.Error(w, "unknown message type: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.handleTypedEvent(r, w, userID, ev, typed)
}

func (s *Server) handleTypedEvent(r *http.Request, w http.ResponseWriter, userID string, ev poolproto.Event, typed any) {
	ok := func() {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
	s.logf("events: %s from user %s", ev.Type, short(userID))

	switch ev.Type {
	case poolproto.TypeHandshake:
		hs := typed.(*poolproto.Handshake)
		if s.handleHandshakeOrCapacity(r, w, userID, hs.RouterID, hs.RouterName, hs.Software, hs.Models) {
			ok()
		}

	case poolproto.TypeCapacity:
		cap := typed.(*poolproto.Capacity)
		if s.handleHandshakeOrCapacity(r, w, userID, cap.RouterID, "", "", cap.Models) {
			ok()
		}

	case poolproto.TypeOfferAccept:
		oa := typed.(*poolproto.OfferAccept)
		job := s.jobForEvent(r, w, userID, oa.JobID)
		if job.ID == "" {
			return
		}
		if !s.routerOwnsJob(r, w, job, userID) {
			return
		}
		_ = s.disp.OfferAccepted(oa.JobID, oa.LeaseMS)
		ok()

	case poolproto.TypeOfferReject:
		or := typed.(*poolproto.OfferReject)
		job := s.jobForEvent(r, w, userID, or.JobID)
		if job.ID == "" {
			return
		}
		if !s.routerOwnsJob(r, w, job, userID) {
			return
		}
		_ = s.disp.OfferRejected(or.JobID, or.Reason)
		ok()

	case poolproto.TypeBodyAbort:
		ba := typed.(*poolproto.BodyAbort)
		job := s.jobForEvent(r, w, userID, ba.JobID)
		if job.ID == "" {
			return
		}
		if !s.routerOwnsJob(r, w, job, userID) {
			return
		}
		_ = s.disp.BodyAborted(ba.JobID, ba.Reason)
		ok()

	case poolproto.TypeCompletion:
		comp := typed.(*poolproto.Completion)
		job := s.jobForEvent(r, w, userID, comp.JobID)
		if job.ID == "" {
			return
		}
		// The completing router must be the job's serving router and own it (D-D6).
		if job.ServingRouter == "" || comp.ServedBy != job.ServingRouter {
			http.Error(w, "completion from non-serving router", http.StatusForbidden)
			return
		}
		if !s.routerOwnsJob(r, w, job, userID) {
			return
		}
		_ = s.disp.JobCompleted(comp.JobID, *comp)
		ok()

	case poolproto.TypeError:
		er := typed.(*poolproto.ErrorReport)
		job := s.jobForEvent(r, w, userID, er.JobID)
		if job.ID == "" {
			return
		}
		if !s.routerOwnsJob(r, w, job, userID) {
			return
		}
		_ = s.disp.ErrorReport(er.JobID, er.Stage, er.Code, er.Message, er.PromptTokens, er.ResponseTokens, er.TQueuedMS, er.TServiceMS)
		ok()

	case poolproto.TypeAffinityReport:
		ar := typed.(*poolproto.AffinityReport)
		rid := ar.RouterID
		if pooldb.UUID(rid) == "NULL" {
			http.Error(w, "invalid router_id", http.StatusBadRequest)
			return
		}
		if !s.dbBool(r, "select 1 from routers where id = %s and owner_id = %s", rid, userID) {
			http.Error(w, "router does not belong to token's owner", http.StatusForbidden)
			return
		}
		_ = s.disp.AffinityReport(rid, ar.Entries)
		ok()

	case poolproto.TypeExclusionUpdate:
		xu := typed.(*poolproto.ExclusionUpdate)
		rid := xu.RouterID
		if pooldb.UUID(rid) == "NULL" {
			http.Error(w, "invalid router_id", http.StatusBadRequest)
			return
		}
		if !s.dbBool(r, "select 1 from routers where id = %s and owner_id = %s", rid, userID) {
			http.Error(w, "router does not belong to token's owner", http.StatusForbidden)
			return
		}
		stmt := fmt.Sprintf(`
insert into exclusions (owner_id, router_id, model_id, excluded, updated_at)
values (%s, %s, %s, %t, now())
on conflict (owner_id, router_id, model_id) do update
  set excluded = excluded.excluded, updated_at = excluded.updated_at`,
			pooldb.UUID(userID), pooldb.UUID(rid), pooldb.S(xu.ModelID), xu.Excluded)
		if err := s.db.Exec(r.Context(), stmt); err != nil {
			s.logf("events: exclusion upsert: %v", err)
		}
		ok()

	case poolproto.TypeEvictionNotice:
		evn := typed.(*poolproto.EvictionNotice)
		rid := evn.RouterID
		if pooldb.UUID(rid) == "NULL" {
			http.Error(w, "invalid router_id", http.StatusBadRequest)
			return
		}
		if !s.dbBool(r, "select 1 from routers where id = %s and owner_id = %s", rid, userID) {
			http.Error(w, "router does not belong to token's owner", http.StatusForbidden)
			return
		}
		if err := s.db.Exec(r.Context(), fmt.Sprintf(`
update router_models set status = 'gone', updated_at = now()
where router_id = %s and model_id = %s`, pooldb.UUID(rid), pooldb.S(evn.ModelID))); err != nil {
			s.logf("events: eviction model update: %v", err)
		}
		if err := s.db.Exec(r.Context(), fmt.Sprintf(`
delete from class_members where router_id = %s and model_id = %s`,
			pooldb.UUID(rid), pooldb.S(evn.ModelID))); err != nil {
			s.logf("events: eviction class_members delete: %v", err)
		}
		// Cancel any pending offers/serves naming the evicted model: re-offer.
		for _, jid := range evn.EvictedJobs {
			_ = s.disp.InternalReject(jid, rid, evn.ModelID, poolproto.RejectEvicted)
		}
		// D-E4/D-E6: pending jobs whose continuity hint names this member are
		// denied with reason=evicted.
		_ = s.disp.HintEvicted(rid)
		ok()

	default:
		http.Error(w, "unsupported event type "+ev.Type, http.StatusBadRequest)
	}
}

// dbBool runs a boolean scalar query and reports whether it returned a row.
func (s *Server) dbBool(r *http.Request, format string, args ...any) bool {
	q := fmt.Sprintf(format, pooldb.UUID(fmt.Sprint(args[0])), pooldb.UUID(fmt.Sprint(args[1])))
	n, err := s.db.FirstBool(r.Context(), q)
	if err != nil || !n {
		return false
	}
	return true
}

// jobForEvent loads the job row for an event-scoped job id and enforces that
// the token's user owns the job's serving router (the job's user_id is the
// originator's user, who is a different user from the serving router).
func (s *Server) jobForEvent(r *http.Request, w http.ResponseWriter, userID, jobID string) jobRow {
	job, ok := s.jobRow(r.Context(), jobID)
	if !ok {
		http.Error(w, "unknown job", http.StatusNotFound)
		return jobRow{}
	}
	return job
}

// routerOwnsJob reports whether the event's user owns the job's serving router
// (the binding routers.owner_id == user, D-H2). A false result writes a 409
// (the job state moved on, or the event is stale/foreign) - the catalog
// requires a response on every POST /pool/events (review M: bare EOF).
func (s *Server) routerOwnsJob(r *http.Request, w http.ResponseWriter, job jobRow, userID string) bool {
	if job.ServingRouter == "" {
		http.Error(w, "job has no serving router (stale event)", http.StatusConflict)
		return false
	}
	if !s.dbBool(r, "select 1 from routers where id = %s and owner_id = %s", job.ServingRouter, userID) {
		http.Error(w, "job's serving router is owned by another user", http.StatusConflict)
		return false
	}
	return true
}

// handleHandshakeOrCapacity upserts the routers row (owner = token's user) and
// replaces the router's router_models + class_members (handshake and capacity
func (s *Server) handleHandshakeOrCapacity(r *http.Request, w http.ResponseWriter, userID, routerID, routerName, software string, models []poolproto.ModelEntry) bool {
	rid := routerID
	if pooldb.UUID(rid) == "NULL" {
		http.Error(w, "invalid router_id", http.StatusBadRequest)
		return false
	}
	// First sighting: the routers row does not exist yet (or belongs to
	// this user). A later event from a different token on an existing row with
	// another owner is rejected below.
	owns, err := s.db.FirstBool(r.Context(), fmt.Sprintf(
		"select 1 from routers where id = %s and owner_id = %s", pooldb.UUID(rid), pooldb.UUID(userID)))
	if err != nil || !owns {
		existing, _ := s.db.FirstString(r.Context(), fmt.Sprintf(
			"select owner_id from routers where id = %s", pooldb.UUID(rid)))
		if existing != "" && existing != userID {
			http.Error(w, "router is owned by another user", http.StatusForbidden)
			return false
		}
	}
	// Capacity events carry no router_name/software (only the handshake does).
	// Insert falls back to a generated name; update keeps the existing values
	// (coalesce) so a capacity event can never clobber the handshake identity
	// (review M: NULL into the not-null column failed every capacity upsert).
	name := routerName
	if name == "" {
		name = "router-" + rid[:8]
	}
	if err := s.db.Exec(r.Context(), fmt.Sprintf(`
insert into routers (id, owner_id, router_name, software, last_seen_at)
values (%s, %s, %s, %s, now())
on conflict (id) do update set
  owner_id = excluded.owner_id,
  router_name = coalesce(excluded.router_name, routers.router_name),
  software = coalesce(excluded.software, routers.software),
  last_seen_at = excluded.last_seen_at`,
		pooldb.UUID(rid), pooldb.UUID(userID), pooldb.S(name), pooldb.S(software))); err != nil {
		s.logf("events: routers upsert: %v", err)
	}
	s.rebuildModels(r, rid, models)
	return true
}
// rebuildModels replaces the router's router_models rows (full list, D-D2)
// and rebuilds its class_members rows (D-C1 advertisement basis).
func (s *Server) rebuildModels(r *http.Request, routerID string, models []poolproto.ModelEntry) {
	rid := routerID
	if err := s.db.Exec(r.Context(), fmt.Sprintf("delete from router_models where router_id = %s", pooldb.UUID(rid))); err != nil {
		s.logf("events: router_models delete for %s: %v", rid, err)
	}
	for _, m := range models {
		stmt := fmt.Sprintf(`
	insert into router_models (router_id, model_id, tags, n_ctx, n_embd, n_slots, busy_slots, status, vram_mb, updated_at)
	values (%s, %s, %s, %d, %d, %d, %d, %s, %s, now())
	on conflict (router_id, model_id) do update set
	tags = excluded.tags, n_ctx = excluded.n_ctx, n_embd = excluded.n_embd,
	n_slots = excluded.n_slots, busy_slots = excluded.busy_slots,
	status = excluded.status, vram_mb = excluded.vram_mb, updated_at = excluded.updated_at`,
		pooldb.UUID(rid), pooldb.S(m.ModelID), tagsLiteral(m.Tags), m.NCtx, m.NEmbD, m.NSlots, m.BusySlots,
		pooldb.S(m.Status), vramLiteral(m.VramMB))
		if err := s.db.Exec(r.Context(), stmt); err != nil {
			s.logf("events: router_models upsert for %s: %v", rid, err)
		}
	}
	// class_members: drop stale rows for this router, then insert the current
	// loaded membership (one row per tag, only known classes).
	// class_members: drop stale rows for this router, then insert the current
	// loaded membership (one row per tag, only known classes).
	if err := s.db.Exec(r.Context(), fmt.Sprintf("delete from class_members where router_id = %s", pooldb.UUID(rid))); err != nil {
		s.logf("events: class_members delete for %s: %v", rid, err)
	}
	for _, m := range models {
		if m.Status != poolproto.ModelLoaded {
			continue
		}
		for _, tag := range m.Tags {
			if !isSafeTag(tag) {
				continue
			}
			stmt := fmt.Sprintf(`
insert into class_members (class_id, router_id, model_id)
select %s, %s, %s
where exists (select 1 from classes where id = %s)
on conflict (class_id, router_id, model_id) do nothing`,
				pooldb.S(tag), pooldb.UUID(rid), pooldb.S(m.ModelID), pooldb.S(tag))
			if err := s.db.Exec(r.Context(), stmt); err != nil {
				s.logf("events: class_members upsert for %s: %v", rid, err)
			}
		}
	}
}

// tagsLiteral renders a []string as a postgres text[] literal.
func tagsLiteral(tags []string) string {
	quoted := make([]string, len(tags))
	for i, t := range tags {
		quoted[i] = strings.ReplaceAll(t, "'", "''")
	}
	return "'" + "{" + strings.Join(quoted, ",") + "}'"
}

// vramLiteral renders *int as an int literal or NULL.
func vramLiteral(n *int) string {
	if n == nil {
		return "NULL"
	}
	return pooldb.I64(int64(*n))
}

// isSafeTag rejects tag values that could break the SQL or the class id space.
func isSafeTag(tag string) bool {
	if tag == "" || len(tag) > 128 {
		return false
	}
	for _, ch := range tag {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	return true
}
