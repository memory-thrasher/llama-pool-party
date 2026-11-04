// Portal backend (WS-14): cookie sessions in postgres (D-A5 durable state),
// the member self-service endpoints, the admin mirror, and the embedded
// SvelteKit SPA (D-N4: served as static files, zero extra processes).
//
// Login is paste-your-opaque-token (D-J2): the cookie value is 256-bit
// random, only its sha256 is stored, and the plaintext token never reaches
// logs or any response after the single creation (anti-memory, D-I1/D-I2).
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"pool-coordinator/internal/config"
	"pool-coordinator/internal/pooldb"
	web "pool-coordinator/web"
)

const portalCookieName = "pool_session"

// maxPriorityModels caps the per-user priority list (D-D5).
const maxPriorityModels = 32

// --- sessions ---------------------------------------------------------------

// portalTTL is the session lifetime (POOL_PORTAL_SESSION_TTL_MS, default 7d).
func (s *Server) portalTTL() time.Duration {
	ms := s.cfg.PortalSessionTTLMS
	if ms <= 0 {
		ms = config.DefaultPortalSessionTTLMS
	}
	return time.Duration(ms) * time.Millisecond
}

// portalActiveUser resolves an opaque token to an ACTIVE user (the portal
// login gate): unrevoked token, user exists, status 'active'. Invited and
// suspended tokens do not log in.
func (s *Server) portalActiveUser(ctx context.Context, token string) (string, error) {
	q := fmt.Sprintf(`
select u.id
from user_tokens t
join users u on t.user_id = u.id
where t.token_hash = %s
  and t.revoked_at is null
  and u.status = 'active'`,
		pooldb.S(sha256hex(token)))
	userID, err := s.db.FirstString(ctx, q)
	if err != nil || userID == "" {
		return "", fmt.Errorf("portal: bad, revoked, or inactive token")
	}
	return userID, nil
}

func (s *Server) handlePortalLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	userID, err := s.portalActiveUser(r.Context(), req.Token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	cookie := newToken()
	ttl := s.portalTTL()
	if err := s.db.Exec(r.Context(), fmt.Sprintf(
		"insert into portal_sessions (user_id, token_hash, expires_at) values (%s, %s, now() + %d * interval '1 millisecond')",
		pooldb.UUID(userID), pooldb.S(sha256hex(cookie)), int64(ttl.Milliseconds()))); err != nil {
		http.Error(w, "failed to create session", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: portalCookieName, Value: cookie, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: int(ttl / time.Second)})
	writeJSON(w, http.StatusOK, struct {
		OK bool `json:"ok"`
	}{true})
}

func (s *Server) handlePortalLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(portalCookieName); err == nil && c.Value != "" {
		_ = s.db.Exec(r.Context(), fmt.Sprintf(
			"delete from portal_sessions where token_hash = %s", pooldb.S(sha256hex(c.Value))))
	}
	// Clear the cookie; a 200 no-op when the session is already gone.
	http.SetCookie(w, &http.Cookie{Name: portalCookieName, Value: "", Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode, MaxAge: -1})
	writeJSON(w, http.StatusOK, struct {
		OK bool `json:"ok"`
	}{true})
}

// portalSessionUser authenticates the cookie session (all /portal/* except
// login): the row must exist, be unexpired, and its user still 'active'
// (a suspended user's live sessions die).
func (s *Server) portalSessionUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	c, err := r.Cookie(portalCookieName)
	if err != nil || c.Value == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	row, err := s.db.FirstString(r.Context(), fmt.Sprintf(`
select u.id
from portal_sessions p
join users u on u.id = p.user_id
where p.token_hash = %s
  and p.expires_at > now()
  and u.status = 'active'`, pooldb.S(sha256hex(c.Value))))
	if err != nil || row == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	return row, true
}

// sweepPortalSessions deletes expired session rows. It runs from the
// dispatcher's 1s sweep (the job sweep's tick) and is called directly by
// tests.
func (s *Server) sweepPortalSessions(ctx context.Context) {
	n, err := s.db.Count(ctx, "with swept as (delete from portal_sessions where expires_at < now() returning 1) select count(*) from swept")
	if err != nil {
		s.logf("portal: session sweep: %v", err)
		return
	}
	if n > 0 {
		s.logf("portal: swept %d expired session(s)", n)
	}
}

// --- member endpoints -------------------------------------------------------

type portalProfile struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	Status      string `json:"status"`
	ActivatedAt string `json:"activated_at,omitempty"`
}

type portalRouter struct {
	ID         string `json:"id"`
	Status     string `json:"status"` // 'online' when the subscribe session is open
	LastSeenAt string `json:"last_seen_at,omitempty"`
}

type portalModel struct {
	Name     string   `json:"name"`
	Loaded   bool     `json:"loaded"`
	Tags     []string `json:"tags"`
	Excluded bool     `json:"excluded"`
}

type portalToken struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	Revoked   bool   `json:"revoked"`
}

type portalMeResp struct {
	User    portalProfile  `json:"user"`
	Routers []portalRouter `json:"routers"`
	Models  []portalModel  `json:"models"`
	Tokens  []portalToken  `json:"tokens"`
}

func (s *Server) handlePortalMe(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return
	}
	ctx := r.Context()

	rows, err := s.db.Query(ctx, fmt.Sprintf(
		"select email, display_name, role, status, coalesce(activated_at::text, '') from users where id = %s",
		pooldb.UUID(userID)))
	if err != nil || len(rows) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	f := strings.Split(rows[0], ",")
	me := portalMeResp{User: portalProfile{Email: f[0], DisplayName: f[1], Role: f[2], Status: f[3]},
		Routers: []portalRouter{}, Models: []portalModel{}, Tokens: []portalToken{}}
	if len(f) > 4 && f[4] != "" {
		me.User.ActivatedAt = f[4]
	}

	// Routers: 'online' means the subscribe session is open right now.
	rrows, err := s.db.Query(ctx, fmt.Sprintf(
		"select id, coalesce(last_seen_at::text, '') from routers where owner_id = %s and deleted_at is null order by created_at",
		pooldb.UUID(userID)))
	if err == nil {
		for _, row := range rrows {
			rf := strings.Split(row, ",")
			rt := portalRouter{ID: rf[0]}
			if len(rf) > 1 && rf[1] != "" {
				rt.LastSeenAt = rf[1]
			}
			if s.Connected(rf[0]) {
				rt.Status = "online"
			} else {
				rt.Status = "offline"
			}
			me.Routers = append(me.Routers, rt)
		}
	}

	// Models: one row per (router, model); exclusion mark per the caller's
	// user (D-F1). tags are chr(1)-joined so psql row splitting is safe.
	mrows, err := s.db.Query(ctx, fmt.Sprintf(`
select format('%%s' || chr(1) || '%%s' || chr(1) || '%%s' || chr(1) || '%%s',
               rm.model_id, rm.status, array_to_string(rm.tags, chr(1)), coalesce(e.excluded, false))
from router_models rm
join routers rt on rt.id = rm.router_id
left join exclusions e on e.owner_id = %s and e.router_id = rm.router_id and e.model_id = rm.model_id
where rt.owner_id = %s and rt.deleted_at is null
order by rm.model_id, rt.id`, pooldb.UUID(userID), pooldb.UUID(userID)))
	if err == nil {
		for _, row := range mrows {
			// chr(1) separators: a model id with a comma must not shift the fields
			mf := strings.Split(row, "\x01")
			if len(mf) < 3 {
				continue
			}
			m := portalModel{Name: mf[0], Loaded: mf[1] == "loaded", Tags: []string{}}
			if mf[2] != "" {
				m.Tags = strings.Split(mf[2], "\x01")
			}
			if len(mf) > 3 {
				m.Excluded = mf[3] == "t"
			}
			me.Models = append(me.Models, m)
		}
	}

	trows, err := s.db.Query(ctx, fmt.Sprintf(
		"select name, created_at, revoked_at is null from user_tokens where user_id = %s order by created_at desc",
		pooldb.UUID(userID)))
	if err == nil {
		for _, row := range trows {
			tf := strings.Split(row, ",")
			if len(tf) < 2 {
				continue
			}
			tk := portalToken{Name: tf[0], CreatedAt: tf[1]}
			if len(tf) > 2 {
				tk.Revoked = tf[2] != "t"
			}
			me.Tokens = append(me.Tokens, tk)
		}
	}

	writeJSON(w, http.StatusOK, me)
}

func (s *Server) handlePortalCreateToken(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return
	}
	s.createTokenCore(w, r, userID)
}

type portalRevokeReq struct {
	TokenID string `json:"token_id"`
	Name    string `json:"name"`
}

func (s *Server) handlePortalRevokeToken(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return
	}
	var req portalRevokeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	tokenID := req.TokenID
	if pooldb.UUID(tokenID) == "NULL" && req.Name != "" {
		id, err := s.db.FirstString(r.Context(), fmt.Sprintf(
			"select id from user_tokens where user_id = %s and name = %s",
			pooldb.UUID(userID), pooldb.S(req.Name)))
		if err != nil || id == "" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		tokenID = id
	}
	s.revokeTokenCore(w, r, userID, tokenID)
}

// --- settings ---------------------------------------------------------------

type portalSettings struct {
	PriorityModels []string `json:"priority_models"`
}

func (s *Server) handlePortalGetSettings(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return
	}
	row, err := s.db.FirstString(r.Context(), fmt.Sprintf(
		"select array_to_string(priority_models, chr(1)) from user_settings where user_id = %s",
		pooldb.UUID(userID)))
	settings := portalSettings{PriorityModels: []string{}}
	if err == nil && row != "" {
		settings.PriorityModels = strings.Split(row, "\x01")
	}
	writeJSON(w, http.StatusOK, settings)
}

func (s *Server) handlePortalPostSettings(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return
	}
	var req portalSettings
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(req.PriorityModels) > maxPriorityModels {
		http.Error(w, "too many models", http.StatusBadRequest)
		return
	}
	for _, m := range req.PriorityModels {
		if !freeTextOK(m, 64) {
			http.Error(w, "bad model name", http.StatusBadRequest)
			return
		}
	}
	if err := s.db.Exec(r.Context(), fmt.Sprintf(
		"insert into user_settings (user_id, priority_models) values (%s, %s) "+
			"on conflict (user_id) do update set priority_models = excluded.priority_models, updated_at = now()",
		pooldb.UUID(userID), textArrayLiteral(req.PriorityModels))); err != nil {
		http.Error(w, "failed to save settings", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, req)
}

// --- admin mirror -----------------------------------------------------------

// portalAdmin authenticates the cookie session and requires role='admin'.
func (s *Server) portalAdmin(w http.ResponseWriter, r *http.Request) (string, bool) {
	userID, ok := s.portalSessionUser(w, r)
	if !ok {
		return "", false
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return "", false
	}
	return userID, true
}

type portalAdminUser struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	ActivatedAt string `json:"activated_at,omitempty"`
	Routers     int    `json:"routers"`
}

func (s *Server) handlePortalAdminUsers(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.portalAdmin(w, r); !ok {
		return
	}
	rows, err := s.db.Query(r.Context(), `
select u.id, u.email, u.display_name, u.role, u.status, u.created_at,
       coalesce(u.activated_at::text, ''),
       (select count(*) from routers rt where rt.owner_id = u.id and rt.deleted_at is null)
from users u order by u.created_at desc`)
	if err != nil {
		http.Error(w, "failed to list users", http.StatusInternalServerError)
		return
	}
	users := []portalAdminUser{}
	for _, row := range rows {
		f := strings.Split(row, ",")
		if len(f) < 7 {
			continue
		}
		u := portalAdminUser{ID: f[0], Email: f[1], DisplayName: f[2], Role: f[3], Status: f[4], CreatedAt: f[5]}
		if f[6] != "" {
			u.ActivatedAt = f[6]
		}
		if n, err := fmt.Sscanf(f[7], "%d", &u.Routers); err != nil || n != 1 {
			u.Routers = 0
		}
		users = append(users, u)
	}
	writeJSON(w, http.StatusOK, struct {
		Users []portalAdminUser `json:"users"`
	}{users})
}

func (s *Server) handlePortalAdminInvite(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalAdmin(w, r)
	if !ok {
		return
	}
	s.adminInviteUser(w, r, userID)
}

func (s *Server) handlePortalAdminSuspend(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalAdmin(w, r)
	if !ok {
		return
	}
	s.adminSuspendUser(w, r, userID)
}

func (s *Server) handlePortalAdminActivate(w http.ResponseWriter, r *http.Request) {
	userID, ok := s.portalAdmin(w, r)
	if !ok {
		return
	}
	s.adminActivateUser(w, r, userID)
}

// --- static serving ---------------------------------------------------------

// portalFS is the embedded SvelteKit build (web/build); the sub error is
// build-time (the placeholder guarantees the tree exists).
var portalFS = func() fs.FS {
	f, err := fs.Sub(web.FS, "build")
	if err != nil {
		panic("web: no build tree: " + err.Error())
	}
	return f
}()

// portalIndex is the SPA entry document, read once.
var portalIndex = sync.OnceValue(func() []byte {
	b, err := web.FS.ReadFile("build/index.html")
	if err != nil {
		return []byte("<!doctype html><title>pool portal</title><p>portal not built</p>")
	}
	return b
})

// handlePortalStatic serves the embedded SPA (D-N4): a real file when the
// path names one, index.html for every other non-API path (client-side
// routing). API prefixes stay 404 so a typo never looks like the portal.
func (s *Server) handlePortalStatic(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if strings.HasPrefix(p, "/api/") || strings.HasPrefix(p, "/pool/") || strings.HasPrefix(p, "/portal/") {
		http.NotFound(w, r)
		return
	}
	rel := strings.TrimPrefix(p, "/")
	if rel == "" {
		rel = "index.html"
	}
	if f, err := fs.Stat(portalFS, rel); err == nil && !f.IsDir() {
		http.ServeFileFS(w, r, portalFS, rel)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(portalIndex())
}
