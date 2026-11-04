package server

// Portal tests (WS-14): cookie sessions (login/logout/expiry/sweep), the
// member endpoints, the admin mirror, the SPA fallback, and the anti-memory
// canary (no token plaintext in logs).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"pool-coordinator/internal/pooldb"
	"pool-coordinator/protocol/go/poolproto"
)

// portalLogin logs in with an opaque token and returns the session cookie.
func portalLogin(t *testing.T, baseURL, token string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"token": token})
	req, _ := http.NewRequest("POST", baseURL+"/portal/login", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("portal login request: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, ""
	}
	for _, c := range resp.Cookies() {
		if c.Name == "pool_session" {
			return resp.StatusCode, c.Value
		}
	}
	t.Fatal("login response has no pool_session cookie")
	return resp.StatusCode, ""
}

// portalDo issues a portal request carrying the session cookie.
func portalDo(t *testing.T, method, url, cookie string, body any) (*http.Response, string) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cookie != "" {
		req.Header.Set("Cookie", "pool_session="+cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, string(raw)
}

func TestPortal_LoginHappyPath(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "login@example.com", "Login User", "member", "active")

	// Login and inspect the Set-Cookie attributes.
	body, _ := json.Marshal(map[string]string{"token": token})
	req, _ := http.NewRequest("POST", env.baseURL+"/portal/login", bytes.NewReader(body))
	loginResp, err := http.DefaultClient.Do(req)
	if err != nil || loginResp.StatusCode != http.StatusOK {
		t.Fatalf("login = %d %v", loginResp.StatusCode, err)
	}
	var sc string
	for _, c := range loginResp.Cookies() {
		if c.Name == "pool_session" {
			sc = c.String()
		}
	}
	if !strings.Contains(sc, "HttpOnly") || !strings.Contains(sc, "SameSite=Lax") || !strings.Contains(sc, "Path=/") {
		io.Copy(io.Discard, loginResp.Body)
		loginResp.Body.Close()
		t.Fatalf("cookie attrs wrong: %s", sc)
	}
	cookie := ""
	for _, c := range loginResp.Cookies() {
		if c.Name == "pool_session" {
			cookie = c.Value
		}
	}
	io.Copy(io.Discard, loginResp.Body)
	loginResp.Body.Close()
	if len(cookie) != 64 {
		t.Fatalf("bad session cookie: %q", cookie)
	}

	// A session row exists, unexpired.
	n, err := env.db.Count(ctx, "select count(*) from portal_sessions")
	if err != nil || n != 1 {
		t.Fatalf("portal_sessions rows = %d (err %v), want 1", n, err)
	}

	// /portal/me works with the cookie.
	resp3, raw := portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("/portal/me status = %d, want 200", resp3.StatusCode)
	}
	var me portalMeResp
	if err := json.Unmarshal([]byte(raw), &me); err != nil {
		t.Fatalf("decode me: %v", err)
	}
	if me.User.Email != "login@example.com" || me.User.Status != "active" {
		t.Fatalf("me = %+v", me.User)
	}

	// No cookie -> 401.
	resp4, _ := portalDo(t, "GET", env.baseURL+"/portal/me", "", nil)
	if resp4.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-cookie /portal/me = %d, want 401", resp4.StatusCode)
	}
}

func TestPortal_LoginRejected(t *testing.T) {
	env := newE2E(t)

	// suspended user: 401, no session row
	_, suspTok := setupAuthUser(t, env, "susplogin@example.com", "Susp Login", "member", "suspended")
	if code, _ := portalLogin(t, env.baseURL, suspTok); code != http.StatusUnauthorized {
		t.Fatalf("suspended login = %d, want 401", code)
	}

	// invited (not activated) user: 401
	_, invTok := setupAuthUser(t, env, "invlogin@example.com", "Inv Login", "member", "invited")
	if code, _ := portalLogin(t, env.baseURL, invTok); code != http.StatusUnauthorized {
		t.Fatalf("invited login = %d, want 401", code)
	}

	// garbage token: 401
	if code, _ := portalLogin(t, env.baseURL, "not-a-token"); code != http.StatusUnauthorized {
		t.Fatalf("garbage login = %d, want 401", code)
	}

	// revoked token: 401
	_, revTok := setupAuthUser(t, env, "revlogin@example.com", "Rev Login", "member", "active")
	revID, _ := env.db.FirstString(context.Background(), "select id from user_tokens where user_id = (select id from users where email = 'revlogin@example.com')")
	if err := env.db.Exec(context.Background(), fmt.Sprintf("update user_tokens set revoked_at = now() where id = %s", pooldb.UUID(revID))); err != nil {
		t.Fatal(err)
	}
	if code, _ := portalLogin(t, env.baseURL, revTok); code != http.StatusUnauthorized {
		t.Fatalf("revoked login = %d, want 401", code)
	}

	// zero session rows created
	n, _ := env.db.Count(context.Background(), "select count(*) from portal_sessions")
	if n != 0 {
		t.Fatalf("sessions after rejected logins = %d, want 0", n)
	}
}

func TestPortal_ExpiredSession(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "expire@example.com", "Expire", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)

	// Age the session past its TTL.
	if err := env.db.Exec(ctx, "update portal_sessions set expires_at = now() - interval '1 minute'"); err != nil {
		t.Fatal(err)
	}

	// Expired = 401.
	resp, _ := portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired session = %d, want 401", resp.StatusCode)
	}

	// The housekeeping sweep (dispatcher tick) deletes the row.
	deadline := time.Now().Add(15 * time.Second)
	for {
		n, err := env.db.Count(ctx, "select count(*) from portal_sessions where expires_at < now()")
		if err == nil && n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired session row not swept (n=%d err=%v)", n, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestPortal_Logout(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "logout@example.com", "Logout", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)

	// Row exists.
	n, _ := env.db.Count(ctx, "select count(*) from portal_sessions")
	if n != 1 {
		t.Fatalf("sessions before logout = %d, want 1", n)
	}

	resp, _ := portalDo(t, "POST", env.baseURL+"/portal/logout", cookie, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status = %d, want 200", resp.StatusCode)
	}
	// Cookie cleared in the response.
	var cleared bool
	for _, c := range resp.Cookies() {
		if c.Name == "pool_session" && (c.MaxAge < 0 || c.Value == "") {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("logout did not clear the cookie")
	}

	// Row gone; the session no longer authenticates.
	n, _ = env.db.Count(ctx, "select count(*) from portal_sessions")
	if n != 0 {
		t.Fatalf("sessions after logout = %d, want 0", n)
	}
	if resp2, _ := portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil); resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("post-logout /portal/me = %d, want 401", resp2.StatusCode)
	}

	// Logout with no session is a 200 no-op.
	if resp3, _ := portalDo(t, "POST", env.baseURL+"/portal/logout", "", nil); resp3.StatusCode != http.StatusOK {
		t.Fatalf("logout no-op = %d, want 200", resp3.StatusCode)
	}
}

func TestPortal_SuspendedSessionDies(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "suspsession@example.com", "Susp Session", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)

	if resp, _ := portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("pre-suspend /portal/me = %d, want 200", resp.StatusCode)
	}

	// Admin suspends the user (via the /api core; same transition).
	memID, _ := env.db.FirstString(ctx, "select id from users where email = 'suspsession@example.com'")
	req, _ := http.NewRequest("POST", env.baseURL+"/api/admin/users/"+memID+"/suspend", nil)
	req.Header.Set("Authorization", "Bearer admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("suspend failed: %d %v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// The live session is dead.
	if resp2, _ := portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil); resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("suspended session /portal/me = %d, want 401", resp2.StatusCode)
	}
}

func TestPortal_MeContent(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "mecontent@example.com", "Me Content", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)

	// A router with a loaded model, then an exclusion mark.
	fr := NewFakeRouter(env.baseURL, token, "me-router", "fake")
	fr.LoadModel("me-model", []string{"pool-text-md"}, 32768, 1, 0, poolproto.ModelLoaded)
	if err := fr.Connect(ctx); err != nil {
		t.Fatalf("fake router connect: %v", err)
	}
	if err := fr.PostExclusion(ctx, "me-model", true); err != nil {
		t.Fatalf("post exclusion: %v", err)
	}

	resp, raw := portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/portal/me = %d, want 200", resp.StatusCode)
	}
	var me portalMeResp
	if err := json.Unmarshal([]byte(raw), &me); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(me.Routers) != 1 || me.Routers[0].ID != fr.ID() || me.Routers[0].Status != "online" {
		t.Fatalf("routers = %+v", me.Routers)
	}
	if len(me.Models) != 1 {
		t.Fatalf("models = %+v", me.Models)
	}
	m := me.Models[0]
	if m.Name != "me-model" || !m.Loaded || len(m.Tags) != 1 || m.Tags[0] != "pool-text-md" || !m.Excluded {
		t.Fatalf("model = %+v", m)
	}
	if len(me.Tokens) < 1 || me.Tokens[0].Name != "default" {
		t.Fatalf("tokens = %+v", me.Tokens)
	}

	// Anti-memory: no hash, no plaintext in the body.
	if strings.Contains(raw, "token_hash") || strings.Contains(raw, token) || strings.Contains(raw, sha256hex(token)) {
		t.Fatal("/portal/me leaked a token hash or plaintext")
	}
}

func TestPortal_Tokens(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "ptok@example.com", "P Tok", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)

	// Create.
	resp, raw := portalDo(t, "POST", env.baseURL+"/portal/tokens", cookie, map[string]string{"name": "portal-tok"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create = %d, want 201", resp.StatusCode)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(raw), &created); err != nil || created.Token == "" {
		t.Fatalf("create body = %s", raw)
	}

	// The new token authenticates router-style (bearer on /api/me).
	req, _ := http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+created.Token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("bearer /api/me = %d %v", resp2.StatusCode, err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()

	// Duplicate name -> 409; bad name -> 400.
	if resp3, _ := portalDo(t, "POST", env.baseURL+"/portal/tokens", cookie, map[string]string{"name": "portal-tok"}); resp3.StatusCode != http.StatusConflict {
		t.Fatalf("dup name = %d, want 409", resp3.StatusCode)
	}
	if resp4, _ := portalDo(t, "POST", env.baseURL+"/portal/tokens", cookie, map[string]string{"name": "a,b"}); resp4.StatusCode != http.StatusBadRequest {
		t.Fatalf("comma name = %d, want 400", resp4.StatusCode)
	}

	// Revoke by name, then by id; re-revoke is a 200 no-op; unknown is 404.
	tokID, _ := env.db.FirstString(ctx, "select id from user_tokens where user_id = (select id from users where email = 'ptok@example.com') and name = 'portal-tok'")
	if resp5, _ := portalDo(t, "POST", env.baseURL+"/portal/tokens/revoke", cookie, map[string]string{"name": "portal-tok"}); resp5.StatusCode != http.StatusOK {
		t.Fatalf("revoke by name = %d, want 200", resp5.StatusCode)
	}
	if resp6, _ := portalDo(t, "POST", env.baseURL+"/portal/tokens/revoke", cookie, map[string]string{"token_id": tokID}); resp6.StatusCode != http.StatusOK {
		t.Fatalf("re-revoke = %d, want 200", resp6.StatusCode)
	}
	if resp7, _ := portalDo(t, "POST", env.baseURL+"/portal/tokens/revoke", cookie, map[string]string{"token_id": "00000000-0000-0000-0000-000000000000"}); resp7.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown revoke = %d, want 404", resp7.StatusCode)
	}
	// Unknown name -> 404.
	if resp8, _ := portalDo(t, "POST", env.baseURL+"/portal/tokens/revoke", cookie, map[string]string{"name": "no-such"}); resp8.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown name revoke = %d, want 404", resp8.StatusCode)
	}

	// The revoked token is dead.
	req2, _ := http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req2.Header.Set("Authorization", "Bearer "+created.Token)
	resp9, _ := http.DefaultClient.Do(req2)
	if resp9.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked token /api/me = %d, want 401", resp9.StatusCode)
	}
	resp9.Body.Close()
}

func TestPortal_Settings(t *testing.T) {
	env := newE2E(t)

	_, token := setupAuthUser(t, env, "settings@example.com", "Settings", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)

	// Empty at first.
	resp, raw := portalDo(t, "GET", env.baseURL+"/portal/settings", cookie, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get settings = %d", resp.StatusCode)
	}
	var got portalSettings
	if err := json.Unmarshal([]byte(raw), &got); err != nil || len(got.PriorityModels) != 0 {
		t.Fatalf("initial settings = %s (%v)", raw, err)
	}

	// Save a list.
	models := []string{"model-a", "model b", "model-c"}
	resp, raw = portalDo(t, "POST", env.baseURL+"/portal/settings", cookie, portalSettings{PriorityModels: models})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post settings = %d (%s)", resp.StatusCode, raw)
	}
	resp, raw = portalDo(t, "GET", env.baseURL+"/portal/settings", cookie, nil)
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.PriorityModels) != 3 || got.PriorityModels[1] != "model b" {
		t.Fatalf("saved list = %v", got.PriorityModels)
	}

	// Bad entries: comma (freeTextOK) and over-cap list.
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/settings", cookie, portalSettings{PriorityModels: []string{"a,b"}}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("comma model = %d, want 400", resp.StatusCode)
	}
	tooMany := make([]string, maxPriorityModels+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("m%d", i)
	}
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/settings", cookie, portalSettings{PriorityModels: tooMany}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("over-cap list = %d, want 400", resp.StatusCode)
	}

	// Clear with an empty list.
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/settings", cookie, portalSettings{PriorityModels: []string{}}); resp.StatusCode != http.StatusOK {
		t.Fatalf("clear = %d, want 200", resp.StatusCode)
	}
	if resp, raw = portalDo(t, "GET", env.baseURL+"/portal/settings", cookie, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("get after clear = %d", resp.StatusCode)
	} else if err := json.Unmarshal([]byte(raw), &got); err != nil || len(got.PriorityModels) != 0 {
		t.Fatalf("cleared settings = %s", raw)
	}
}

func TestPortal_AdminGate(t *testing.T) {
	env := newE2E(t)

	// Member session -> 403 on the admin mirror.
	_, token := setupAuthUser(t, env, "gate@example.com", "Gate", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)
	if resp, _ := portalDo(t, "GET", env.baseURL+"/portal/admin/users", cookie, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member admin list = %d, want 403", resp.StatusCode)
	}
	body, _ := json.Marshal(inviteUserReq{Email: "x@example.com", DisplayName: "X"})
	req, _ := http.NewRequest("POST", env.baseURL+"/portal/admin/users/invite", bytes.NewReader(body))
	req.Header.Set("Cookie", "pool_session="+cookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member invite = %d %v, want 403", resp.StatusCode, err)
	}
	resp.Body.Close()

	// No session -> 401 (before the role check).
	if resp, _ := portalDo(t, "GET", env.baseURL+"/portal/admin/users", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-session admin list = %d, want 401", resp.StatusCode)
	}

	// Admin session (bootstrap token) -> 200.
	_, adminCookie := portalLogin(t, env.baseURL, "admin")
	resp, raw := portalDo(t, "GET", env.baseURL+"/portal/admin/users", adminCookie, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin list = %d, want 200", resp.StatusCode)
	}
	var list struct {
		Users []portalAdminUser `json:"users"`
	}
	if err := json.Unmarshal([]byte(raw), &list); err != nil || len(list.Users) < 2 {
		t.Fatalf("admin list = %s (%v)", raw, err)
	}
}

func TestPortal_AdminSuspendActivate(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "susptarget@example.com", "Susp Target", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, token)
	_, adminCookie := portalLogin(t, env.baseURL, "admin")

	memberID, _ := env.db.FirstString(ctx, "select id from users where email = 'susptarget@example.com'")
	adminID, _ := env.db.FirstString(ctx, "select id from users where role = 'admin' limit 1")

	// Suspend the member; their live session dies.
	resp, _ := portalDo(t, "POST", env.baseURL+"/portal/admin/users/"+memberID+"/suspend", adminCookie, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("suspend = %d, want 200", resp.StatusCode)
	}
	if resp, _ = portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("suspended session = %d, want 401", resp.StatusCode)
	}

	// Re-suspend -> 400; suspend admin -> 400; suspend self -> 400; unknown -> 404.
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/admin/users/"+memberID+"/suspend", adminCookie, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("re-suspend = %d, want 400", resp.StatusCode)
	}
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/admin/users/"+adminID+"/suspend", adminCookie, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("suspend admin = %d, want 400", resp.StatusCode)
	}
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/admin/users/00000000-0000-0000-0000-000000000000/suspend", adminCookie, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("suspend unknown = %d, want 404", resp.StatusCode)
	}

	// Activate; double-activate -> 409.
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/admin/users/"+memberID+"/activate", adminCookie, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("activate = %d, want 200", resp.StatusCode)
	}
	if resp, _ = portalDo(t, "POST", env.baseURL+"/portal/admin/users/"+memberID+"/activate", adminCookie, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("double activate = %d, want 409", resp.StatusCode)
	}
}

func TestPortal_AdminInviteOnce(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, adminCookie := portalLogin(t, env.baseURL, "admin")

	resp, raw := portalDo(t, "POST", env.baseURL+"/portal/admin/users/invite", adminCookie,
		inviteUserReq{Email: "ported@example.com", DisplayName: "Ported"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("invite = %d (%s), want 201", resp.StatusCode, raw)
	}
	var res inviteUserResp
	if err := json.Unmarshal([]byte(raw), &res); err != nil || res.Token == "" || len(res.Token) != 64 {
		t.Fatalf("invite body = %s (%v)", raw, err)
	}

	// Plaintext appears EXACTLY once in the response.
	if n := strings.Count(raw, res.Token); n != 1 {
		t.Fatalf("plaintext token count in invite response = %d, want 1", n)
	}
	// No hash in the response.
	if strings.Contains(raw, sha256hex(res.Token)) {
		t.Fatal("invite response contains the token hash")
	}

	// The invited user activates with that token, then it authenticates
	// router-style (bearer).
	req, _ := http.NewRequest("POST", env.baseURL+"/api/auth/activate", nil)
	req.Header.Set("Authorization", "Bearer "+res.Token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil || resp2.StatusCode != http.StatusOK {
		t.Fatalf("activate = %d %v", resp2.StatusCode, err)
	}
	resp2.Body.Close()

	req2, _ := http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req2.Header.Set("Authorization", "Bearer "+res.Token)
	resp3, _ := http.DefaultClient.Do(req2)
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("invited token /api/me = %d, want 200", resp3.StatusCode)
	}
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()

	// The user shows in the portal admin list with the right status.
	_, raw = portalDo(t, "GET", env.baseURL+"/portal/admin/users", adminCookie, nil)
	var list struct {
		Users []portalAdminUser `json:"users"`
	}
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range list.Users {
		if u.Email == "ported@example.com" {
			found = true
			if u.Status != "active" {
				t.Fatalf("ported status = %s, want active", u.Status)
			}
		}
	}
	if !found {
		t.Fatal("ported user not in the portal list")
	}

	// Duplicate email -> 409.
	if resp4, _ := portalDo(t, "POST", env.baseURL+"/portal/admin/users/invite", adminCookie,
		inviteUserReq{Email: "ported@example.com", DisplayName: "Ported"}); resp4.StatusCode != http.StatusConflict {
		t.Fatalf("dup invite = %d, want 409", resp4.StatusCode)
	}
	_ = ctx
}

func TestPortal_AntiMemory(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	// Distinct plaintexts that must never reach the logs: the login token,
	// a portal-created token, and a portal-invite token.
	_, loginTok := setupAuthUser(t, env, "antimem@example.com", "Anti Mem", "member", "active")
	_, cookie := portalLogin(t, env.baseURL, loginTok)
	_, raw := portalDo(t, "POST", env.baseURL+"/portal/tokens", cookie, map[string]string{"name": "canary-tok"})
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(raw), &created); err != nil {
		t.Fatal(err)
	}
	_, adminCookie := portalLogin(t, env.baseURL, "admin")
	_, raw = portalDo(t, "POST", env.baseURL+"/portal/admin/users/invite", adminCookie,
		inviteUserReq{Email: "antimem2@example.com", DisplayName: "Anti Mem 2"})
	var invited inviteUserResp
	if err := json.Unmarshal([]byte(raw), &invited); err != nil {
		t.Fatal(err)
	}

	// Exercise a little more (me, settings) so any logging path would show.
	portalDo(t, "GET", env.baseURL+"/portal/me", cookie, nil)
	portalDo(t, "POST", env.baseURL+"/portal/settings", cookie, portalSettings{PriorityModels: []string{"m1"}})

	logs := env.logs.String()
	for _, secret := range []string{loginTok, created.Token, invited.Token} {
		if strings.Contains(logs, secret) {
			t.Fatalf("token plaintext found in coordinator logs (anti-memory violation):\n%s",
				truncate([]byte(logs), 4000))
		}
	}
	_ = ctx
}

func TestPortal_SPAStatic(t *testing.T) {
	env := newE2E(t)

	get := func(path string) (int, string, string) {
		resp, raw := portalDo(t, "GET", env.baseURL+path, "", nil)
		ct := resp.Header.Get("Content-Type")
		return resp.StatusCode, ct, raw
	}

	// / serves the SPA (text/html).
	code, ct, raw := get("/")
	if code != http.StatusOK || !strings.Contains(ct, "text/html") || raw == "" {
		t.Fatalf("GET / = %d %q len=%d", code, ct, len(raw))
	}

	// SPA routes fall back to index.html.
	for _, p := range []string{"/dashboard", "/settings", "/admin"} {
		code, ct, _ := get(p)
		if code != http.StatusOK || !strings.Contains(ct, "text/html") {
			t.Fatalf("GET %s = %d %q", p, code, ct)
		}
	}

	// API prefixes stay 404 (a typo never looks like the portal).
	for _, p := range []string{"/api/nope", "/pool/nope", "/portal/nope"} {
		code, _, _ := get(p)
		if code != http.StatusNotFound {
			t.Fatalf("GET %s = %d, want 404", p, code)
		}
	}

	// Banked routes are untouched.
	resp, _ := portalDo(t, "GET", env.baseURL+"/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", resp.StatusCode)
	}
}
