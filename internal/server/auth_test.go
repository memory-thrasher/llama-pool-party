package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"pool-coordinator/internal/pooldb"
)

func setupAuthUser(t *testing.T, env *e2eEnv, email, displayName, role, status string) (string, string) {
	t.Helper()
	ctx := context.Background()
	
	// 1. Create user
	err := env.db.Exec(ctx, fmt.Sprintf("insert into users (email, display_name, role, status) values (%s, %s, %s, %s)",
		pooldb.S(email), pooldb.S(displayName), pooldb.S(role), pooldb.S(status)))
	if err != nil {
		t.Fatalf("setupAuthUser: failed to create user: %v", err)
	}
	
	userID, err := env.db.FirstString(ctx, fmt.Sprintf("select id from users where email = %s", pooldb.S(email)))
	if err != nil || userID == "" {
		t.Fatalf("setupAuthUser: failed to get user id: %v", err)
	}

	// 2. Create token
	token := newToken()
	tokenHash := sha256hex(token)
	err = env.db.Exec(ctx, fmt.Sprintf("insert into user_tokens (user_id, name, token_hash) values (%s, 'default', %s)",
		pooldb.UUID(userID), pooldb.S(tokenHash)))
	if err != nil {
		t.Fatalf("setupAuthUser: failed to create token: %v", err)
	}

	return userID, token
}

func TestAuth_InviteFlow(t *testing.T) {
	env := newE2E(t)

	// Admin invites user
	reqBody, _ := json.Marshal(inviteUserReq{Email: "new@example.com", DisplayName: "New User"})
	req, _ := http.NewRequest("POST", env.baseURL+"/api/admin/users/invite", io.NopCloser(bytes.NewReader(reqBody)))
	req.Header.Set("Authorization", "Bearer admin")
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("invite failed: status=%d, err=%v", resp.StatusCode, err)
	}

	var res inviteUserResp
	json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()

	if res.Token == "" || len(res.Token) != 64 {
		t.Fatalf("invalid token: %s", res.Token)
	}

	// That token authenticates on GET /api/me
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+res.Token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("auth failed: status=%d, err=%v", resp.StatusCode, err)
	}

	var meRes meResponse
	json.NewDecoder(resp.Body).Decode(&meRes)
	resp.Body.Close()

	if meRes.User.Email != "new@example.com" {
		t.Fatalf("wrong user: %s", meRes.User.Email)
	}

	// Verify no token_hash in response
	// Since we are decoding into a struct, we can't easily check for unexpected fields
	// in a simple way without re-encoding or using a map.
	// Let's use a map to check for the presence of "token_hash" in the raw body.
	
	// Re-do the request with a buffer to check raw body
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+res.Token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	resp.Body.Close()

	if strings.Contains(buf.String(), "token_hash") {
		t.Fatal("response contains token_hash")
	}
}

func TestAuth_Activate(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "invited@example.com", "Invited User", "member", "invited")

	// POST /api/auth/activate
	req, _ := http.NewRequest("POST", env.baseURL+"/api/auth/activate", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("activate failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Check status
	rows, err := env.db.Query(ctx, "select status from users where email = 'invited@example.com'")
	if err != nil || len(rows) != 1 || strings.Split(rows[0], ",")[0] != "active" {
		t.Fatalf("user not activated: %v", rows)
	}

	// Second call is a 200 no-op
	req, _ = http.NewRequest("POST", env.baseURL+"/api/auth/activate", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("second activate failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()
}

func TestAuth_Suspend(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "member@example.com", "Member User", "member", "active")

	// Admin suspends member
	req, _ := http.NewRequest("POST", env.baseURL+"/api/admin/users/suspend", nil)
	// Wait, we need the ID.
	userID, _ := env.db.FirstString(ctx, "select id from users where email = 'member@example.com'")
	req.URL.Path = "/api/admin/users/" + userID + "/suspend"
	req.Header.Set("Authorization", "Bearer admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("suspend failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Member's token gets 401 on /api/me
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("member should be unauthorized: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Admin activates
	req, _ = http.NewRequest("POST", env.baseURL+"/api/admin/users/"+userID+"/activate", nil)
	req.Header.Set("Authorization", "Bearer admin")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("activate failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Works again
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("member should be authorized: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()
}

func TestAuth_Revoke(t *testing.T) {
	env := newE2E(t)
	ctx := context.Background()

	_, token := setupAuthUser(t, env, "revokeme@example.com", "Revoke Me", "member", "active")

	// Admin revokes token
	tokenID, _ := env.db.FirstString(ctx, "select id from user_tokens where user_id = (select id from users where email = 'revokeme@example.com') and name = 'default'")

	req, _ := http.NewRequest("POST", env.baseURL+"/api/admin/tokens/revoke", nil)
	req.Header.Set("Authorization", "Bearer admin")
	reqBody, _ := json.Marshal(revokeTokenReq{TokenID: tokenID})
	req.Body = io.NopCloser(bytes.NewReader(reqBody))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("admin revoke failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Token gets 401
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("token should be unauthorized: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Self-revoke
	_, token2 := setupAuthUser(t, env, "selfrevoke@example.com", "Self Revoke", "member", "active")
	tokenID2, _ := env.db.FirstString(ctx, "select id from user_tokens where user_id = (select id from users where email = 'selfrevoke@example.com') and name = 'default'")

	req, _ = http.NewRequest("POST", env.baseURL+"/api/me/tokens/revoke", nil)
	req.Header.Set("Authorization", "Bearer "+token2)
	reqBody, _ = json.Marshal(revokeTokenReq{TokenID: tokenID2})
	req.Body = io.NopCloser(bytes.NewReader(reqBody))
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("self revoke failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Token gets 401
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+token2)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("token should be unauthorized: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()
}

func TestAuth_MeTokens(t *testing.T) {
	env := newE2E(t)

	_, token := setupAuthUser(t, env, "metokens@example.com", "Me Tokens", "member", "active")

	// Create named token
	req, _ := http.NewRequest("POST", env.baseURL+"/api/me/tokens", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	reqBody, _ := json.Marshal(map[string]string{"name": "my-token"})
	req.Body = io.NopCloser(bytes.NewReader(reqBody))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("create token failed: status=%d, err=%v", resp.StatusCode, err)
	}
	var res struct {
		Token string `json:"token"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()

	// Authenticates
	req, _ = http.NewRequest("GET", env.baseURL+"/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+res.Token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("new token auth failed: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Duplicate name -> 409
	req, _ = http.NewRequest("POST", env.baseURL+"/api/me/tokens", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	reqBody, _ = json.Marshal(map[string]string{"name": "my-token"})
	req.Body = io.NopCloser(bytes.NewReader(reqBody))
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate name should be 409: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Revoking someone else's token -> 404
	otherTokenID, _ := env.db.FirstString(context.Background(), "select id from user_tokens where name = 'default' and user_id != (select id from users where email = 'metokens@example.com')")
	req, _ = http.NewRequest("POST", env.baseURL+"/api/me/tokens/revoke", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	reqBody, _ = json.Marshal(revokeTokenReq{TokenID: otherTokenID})
	req.Body = io.NopCloser(bytes.NewReader(reqBody))
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("revoking others token should be 404: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()
}

func TestAuth_AdminGate(t *testing.T) {
	env := newE2E(t)

	_, token := setupAuthUser(t, env, "member@example.com", "Member User", "member", "active")

	// Calling any /api/admin/* -> 403
	req, _ := http.NewRequest("GET", env.baseURL+"/api/admin/users", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member calling admin should be 403: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	req, _ = http.NewRequest("POST", env.baseURL+"/api/admin/users/invite", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("member calling admin should be 403: status=%d, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()
}
