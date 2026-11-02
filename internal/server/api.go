package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"pool-coordinator/internal/pooldb"
)

// --- admin handlers -------------------------------------------------------

func (s *Server) handleAdminInviteUser(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var req inviteUserReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if req.Email == "" || !strings.Contains(req.Email, "@") || len(req.Email) > 320 {
		http.Error(w, "bad email", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	token := newToken()
	tokenHash := sha256hex(token)

	script := fmt.Sprintf(`
insert into users (email, display_name, role, status)
values (%s, %s, 'member', 'invited');
insert into user_tokens (user_id, name, token_hash)
select id, 'default', %s from users where email = %s;`,
		pooldb.S(req.Email), pooldb.S(req.DisplayName), pooldb.S(tokenHash), pooldb.S(req.Email))

	if err := s.db.ExecMulti(ctx, script); err != nil {
		http.Error(w, "failed to invite user", http.StatusInternalServerError)
		return
	}

	rows, err := s.db.Query(ctx, fmt.Sprintf("select id, email, display_name, role, status, created_at, activated_at from users where email = %s", pooldb.S(req.Email)))
	if err != nil || len(rows) == 0 {
		http.Error(w, "failed to fetch user", http.StatusInternalServerError)
		return
	}
	f := strings.Split(rows[0], ",")
	u := userResponse{
		ID:          f[0],
		Email:       f[1],
		DisplayName: f[2],
		Role:        f[3],
		Status:      f[4],
		CreatedAt:   f[5],
	}
	if len(f) > 6 && f[6] != "" && f[6] != "NULL" {
		u.ActivatedAt = f[6]
	}

	writeJSON(w, http.StatusCreated, inviteUserResp{User: u, Token: token})
}

func (s *Server) handleAdminListUsers(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	ctx := r.Context()
	rows, err := s.db.Query(ctx, "select id, email, display_name, role, status, created_at, activated_at from users order by created_at desc")
	if err != nil {
		http.Error(w, "failed to list users", http.StatusInternalServerError)
		return
	}

	var users []userWithTokens
	for _, row := range rows {
		f := strings.Split(row, ",")
		u := userWithTokens{
			userResponse: userResponse{
				ID:          f[0],
				Email:       f[1],
				DisplayName: f[2],
				Role:        f[3],
				Status:      f[4],
				CreatedAt:   f[5],
			},
		}
		if len(f) > 6 && f[6] != "" && f[6] != "NULL" {
			u.ActivatedAt = f[6]
		}

		trows, err := s.db.Query(ctx, fmt.Sprintf("select name, created_at, revoked_at from user_tokens where user_id = %s order by created_at desc", pooldb.UUID(f[0])))
		if err == nil {
			for _, trow := range trows {
				tf := strings.Split(trow, ",")
				ut := tokenResponse{
					Name:       tf[0],
					CreatedAt:  tf[1],
				}
				if len(tf) > 2 && tf[2] != "" && tf[2] != "NULL" {
					ut.RevokedAt = tf[2]
				}
				u.Tokens = append(u.Tokens, ut)
			}
		}
		users = append(users, u)
	}

	writeJSON(w, http.StatusOK, listUsersResp{Users: users})
}

func (s *Server) handleAdminSuspendUser(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	id := r.PathValue("id")
	if pooldb.UUID(id) == "NULL" {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	isAdmin, err := s.db.FirstBool(ctx, fmt.Sprintf("select 1 from users where id = %s and role = 'admin'", pooldb.UUID(id)))
	if err == nil && isAdmin {
		http.Error(w, "cannot suspend admin", http.StatusConflict)
		return
	}

	status, err := s.db.FirstString(ctx, fmt.Sprintf("select status from users where id = %s", pooldb.UUID(id)))
	if err != nil || status == "" {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if status == "suspended" {
		http.Error(w, "already suspended", http.StatusConflict)
		return
	}

	if err := s.db.Exec(ctx, fmt.Sprintf("update users set status = 'suspended' where id = %s", pooldb.UUID(id))); err != nil {
		http.Error(w, "failed to suspend", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAdminActivateUser(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	id := r.PathValue("id")
	if pooldb.UUID(id) == "NULL" {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	status, err := s.db.FirstString(ctx, fmt.Sprintf("select status from users where id = %s", pooldb.UUID(id)))
	if err != nil || status == "" {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if status == "active" {
		http.Error(w, "already active", http.StatusConflict)
		return
	}

	if err := s.db.Exec(ctx, fmt.Sprintf("update users set status = 'active', activated_at = now() where id = %s", pooldb.UUID(id))); err != nil {
		http.Error(w, "failed to activate", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAdminRevokeToken(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.isAdmin(r.Context(), userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var req revokeTokenReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if pooldb.UUID(req.TokenID) == "NULL" {
		http.Error(w, "bad token id", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := s.db.Exec(ctx, fmt.Sprintf("update user_tokens set revoked_at = now() where id = %s", pooldb.UUID(req.TokenID))); err != nil {
		http.Error(w, "failed to revoke", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- self-service handlers -----------------------------------------------

func (s *Server) handleSelfActivate(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	status, err := s.db.FirstString(ctx, fmt.Sprintf("select status from users where id = %s", pooldb.UUID(userID)))
	if err != nil || status == "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if status == "active" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if status == "suspended" {
		http.Error(w, "suspended", http.StatusForbidden)
		return
	}
	if status != "invited" {
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}

	if err := s.db.Exec(ctx, fmt.Sprintf("update users set status = 'active', activated_at = now() where id = %s", pooldb.UUID(userID))); err != nil {
		http.Error(w, "failed to activate", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	rows, err := s.db.Query(ctx, fmt.Sprintf("select id, email, display_name, role, status, created_at, activated_at from users where id = %s", pooldb.UUID(userID)))
	if err != nil || len(rows) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	f := strings.Split(rows[0], ",")
	u := userResponse{
		ID:          f[0],
		Email:       f[1],
		DisplayName: f[2],
		Role:        f[3],
		Status:      f[4],
		CreatedAt:   f[5],
	}
	if len(f) > 6 && f[6] != "" && f[6] != "NULL" {
		u.ActivatedAt = f[6]
	}

	var tokens []tokenResponse
	trows, err := s.db.Query(ctx, fmt.Sprintf("select name, created_at, revoked_at from user_tokens where user_id = %s order by created_at desc", pooldb.UUID(userID)))
	if err == nil {
		for _, trow := range trows {
			tf := strings.Split(trow, ",")
			ut := tokenResponse{
				Name:       tf[0],
				CreatedAt:  tf[1],
			}
			if len(tf) > 2 && tf[2] != "" && tf[2] != "NULL" {
				ut.RevokedAt = tf[2]
			}
			tokens = append(tokens, ut)
		}
	}

	writeJSON(w, http.StatusOK, meResponse{User: u, Tokens: tokens})
}

func (s *Server) handleMeCreateToken(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	n, err := s.db.Count(ctx, fmt.Sprintf("select count(*) from user_tokens where user_id = %s and name = %s", pooldb.UUID(userID), pooldb.S(req.Name)))
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	if n > 0 {
		http.Error(w, "name taken", http.StatusConflict)
		return
	}

	token := newToken()
	tokenHash := sha256hex(token)

	if err := s.db.Exec(ctx, fmt.Sprintf("insert into user_tokens (user_id, name, token_hash) values (%s, %s, %s)",
		pooldb.UUID(userID), pooldb.S(req.Name), pooldb.S(tokenHash))); err != nil {
		http.Error(w, "failed to create token", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, struct {
		Token string `json:"token"`
	}{Token: token})
}

func (s *Server) handleMeRevokeToken(w http.ResponseWriter, r *http.Request) {
	userID, err := s.authUser(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req revokeTokenReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	n, err := s.db.FirstBool(ctx, fmt.Sprintf("select 1 from user_tokens where id = %s and user_id = %s", pooldb.UUID(req.TokenID), pooldb.UUID(userID)))
	if err != nil || !n {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := s.db.Exec(ctx, fmt.Sprintf("update user_tokens set revoked_at = now() where id = %s", pooldb.UUID(req.TokenID))); err != nil {
		http.Error(w, "failed to revoke", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
