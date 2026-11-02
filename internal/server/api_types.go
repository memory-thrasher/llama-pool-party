package server

// API Request/Response types

type inviteUserReq struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

type userResponse struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	ActivatedAt string `json:"activated_at,omitempty"`
	Tokens      []tokenResponse `json:"tokens,omitempty"`
}

type tokenResponse struct {
	Name       string `json:"name"`
	CreatedAt  string `json:"created_at"`
	RevokedAt  string `json:"revoked_at,omitempty"`
}

type inviteUserResp struct {
	User  userResponse `json:"user"`
	Token string       `json:"token"`
}

type listUsersResp struct {
	Users []userWithTokens `json:"users"`
}

type userWithTokens struct {
	userResponse
	Tokens []tokenResponse `json:"tokens"`
}

type revokeTokenReq struct {
	TokenID string `json:"token_id"`
}

type meResponse struct {
	User   userResponse    `json:"user"`
	Tokens []tokenResponse `json:"tokens"`
}
