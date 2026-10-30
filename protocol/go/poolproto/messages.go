package poolproto

// Enum values for protocol v1. Each mirrors a "enum" in ../schemas.

// Priority of a pool job (work_offer, job_intake).
const (
	PriorityLive  = "live"
	PriorityBatch = "batch"
)

// Source of a dispatch: how the job reached dispatch (work_offer).
const (
	SourceQueue = "queue"
	SourceSkip  = "skip"
	SourcePin   = "pin"
)

// Reason for offer_reject.
const (
	RejectModelGone       = "model_gone"
	RejectCtxInsufficient = "ctx_insufficient"
	RejectNoSlot          = "no_slot"
	RejectBusy            = "busy"
	RejectEvicted         = "evicted"
	RejectOther           = "other"
)

// Reason for body_abort.
const (
	AbortModelGone = "model_gone"
	AbortNoSlot    = "no_slot"
	AbortEvicted   = "evicted"
	AbortOther     = "other"
)

// Stage for error. PreToken enables hop-on-error (D-G1).
const (
	StagePreToken  = "pre_token"
	StageMidStream = "mid_stream"
)

// Code for error.
const (
	ErrorModelGone          = "model_gone"
	ErrorOverflow           = "overflow"
	ErrorCrash              = "crash"
	ErrorTimeout            = "timeout"
	ErrorClientDisconnected = "client_disconnected"
	ErrorOther              = "other"
)

// Reason for eviction_notice.
const (
	EvictionOwnerUnload = "owner_unload"
	EvictionOther       = "other"
)

// Status of a model in handshake/capacity records.
const (
	ModelLoaded   = "loaded"
	ModelExcluded = "excluded"
)

// ModelEntry is one local model's capacity record. Shared shape in
// Handshake and Capacity (catalog §3.2).
type ModelEntry struct {
	ModelID   string   `json:"model_id"`
	Tags      []string `json:"tags"`
	NCtx      int      `json:"n_ctx"`
	NEmbD     int      `json:"n_embd"`
	NSlots    int      `json:"n_slots"`
	BusySlots int      `json:"busy_slots"`
	Status    string   `json:"status"` // ModelLoaded | ModelExcluded
	VramMB    *int     `json:"vram_mb"`
}

// Handshake is the first envelope the router sends after subscribing
// (catalog §3.1).
type Handshake struct {
	Proto      int          `json:"proto"` // const 1 in protocol v1
	RouterID   string       `json:"router_id"`
	RouterName string       `json:"router_name"`
	Software   string       `json:"software"`
	Models     []ModelEntry `json:"models"`
}

// Capacity reports a membership/capacity change (catalog §3.2).
type Capacity struct {
	RouterID  string       `json:"router_id"`
	Models    []ModelEntry `json:"models"`
	UpdatedAt string       `json:"updated_at"` // RFC 3339
}

// WorkOffer is phase 1 of two-phase dispatch (catalog §3.3).
type WorkOffer struct {
	JobID           string  `json:"job_id"`
	Tag             string  `json:"tag"`
	EstPromptTokens int     `json:"est_prompt_tokens"`
	EstCtxNeeded    int     `json:"est_ctx_needed"`
	DeadlineMS      *int    `json:"deadline_ms"`
	Priority        string  `json:"priority"` // PriorityLive | PriorityBatch
	Source          string  `json:"source"`   // SourceQueue | SourceSkip | SourcePin
	ConvKey         *string `json:"conv_key"` // 64-hex continuity key, or nil
}

// OfferAccept is the router's short-lease commit (catalog §3.4).
type OfferAccept struct {
	JobID   string `json:"job_id"`
	LeaseMS int    `json:"lease_ms"`
	ModelID string `json:"model_id"`
	NCtx    int    `json:"n_ctx"`
}

// OfferReject declines an offer; the coordinator moves on without the body
// (catalog §3.5).
type OfferReject struct {
	JobID  string  `json:"job_id"`
	Reason string  `json:"reason"` // Reject*
	Detail *string `json:"detail"` // <=200 chars, never prompt/response content
}

// BodyAbort is sent when the router aborts the body stream after accepting
// (catalog §3.6).
type BodyAbort struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"` // Abort*
}

// Completion reports a finished job (catalog §3.7).
type Completion struct {
	JobID          string  `json:"job_id"`
	OK             bool    `json:"ok"` // const true
	ConvKey        *string `json:"conv_key"`
	ModelID        string  `json:"model_id"`
	PromptTokens   int     `json:"prompt_tokens"`
	ResponseTokens int     `json:"response_tokens"`
	CachedTokens   *int    `json:"cached_tokens"`
	IdleMSHint     *int    `json:"idle_ms_hint"`
	TQueuedMS      int     `json:"t_queued_ms"`
	TServiceMS     int     `json:"t_service_ms"`
	StopReason     *string `json:"stop_reason"`
	ServedBy       string  `json:"served_by"` // router_id
}

// ErrorReport reports a failed job (catalog §3.8). Named ErrorReport (not
// Error) so it does not collide with Go's builtin error.
type ErrorReport struct {
	JobID          string  `json:"job_id"`
	OK             bool    `json:"ok"`      // const false
	Stage          string  `json:"stage"`   // StagePreToken | StageMidStream
	Code           string  `json:"code"`    // Error*
	Message        string  `json:"message"` // <=200 chars, never prompt/response content
	PromptTokens   *int    `json:"prompt_tokens"`
	ResponseTokens *int    `json:"response_tokens"`
	ConvKey        *string `json:"conv_key"`
	TQueuedMS      *int    `json:"t_queued_ms"`
	TServiceMS     *int    `json:"t_service_ms"`
}

// AffinityEntry is one cache-liveness hint (catalog §3.9).
type AffinityEntry struct {
	ConvKey      string `json:"conv_key"` // 64-hex
	IdleMS       int    `json:"idle_ms"`
	CachedTokens *int   `json:"cached_tokens"`
}

// AffinityReport is an optional batch of cache-liveness hints, <=16 entries
// (catalog §3.9).
type AffinityReport struct {
	RouterID string          `json:"router_id"`
	Entries  []AffinityEntry `json:"entries"`
}

// ExclusionUpdate is the owner's exclusion mark (D-F1, catalog §3.10).
type ExclusionUpdate struct {
	RouterID string `json:"router_id"`
	ModelID  string `json:"model_id"`
	Excluded bool   `json:"excluded"`
}

// EvictionNotice announces a really-unloaded local model (catalog §3.11).
type EvictionNotice struct {
	RouterID    string   `json:"router_id"`
	ModelID     string   `json:"model_id"`
	EvictedJobs []string `json:"evicted_jobs"` // job_ids now gone; may be empty
	Reason      string   `json:"reason"`       // EvictionOwnerUnload | EvictionOther
}

// JobIntake is the metadata body of POST /pool/jobs (catalog §4). The raw
// client body travels separately via PUT /pool/jobs/{job_id}/body.
type JobIntake struct {
	Tag             string  `json:"tag"`
	EstPromptTokens int     `json:"est_prompt_tokens"`
	EstCtxNeeded    int     `json:"est_ctx_needed"`
	Priority        string  `json:"priority"` // PriorityLive | PriorityBatch
	ConvKey         *string `json:"conv_key"`
	ClientHint      *string `json:"client_hint"` // e.g. harness name; metadata only
}
