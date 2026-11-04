// Package config loads coordinator configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Defaults shared across the service.
const (
	DefaultAddr      = "127.0.0.1:8080"
	DefaultStaticDir = "./static"
	DefaultLogLevel  = "info"
)

// Defaults for the pool protocol plane (WS-06).
const (
	// DefaultDataDir is where request bodies are staged while queued (POOL_DATA_DIR).
	DefaultDataDir = "./data"
	// DefaultOfferLeaseMS is the default lease a router holds on an accepted
	// job before the coordinator may re-offer it (POOL_OFFER_LEASE_MS).
	DefaultOfferLeaseMS = 5000
	// DefaultMaxBounces is the per-job offer-bounce limit before the
	// job is parked and retried after a delay (POOL_MAX_BOUNCES, D-G1).
	DefaultMaxBounces = 3
	// DefaultHeartbeatMS is the SSE heartbeat interval on /pool/subscribe
	// (POOL_HEARTBEAT_MS).
	DefaultHeartbeatMS = 25000
	// DefaultContIdleMS is the skip-the-line window, ms (POOL_CONT_IDLE_MS,
	// D-E2): a continuation is served ahead of the queue on the member that
	// last served the conversation while it is plausibly still cached.
	DefaultContIdleMS = 60000
	// DefaultContPreferMS is the soft-preference window, ms (POOL_CONT_PREFER_MS,
	// D-E2/D-E3): beyond the skip window, up to this age, the hinted member is
	// still offered first - no queue priority.
	DefaultContPreferMS = 600000
	// DefaultJobExpiryMS caps how long a queued (pending/offered) job may wait
	// before it fails (POOL_JOB_EXPIRY_MS, D-D2 MVP).
	DefaultJobExpiryMS = 1800000
	// DefaultRunningCapMS caps how long a job may stay running (from the body
	// pull) before it fails with a clean terminal error (POOL_RUNNING_CAP_MS,
	// D-G2): a serving router that drops mid-push or streams forever must not
	// leave the job non-terminal.
	DefaultRunningCapMS = 300000
	// DefaultFairWindowMS is the D-F2 fairness rolling window, ms
	// (POOL_FAIR_WINDOW_MS): the served/consumed token sums cover this much
	// recent history (default 7 days).
	DefaultFairWindowMS = 604800000
	// DefaultFairTopK is the D-F3 selection window (POOL_FAIR_TOPK): the
	// fairness pick considers the k oldest eligible pending jobs.
	DefaultFairTopK = 10
	// DefaultParkBackoffMS is the base backoff between re-offers of a parked
	// job (POOL_PARK_BACKOFF_MS); it doubles per consecutive park, capped at
	// ~10x the base (~5s at the default) (DISP-RESIDUAL-1).
	DefaultParkBackoffMS = 500
	// DefaultDedupeWindowMS is the intake dedupe window, ms (POOL_DEDUPE_WINDOW_MS,
	// D-G5): a re-intake with the same (user_id, request_id) younger than this
	// returns the existing job (200); beyond it the id is released and the
	// prompt is fresh work (201).
	DefaultDedupeWindowMS = 300000
	// DefaultBatchWeight is the D-F2 weight of a batch job's consumed tokens
	// (POOL_BATCH_WEIGHT, D-H1): a small reward for choosing batch over
	// real-time.
	DefaultBatchWeight = 0.5
	// DefaultBatchExpiryMS caps how long a priority=batch job may wait before
	// it fails (POOL_BATCH_EXPIRY_MS, D-H1); live jobs keep DefaultJobExpiryMS.
	DefaultBatchExpiryMS = 86400000
)

// Config is the coordinator's runtime configuration, populated from env vars.
type Config struct {
	// Addr is the listen address (COORD_ADDR).
	Addr string
	// DSN is the postgres connection string (COORD_DSN).
	DSN string
	// StaticDir is the portal's static asset directory (COORD_STATIC_DIR).
	StaticDir string
	// LogLevel is the logging verbosity (COORD_LOG_LEVEL).
	LogLevel string
	// DataDir is where request bodies are staged while queued (POOL_DATA_DIR).
	DataDir string
	// OfferLeaseMS is the default offer lease after accept (POOL_OFFER_LEASE_MS).
	OfferLeaseMS int
	// MaxBounces caps offer rejections per job before parking (POOL_MAX_BOUNCES).
	MaxBounces int
	// HeartbeatMS is the SSE heartbeat interval (POOL_HEARTBEAT_MS).
	HeartbeatMS int
	// ContIdleMS is the skip-the-line window, ms (POOL_CONT_IDLE_MS, D-E2).
	ContIdleMS int
	// ContPreferMS is the soft-preference window, ms (POOL_CONT_PREFER_MS,
	// D-E2/D-E3).
	ContPreferMS int
	// JobExpiryMS caps pending/offered jobs (POOL_JOB_EXPIRY_MS).
	JobExpiryMS int
	// RunningCapMS caps running jobs (POOL_RUNNING_CAP_MS, D-G2).
	RunningCapMS int
	// FairWindowMS is the D-F2 fairness rolling window, ms (POOL_FAIR_WINDOW_MS).
	FairWindowMS int
	// FairTopK is the D-F3 selection window: k oldest pending jobs
	// (POOL_FAIR_TOPK).
	FairTopK int
	// ParkBackoffMS is the base parked-job re-offer backoff, ms
	// (POOL_PARK_BACKOFF_MS).
	ParkBackoffMS int
	// DedupeWindowMS is the intake dedupe window, ms (POOL_DEDUPE_WINDOW_MS, D-G5).
	DedupeWindowMS int
	// BatchWeight is the D-F2 weight of a batch job's consumed tokens
	// (POOL_BATCH_WEIGHT, D-H1).
	BatchWeight float64
	// BatchExpiryMS caps pending/offered priority=batch jobs (POOL_BATCH_EXPIRY_MS).
	BatchExpiryMS int
}

// env returns the value of name, or def when unset/empty.
func env(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		Addr:           env("COORD_ADDR", DefaultAddr),
		DSN:            env("COORD_DSN", ""),
		StaticDir:      env("COORD_STATIC_DIR", DefaultStaticDir),
		LogLevel:       env("COORD_LOG_LEVEL", DefaultLogLevel),
		DataDir:        env("POOL_DATA_DIR", DefaultDataDir),
		OfferLeaseMS:   atoi(env("POOL_OFFER_LEASE_MS", ""), DefaultOfferLeaseMS),
		MaxBounces:     atoi(env("POOL_MAX_BOUNCES", ""), DefaultMaxBounces),
		HeartbeatMS:    atoi(env("POOL_HEARTBEAT_MS", ""), DefaultHeartbeatMS),
		ContIdleMS:     atoi(env("POOL_CONT_IDLE_MS", ""), DefaultContIdleMS),
		ContPreferMS:   atoi(env("POOL_CONT_PREFER_MS", ""), DefaultContPreferMS),
		JobExpiryMS:    atoi(env("POOL_JOB_EXPIRY_MS", ""), DefaultJobExpiryMS),
		RunningCapMS:   atoi(env("POOL_RUNNING_CAP_MS", ""), DefaultRunningCapMS),
		FairWindowMS:   atoi(env("POOL_FAIR_WINDOW_MS", ""), DefaultFairWindowMS),
		FairTopK:       atoi(env("POOL_FAIR_TOPK", ""), DefaultFairTopK),
		ParkBackoffMS:  atoi(env("POOL_PARK_BACKOFF_MS", ""), DefaultParkBackoffMS),
		DedupeWindowMS: atoi(env("POOL_DEDUPE_WINDOW_MS", ""), DefaultDedupeWindowMS),
		BatchWeight:    atof(env("POOL_BATCH_WEIGHT", ""), DefaultBatchWeight),
		BatchExpiryMS:  atoi(env("POOL_BATCH_EXPIRY_MS", ""), DefaultBatchExpiryMS),
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return cfg, fmt.Errorf("config: invalid COORD_LOG_LEVEL %q", cfg.LogLevel)
	}
	return cfg, nil
}

// Validate returns an error if the configuration is not usable.
func (c Config) Validate() error {
	if c.DSN == "" {
		return errors.New("config: COORD_DSN is required for database-backed operation")
	}
	return nil
}

// atoi parses v as a positive int, falling back to def when v is empty
// or not a positive integer.
func atoi(v string, def int) int {
	if v == "" {
		return def
	}
	n := 0
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return def
		}
		n = n*10 + int(ch-'0')
		if n > 1<<30 {
			return def
		}
	}
	if n == 0 {
		return def
	}
	return n
}

// atof parses v as a float in (0, 1], falling back to def when v is empty or
// out of range (POOL_BATCH_WEIGHT): a weight above 1 would count batch
// consumption MORE than live, inverting the reward-for-batching intent.
func atof(v string, def float64) float64 {
	if v == "" {
		return def
	}
	var n float64
	_, err := fmt.Sscanf(v, "%g", &n)
	if err != nil || n <= 0 || n > 1 {
		return def
	}
	return n
}
