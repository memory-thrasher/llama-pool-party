-- Schema v1 for the pool coordinator (WS-02).
-- Idempotent: every statement is CREATE IF NOT EXISTS / ON CONFLICT DO NOTHING.
-- Run via: pool-coordinator/internal/schema Apply(ctx, dsn) or `make schema-apply`.

-- gen_random_uuid() lives in pgcrypto (or pg_catalog in PG13+; keep pgcrypto per spec).
create extension if not exists pgcrypto;

-- D-B3 size classes: xs <10B < sm <25B < md <60B < lg <250B < xl <1.5T < 2xl.
-- Boundaries: min inclusive, max exclusive; xs has no min; 2xl has no max.
-- class_kind = 'size' rows below. (pool-text-* capability classes are curated later, D-B2.)
create table if not exists classes (
    id            text primary key,            -- e.g. 'pool-text-xs'
    kind          text not null default 'size' check (kind in ('size', 'capability')),
    size_bucket    text check (size_bucket in ('xs', 'sm', 'md', 'lg', 'xl', '2xl')),
    capability   text,                        -- null for size classes, e.g. 'text', 'image'
    min_params  bigint,                        -- null = no floor
    max_params bigint,                        -- null = no ceiling
    advertised_ctx int,                       -- main context window advertised by the coordinator (D-C3)
    updated_at   timestamptz not null default now()
);

comment on table classes is 'Requestable identifier for a pool capability bucket (size or capability class).';

create table if not exists users (
    id           uuid primary key default gen_random_uuid(),
    email        text unique not null,
    display_name  text not null,
    role       text not null default 'member' check (role in ('member', 'admin')),
    status     text not null default 'active' check (status in ('invited', 'active', 'suspended')),
    created_at  timestamptz not null default now(),
    activated_at timestamptz
);
comment on table users is 'Pool members and admins; invite-only (D-J3).';

create table if not exists user_tokens (
    id          uuid primary key default gen_random_uuid(),
    user_id   uuid not null references users(id) on delete cascade,
    name        text not null default 'default',
    token_hash text not null,                -- sha256 hex of the opaque token (D-J2: hash only)
    created_at timestamptz not null default now(),
    revoked_at   timestamptz
);
comment on table user_tokens is 'Opaque 256-bit user tokens; only the sha256 hash is stored (D-J2).';
-- Plain unique index per spec (D-J2): UNIQUE (user_id, name). The partial form
-- (where revoked_at is null) is NOT spec-conforming: spec-conforming
-- `on conflict (user_id, name)` upserts fail against a partial index, and a user
-- can accumulate multiple revoked 'default' rows.
create unique index if not exists user_tokens_user_name_uq on user_tokens (user_id, name);

create table if not exists routers (
    id          uuid primary key,            -- router_id from the handshake
    owner_id    uuid not null references users(id) on delete cascade,
    router_name text not null,
    software   text,
    last_seen_at timestamptz,
    created_at  timestamptz not null default now(),
    deleted_at    timestamptz
);
comment on table routers is 'Connected member routers (outbound-only, D-A2/D-K1).';

-- Partial unique index per spec: UNIQUE (owner_id, router_name) where deleted_at is null.
-- Implemented as CREATE UNIQUE INDEX IF NOT EXISTS (a plain UNIQUE constraint would
-- also be idempotent, but the index form keeps re-application simple and matches the
-- spec's index wording).
create unique index if not exists routers_active_owner_name_uq on routers (owner_id, router_name) where deleted_at is null;

-- v1 amendment (pre-release): 'gone' status for models announced
-- unloaded by eviction_notice (WS-06 dispatch engine, D-D6).
create table if not exists router_models (
    id         bigserial primary key,
    router_id  uuid not null references routers(id) on delete cascade,
    model_id   text not null,
    tags       text[] not null default '{}',
    n_ctx      int not null,
    n_embd    int not null default 0,
    n_slots    int not null default 1,
    busy_slots int not null default 0,
    status     text not null default 'loaded' check (status in ('loaded', 'excluded', 'gone')),
    vram_mb    int,
    updated_at timestamptz not null default now()
);
comment on table router_models is 'Model load slots per router; exclusion marks are status=''excluded'' (D-F1).';
create unique index if not exists router_models_router_model_uq on router_models (router_id, model_id);

create table if not exists class_members (
    class_id  text not null references classes(id) on delete cascade,
    router_id uuid not null references routers(id) on delete cascade,
    model_id  text not null,
    added_at timestamptz not null default now(),
    primary key (class_id, router_id, model_id)
);
comment on table class_members is 'Which routers/models are loaded per class (D-C1 advertisement basis).';

create table if not exists records (
    id              bigserial primary key,
    job_id           uuid not null,
    user_id         uuid references users(id) on delete set null,
    class_id        text,
    model_id        text,
    router_id        uuid,
    priority        text check (priority in ('live', 'batch')),
    prompt_tokens    int,
    response_tokens int,
    cached_tokens   int,
    t_queued_ms     int,
    t_service_ms   int,
    ok              boolean,
    error_code      text,
    stop_reason      text,
    created_at        timestamptz not null default now()
);
comment on table records is 'Per-request telemetry; NO body columns, ever (D-I1).';
create index if not exists records_user_created_idx on records (user_id, created_at desc);
create index if not exists records_class_created_idx on records (class_id, created_at desc);

create table if not exists batches (
    id          uuid primary key default gen_random_uuid(),
    user_id      uuid not null references users(id) on delete cascade,
    status      text not null default 'pending' check (status in ('pending', 'running', 'done', 'failed', 'cancelled')),
    job_count int not null default 0,
    done_count int not null default 0,
    created_at timestamptz not null default now(),
    finished_at timestamptz
);
comment on table batches is 'Batch job containers; per-job queue rows arrive in WS-10.';

-- v1 amendment (pre-release): request_id/originating_router_id/serving_router_id
-- (WS-06 intake idempotency D-G5, origin tracking D-H2, dispatch state D-D2).
create table if not exists queue_jobs (
    id                 uuid primary key,   -- job_id
    user_id            uuid not null references users(id) on delete cascade,
    class_id          text not null,
    priority         text not null check (priority in ('live', 'batch')),
    est_prompt_tokens  int not null default 0,
    est_ctx_needed     int not null default 0,
    conv_key         text check (conv_key is null or length(conv_key) = 64),
    status           text not null default 'pending' check (status in ('pending', 'offered', 'running', 'done', 'failed', 'evicted', 'expired')),
    request_id         uuid,                    -- intake idempotency key (D-G5)
    originating_router_id uuid,              -- router that owns the client socket (D-H2)
    serving_router_id   uuid,              -- current offer/serve target
    accepted_at         timestamptz,       -- offer_accept received (gates body pull + response push)
    created_at         timestamptz not null default now(),
    updated_at        timestamptz not null default now()
);
-- v1 amendment (pre-release, WS-06 review): accept gate for body pull /
-- response push (an offered-but-unaccepted router must not read the body or
-- push a response). Existing databases: add the column idempotently.
alter table queue_jobs add column if not exists accepted_at timestamptz;
comment on table queue_jobs is 'Coordinator-owned job queue; fairness selection (WS-11) scans here.';
create index if not exists queue_jobs_status_idx on queue_jobs (priority, status, created_at);

-- v1 amendment (pre-release): exact idempotency on intake (WS-06): same
-- request_id -> conflict -> return the existing job_id.
create unique index if not exists queue_jobs_request_id_uq on queue_jobs (request_id) where request_id is not null;



create table if not exists continuity (
    conv_key        text primary key,   -- 64-hex (SHA-256 of first 8KB of raw client payload, D-E1)
    last_router_id  uuid,
    last_model_id    text,
    last_served_at timestamptz not null default now(),
    last_ok        boolean not null default false,
    idle_hint_ms    int
);
comment on table continuity is 'Continuity/affinity tracking for soft pin and skip-the-line (D-E2/D-E3).';

create table if not exists exclusions (
    owner_id    uuid not null references users(id) on delete cascade,
    router_id      uuid not null references routers(id) on delete cascade,
    model_id     text not null,
    excluded     boolean not null default true,
    updated_at    timestamptz not null default now(),
    primary key (owner_id, router_id, model_id)
);
comment on table exclusions is 'Per-user exclusion marks, set via load/unload on the router (D-F1).';

-- Seed rows: the six default size classes (D-B3).
-- min inclusive, max exclusive; xs has no min; 2xl has no max.
insert into classes (id, kind, size_bucket, capability, min_params, max_params)
values
    ('pool-text-xs', 'size', 'xs',  'text', null,       10_000_000_000),
    ('pool-text-sm', 'size', 'sm',  'text', 10_000_000_000, 25_000_000_000),
    ('pool-text-md', 'size', 'md',  'text', 25_000_000_000, 60_000_000_000),
    ('pool-text-lg', 'size', 'lg',  'text', 60_000_000_000, 250_000_000_000),
    ('pool-text-xl', 'size', 'xl',  'text', 250_000_000_000, 1_500_000_000_000),
    ('pool-text-2xl', 'size', '2xl', 'text', 1_500_000_000_000, null)
on conflict (id) do nothing;
