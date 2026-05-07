-- VoiceBot Post-Call Processing — Schema Migration 001
-- Adds: interaction_audit_log, interaction_analysis, customer_llm_budgets,
--       postcall_dead_letter, and new columns on interactions.
--
-- Safe to run on the existing schema created by data/schema.sql.
-- All statements use IF NOT EXISTS / IF EXISTS guards.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Immutable audit log
--    One row per stage-transition per interaction.
--    Never updated — only appended. Survives retries and overwrites.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS interaction_audit_log (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id  UUID        NOT NULL REFERENCES interactions(id),
    customer_id     UUID        NOT NULL,
    campaign_id     UUID        NOT NULL,
    -- event_type values (enforced by application, not DB constraint for flexibility):
    --   interaction_ended | task_started | recording_uploaded | recording_failed
    --   llm_requested | llm_completed | llm_rate_limited | signal_jobs_triggered
    --   lead_stage_updated | task_completed | task_dead_lettered
    event_type      VARCHAR(100) NOT NULL,
    event_data      JSONB        NOT NULL DEFAULT '{}',
    tokens_used     INTEGER,          -- populated for llm_completed events
    error_detail    TEXT,             -- populated for *_failed events
    occurred_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_interaction
    ON interaction_audit_log(interaction_id);
CREATE INDEX IF NOT EXISTS idx_audit_customer_time
    ON interaction_audit_log(customer_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_audit_event_type
    ON interaction_audit_log(event_type, occurred_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Interaction analysis results
--    Replaces the JSONB overwrite pattern on interactions.interaction_metadata.
--    The JSONB column is preserved for dashboard hot-cache reads; this table
--    is the source of truth for analysis results and supports retries safely
--    (unique index prevents duplicate writes).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS interaction_analysis (
    id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id  UUID         NOT NULL REFERENCES interactions(id),
    call_stage      VARCHAR(100),
    lane            VARCHAR(20)  NOT NULL DEFAULT 'cold', -- hot | cold | skip
    entities        JSONB        NOT NULL DEFAULT '{}',
    summary         TEXT,
    tokens_used     INTEGER,
    llm_provider    VARCHAR(50),
    llm_model       VARCHAR(100),
    llm_latency_ms  FLOAT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Unique: one analysis result per interaction (idempotent retries safe)
CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_interaction
    ON interaction_analysis(interaction_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Per-customer LLM token budgets
--    Seeded with defaults; updated by ops without deployment.
--    tokens_per_minute = 0 means "draw from shared pool only".
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_llm_budgets (
    customer_id         UUID    PRIMARY KEY,
    tokens_per_minute   INTEGER NOT NULL DEFAULT 0,
    burst_multiplier    FLOAT   NOT NULL DEFAULT 1.5,
    -- lower priority number = higher priority when contending for shared pool
    priority            INTEGER NOT NULL DEFAULT 10,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed two sample customers to match schema.sql sample data
INSERT INTO customer_llm_budgets (customer_id, tokens_per_minute, priority)
VALUES
    ('d0000000-0000-0000-0000-000000000001', 20000, 5),
    ('d0000000-0000-0000-0000-000000000002', 10000, 10)
ON CONFLICT (customer_id) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Dead-letter store
--    Interactions that exhaust all Celery retries land here — durable in
--    Postgres, survives Redis restarts, replayable by ops.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS postcall_dead_letter (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id  UUID        NOT NULL,    -- no FK: interaction row may be corrupt
    customer_id     UUID,
    campaign_id     UUID,
    payload         JSONB       NOT NULL,    -- full Celery task payload for replay
    last_error      TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at     TIMESTAMPTZ              -- set by ops when manually replayed
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_interaction
    ON postcall_dead_letter(interaction_id);
CREATE INDEX IF NOT EXISTS idx_dead_letter_customer
    ON postcall_dead_letter(customer_id, failed_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. New columns on interactions
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE interactions
    ADD COLUMN IF NOT EXISTS processing_lane   VARCHAR(20),
    -- hot | cold | skip — set at webhook receipt, before Celery runs
    ADD COLUMN IF NOT EXISTS analysis_status   VARCHAR(50) NOT NULL DEFAULT 'pending',
    -- pending | processing | completed | failed | skipped
    ADD COLUMN IF NOT EXISTS recording_status  VARCHAR(50) NOT NULL DEFAULT 'pending';
    -- pending | uploaded | not_available | error
