# Post-Call Processing Pipeline — Design Document

**Author:** Krutik Raythatha
**Date:** 2026-05-08

---

## 1. Assumptions

_Explicit assumptions, to be discussed in the follow-up._

1. **Hot call classification via keywords is sufficient for routing.** LLM-based classification of urgency would be more accurate but would consume quota before we've decided whether to spend quota — a circular problem. Keyword heuristics cover all 8 transcript fixtures with correct expected_lane outcomes.

2. **"Immediate" means within the current minute's rate-limit window, not sub-second.** A rebook confirmation needs to be processed before the sales team's next refresh cycle (~minutes), not in milliseconds. This allows rate-limit aware queuing without violating the business SLA.

3. **The LLM token count in the provider's response is the authoritative billing figure.** We treat the `usage.total_tokens` field in the response as ground truth. Pre-flight estimates (`LLM_AVG_TOKENS_PER_CALL`) are used only for budget reservation; actuals correct them immediately after the response.

4. **Recordings and LLM analysis are fully independent.** The LLM reads the transcript text, not the audio. They can (and should) run in parallel. The original sequential design was historical accident, not a requirement.

5. **A Redis restart is a credible failure mode; a Postgres restart is not.** Redis is used as a broker and cache — it is expected to occasionally restart (upgrades, OOM kills). Postgres with WAL is treated as durable storage. Dead-letter lives in Postgres; the audit log lives in Postgres.

6. **Multiple Celery workers run simultaneously.** Scale-out is horizontal (more workers), not vertical. The rate limiter must be safe for concurrent workers — hence Redis atomic INCRBY, not in-process counters.

7. **Per-customer token budgets are pre-configured by ops** (row in `customer_llm_budgets`), not dynamically negotiated. This matches the "pre-allocated budget" language in the README and avoids complex real-time auction logic.

8. **Unallocated headroom (global TPM minus sum of dedicated budgets) is shared pool**, first-come-first-served. Customers without dedicated allocations draw from here. This is fair and simple; a more sophisticated model (weighted fair queuing) is a known future improvement.

9. **The Exotel API is poll-friendly.** Per `recording.py` comments: no rate-limit on the status endpoint. The poll interval (5s initial, ×1.5 backoff) is conservative.

10. **Signal jobs failures do not fail the task.** The analysis result is already written to Postgres when signal_jobs fire. A failed CRM push is a downstream problem — the interaction is not lost. CRM retry with status tracking is a "should implement" item.

---

## 2. Problem Diagnosis

The root problem is **no rate-limit awareness at 100K call scale**.

At 100K calls with 500 RPM and 90K TPM limits:
- Peak rate: ~5,000+ requests/min attempted
- Provider allows: 500 requests/min
- Result: 429 storm → Celery retry avalanche → 100K tasks all retrying every 60s → Redis fills → broker fails → tasks lost

Four compounding failures enable this:

**1. The 45-second sleep** ties a Celery worker slot to each call for 45s regardless of recording availability. At 100K calls with even 10 workers, this creates a massive queue backlog. Worker slots are the bottleneck before the LLM even becomes one.

**2. The binary circuit breaker** trips at 90% RPM (the wrong metric — providers rate-limit on TPM) and freezes all dialling for 1800s. By the time it trips, 429s are already happening. The freeze is too late and too blunt.

**3. The Redis double-SPOF retry queue** has the same failure mode as the thing it's backing up. A Redis restart loses the Celery broker queue AND the retry queue simultaneously. There is no durable fallback.

**4. The premature signal_jobs fire** sends empty `{}` payloads to downstream systems (WhatsApp, CRM) before the LLM has run. Downstream systems receive two calls: one empty, one real. This is a correctness bug, not just a performance issue.

---

## 3. Architecture Overview

```
POST /session/{sid}/interaction/{iid}/end
        │
        ▼
  FastAPI endpoint
  ├── Load interaction, mark ENDED
  ├── Classify lane: hot | cold | skip   ← keyword heuristic, no LLM
  ├── Audit: INTERACTION_ENDED
  └── skip → write analysis_status=skipped, return 200
      hot  → enqueue to postcall_hot queue
      cold → enqueue to postcall_cold queue
              │
              ▼
    process_{hot|cold}_interaction  (Celery, acks_late=True)
    ├── Audit: TASK_STARTED
    ├── Short-transcript gate (re-check, safe after crash re-queue)
    ├── asyncio.gather (PARALLEL):
    │     ├── fetch_and_upload_recording()
    │     │     └── poll Exotel: 5s, 7.5s, 11s... up to 12 attempts
    │     │         every outcome emits structured audit event
    │     └── rate_limiter.acquire(customer_id, estimated_tokens)
    │           ├── Check global TPM window (Redis INCRBY sliding)
    │           ├── Check per-customer dedicated budget (Redis HINCRBY)
    │           └── Backoff + retry until budget available or timeout
    │               hot timeout: 120s | cold timeout: 600s
    ├── Audit: LLM_BUDGET_ACQUIRED
    ├── PostCallProcessor._call_llm()
    ├── rate_limiter.record_actual_usage()  ← correct estimate with real count
    ├── Audit: LLM_COMPLETED (with tokens_used)
    ├── UPSERT interaction_analysis (idempotent on retry)
    ├── trigger_signal_jobs() — ONCE, with real result
    ├── update_lead_stage()
    ├── Audit: TASK_COMPLETED
    └── On exhausted retries: write postcall_dead_letter (Postgres)
```

### Key design decisions

1. **Two queues (hot/cold) with a single shared rate limiter.** Separate queues allow different worker counts but use the same Redis TPM window. Hot workers call `acquire(priority=1)`, cold call `acquire(priority=10)`, which gives hot shorter backoff intervals under contention.

2. **Optimistic INCRBY then rollback.** The rate limiter increments the TPM counter, checks if it's over the limit, and decrements if so. This is simpler than a Lua script and safe under concurrent workers because INCRBY is atomic. The brief over-increment window (nanoseconds) is acceptable.

3. **asyncio.gather for recording + LLM.** Recording fetch is I/O bound (HTTP poll). LLM acquisition is wait-bound (rate limiter backoff). Both can proceed concurrently. If recording fails, LLM analysis still runs — they're independent.

4. **Postgres dead-letter instead of Redis retry queue.** Redis durability is the problem; using Redis to recover from Redis failures is circular. One write to `postcall_dead_letter` persists across Redis restarts.

---

## 4. Rate Limit Management

### How you track rate limit usage

Two Redis sliding-window counters, set with INCRBY + 60-second EXPIRE on every LLM request:

```
llm:tpm:global   → total tokens used in the current 60-second window
llm:rpm:global   → total requests in the current 60-second window
```

After each LLM response, `record_actual_usage()` applies the delta (actual - estimated) to correct the estimate. This means the window reflects real usage, not estimates.

### How you decide what to process now vs. defer

1. **Lane classification at webhook receipt:** keyword heuristic → hot or cold queue
2. **Rate limiter acquire():** blocks hot tasks for up to 120s, cold tasks for up to 600s
3. **Hot workers exhaust headroom → cold tasks wait longer** (not explicitly preempted, but hot tasks use shorter backoff so they acquire faster under contention)

Effectively: hot calls always get processed within the current or next minute's budget window. Cold calls get processed in order of queue depth, deferred when hot lane is consuming all budget.

### What happens when the limit is hit (recovery, not crash)

- `acquire()` backs off with exponential sleep (0.5s base for hot, 2s for cold, ×1.5, capped at 30s, jitter ±20%)
- After the current 60-second window resets, the TTL expires and INCRBY starts fresh
- No 429 is propagated to any caller — the limiter absorbs wait internally
- On timeout (120s hot / 600s cold): write to `postcall_dead_letter`, audit event emitted
- Dead-lettered tasks are replayable by ops without needing the original log entry

---

## 5. Per-Customer Token Budgeting

**Schema:** `customer_llm_budgets(customer_id, tokens_per_minute, burst_multiplier, priority)`

**Allocation model:**
- Total effective TPM = `LLM_TOKENS_PER_MINUTE × LLM_TPM_SAFETY_MARGIN` (85% of 90K = 76,500)
- Shared pool = 20% of total (configurable) = 15,300 tokens/min minimum
- Dedicated budgets fill from remaining 80%

**Redis key:** `llm:budget:{customer_id}:{minute_bucket}` where `minute_bucket = int(time() // 60)`

**Guarantee for a pre-allocated customer:**
- Customer A with 20,000 tokens/min allocated: can process ~13 calls/min uncontested
- Even if Customer B exhausts the shared pool, Customer A's dedicated budget is unaffected (separate Redis key)

**When a customer exceeds their budget:**
- Dedicated counter exceeds `tokens_per_minute` → fall back to shared pool
- If shared pool is also exhausted → wait (backoff loop, same as global limit hit)
- Budget resets each calendar minute (TTL expires naturally)

**Unallocated headroom:**
- Any TPM not reserved by dedicated budgets contributes to shared pool
- First-come-first-served among shared-pool customers (no weighted queuing)
- When Customer A's burst is temporarily satisfied and they have unused budget, that headroom is available to others (their counter doesn't block the global window)

---

## 6. Differentiated Processing

**Mechanism: Keyword heuristic at webhook receipt, not a classification LLM call.**

Why keywords and not a second LLM call:
- A classification call would itself consume budget before we've decided to spend budget (circular)
- The 8 transcript fixtures show that outcome classification from keywords has 100% accuracy on the provided test set
- "confirmed", "rebook", "book", "demo", "escalat", "urgent" reliably signal hot outcomes
- Hinglish ambiguity ("dekhta hoon" = "I'll see") is handled conservatively → cold (slightly delayed, not lost)

**What "hot" means in practice** (from transcript analysis):
- `rebook_confirmed`: customer explicitly confirms reschedule → sales team needs to send confirmation WhatsApp immediately
- `demo_booked`: booked demo requires calendar invite generation within minutes
- `escalation_needed`: angry customer who threatened complaint → human manager needs to call within 60 minutes

**What "cold" means:**
- `not_interested`: update lead stage to closed_lost; no time-sensitive downstream action
- `callback_requested`: schedule follow-up; callback system can wait for analysis
- `already_purchased`: update CRM record; no urgency

The lane classification (`processing_lane` column on `interactions`) is written to Postgres at webhook time so it's queryable even if the Celery task hasn't run yet.

---

## 7. Recording Pipeline

**Replacement for `asyncio.sleep(45s)`:**

```python
for attempt in range(RECORDING_POLL_MAX_ATTEMPTS):  # default: 12
    url = await _fetch_exotel_recording_url(call_sid, account_id)
    if url:
        s3_key = await _upload_to_s3(url, interaction_id)
        await audit_logger.log_event(... RECORDING_UPLOADED ...)
        return RecordingResult(status="uploaded", s3_key=s3_key, attempts=attempt+1)
    
    await asyncio.sleep(interval × backoff_factor^attempt + jitter)

# All attempts exhausted
await audit_logger.log_event(... RECORDING_FAILED ...)
logger.error("recording_permanently_failed", extra={"interaction_id": ..., "attempts": 12})
return RecordingResult(status="not_available", reason="max_attempts_exceeded")
```

**Poll schedule (defaults):** 5s, 7.5s, 11.2s, 16.8s, 25.3s, 30s, 30s... ≈ 120s total coverage

**What a failure looks like to the on-call engineer:**
1. `recording_permanently_failed` log event at ERROR level with `interaction_id` and `call_sid`
2. Corresponding `RECORDING_FAILED` row in `interaction_audit_log` (queryable: `SELECT * FROM interaction_audit_log WHERE interaction_id=$1`)
3. `interactions.recording_status = 'not_available'` (queryable: `SELECT count(*) FROM interactions WHERE recording_status='not_available' AND ended_at > NOW() - INTERVAL '1 hour'`)
4. A Grafana alert pattern on `recording_permanently_failed` log events (wire to PagerDuty)

**Key difference:** The old system logged at DEBUG (invisible in prod). The new system logs at ERROR and writes to Postgres.

---

## 8. Reliability & Durability

**No analysis result is permanently lost:**

1. **Celery `acks_late=True` + `reject_on_worker_lost=True`**: task is only acknowledged after completion. Worker crash → NACK → redelivered.

2. **`postcall_dead_letter` in Postgres**: when all Celery retries are exhausted, the full payload is written to this table. Ops can replay via `UPDATE postcall_dead_letter SET replayed_at=NOW()` and re-enqueue manually. No need to find the payload in logs.

3. **UPSERT on `interaction_analysis`**: unique index on `interaction_id` means retry writes don't create duplicate rows. Idempotent.

4. **Audit log is append-only**: retries add new rows, not overwrite. The full history of all attempts is preserved.

5. **Redis failure handling (fail-open)**: if Redis is temporarily unavailable, `rate_limiter._try_acquire()` returns a "fail-open" AcquireResult, allowing the LLM call to proceed. This prevents Redis blips from stopping all processing. Counter will be slightly off until Redis recovers.

**What we don't solve:**
- Celery broker (Redis) restart loses tasks that were in-flight but not yet acked. `acks_late=True` mitigates this but doesn't eliminate it for tasks mid-execution. Full solution: replace Redis broker with RabbitMQ or SQS (out of scope for this exercise).

---

## 9. Auditability & Observability

### What you log

Every processing stage writes to `interaction_audit_log` with these fields:
```
interaction_id, customer_id, campaign_id, event_type, event_data (JSONB),
tokens_used, error_detail, occurred_at
```

**Event types and what they tell you:**

| Event | What it tells you |
|-------|------------------|
| `interaction_ended` | Call ended, lane classified, timestamp |
| `task_started` | Celery task picked up, which attempt number |
| `recording_poll_attempt` | Which attempt, how far into the poll loop |
| `recording_uploaded` | S3 key, how many attempts it took |
| `recording_failed` | Permanent; number of attempts, call_sid |
| `llm_requested` | Estimated tokens, lane |
| `llm_budget_acquired` | Waited seconds, source (dedicated/shared), TPM utilisation at acquire time |
| `llm_rate_limited` | Task dead-lettered due to budget timeout |
| `llm_completed` | Actual tokens, latency, call_stage |
| `signal_jobs_triggered` | call_stage that triggered downstream actions |
| `lead_stage_updated` | New stage |
| `task_completed` | End-to-end duration (from task_started.occurred_at) |
| `task_dead_lettered` | Final failure; payload preserved in postcall_dead_letter |
| `short_call_skipped` | How many turns; confirms no LLM was consumed |

**Debugging a failed interaction 3 days later:**

```sql
-- 1. Find the full timeline
SELECT event_type, event_data, tokens_used, error_detail, occurred_at
FROM interaction_audit_log
WHERE interaction_id = 'f0000000-...'
ORDER BY occurred_at;

-- 2. If dead-lettered, get the full payload for replay
SELECT payload, last_error, attempts, failed_at
FROM postcall_dead_letter
WHERE interaction_id = 'f0000000-...';

-- 3. Check recording status
SELECT recording_status, recording_s3_key, analysis_status
FROM interactions
WHERE id = 'f0000000-...';
```

### Alert conditions

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| `recording_permanently_failed` | Any recording fails all retries | WARNING | Investigate Exotel status |
| `postcall_dead_lettered` | Any task exhausts retries | ERROR | Replay from dead_letter table |
| `llm_rate_limit_approaching` | TPM > 85% utilisation | WARNING | Check cold queue depth |
| `customer_budget_exhausted` | Customer uses 100% of dedicated budget | INFO | Review budget allocation |
| `postcall_queue_high` | Queue depth > 5000 | WARNING | Scale up Celery workers |

---

## 10. Data Model

```sql
-- Migration: data/migrations/001_add_postcall_tables.sql

-- Immutable audit log (never updated, only appended)
CREATE TABLE interaction_audit_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id  UUID NOT NULL REFERENCES interactions(id),
    customer_id     UUID NOT NULL,
    campaign_id     UUID NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    event_data      JSONB NOT NULL DEFAULT '{}',
    tokens_used     INTEGER,
    error_detail    TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Separate analysis results table (replaces JSONB overwrite)
CREATE TABLE interaction_analysis (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id  UUID NOT NULL REFERENCES interactions(id),
    call_stage      VARCHAR(100),
    lane            VARCHAR(20) NOT NULL DEFAULT 'cold',
    entities        JSONB NOT NULL DEFAULT '{}',
    summary         TEXT,
    tokens_used     INTEGER,
    llm_provider    VARCHAR(50),
    llm_model       VARCHAR(100),
    llm_latency_ms  FLOAT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ON interaction_analysis(interaction_id);  -- idempotent retries

-- Per-customer LLM budget configuration
CREATE TABLE customer_llm_budgets (
    customer_id         UUID PRIMARY KEY,
    tokens_per_minute   INTEGER NOT NULL DEFAULT 0,
    burst_multiplier    FLOAT NOT NULL DEFAULT 1.5,
    priority            INTEGER NOT NULL DEFAULT 10,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Durable dead-letter (Postgres, survives Redis restart)
CREATE TABLE postcall_dead_letter (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id  UUID NOT NULL,
    customer_id     UUID,
    campaign_id     UUID,
    payload         JSONB NOT NULL,
    last_error      TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at     TIMESTAMPTZ
);

-- New columns on interactions
ALTER TABLE interactions
    ADD COLUMN processing_lane   VARCHAR(20),
    ADD COLUMN analysis_status   VARCHAR(50) NOT NULL DEFAULT 'pending',
    ADD COLUMN recording_status  VARCHAR(50) NOT NULL DEFAULT 'pending';
```

**Why a separate `interaction_analysis` table instead of JSONB overwrite?**
The existing `interaction_metadata` JSONB column was both the analysis store and the dashboard cache. Writing analysis there meant: (a) retries overwrote previous results silently, (b) no unique constraint was possible, (c) history was lost. Separating them gives idempotent writes + audit history while `interaction_metadata` can still be used as a dashboard hot-cache read via a trigger or explicit update.

---

## 11. Security

**Sensitive data in this system:**
- Transcript text: contains conversation content, lead names, intent signals
- Lead PII: name, phone, email in `leads` table
- Recording audio: stored in S3
- LLM API key: grants access to the provider's full account

**Protection strategy:**

| Data | At Rest | In Transit |
|------|---------|------------|
| Transcripts (`conversation_data` JSONB) | Postgres encryption at rest (pgcrypto or AWS RDS encryption) | TLS between app and Postgres |
| Recording audio (S3) | S3 SSE-S3 or SSE-KMS encryption | HTTPS for Exotel download + S3 upload |
| Lead PII | Same Postgres encryption; `email` and `phone` should be encrypted columns (pgcrypto) | TLS |
| LLM API key | Environment variable, not in code; use secrets manager (AWS Secrets Manager / Vault) | HTTPS to provider |
| Celery payloads (Redis) | Redis AUTH + TLS; payloads contain transcript_text — encryption at rest via Redis Enterprise or proxy | TLS |

**Access controls:**
- `customer_id` is present on every interaction and every audit event — enables row-level security if needed
- LLM spending is attributable: `interaction_audit_log.tokens_used` × `customer_id` for billing
- Dead-letter payloads contain full transcript — access to `postcall_dead_letter` should be restricted to ops role

**Nice-to-have (not implemented in this submission):** encrypt `conversation_data` column at the application layer using a per-customer key from a KMS. This allows revoking a customer's data access without re-keying the entire database.

---

## 12. API Interface

**No changes to the API contract.**

`POST /session/{session_id}/interaction/{interaction_id}/end` retains the same path, request body (`InteractionEndRequest`), and HTTP status codes.

**One addition to the response:** the `lane` field in `InteractionEndResponse` (hot/cold/skip) is new. This is a non-breaking addition — existing clients ignore unknown fields.

**Why kept unchanged:**
- Exotel sends this webhook automatically on call disconnect. Re-configuring Exotel webhooks requires ops intervention and a maintenance window.
- The endpoint contract is the right one — fast response, async processing. The problems were all in what happened after the 200.

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What Chosen Instead |
|--------|---------------|-------------------------------------|
| Replace Celery with Temporal | Temporal offers durable workflow execution with built-in retry, visibility, and versioning | Scope: Temporal requires infrastructure change + new SDK. Celery is in the stack; fixing its durability gaps (dead-letter in Postgres) addresses the immediate risk |
| LLM-based lane classification | More accurate than keywords for ambiguous Hinglish calls | Circular: consumes quota before deciding to spend quota. Keywords cover 100% of provided fixtures. Noted as future improvement |
| Redis Sorted Set for rate limiter (score = expiry) | Atomic and O(log n) | INCRBY + EXPIRE is simpler, well-understood, and sufficient at our call rate |
| RabbitMQ as Celery broker | Full durability — messages survive broker restart | Infra change beyond this exercise scope. Dead-letter in Postgres mitigates the worst case (no permanent loss) |
| Token bucket vs sliding window | Token bucket is theoretically more precise | Sliding window with 60s TTL matches the provider's own measurement window. Simpler implementation, same practical result |
| Per-agent backpressure (old) | Original design intent | LLM quota is global — per-agent freeze doesn't match the resource being throttled. Global proportional rate is correct |

---

## 14. Known Weaknesses

1. **Celery broker is still Redis.** `acks_late=True` prevents most losses on crash, but tasks mid-execution during a Redis restart can be lost. Full fix: replace broker with RabbitMQ or SQS.

2. **Keyword classification is brittle for new call types.** If the business adds a new call type ("insurance_claim") with no existing keywords, it routes cold until someone adds the keyword. A lightweight embedding model would generalise better.

3. **Per-customer budget loaded from DB on every acquire().** Should be cached in Redis with a short TTL (e.g., 5 minutes) to avoid a DB round-trip per LLM request at 100K scale.

4. **Signal jobs failures are swallowed.** CRM push failures log a warning but don't trigger a retry. The "should implement" CRM retry with status tracking was deferred.

5. **asyncio.gather + return_exceptions=True** means if both recording AND rate_limiter raise, the task handles them sequentially. This is correct but the exception handling in `_process_interaction` is not exhaustive — unexpected exception types from gather may propagate unexpectedly.

6. **No Exotel webhook signature verification.** The endpoint accepts any POST. In production, verify the `X-Exotel-Signature` header.

---

## 15. What I Would Do With More Time

1. **Replace Redis broker with RabbitMQ** — eliminates the last remaining silent-drop risk. This is the highest-impact infrastructure change.

2. **Cache `customer_llm_budgets` in Redis** — avoid a DB query per `acquire()` call. 5-minute TTL, invalidated on budget updates.

3. **CRM push with retry and status tracking** — add a `crm_push_log` table; retry failed pushes with exponential backoff independent of the main processing task.

4. **Per-customer budget real-time dashboard** — expose `llm:usage:{customer_id}:{bucket}` via a `/admin/usage` endpoint so ops can see live per-customer consumption without querying Redis manually.

5. **Exotel webhook signature verification** — validate `X-Exotel-Signature` header before processing any webhook.

6. **Weighted fair queuing for shared pool** — replace first-come-first-served in the shared pool with a proportional allocation based on `priority` column in `customer_llm_budgets`.
