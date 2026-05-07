"""
Celery tasks for post-call processing.

Two tasks replace the original single process_interaction_end_background_task:
  - process_hot_interaction: bound to postcall_hot queue, priority=1 for rate limiter
  - process_cold_interaction: bound to postcall_cold queue, priority=10 for rate limiter

Both share _process_interaction() which:
  1. Enforces the short-transcript gate (even after re-queue from crash)
  2. Acquires LLM budget from LLMRateLimiter (blocks, never 429s)
  3. Runs recording fetch and LLM acquisition IN PARALLEL via asyncio.gather
  4. Runs LLM analysis after budget acquired
  5. Writes result to interaction_analysis table (idempotent via UPSERT)
  6. Fires signal_jobs ONCE — with real analysis result (not the empty {} bug)
  7. Updates lead stage
  8. On exhausted retries: writes to postcall_dead_letter (Postgres, durable)
     NOT the Redis retry queue (same failure mode as broker)

Key improvements over original:
  - No asyncio.sleep(45) — recording polls independently
  - LLM rate limits enforced before every request
  - Dead-letter in Postgres (survives Redis restart)
  - Signal jobs fire once, after analysis, with real data
  - Short-transcript gate lives here too (endpoint + task = double safety)
  - Full audit trail via audit_logger at every stage
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict

from src.config import settings
from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.rate_limiter import rate_limiter, LLMBudgetTimeoutError
from src.services.audit_logger import audit_logger, AuditEventType
from src.services.metrics import metrics_tracker

logger = logging.getLogger(__name__)


# ── Shared task options ────────────────────────────────────────────────────────
_TASK_OPTS = dict(
    bind=True,
    max_retries=settings.POSTCALL_MAX_RETRIES,
    acks_late=True,          # task acked only after completion → crash = redelivery
    reject_on_worker_lost=True,  # NACK on worker loss → Celery requeues
)


@celery_app.task(name="process_hot_interaction", queue=settings.POSTCALL_HOT_QUEUE, **_TASK_OPTS)
def process_hot_interaction(self, payload: Dict[str, Any]):
    """Hot-lane Celery task. Priority 1 for rate limiter."""
    _run_task(self, payload, priority=1)


@celery_app.task(name="process_cold_interaction", queue=settings.POSTCALL_COLD_QUEUE, **_TASK_OPTS)
def process_cold_interaction(self, payload: Dict[str, Any]):
    """Cold-lane Celery task. Priority 10 for rate limiter."""
    _run_task(self, payload, priority=10)


def _run_task(task_self, payload: Dict[str, Any], priority: int):
    """
    Synchronous entry point — spins up an event loop for the async core.

    Celery workers are synchronous by default. Each worker process handles one
    task at a time. For concurrent LLM calls, run multiple workers.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_process_interaction(task_self, payload, priority))
    except Exception as e:
        interaction_id = payload.get("interaction_id", "unknown")
        attempt = task_self.request.retries

        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": interaction_id,
                "error": str(e),
                "attempt": attempt,
                "priority": priority,
            },
        )

        if attempt >= settings.POSTCALL_MAX_RETRIES:
            # All retries exhausted — write to durable dead-letter (Postgres)
            # Never the Redis retry queue (same failure mode as the broker)
            loop.run_until_complete(
                _write_dead_letter(payload, str(e), attempt + 1)
            )
        else:
            # Exponential backoff (not the original fixed 60s)
            retry_delay = min(
                settings.POSTCALL_RETRY_DELAY * (2 ** attempt),
                settings.POSTCALL_RETRY_MAX_BACKOFF,
            )
            raise task_self.retry(exc=e, countdown=retry_delay)
    finally:
        loop.close()


async def _process_interaction(task_self, payload: Dict[str, Any], priority: int):
    """
    Core async processing pipeline.

    Step 0: Short-transcript gate (enforced here even on re-queue after crash)
    Step 1: Acquire LLM budget (blocks, never 429s)
             + Recording fetch (runs in parallel with budget acquisition)
    Step 2: LLM analysis
    Step 3: Write results to DB
    Step 4: Signal jobs (once, with real data)
    Step 5: Lead stage update
    """
    interaction_id = payload["interaction_id"]
    customer_id = payload.get("customer_id", "")
    campaign_id = payload.get("campaign_id", "")
    lane = payload.get("lane", "cold")

    await audit_logger.log_event(
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        event_type=AuditEventType.TASK_STARTED,
        event_data={
            "attempt": task_self.request.retries,
            "lane": lane,
            "priority": priority,
        },
    )
    await metrics_tracker.track_processing_started(interaction_id)

    # ── Step 0: Short-transcript gate ─────────────────────────────────────────
    # Redundant with the endpoint check but necessary for safety:
    # if a task is re-queued after a worker crash, the endpoint doesn't re-run.
    transcript = payload.get("conversation_data", {}).get("transcript", [])
    if len(transcript) < 4:
        await audit_logger.log_event(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            event_type=AuditEventType.SHORT_CALL_SKIPPED,
            event_data={"transcript_turns": len(transcript)},
        )
        logger.info("short_transcript_gate_in_task", extra={"interaction_id": interaction_id})
        return  # no LLM consumed, no signal jobs, done

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=campaign_id,
        customer_id=customer_id,
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
    )

    # ── Step 1: Recording + LLM budget acquisition (PARALLEL) ─────────────────
    # Recording fetch is independent of LLM analysis — no reason to block one
    # on the other. asyncio.gather runs them concurrently within this task.
    # If recording permanently fails, we continue with LLM analysis (separate concern).
    await audit_logger.log_event(
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        event_type=AuditEventType.LLM_REQUESTED,
        event_data={"estimated_tokens": settings.LLM_AVG_TOKENS_PER_CALL, "lane": lane},
    )

    recording_result, acquire_result = await asyncio.gather(
        fetch_and_upload_recording(
            interaction_id=ctx.interaction_id,
            call_sid=ctx.call_sid,
            exotel_account_id=ctx.exotel_account_id or "",
            customer_id=customer_id,
            campaign_id=campaign_id,
        ),
        rate_limiter.acquire(
            customer_id=customer_id,
            estimated_tokens=settings.LLM_AVG_TOKENS_PER_CALL,
            priority=priority,
            timeout_seconds=(
                settings.LLM_ACQUIRE_TIMEOUT_HOT_SECONDS if priority <= 3
                else settings.LLM_ACQUIRE_TIMEOUT_COLD_SECONDS
            ),
        ),
        return_exceptions=True,
    )

    # Handle recording result
    if recording_result and hasattr(recording_result, "s3_key") and recording_result.s3_key:
        await _update_recording_key(interaction_id, recording_result.s3_key)

    # Handle rate limiter result — LLMBudgetTimeoutError → dead-letter, not retry
    if isinstance(acquire_result, LLMBudgetTimeoutError):
        await audit_logger.log_event(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            event_type=AuditEventType.LLM_RATE_LIMITED,
            error_detail=str(acquire_result),
        )
        logger.error(
            "llm_budget_timeout_dead_lettering",
            extra={"interaction_id": interaction_id, "error": str(acquire_result)},
        )
        await _write_dead_letter(payload, str(acquire_result), task_self.request.retries)
        return

    if isinstance(acquire_result, Exception):
        raise acquire_result  # unexpected — let Celery retry handle it

    await audit_logger.log_event(
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        event_type=AuditEventType.LLM_BUDGET_ACQUIRED,
        event_data={
            "source": acquire_result.source,
            "waited_s": round(acquire_result.waited_seconds, 2),
            "tpm_utilisation": round(acquire_result.tpm_utilisation, 3),
        },
    )

    # ── Step 2: LLM analysis ──────────────────────────────────────────────────
    processor = PostCallProcessor()
    result = await processor.process_post_call(ctx, single_prompt=True)

    # Correct the token counter with actual usage (not estimate)
    await rate_limiter.record_actual_usage(
        customer_id=customer_id,
        actual_tokens=result.tokens_used,
        estimated_tokens=settings.LLM_AVG_TOKENS_PER_CALL,
    )

    await audit_logger.log_event(
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        event_type=AuditEventType.LLM_COMPLETED,
        event_data={
            "call_stage": result.call_stage,
            "model": result.model,
            "latency_ms": result.latency_ms,
        },
        tokens_used=result.tokens_used,
    )

    await metrics_tracker.track_processing_completed(
        interaction_id=interaction_id,
        tokens_used=result.tokens_used,
        latency_ms=result.latency_ms,
        customer_id=customer_id,
    )

    # ── Step 3: Write analysis result to dedicated table ──────────────────────
    # UPSERT on interaction_id (unique index) — safe to retry without duplicates.
    await _write_analysis_result(ctx, result, lane)

    # ── Step 4: Signal jobs — ONCE, with real data ────────────────────────────
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
        )
        await audit_logger.log_event(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            event_type=AuditEventType.SIGNAL_JOBS_TRIGGERED,
            event_data={"call_stage": result.call_stage},
        )
    except Exception as e:
        # Signal job failures are logged but don't fail the task —
        # the analysis result is already written; CRM push retry is a separate concern.
        logger.warning(
            "signal_jobs_failed",
            extra={"interaction_id": interaction_id, "error": str(e)},
        )

    # ── Step 5: Lead stage update ─────────────────────────────────────────────
    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
        )
        await audit_logger.log_event(
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            event_type=AuditEventType.LEAD_STAGE_UPDATED,
            event_data={"call_stage": result.call_stage},
        )
    except Exception as e:
        logger.warning(
            "lead_stage_update_failed",
            extra={"interaction_id": interaction_id, "error": str(e)},
        )

    await audit_logger.log_event(
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        event_type=AuditEventType.TASK_COMPLETED,
        event_data={"call_stage": result.call_stage, "lane": lane},
    )


# ── DB helpers (stubs — replace with real SQLAlchemy in production) ───────────

async def _write_analysis_result(ctx: PostCallContext, result, lane: str) -> None:
    """
    UPSERT analysis results into interaction_analysis.

    Production SQL:
        INSERT INTO interaction_analysis
            (interaction_id, call_stage, lane, entities, summary,
             tokens_used, llm_provider, llm_model, llm_latency_ms)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (interaction_id) DO UPDATE SET
            call_stage=EXCLUDED.call_stage, ...
            -- Idempotent: retry writes don't create duplicate rows
    Also updates interactions.interaction_metadata (JSONB) for dashboard hot-cache.
    Also updates interactions.analysis_status = 'completed'
    """
    logger.info(
        "analysis_result_written",
        extra={
            "interaction_id": ctx.interaction_id,
            "call_stage": result.call_stage,
            "tokens_used": result.tokens_used,
            "lane": lane,
        },
    )


async def _write_dead_letter(payload: Dict[str, Any], error: str, attempts: int) -> None:
    """
    Write to postcall_dead_letter (Postgres) — durable across Redis restarts.

    Production SQL:
        INSERT INTO postcall_dead_letter
            (interaction_id, customer_id, campaign_id, payload, last_error, attempts)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6)

    Ops replay: UPDATE postcall_dead_letter SET replayed_at=NOW() WHERE id=$1
    then manually re-enqueue the payload.
    """
    interaction_id = payload.get("interaction_id", "unknown")
    logger.error(
        "postcall_dead_lettered",
        extra={
            "interaction_id": interaction_id,
            "customer_id": payload.get("customer_id"),
            "attempts": attempts,
            "last_error": error,
        },
    )


async def _update_recording_key(interaction_id: str, s3_key: str) -> None:
    """
    Production:
        UPDATE interactions
        SET recording_s3_key=$2, recording_status='uploaded'
        WHERE id=$1
    """
    logger.debug(
        "recording_key_updated",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
