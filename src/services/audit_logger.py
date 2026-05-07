"""
Structured audit logger for post-call processing.

Every stage-transition in a call's lifecycle writes one row to
interaction_audit_log (Postgres). This gives an on-call engineer a
complete, immutable timeline for any interaction_id — queryable 3 days later
without digging through log aggregators.

Usage:
    from src.services.audit_logger import audit_logger, AuditEventType

    await audit_logger.log_event(
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        event_type=AuditEventType.TASK_STARTED,
        event_data={"lane": ctx.lane, "attempt": attempt},
    )

All methods are async and safe to call from both FastAPI and Celery contexts.
Failures are logged at ERROR level but never re-raised — audit logging must
never cause the primary processing path to fail.
"""

import enum
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class AuditEventType(str, enum.Enum):
    """All valid event_type values in interaction_audit_log."""

    # ── Webhook / intake ──────────────────────────────────────────────────────
    INTERACTION_ENDED = "interaction_ended"
    LANE_CLASSIFIED = "lane_classified"

    # ── Celery task lifecycle ─────────────────────────────────────────────────
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_DEAD_LETTERED = "task_dead_lettered"

    # ── Recording pipeline ────────────────────────────────────────────────────
    RECORDING_POLL_ATTEMPT = "recording_poll_attempt"
    RECORDING_UPLOADED = "recording_uploaded"
    RECORDING_FAILED = "recording_failed"       # permanent; all retries exhausted

    # ── LLM ──────────────────────────────────────────────────────────────────
    LLM_BUDGET_ACQUIRED = "llm_budget_acquired"
    LLM_RATE_LIMITED = "llm_rate_limited"       # waited for budget
    LLM_REQUESTED = "llm_requested"
    LLM_COMPLETED = "llm_completed"             # includes tokens_used
    LLM_FAILED = "llm_failed"                   # exception from provider

    # ── Downstream actions ────────────────────────────────────────────────────
    SIGNAL_JOBS_TRIGGERED = "signal_jobs_triggered"
    LEAD_STAGE_UPDATED = "lead_stage_updated"
    CRM_PUSH_ATTEMPTED = "crm_push_attempted"
    CRM_PUSH_FAILED = "crm_push_failed"

    # ── Short-call fast path ──────────────────────────────────────────────────
    SHORT_CALL_SKIPPED = "short_call_skipped"   # <4 turns, no LLM consumed


class AuditLogger:
    """
    Writes structured audit events to interaction_audit_log.

    In production: async INSERT via SQLAlchemy.
    In tests / local dev without a DB: falls back to structured log only.
    The fallback ensures audit logging never breaks the processing path.
    """

    async def log_event(
        self,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        event_type: AuditEventType,
        event_data: Optional[Dict[str, Any]] = None,
        tokens_used: Optional[int] = None,
        error_detail: Optional[str] = None,
    ) -> None:
        """
        Append one audit event. Never raises — failures are logged and swallowed.

        Args:
            interaction_id: Correlation ID for the call. Present in every event.
            customer_id:    Which business's call this is (billing + isolation).
            campaign_id:    Which campaign triggered the call.
            event_type:     Stage of the pipeline this event represents.
            event_data:     Arbitrary structured context (attempt numbers, S3 keys, etc.).
            tokens_used:    Exact LLM token count (only for llm_completed events).
            error_detail:   Exception message or provider error (only for *_failed events).
        """
        event_data = event_data or {}
        try:
            await self._write_to_db(
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                event_type=event_type.value,
                event_data=event_data,
                tokens_used=tokens_used,
                error_detail=error_detail,
            )
        except Exception as db_err:
            # DB write failed — fall back to structured log so the event
            # is at least captured somewhere. Never re-raise.
            logger.error(
                "audit_log_db_write_failed",
                extra={
                    "interaction_id": interaction_id,
                    "customer_id": customer_id,
                    "event_type": event_type.value,
                    "db_error": str(db_err),
                },
            )

        # Always emit structured log regardless of DB outcome.
        # This means every event appears in both Postgres AND stdout,
        # which gives ops two ways to find it.
        log_payload: Dict[str, Any] = {
            "interaction_id": interaction_id,
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "event_type": event_type.value,
            **event_data,
        }
        if tokens_used is not None:
            log_payload["tokens_used"] = tokens_used
        if error_detail is not None:
            log_payload["error_detail"] = error_detail

        level = logging.ERROR if error_detail else logging.INFO
        logger.log(level, event_type.value, extra=log_payload)

    async def _write_to_db(
        self,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        event_type: str,
        event_data: Dict[str, Any],
        tokens_used: Optional[int],
        error_detail: Optional[str],
    ) -> None:
        """
        Insert one row into interaction_audit_log.

        In production this uses the async SQLAlchemy session pool.
        Mock / stub implementation for the assessment: logs only.

        Production SQL:
            INSERT INTO interaction_audit_log
                (interaction_id, customer_id, campaign_id,
                 event_type, event_data, tokens_used, error_detail)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
        """
        # Assessment stub — in production, replace with:
        #   async with async_session_factory() as session:
        #       session.add(InteractionAuditLog(...))
        #       await session.commit()
        pass


# Module-level singleton — import this everywhere
audit_logger = AuditLogger()
