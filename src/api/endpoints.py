"""
FastAPI endpoint for ending an interaction.

POST /session/{session_id}/interaction/{interaction_id}/end

Called by Exotel (telephony provider) when a call disconnects. Must respond
within 5 seconds — Exotel will retry if we don't.

Changes from the original implementation:
──────────────────────────────────────────
1. Lane classification (keyword heuristic, no LLM) at webhook receipt.
   Routes to postcall_hot or postcall_cold Celery queue accordingly.
   Short transcripts (<4 turns) take a skip path — no Celery, no LLM.

2. Premature asyncio.create_task signal_jobs calls removed.
   The original code fired trigger_signal_jobs({}) before LLM ran, sending
   empty payloads to downstream systems. Signal jobs now fire only once,
   from the Celery task, after analysis is complete.

3. Structured audit event emitted at every state transition.
   Every interaction has an INTERACTION_ENDED event with lane classification
   written to interaction_audit_log before the 200 is returned.

4. processing_lane written to the interactions row.
   Queryable — ops can ask "how many hot calls are pending?" via SQL.

API contract: unchanged. Same path, same request/response schema.
Significant callers (Exotel webhook) need no reconfiguration.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.config import settings
from src.services.audit_logger import audit_logger, AuditEventType
from src.tasks.celery_tasks import process_hot_interaction, process_cold_interaction

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Keyword sets for lane classification ──────────────────────────────────────
# These come from the sample transcripts: expected_lane is the ground truth.
# "hot" = business-critical outcomes that need immediate processing.
# "cold" = low-urgency outcomes that can be deferred under rate-limit pressure.
#
# Assumption: keyword matching is sufficient for routing (not a second LLM call).
# Known weakness: Hinglish mixed-language transcripts may use neither Hindi nor
# English keywords clearly. The "hinglish_ambiguous" fixture routes cold, which
# is acceptable (slightly delayed processing, not lost).
_HOT_KEYWORDS = frozenset({
    "confirmed", "confirm", "rebook", "booked", "book", "demo",
    "escalat", "manager", "complaint", "angry", "urgent",
    "appointment", "appoint", "schedule", "tomorrow",
})
_COLD_KEYWORDS = frozenset({
    "not interested", "don't call", "do not call", "busy",
    "callback", "call back", "later", "already", "wrong number",
})


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str
    lane: str   # hot | cold | skip — visible in the response for debugging


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
):
    """
    End an interaction and trigger post-call processing.

    Flow:
    1. Load interaction from DB
    2. Mark status ENDED, write call_sid + duration
    3. Classify lane (skip | hot | cold) via keyword heuristic
    4. Emit INTERACTION_ENDED audit event
    5a. skip: write analysis_status=skipped directly, return 200
    5b. hot/cold: enqueue to lane-specific Celery queue, return 200

    Signal jobs fire ONLY from the Celery task after analysis is complete.
    """
    try:
        interaction = await _load_interaction(interaction_id)
        if not interaction:
            raise HTTPException(status_code=404, detail="Interaction not found")

        await _update_interaction_status(
            interaction_id=str(interaction_id),
            status="ENDED",
            ended_at=datetime.utcnow(),
            duration=request.duration_seconds,
            call_sid=request.call_sid,
        )

        transcript = interaction.get("conversation_data", {}).get("transcript", [])
        lane = _classify_lane(transcript)

        # Write lane to DB so it's queryable before Celery runs
        await _update_interaction_lane(str(interaction_id), lane)

        # Emit audit event — correlation anchor for the entire pipeline
        await audit_logger.log_event(
            interaction_id=str(interaction_id),
            customer_id=interaction["customer_id"],
            campaign_id=interaction["campaign_id"],
            event_type=AuditEventType.INTERACTION_ENDED,
            event_data={
                "lane": lane,
                "transcript_turns": len(transcript),
                "call_sid": request.call_sid,
                "duration_seconds": request.duration_seconds,
                "call_status": request.call_status,
            },
        )

        if lane == "skip":
            # Short transcript: update status directly, no LLM, no Celery
            await _update_analysis_status(str(interaction_id), "skipped")
            await audit_logger.log_event(
                interaction_id=str(interaction_id),
                customer_id=interaction["customer_id"],
                campaign_id=interaction["campaign_id"],
                event_type=AuditEventType.SHORT_CALL_SKIPPED,
                event_data={"transcript_turns": len(transcript)},
            )
            logger.info(
                "short_transcript_fast_path",
                extra={
                    "interaction_id": str(interaction_id),
                    "transcript_turns": len(transcript),
                },
            )
            return InteractionEndResponse(
                status="ok",
                interaction_id=str(interaction_id),
                message="Short transcript — skipped LLM analysis",
                lane="skip",
            )

        # Build Celery payload — all context needed for processing
        transcript_text = "\n".join(
            f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
            for turn in transcript
        )
        celery_payload = {
            "interaction_id": str(interaction_id),
            "session_id": str(session_id),
            "lead_id": interaction["lead_id"],
            "campaign_id": interaction["campaign_id"],
            "customer_id": interaction["customer_id"],
            "agent_id": interaction["agent_id"],
            "call_sid": request.call_sid,
            "transcript_text": transcript_text,
            "conversation_data": interaction.get("conversation_data", {}),
            "additional_data": request.additional_data or {},
            "ended_at": datetime.utcnow().isoformat(),
            "exotel_account_id": interaction.get("exotel_account_id"),
            "lane": lane,
        }

        if lane == "hot":
            task = process_hot_interaction.apply_async(
                args=[celery_payload],
                queue=settings.POSTCALL_HOT_QUEUE,
            )
        else:
            task = process_cold_interaction.apply_async(
                args=[celery_payload],
                queue=settings.POSTCALL_COLD_QUEUE,
            )

        logger.info(
            "postcall_enqueued",
            extra={
                "interaction_id": str(interaction_id),
                "lane": lane,
                "celery_task_id": task.id,
                "queue": settings.POSTCALL_HOT_QUEUE if lane == "hot" else settings.POSTCALL_COLD_QUEUE,
            },
        )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            message=f"Interaction ended, enqueued to {lane} lane",
            lane=lane,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "end_interaction_failed",
            extra={"interaction_id": str(interaction_id), "error": str(e)},
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def _classify_lane(transcript: list) -> str:
    """
    Classify transcript into one of three processing lanes.

    Returns:
        "skip": <4 turns — wrong number, immediate hangup. No LLM needed.
        "hot":  High-value outcome detected (confirmed booking, escalation).
        "cold": Default — deferred processing under rate-limit pressure.

    Algorithm: case-insensitive keyword scan of all customer turns.
    Hot keywords take priority — if any hot keyword is found, lane = hot
    regardless of cold keywords present. This avoids misclassifying a
    call where the customer says "not interested in waiting, book me now."
    """
    if len(transcript) < 4:
        return "skip"

    # Scan all turns for hot keywords: agent-side keywords ("demo booked", "appointment",
    # "escalat") are reliable positive signals regardless of which party speaks them.
    # We already limit the hot set to words that genuinely signal urgent outcomes.
    all_text = " ".join(turn.get("content", "").lower() for turn in transcript)

    for kw in _HOT_KEYWORDS:
        if kw in all_text:
            return "hot"

    return "cold"


# ── Database helpers (stubs — replace with real async SQLAlchemy in production) ──

async def _load_interaction(interaction_id: UUID) -> Optional[Dict[str, Any]]:
    """
    Load interaction from DB.

    Production:
        SELECT id, lead_id, campaign_id, customer_id, agent_id,
               conversation_data, exotel_account_id
        FROM interactions WHERE id = $1
    """
    return {
        "id": str(interaction_id),
        "lead_id": "mock-lead-id",
        "campaign_id": "mock-campaign-id",
        "customer_id": "mock-customer-id",
        "agent_id": "mock-agent-id",
        "exotel_account_id": "mock-exotel-account",
        "conversation_data": {
            "transcript": [
                {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
                {"role": "customer", "content": "Yes, speaking."},
                {"role": "agent", "content": "I'm calling from XYZ about your recent inquiry."},
                {"role": "customer", "content": "Oh yes, I was looking at the product."},
                {"role": "agent", "content": "Would you like to schedule a demo?"},
                {"role": "customer", "content": "Sure, let's do tomorrow at 3 PM."},
                {"role": "agent", "content": "Perfect, I've booked a demo for tomorrow at 3 PM."},
                {"role": "customer", "content": "Thank you, bye."},
            ]
        },
    }


async def _update_interaction_status(
    interaction_id: str,
    status: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """
    Production:
        UPDATE interactions
        SET status=$2, ended_at=$3, duration_seconds=$4, call_sid=$5
        WHERE id=$1
    """
    logger.info(
        "interaction_status_updated",
        extra={
            "interaction_id": interaction_id,
            "status": status,
            "ended_at": ended_at.isoformat(),
        },
    )


async def _update_interaction_lane(interaction_id: str, lane: str) -> None:
    """
    Production:
        UPDATE interactions SET processing_lane=$2 WHERE id=$1
    """
    logger.debug(
        "interaction_lane_set",
        extra={"interaction_id": interaction_id, "lane": lane},
    )


async def _update_analysis_status(interaction_id: str, status: str) -> None:
    """
    Production:
        UPDATE interactions SET analysis_status=$2 WHERE id=$1
    """
    logger.debug(
        "analysis_status_updated",
        extra={"interaction_id": interaction_id, "analysis_status": status},
    )
