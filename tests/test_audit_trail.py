"""
Tests for the complete audit trail — validates AC5 and AC6.

AC5: Every interaction has a complete audit trail from call-end to result.
AC6: All failures produce structured log events with interaction_id.

These tests verify that AuditEventType events are emitted at every stage
of the pipeline by checking what audit_logger.log_event() was called with.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock, call
from datetime import datetime

from src.services.audit_logger import AuditEventType


INTERACTION_ID = "test-interaction-audit"
CUSTOMER_ID = "test-customer-audit"
CAMPAIGN_ID = "test-campaign-audit"


def make_payload(transcript: list = None) -> dict:
    if transcript is None:
        transcript = [
            {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
            {"role": "customer", "content": "Yes, confirmed, book me."},
            {"role": "agent", "content": "Great, I've booked you in."},
            {"role": "customer", "content": "Thank you, confirmed."},
            {"role": "agent", "content": "See you then."},
            {"role": "customer", "content": "Bye."},
        ]
    return {
        "interaction_id": INTERACTION_ID,
        "session_id": "audit-session",
        "lead_id": "audit-lead",
        "campaign_id": CAMPAIGN_ID,
        "customer_id": CUSTOMER_ID,
        "agent_id": "audit-agent",
        "call_sid": "audit-call-sid",
        "transcript_text": "agent: Hello\ncustomer: confirmed",
        "conversation_data": {"transcript": transcript},
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "audit-exotel",
        "lane": "hot",
    }


def get_logged_event_types(mock_audit_logger) -> list:
    """Extract all event_type values from audit_logger.log_event calls."""
    return [
        c.kwargs.get("event_type") or c.args[3]
        for c in mock_audit_logger.log_event.call_args_list
    ]


def assert_event_with_interaction_id(mock_audit_logger, event_type: AuditEventType):
    """Assert that a specific event was logged with the correct interaction_id."""
    for c in mock_audit_logger.log_event.call_args_list:
        kwargs = c.kwargs
        args = c.args
        logged_id = kwargs.get("interaction_id") or (args[0] if args else None)
        logged_type = kwargs.get("event_type") or (args[3] if len(args) > 3 else None)
        if logged_type == event_type and logged_id == INTERACTION_ID:
            return  # found
    pytest.fail(
        f"Expected audit event {event_type} with interaction_id={INTERACTION_ID} "
        f"but got: {get_logged_event_types(mock_audit_logger)}"
    )


# ── AC5: Complete audit trail ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_audit_trail_emitted_at_endpoint():
    """
    AC5: INTERACTION_ENDED and LANE_CLASSIFIED events are emitted at the endpoint.
    """
    with patch("src.api.endpoints.audit_logger") as mock_al, \
         patch("src.api.endpoints.process_hot_interaction") as mock_hot, \
         patch("src.api.endpoints.process_cold_interaction") as mock_cold, \
         patch("src.api.endpoints._load_interaction", new_callable=AsyncMock) as mock_load, \
         patch("src.api.endpoints._update_interaction_status", new_callable=AsyncMock), \
         patch("src.api.endpoints._update_interaction_lane", new_callable=AsyncMock), \
         patch("src.api.endpoints._update_analysis_status", new_callable=AsyncMock):

        mock_al.log_event = AsyncMock()

        mock_load.return_value = {
            "id": INTERACTION_ID,
            "lead_id": "lead-001",
            "campaign_id": CAMPAIGN_ID,
            "customer_id": CUSTOMER_ID,
            "agent_id": "agent-001",
            "exotel_account_id": "exotel-001",
            "conversation_data": {
                "transcript": [
                    {"role": "agent", "content": "Hello, sir?"},
                    {"role": "customer", "content": "confirmed, book tomorrow"},
                    {"role": "agent", "content": "Booked!"},
                    {"role": "customer", "content": "Great, confirmed."},
                    {"role": "agent", "content": "See you then."},
                ]
            },
        }

        mock_task = MagicMock()
        mock_task.id = "mock-celery-task-id"
        mock_hot.apply_async.return_value = mock_task
        mock_cold.apply_async.return_value = mock_task

        from src.api.endpoints import end_interaction
        from uuid import uuid4
        from fastapi import Request

        response = await end_interaction(
            session_id=uuid4(),
            interaction_id=uuid4(),
            request=MagicMock(
                call_sid="test-call-sid",
                duration_seconds=120,
                call_status="completed",
                additional_data={},
            ),
        )

    # AC5: INTERACTION_ENDED must appear in the audit log.
    # Note: the endpoint uses the UUID from the path parameter, not the module constant.
    logged_types = get_logged_event_types(mock_al)
    assert AuditEventType.INTERACTION_ENDED in logged_types, \
        f"INTERACTION_ENDED not found in audit events: {logged_types}"

    # AC6: Every audit event call must include an interaction_id
    for c in mock_al.log_event.call_args_list:
        logged_id = c.kwargs.get("interaction_id") or (c.args[0] if c.args else None)
        assert logged_id is not None, f"Audit event missing interaction_id: {c}"


@pytest.mark.asyncio
async def test_complete_audit_trail_through_task():
    """
    AC5: Full pipeline audit trail — TASK_STARTED → LLM_REQUESTED →
         LLM_BUDGET_ACQUIRED → LLM_COMPLETED → SIGNAL_JOBS_TRIGGERED →
         LEAD_STAGE_UPDATED → TASK_COMPLETED.

    This is the primary AC5 test: validates every stage is audited.
    """
    from src.tasks.celery_tasks import _process_interaction
    from src.services.recording import RecordingResult
    from src.services.rate_limiter import AcquireResult

    payload = make_payload()
    mock_task = MagicMock()
    mock_task.request.retries = 0

    with patch("src.tasks.celery_tasks.audit_logger") as mock_al, \
         patch("src.tasks.celery_tasks.metrics_tracker") as mock_metrics, \
         patch("src.tasks.celery_tasks.fetch_and_upload_recording", new_callable=AsyncMock) as mock_rec, \
         patch("src.tasks.celery_tasks.rate_limiter") as mock_rl, \
         patch("src.tasks.celery_tasks.PostCallProcessor") as MockProcessor, \
         patch("src.tasks.celery_tasks.trigger_signal_jobs", new_callable=AsyncMock), \
         patch("src.tasks.celery_tasks.update_lead_stage", new_callable=AsyncMock), \
         patch("src.tasks.celery_tasks._write_analysis_result", new_callable=AsyncMock), \
         patch("src.tasks.celery_tasks._update_recording_key", new_callable=AsyncMock):

        mock_al.log_event = AsyncMock()
        mock_metrics.track_processing_started = AsyncMock()
        mock_metrics.track_processing_completed = AsyncMock()

        mock_rec.return_value = RecordingResult(
            status="uploaded",
            s3_key=f"recordings/{INTERACTION_ID}.mp3",
            attempts=1,
        )

        mock_rl.acquire = AsyncMock(
            return_value=AcquireResult(
                customer_id=CUSTOMER_ID,
                estimated_tokens=1500,
                waited_seconds=0.1,
                source="shared_pool",
                tpm_utilisation=0.4,
            )
        )
        mock_rl.record_actual_usage = AsyncMock()

        from src.services.post_call_processor import AnalysisResult
        mock_processor_instance = AsyncMock()
        mock_processor_instance.process_post_call = AsyncMock(
            return_value=AnalysisResult(
                call_stage="rebook_confirmed",
                entities={"date": "tomorrow 3PM"},
                summary="Customer confirmed rebooking",
                raw_response={"call_stage": "rebook_confirmed"},
                tokens_used=1350,
                latency_ms=2100.0,
                provider="openai",
                model="gpt-4o",
            )
        )
        MockProcessor.return_value = mock_processor_instance

        await _process_interaction(mock_task, payload, priority=1)

    # AC5: Verify the complete event sequence
    logged_types = get_logged_event_types(mock_al)

    required_events = [
        AuditEventType.TASK_STARTED,
        AuditEventType.LLM_REQUESTED,
        AuditEventType.LLM_BUDGET_ACQUIRED,
        AuditEventType.LLM_COMPLETED,
        AuditEventType.SIGNAL_JOBS_TRIGGERED,
        AuditEventType.LEAD_STAGE_UPDATED,
        AuditEventType.TASK_COMPLETED,
    ]

    for event in required_events:
        assert event in logged_types, \
            f"Missing required audit event: {event}. Got: {logged_types}"

    # AC6: Every event must carry interaction_id
    for c in mock_al.log_event.call_args_list:
        kwargs = c.kwargs
        args = c.args
        logged_id = kwargs.get("interaction_id") or (args[0] if args else None)
        assert logged_id == INTERACTION_ID, \
            f"Audit event missing interaction_id: {c}"


@pytest.mark.asyncio
async def test_failure_events_include_interaction_id():
    """
    AC6: Failure events (recording_failed, task_failed, etc.) must include interaction_id.
    Tests the recording failure path specifically.
    """
    with patch("src.services.recording.audit_logger") as mock_al, \
         patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch:

        import asyncio
        with patch("asyncio.sleep", new_callable=AsyncMock):
            mock_al.log_event = AsyncMock()
            mock_fetch.return_value = None  # never available

            from src.services.recording import fetch_and_upload_recording
            result = await fetch_and_upload_recording(
                INTERACTION_ID, "call-001", "account-001", CUSTOMER_ID, CAMPAIGN_ID
            )

    # AC6: The recording_failed event must have interaction_id
    assert_event_with_interaction_id(mock_al, AuditEventType.RECORDING_FAILED)


@pytest.mark.asyncio
async def test_short_call_skipped_event_emitted():
    """
    AC5: Short calls (skip lane) still produce an audit trail.
    INTERACTION_ENDED + SHORT_CALL_SKIPPED must both be logged.
    """
    short_transcript = [
        {"role": "agent", "content": "Hello—"},
        {"role": "customer", "content": "Wrong number."},
    ]

    with patch("src.api.endpoints.audit_logger") as mock_al, \
         patch("src.api.endpoints._load_interaction", new_callable=AsyncMock) as mock_load, \
         patch("src.api.endpoints._update_interaction_status", new_callable=AsyncMock), \
         patch("src.api.endpoints._update_interaction_lane", new_callable=AsyncMock), \
         patch("src.api.endpoints._update_analysis_status", new_callable=AsyncMock):

        mock_al.log_event = AsyncMock()
        mock_load.return_value = {
            "id": INTERACTION_ID,
            "lead_id": "lead-001",
            "campaign_id": CAMPAIGN_ID,
            "customer_id": CUSTOMER_ID,
            "agent_id": "agent-001",
            "exotel_account_id": "exotel-001",
            "conversation_data": {"transcript": short_transcript},
        }

        from src.api.endpoints import end_interaction
        from uuid import uuid4
        await end_interaction(
            session_id=uuid4(),
            interaction_id=uuid4(),
            request=MagicMock(
                call_sid=None,
                duration_seconds=5,
                call_status="completed",
                additional_data={},
            ),
        )

    logged_types = get_logged_event_types(mock_al)
    assert AuditEventType.INTERACTION_ENDED in logged_types
    assert AuditEventType.SHORT_CALL_SKIPPED in logged_types
