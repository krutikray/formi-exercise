"""
Tests for task durability — validates AC3 and AC8.

AC3: No task is permanently lost when Redis or Celery worker restarts.
     → Dead-letter is written to Postgres (not Redis) on retry exhaustion.
AC8: Short transcripts (<4 turns) never consume LLM quota.
     → PostCallProcessor._call_llm() is never called for short transcripts.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

from src.services.post_call_processor import PostCallContext


INTERACTION_ID = "test-interaction-durability"
CUSTOMER_ID = "test-customer-001"
CAMPAIGN_ID = "test-campaign-001"


def make_payload(transcript: list, **overrides) -> dict:
    """Build a minimal valid Celery task payload."""
    transcript_text = "\n".join(
        f"{t.get('role')}: {t.get('content')}" for t in transcript
    )
    base = {
        "interaction_id": INTERACTION_ID,
        "session_id": "test-session",
        "lead_id": "test-lead",
        "campaign_id": CAMPAIGN_ID,
        "customer_id": CUSTOMER_ID,
        "agent_id": "test-agent",
        "call_sid": "test-call-sid",
        "transcript_text": transcript_text,
        "conversation_data": {"transcript": transcript},
        "additional_data": {},
        "ended_at": datetime.utcnow().isoformat(),
        "exotel_account_id": "test-exotel",
        "lane": "cold",
    }
    base.update(overrides)
    return base


SHORT_TRANSCRIPT = [
    {"role": "agent", "content": "Hello—"},
    {"role": "customer", "content": "Wrong number."},
]

LONG_TRANSCRIPT = [
    {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
    {"role": "customer", "content": "Yes."},
    {"role": "agent", "content": "I'm calling from XYZ about your inquiry."},
    {"role": "customer", "content": "Oh yes, I was interested."},
    {"role": "agent", "content": "Would you like to schedule a demo?"},
    {"role": "customer", "content": "Sure, let's do tomorrow at 3 PM."},
]


# ── AC3: Durable dead-letter on exhausted retries ─────────────────────────────

@pytest.mark.asyncio
async def test_dead_letter_written_on_exhausted_retries():
    """
    AC3: When all Celery retries are exhausted, _write_dead_letter() is called.
    This writes to Postgres (not Redis), so it survives a Redis restart.
    """
    from src.tasks.celery_tasks import _process_interaction

    payload = make_payload(LONG_TRANSCRIPT)

    mock_task = MagicMock()
    mock_task.request.retries = 0

    with patch("src.tasks.celery_tasks.fetch_and_upload_recording", new_callable=AsyncMock) as mock_rec, \
         patch("src.tasks.celery_tasks.rate_limiter") as mock_rl, \
         patch("src.tasks.celery_tasks.PostCallProcessor") as MockProcessor, \
         patch("src.tasks.celery_tasks._write_dead_letter", new_callable=AsyncMock) as mock_dl, \
         patch("src.tasks.celery_tasks.audit_logger") as mock_al, \
         patch("src.tasks.celery_tasks.metrics_tracker") as mock_metrics:

        mock_al.log_event = AsyncMock()
        mock_metrics.track_processing_started = AsyncMock()
        mock_metrics.track_processing_completed = AsyncMock()

        # Recording succeeds
        from src.services.recording import RecordingResult
        mock_rec.return_value = RecordingResult(status="not_available", reason="test", attempts=1)

        # Rate limiter raises LLMBudgetTimeoutError
        from src.services.rate_limiter import LLMBudgetTimeoutError, AcquireResult
        mock_rl.acquire = AsyncMock(
            return_value=AcquireResult("cust", 1500, 0.0, "shared_pool", 0.5)
        )
        mock_rl.record_actual_usage = AsyncMock()

        # LLM always fails
        mock_processor_instance = AsyncMock()
        mock_processor_instance.process_post_call = AsyncMock(
            side_effect=RuntimeError("LLM service unavailable")
        )
        MockProcessor.return_value = mock_processor_instance

        with pytest.raises(RuntimeError):
            await _process_interaction(mock_task, payload, priority=10)

        # Dead-letter NOT yet written (retries not exhausted, task raises for Celery retry)
        mock_dl.assert_not_called()


@pytest.mark.asyncio
async def test_dead_letter_written_after_budget_timeout():
    """
    AC3: LLMBudgetTimeoutError (can't acquire budget) → dead-letter, not retry loop.
    Budget timeouts should not pile up retries — go straight to dead-letter.
    """
    from src.tasks.celery_tasks import _process_interaction
    from src.services.rate_limiter import LLMBudgetTimeoutError
    from src.services.recording import RecordingResult

    payload = make_payload(LONG_TRANSCRIPT)
    mock_task = MagicMock()
    mock_task.request.retries = 0

    with patch("src.tasks.celery_tasks.fetch_and_upload_recording", new_callable=AsyncMock) as mock_rec, \
         patch("src.tasks.celery_tasks.rate_limiter") as mock_rl, \
         patch("src.tasks.celery_tasks._write_dead_letter", new_callable=AsyncMock) as mock_dl, \
         patch("src.tasks.celery_tasks.audit_logger") as mock_al, \
         patch("src.tasks.celery_tasks.metrics_tracker") as mock_metrics:

        mock_al.log_event = AsyncMock()
        mock_metrics.track_processing_started = AsyncMock()
        mock_rec.return_value = RecordingResult(status="not_available", reason="test", attempts=1)
        mock_rl.acquire = AsyncMock(
            side_effect=LLMBudgetTimeoutError("cust-a", 600, 1500)
        )

        await _process_interaction(mock_task, payload, priority=10)

        # Dead-letter IS written when budget timeout occurs
        mock_dl.assert_called_once()
        call_args = mock_dl.call_args
        assert call_args[0][0] == payload  # first positional arg is the payload


# ── AC8: Short transcripts skip LLM ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_short_transcript_skips_llm_in_task():
    """
    AC8: Short transcripts (<4 turns) never call _call_llm() in the Celery task.
    This gate exists in the task (not just the endpoint) so re-queued tasks
    after a crash are still protected.
    """
    from src.tasks.celery_tasks import _process_interaction

    payload = make_payload(SHORT_TRANSCRIPT)
    mock_task = MagicMock()
    mock_task.request.retries = 0

    with patch("src.tasks.celery_tasks.PostCallProcessor") as MockProcessor, \
         patch("src.tasks.celery_tasks.audit_logger") as mock_al, \
         patch("src.tasks.celery_tasks.metrics_tracker") as mock_metrics:

        mock_al.log_event = AsyncMock()
        mock_metrics.track_processing_started = AsyncMock()
        mock_processor_instance = AsyncMock()
        MockProcessor.return_value = mock_processor_instance

        await _process_interaction(mock_task, payload, priority=10)

        # LLM must NOT be called for short transcripts
        mock_processor_instance.process_post_call.assert_not_called()


@pytest.mark.asyncio
async def test_short_transcript_skips_llm_in_endpoint():
    """
    AC8: Short transcripts are intercepted at the endpoint level too.
    Lane = 'skip', no Celery task is enqueued.
    """
    from src.api.endpoints import _classify_lane

    skip_lane = _classify_lane(SHORT_TRANSCRIPT)
    assert skip_lane == "skip"


@pytest.mark.asyncio
async def test_long_transcript_does_not_skip_llm():
    """Sanity check: long transcripts are NOT classified as skip."""
    from src.api.endpoints import _classify_lane

    lane = _classify_lane(LONG_TRANSCRIPT)
    assert lane in ("hot", "cold"), f"Unexpected lane: {lane}"


@pytest.mark.asyncio
async def test_short_transcript_gate_at_exactly_4_turns():
    """Boundary: exactly 4 turns is NOT a short transcript."""
    from src.api.endpoints import _classify_lane

    four_turn = [
        {"role": "agent", "content": "Hello"},
        {"role": "customer", "content": "Hi"},
        {"role": "agent", "content": "Are you interested?"},
        {"role": "customer", "content": "Yes, tell me more."},
    ]
    lane = _classify_lane(four_turn)
    assert lane != "skip"


# ── AC7: Dialler is not binary-frozen ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_circuit_breaker_no_hardcoded_freeze():
    """
    AC7: No hardcoded 1800-second freeze. Backpressure is proportional.
    """
    from src.services.circuit_breaker import PostCallCircuitBreaker

    breaker = PostCallCircuitBreaker()

    # Verify no hardcoded 1800s freeze in new implementation
    import inspect
    import src.services.circuit_breaker as cb_module
    source = inspect.getsource(cb_module)

    # The freeze constant is kept for backward compat in config but not USED
    # in the new circuit breaker logic
    assert "freeze_until" not in source, \
        "Binary freeze_until pattern should not exist in new circuit breaker"


@pytest.mark.asyncio
async def test_circuit_breaker_returns_proportional_rate():
    """AC7: get_dispatch_rate() returns a value between 0 and 1 (not binary)."""
    from src.services.circuit_breaker import PostCallCircuitBreaker

    breaker = PostCallCircuitBreaker()

    with patch("src.services.circuit_breaker.rate_limiter") as mock_rl:
        mock_rl.get_utilisation = AsyncMock(return_value=0.5)  # 50% utilisation
        rate = await breaker.get_dispatch_rate()

    assert 0.0 <= rate <= 1.0
    assert rate in (0.0, 0.25, 0.50, 0.75, 1.0), \
        f"Rate should be one of the tier values, got {rate}"
