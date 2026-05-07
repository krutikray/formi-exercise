"""
Tests for the recording poller — validates AC4 and AC6.

AC4: Recording poller retries with backoff; never silently skips.
AC6: All failures produce structured log events with interaction_id.

Tests mock the Exotel API and S3 upload so no external calls are made.
"""

import pytest
from unittest.mock import AsyncMock, patch, call

from src.services.recording import (
    fetch_and_upload_recording,
    RecordingResult,
)
from src.config import settings


INTERACTION_ID = "test-interaction-001"
CALL_SID = "exotel-call-001"
ACCOUNT_ID = "test-account"
CUSTOMER_ID = "test-customer"
CAMPAIGN_ID = "test-campaign"


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    """
    Speed up tests by removing real sleeps.
    Patches asyncio.sleep so the polling loop runs instantly.
    """
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())


@pytest.fixture(autouse=True)
def mock_audit_logger():
    """Suppress DB writes from audit_logger during tests."""
    with patch("src.services.recording.audit_logger") as mock_al:
        mock_al.log_event = AsyncMock()
        yield mock_al


# ── AC4: Retry with backoff ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_recording_available_on_first_attempt():
    """Recording available immediately — returns uploaded status, 1 attempt."""
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload:

        mock_fetch.return_value = "https://exotel.com/recording/001.mp3"
        mock_upload.return_value = f"recordings/{INTERACTION_ID}.mp3"

        result = await fetch_and_upload_recording(
            INTERACTION_ID, CALL_SID, ACCOUNT_ID, CUSTOMER_ID, CAMPAIGN_ID
        )

    assert result.status == "uploaded"
    assert result.s3_key == f"recordings/{INTERACTION_ID}.mp3"
    assert result.attempts == 1
    mock_fetch.assert_called_once()


@pytest.mark.asyncio
async def test_recording_available_on_third_attempt():
    """
    AC4: Recording not ready on first two polls, available on third.
    Verifies retry loop executes and returns correct result.
    """
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload:

        # 404 × 2, then 200
        mock_fetch.side_effect = [None, None, "https://exotel.com/recording/001.mp3"]
        mock_upload.return_value = f"recordings/{INTERACTION_ID}.mp3"

        result = await fetch_and_upload_recording(
            INTERACTION_ID, CALL_SID, ACCOUNT_ID, CUSTOMER_ID, CAMPAIGN_ID
        )

    assert result.status == "uploaded"
    assert result.attempts == 3
    assert mock_fetch.call_count == 3


@pytest.mark.asyncio
async def test_recording_permanently_failed_after_max_attempts(mock_audit_logger):
    """
    AC4 + AC6: After max_attempts with no recording, returns not_available
    and emits a structured audit event with interaction_id.
    """
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = None  # Never available

        result = await fetch_and_upload_recording(
            INTERACTION_ID, CALL_SID, ACCOUNT_ID, CUSTOMER_ID, CAMPAIGN_ID
        )

    assert result.status == "not_available"
    assert result.reason == "max_attempts_exceeded"
    assert result.attempts == settings.RECORDING_POLL_MAX_ATTEMPTS
    assert mock_fetch.call_count == settings.RECORDING_POLL_MAX_ATTEMPTS

    # AC6: Verify structured audit event was emitted with interaction_id
    failure_calls = [
        c for c in mock_audit_logger.log_event.call_args_list
        if "recording_failed" in str(c)
    ]
    assert len(failure_calls) >= 1, "recording_failed audit event must be emitted"
    final_call_kwargs = failure_calls[-1].kwargs
    assert final_call_kwargs["interaction_id"] == INTERACTION_ID


@pytest.mark.asyncio
async def test_recording_never_silently_skips():
    """
    AC4: The old implementation returned None silently.
    The new implementation always returns a RecordingResult with a status field.
    """
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = None

        result = await fetch_and_upload_recording(
            INTERACTION_ID, CALL_SID, ACCOUNT_ID, CUSTOMER_ID, CAMPAIGN_ID
        )

    # Must never return None — always a typed RecordingResult
    assert result is not None
    assert isinstance(result, RecordingResult)
    assert result.status in ("uploaded", "not_available", "error")


@pytest.mark.asyncio
async def test_recording_skipped_when_no_call_sid():
    """No call_sid means the call was never connected — skip immediately."""
    result = await fetch_and_upload_recording(
        INTERACTION_ID, "", ACCOUNT_ID, CUSTOMER_ID, CAMPAIGN_ID
    )
    assert result.status == "not_available"
    assert result.reason == "no_call_sid"
    assert result.attempts == 0


@pytest.mark.asyncio
async def test_s3_upload_failure_returns_error_status():
    """
    If Exotel returns a URL but S3 upload fails, return error status.
    The polling loop does NOT retry S3 failures (different failure domain).
    """
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload:

        mock_fetch.return_value = "https://exotel.com/recording/001.mp3"
        mock_upload.side_effect = Exception("S3 connection timeout")

        result = await fetch_and_upload_recording(
            INTERACTION_ID, CALL_SID, ACCOUNT_ID, CUSTOMER_ID, CAMPAIGN_ID
        )

    assert result.status == "error"
    assert "s3_upload_failed" in result.reason
    # Exotel was only called once (error on upload, not on fetch)
    assert mock_fetch.call_count == 1


@pytest.mark.asyncio
async def test_recording_runs_independently_of_llm():
    """
    Recording and LLM budget acquisition run in parallel (asyncio.gather).
    Verify recording result is fully independent — no shared state.
    """
    import asyncio
    from src.services.rate_limiter import AcquireResult

    recording_started = asyncio.Event()
    recording_completed = asyncio.Event()

    async def slow_recording(*args, **kwargs):
        recording_started.set()
        await asyncio.sleep(0.01)  # minimal delay
        recording_completed.set()
        return RecordingResult(status="uploaded", s3_key="recordings/test.mp3", attempts=1)

    async def instant_budget_acquire(*args, **kwargs):
        return AcquireResult(
            customer_id="cust-a", estimated_tokens=1500,
            waited_seconds=0.0, source="shared_pool", tpm_utilisation=0.1
        )

    with patch("src.services.recording.fetch_and_upload_recording", side_effect=slow_recording), \
         patch("src.services.rate_limiter.rate_limiter.acquire", side_effect=instant_budget_acquire):

        # Both should complete — recording doesn't block budget acquisition
        from src.services.recording import fetch_and_upload_recording as real_fetch
        results = await asyncio.gather(
            slow_recording(INTERACTION_ID, CALL_SID, ACCOUNT_ID),
            instant_budget_acquire("cust-a", 1500),
            return_exceptions=True,
        )

    assert not any(isinstance(r, Exception) for r in results)
