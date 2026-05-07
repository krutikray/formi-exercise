"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
  After a call ends, Exotel processes the audio and makes a recording URL
  available via their REST API. The time between call-end and URL availability
  varies: typically 10–30 seconds, but can be 60–90s under load on their end.

  The URL is fetched via:
      GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording
  Returns 200 + recording_url if ready, 404 if not yet available.

Previous approach (REPLACED):
  await asyncio.sleep(45)   ← wasted worker slot, missed late recordings silently

New approach:
  Poll with exponential backoff. Each attempt is non-blocking between sleeps.
  Every outcome (success, permanent failure, individual attempt) emits a
  structured audit event with interaction_id. Nothing is silent.

  Poll schedule (defaults): 5s, 7.5s, 11.2s, 16.8s, 25.3s, 30s, 30s...
  Total window: ~120s for 12 attempts, covering the Exotel 60–90s tail.

  Recording and LLM acquisition now run in parallel (asyncio.gather in the
  Celery task) — the LLM doesn't wait for the recording.
"""

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Optional

import httpx

from src.config import settings
from src.services.audit_logger import audit_logger, AuditEventType

logger = logging.getLogger(__name__)


@dataclass
class RecordingResult:
    """
    Always returned — never returns None.
    Callers check .status to decide whether to update the DB.
    """
    status: str          # "uploaded" | "not_available" | "error"
    s3_key: Optional[str] = None
    reason: Optional[str] = None
    attempts: int = 0


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    customer_id: str = "",
    campaign_id: str = "",
) -> RecordingResult:
    """
    Poll Exotel for the recording URL with exponential backoff, then upload to S3.

    Returns a RecordingResult with status in ["uploaded", "not_available", "error"].
    Every outcome emits a structured audit event — nothing is silent.

    This function is designed to run concurrently with LLM budget acquisition
    via asyncio.gather() in the Celery task. It does not block LLM analysis.
    """
    if not call_sid:
        # No call_sid means the telephony provider didn't assign one (e.g. unanswered)
        logger.info(
            "recording_skipped_no_call_sid",
            extra={"interaction_id": interaction_id},
        )
        return RecordingResult(status="not_available", reason="no_call_sid", attempts=0)

    for attempt in range(settings.RECORDING_POLL_MAX_ATTEMPTS):
        try:
            recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)

            if recording_url:
                # Recording is ready — upload to S3
                try:
                    s3_key = await _upload_to_s3(recording_url, interaction_id)
                    await audit_logger.log_event(
                        interaction_id=interaction_id,
                        customer_id=customer_id,
                        campaign_id=campaign_id,
                        event_type=AuditEventType.RECORDING_UPLOADED,
                        event_data={
                            "s3_key": s3_key,
                            "attempt": attempt + 1,
                            "call_sid": call_sid,
                        },
                    )
                    logger.info(
                        "recording_uploaded",
                        extra={
                            "interaction_id": interaction_id,
                            "s3_key": s3_key,
                            "attempt": attempt + 1,
                        },
                    )
                    return RecordingResult(
                        status="uploaded", s3_key=s3_key, attempts=attempt + 1
                    )
                except Exception as upload_err:
                    # URL was available but upload failed — don't retry the whole
                    # polling loop; this is a different failure mode (S3/network issue)
                    await audit_logger.log_event(
                        interaction_id=interaction_id,
                        customer_id=customer_id,
                        campaign_id=campaign_id,
                        event_type=AuditEventType.RECORDING_FAILED,
                        event_data={"attempt": attempt + 1, "call_sid": call_sid},
                        error_detail=f"S3 upload failed: {upload_err}",
                    )
                    logger.error(
                        "recording_upload_to_s3_failed",
                        extra={
                            "interaction_id": interaction_id,
                            "error": str(upload_err),
                        },
                    )
                    return RecordingResult(
                        status="error",
                        reason=f"s3_upload_failed: {upload_err}",
                        attempts=attempt + 1,
                    )

            # Recording not yet available — log the attempt and wait
            await audit_logger.log_event(
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                event_type=AuditEventType.RECORDING_POLL_ATTEMPT,
                event_data={
                    "attempt": attempt + 1,
                    "max_attempts": settings.RECORDING_POLL_MAX_ATTEMPTS,
                    "call_sid": call_sid,
                },
            )

        except Exception as poll_err:
            # Exotel API call failed (network error, timeout, etc.)
            # Log and continue to next attempt
            logger.warning(
                "recording_poll_attempt_failed",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt + 1,
                    "error": str(poll_err),
                },
            )

        # Exponential backoff with jitter before next attempt
        if attempt < settings.RECORDING_POLL_MAX_ATTEMPTS - 1:
            sleep_s = min(
                settings.RECORDING_POLL_INTERVAL_SECONDS
                * (settings.RECORDING_POLL_BACKOFF_FACTOR ** attempt),
                settings.RECORDING_POLL_MAX_SLEEP_SECONDS,
            )
            # Jitter: ±20% of sleep_s to spread load across multiple workers
            jitter = random.uniform(-0.2 * sleep_s, 0.2 * sleep_s)
            await asyncio.sleep(max(0, sleep_s + jitter))

    # All attempts exhausted — permanent failure
    # This is an ERROR-level, alertable event (not DEBUG like the old implementation)
    await audit_logger.log_event(
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        event_type=AuditEventType.RECORDING_FAILED,
        event_data={
            "attempts": settings.RECORDING_POLL_MAX_ATTEMPTS,
            "call_sid": call_sid,
        },
        error_detail=f"Recording not available after {settings.RECORDING_POLL_MAX_ATTEMPTS} attempts",
    )
    logger.error(
        "recording_permanently_failed",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "attempts": settings.RECORDING_POLL_MAX_ATTEMPTS,
        },
    )
    return RecordingResult(
        status="not_available",
        reason="max_attempts_exceeded",
        attempts=settings.RECORDING_POLL_MAX_ATTEMPTS,
    )


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns the recording URL if available, None if not yet ready (404) or error.
    Note: 404 (not ready) and genuine errors (call had no recording) both return
    None here. The polling loop treats them identically — retry until max_attempts.
    """
    if not account_id or not call_sid:
        return None

    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("recording_url")
            # 404 = not yet ready; any other status = error — both return None
            return None
    except httpx.HTTPError:
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production: stream from recording_url → boto3 multipart upload to S3_BUCKET.
    S3 key format: recordings/{interaction_id}.mp3

    After a successful upload, the caller is responsible for writing
    recording_s3_key back to the interactions row (done in the Celery task).
    If this crashes after upload but before the DB write, the file exists in S3
    but the row doesn't know — a reconciliation job can detect orphaned keys
    via S3 list + DB join.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    # Assessment stub — in production:
    #   async with aioboto3.resource("s3") as s3:
    #       async with httpx.AsyncClient() as client:
    #           resp = await client.get(recording_url)
    #           await s3.Object(settings.S3_BUCKET, s3_key).put(Body=resp.content)

    logger.info(
        "recording_s3_upload_completed",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key
