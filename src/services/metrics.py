"""
PostCallMetricsTracker — Records timing, outcome, and token usage metrics.

Extended from the original to add per-customer Redis usage buckets.
This makes "how many tokens did Customer X use this hour?" answerable with
a single Redis HGETALL — no log scanning required.

What we can now answer in real-time (from Redis):
  - How many tokens did Customer X use in the current minute?  → llm:usage:{cid}:{bucket}
  - What is global TPM utilisation right now?                 → llm:tpm:global
  - How deep is the hot/cold processing queue?                → Celery inspect (external)

What still requires log aggregation:
  - p95 LLM latency over the past hour
  - Percentage of calls failing at each step
  - Recording poll attempt distribution

Alert conditions (log-pattern-based for now; wire to Grafana/Datadog in prod):
  - recording_permanently_failed   → WARNING alert: recording missed
  - postcall_dead_lettered         → ERROR alert: permanent analysis loss
  - llm_rate_limit_approaching     → WARNING: TPM >85% for 2+ minutes
  - postcall_failed_permanently    → ERROR: backward-compat alias
"""

import logging
import time

from src.utils.redis_client import redis_client
from src.services.rate_limiter import REDIS_KEY_TPM_USAGE_PREFIX

logger = logging.getLogger(__name__)

# Alert threshold: log a warning when TPM utilisation crosses this fraction
TPM_ALERT_THRESHOLD = 0.85


class PostCallMetricsTracker:

    async def track_processing_started(self, interaction_id: str) -> None:
        """Record the wall-clock start time for an interaction's processing."""
        await redis_client.set(
            f"postcall:metrics:{interaction_id}:start",
            str(time.time()),
            ex=3600,
        )

    async def track_processing_completed(
        self,
        interaction_id: str,
        tokens_used: int,
        latency_ms: float,
        customer_id: str = "",
    ) -> None:
        """
        Log completion metrics and update per-customer usage counters in Redis.

        Per-customer bucket key: llm:usage:{customer_id}:{minute_bucket}
        TTL: 3600s (1 hour) — long enough to answer "usage this hour?" queries.
        """
        start = await redis_client.get(f"postcall:metrics:{interaction_id}:start")
        wall_time_s = time.time() - float(start) if start else 0

        logger.info(
            "postcall_metrics",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "tokens_used": tokens_used,
                "llm_latency_ms": latency_ms,
                "total_wall_time_s": round(wall_time_s, 2),
            },
        )

        # Update per-customer token usage bucket (for billing + alerting)
        if customer_id and tokens_used > 0:
            minute_bucket = int(time.time() // 60)
            usage_key = f"{REDIS_KEY_TPM_USAGE_PREFIX}:{customer_id}:{minute_bucket}"
            try:
                await redis_client.incrby(usage_key, tokens_used)
                await redis_client.expire(usage_key, 3600)
            except Exception as e:
                logger.warning(
                    "customer_usage_bucket_update_failed",
                    extra={"customer_id": customer_id, "error": str(e)},
                )

        # Check TPM utilisation and alert if approaching limit
        await self._check_and_alert_tpm()

    async def track_processing_failed(
        self, interaction_id: str, error: str
    ) -> None:
        """
        Log a permanent processing failure.
        A Grafana alert on postcall_failed_permanently should page on-call.
        """
        logger.error(
            "postcall_failed_permanently",
            extra={
                "interaction_id": interaction_id,
                "error": error,
            },
        )

    async def _check_and_alert_tpm(self) -> None:
        """
        Emit a warning log when global TPM utilisation exceeds the alert threshold.
        In production, wire this to Grafana/Datadog via log pattern matching
        or a metrics push.
        """
        try:
            from src.services.rate_limiter import (
                rate_limiter, REDIS_KEY_TPM_GLOBAL
            )
            from src.config import settings

            current = int(await redis_client.get(REDIS_KEY_TPM_GLOBAL) or 0)
            effective_tpm = int(
                settings.LLM_TOKENS_PER_MINUTE * settings.LLM_TPM_SAFETY_MARGIN
            )
            utilisation = current / effective_tpm if effective_tpm > 0 else 0.0

            if utilisation >= TPM_ALERT_THRESHOLD:
                logger.warning(
                    "llm_rate_limit_approaching",
                    extra={
                        "tpm_current": current,
                        "tpm_effective_cap": effective_tpm,
                        "utilisation": round(utilisation, 3),
                        "threshold": TPM_ALERT_THRESHOLD,
                    },
                )
        except Exception:
            pass  # metric alert failure must never affect processing


metrics_tracker = PostCallMetricsTracker()
