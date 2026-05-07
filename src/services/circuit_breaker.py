"""
Gradual backpressure for the dialler — replaces the binary circuit breaker.

Previous behaviour (REPLACED):
  - At >=90% LLM RPM capacity: freeze ALL outbound dialling for ALL agents for 1800s
  - Wrong metric (RPM instead of TPM)
  - Wrong granularity (agent-level freeze when quota is shared globally)
  - Binary: 89% = full speed, 90% = complete stop for 30 minutes

New behaviour:
  - Proportional dispatch-rate multiplier based on real TPM utilisation
  - Reads from LLMRateLimiter's Redis TPM counter (same source of truth)
  - Returns a float [0.0, 1.0] — dialler multiplies its call-dispatch interval
  - No hardcoded freeze: worst case is a 10-second pause, then re-check
  - No per-agent granularity: LLM quota is global, backpressure should be too

The dialler (not in this repo) calls get_dispatch_rate() before dispatching
each outbound batch and adjusts its pacing accordingly. A multiplier of 0.5
means it should dispatch calls at half the normal rate — not stop entirely.

The original check_capacity(agent_id) is preserved for backward compatibility
with any dialler code that calls it. It delegates to get_dispatch_rate() and
returns True only when the rate is > 0.
"""

import logging
import time
from typing import Dict, Optional

from src.config import settings
from src.services.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)


class PostCallCircuitBreaker:
    """
    Provides backpressure signals to the dialler based on LLM TPM utilisation.

    Uses the same Redis TPM counter as LLMRateLimiter — single source of truth.
    No per-agent state: LLM quota is a global resource, and backpressure should
    reflect global load, not individual agent activity.
    """

    async def get_dispatch_rate(self) -> float:
        """
        Return a dispatch-rate multiplier in [0.0, 1.0].

        TPM utilisation | Multiplier | Behaviour
        < CB_TIER_FULL  |    1.0     | Full speed
        < CB_TIER_75    |    0.75    | Slight reduction
        < CB_TIER_50    |    0.50    | Moderate reduction
        < CB_TIER_25    |    0.25    | Heavy reduction
        >= CB_TIER_25   |    0.0     | Brief pause (CB_PAUSE_SECONDS), then re-check

        Values come from settings — tunable without deployment.
        """
        utilisation = await rate_limiter.get_utilisation()

        if utilisation < settings.CB_TIER_FULL:
            rate = 1.0
        elif utilisation < settings.CB_TIER_75:
            rate = 0.75
        elif utilisation < settings.CB_TIER_50:
            rate = 0.50
        elif utilisation < settings.CB_TIER_25:
            rate = 0.25
        else:
            rate = 0.0

        logger.debug(
            "dispatch_rate_computed",
            extra={
                "tpm_utilisation": round(utilisation, 3),
                "dispatch_rate": rate,
            },
        )

        if rate < 1.0:
            logger.info(
                "dialler_backpressure_active",
                extra={
                    "tpm_utilisation": round(utilisation, 3),
                    "dispatch_rate": rate,
                    "pause_seconds": settings.CB_PAUSE_SECONDS if rate == 0.0 else 0,
                },
            )

        return rate

    async def check_capacity(self, agent_id: str) -> bool:
        """
        Legacy compatibility shim — returns True if dispatch rate > 0.

        Called by dialler code that predates the gradual backpressure model.
        New dialler integrations should call get_dispatch_rate() directly.
        """
        rate = await self.get_dispatch_rate()
        if rate == 0.0:
            logger.warning(
                "capacity_check_blocking",
                extra={
                    "agent_id": agent_id,
                    "pause_seconds": settings.CB_PAUSE_SECONDS,
                    "reason": "tpm_utilisation_critical",
                },
            )
        return rate > 0.0

    # ── Legacy methods — kept for backward compat, no longer primary path ─────

    async def record_postcall_start(self):
        """
        No-op. Token tracking now handled by LLMRateLimiter.acquire() and
        LLMRateLimiter.record_actual_usage().
        Kept so existing callers in post_call_processor.py don't break.
        """
        pass

    async def record_postcall_end(self):
        """No-op. See record_postcall_start."""
        pass


circuit_breaker = PostCallCircuitBreaker()
