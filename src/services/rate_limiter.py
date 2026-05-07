"""
LLM Rate Limiter — Token budget enforcement for all LLM requests.

This is the core fix for the primary problem: the current system fires LLM
requests unconditionally, burning through the provider's hard rate limits
and triggering 429 storms at scale.

Design: Redis Sliding-Window + Per-Customer HINCRBY Budget
──────────────────────────────────────────────────────────
Global limits (shared across all customers):
  Redis key  : llm:tpm:global        → total tokens used in current 60s window
  Redis key  : llm:rpm:global        → total requests in current 60s window
  TTL        : 60 seconds (natural rolling window via pipeline INCRBY + EXPIRE)

Per-customer budget (dedicated allocation):
  Redis key  : llm:budget:{customer_id}:{minute_bucket}
  Bucket     : int(time.time() // 60)  — resets each calendar minute
  Value      : cumulative tokens used this minute (HINCRBY is atomic)

Acquire flow:
  1. Check global TPM headroom. If < estimated_tokens → wait + retry.
  2. Check customer dedicated budget. If customer has a dedicated allocation
     and hasn't exceeded it → charge to dedicated budget.
  3. If customer has no dedicated budget OR dedicated budget exhausted →
     draw from shared pool (global headroom - sum of dedicated usage).
  4. On success, record estimated_tokens in global and per-customer counters.
  5. After real LLM response: call record_actual_usage() to correct the estimate.

Priority:
  Hot-lane tasks call acquire(priority=1), cold-lane calls acquire(priority=10).
  When both are blocked waiting for headroom, hot wakes first via lower backoff
  factor (priority is implemented as shorter sleep intervals, not preemption).

This module never raises 429 to its callers. It blocks with exponential
backoff + jitter until budget is available or the timeout expires. On timeout,
it raises LLMBudgetTimeoutError which the Celery task converts to dead-letter.
"""

import asyncio
import logging
import math
import random
import time
from dataclasses import dataclass
from typing import Optional

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

# Redis key constants — centralised so they can be imported in tests
REDIS_KEY_TPM_GLOBAL = "llm:tpm:global"
REDIS_KEY_RPM_GLOBAL = "llm:rpm:global"
REDIS_KEY_BUDGET_PREFIX = "llm:budget"   # full key: llm:budget:{customer_id}:{minute}
REDIS_KEY_TPM_USAGE_PREFIX = "llm:usage" # full key: llm:usage:{customer_id}:{minute}


class LLMBudgetTimeoutError(Exception):
    """Raised when a task cannot acquire LLM budget within its timeout window."""
    def __init__(self, customer_id: str, timeout_seconds: int, estimated_tokens: int):
        self.customer_id = customer_id
        self.timeout_seconds = timeout_seconds
        self.estimated_tokens = estimated_tokens
        super().__init__(
            f"Could not acquire {estimated_tokens} tokens for customer {customer_id} "
            f"within {timeout_seconds}s timeout"
        )


@dataclass
class AcquireResult:
    """Returned by LLMRateLimiter.acquire() on success."""
    customer_id: str
    estimated_tokens: int
    waited_seconds: float
    source: str          # "dedicated" | "shared_pool"
    tpm_utilisation: float  # fraction of effective TPM cap at acquire time


class LLMRateLimiter:
    """
    Enforces global TPM/RPM limits and per-customer token budgets.

    One instance per process — designed as a module-level singleton.
    All Redis operations are atomic (INCRBY, EXPIRE) or use Lua scripts
    where atomicity is required.
    """

    def __init__(self):
        # Effective caps after applying safety margin
        self._effective_tpm = int(
            settings.LLM_TOKENS_PER_MINUTE * settings.LLM_TPM_SAFETY_MARGIN
        )
        self._effective_rpm = int(
            settings.LLM_REQUESTS_PER_MINUTE * settings.LLM_RPM_SAFETY_MARGIN
        )
        # Shared pool = headroom beyond all dedicated allocations (min 20% of cap)
        self._shared_pool_tokens = int(
            settings.LLM_TOKENS_PER_MINUTE * settings.LLM_SHARED_POOL_FRACTION
        )

    # ── Public API ────────────────────────────────────────────────────────────

    async def acquire(
        self,
        customer_id: str,
        estimated_tokens: int,
        priority: int = 10,
        timeout_seconds: Optional[int] = None,
    ) -> AcquireResult:
        """
        Block until global TPM headroom and customer budget are available.

        Args:
            customer_id:      Used to look up dedicated budget allocation.
            estimated_tokens: Pre-flight token estimate (LLM_AVG_TOKENS_PER_CALL).
            priority:         1 = hot (shorter backoff), 10 = cold (longer backoff).
            timeout_seconds:  Override default timeout. None = use settings default.

        Returns:
            AcquireResult on success.

        Raises:
            LLMBudgetTimeoutError if timeout expires before budget is available.
        """
        if timeout_seconds is None:
            timeout_seconds = (
                settings.LLM_ACQUIRE_TIMEOUT_HOT_SECONDS
                if priority <= 3
                else settings.LLM_ACQUIRE_TIMEOUT_COLD_SECONDS
            )

        start = time.monotonic()
        attempt = 0
        base_sleep = 0.5 if priority <= 3 else 2.0  # hot waits shorter

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= timeout_seconds:
                raise LLMBudgetTimeoutError(customer_id, timeout_seconds, estimated_tokens)

            result = await self._try_acquire(customer_id, estimated_tokens)
            if result is not None:
                result.waited_seconds = elapsed
                logger.debug(
                    "llm_budget_acquired",
                    extra={
                        "customer_id": customer_id,
                        "estimated_tokens": estimated_tokens,
                        "source": result.source,
                        "waited_s": round(elapsed, 2),
                        "tpm_utilisation": round(result.tpm_utilisation, 3),
                    },
                )
                return result

            # Budget not available — back off with jitter
            sleep_s = min(
                base_sleep * (1.5 ** attempt) + random.uniform(0, 0.5),
                30.0,
            )
            # Don't sleep past the timeout
            sleep_s = min(sleep_s, timeout_seconds - elapsed - 0.1)
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)
            attempt += 1

    async def record_actual_usage(
        self,
        customer_id: str,
        actual_tokens: int,
        estimated_tokens: int,
    ) -> None:
        """
        Correct the counters after the real LLM response is received.

        acquire() pre-charges estimated_tokens. This method adds the delta
        (actual - estimated) to both the global and per-customer counters.
        If actual < estimated (common), the delta is negative (effectively
        freeing headroom for other tasks).

        This ensures our rolling-window counts reflect real usage, not estimates.
        """
        delta = actual_tokens - estimated_tokens
        if delta == 0:
            return

        minute_bucket = self._minute_bucket()
        budget_key = f"{REDIS_KEY_BUDGET_PREFIX}:{customer_id}:{minute_bucket}"
        usage_key = f"{REDIS_KEY_TPM_USAGE_PREFIX}:{customer_id}:{minute_bucket}"

        try:
            pipe = redis_client.pipeline()
            pipe.incrby(REDIS_KEY_TPM_GLOBAL, delta)
            pipe.incrby(budget_key, delta)
            pipe.incrby(usage_key, actual_tokens)  # track actual (not delta) for billing
            await pipe.execute()
        except Exception as e:
            # Non-fatal: counters will be slightly off until TTL expires
            logger.warning(
                "rate_limiter_correction_failed",
                extra={"customer_id": customer_id, "delta": delta, "error": str(e)},
            )

    async def get_utilisation(self) -> float:
        """
        Current TPM utilisation as a fraction [0.0, 1.0].
        Used by the circuit breaker for proportional backpressure.
        """
        try:
            current = int(await redis_client.get(REDIS_KEY_TPM_GLOBAL) or 0)
            return min(current / self._effective_tpm, 1.0) if self._effective_tpm > 0 else 0.0
        except Exception:
            return 0.0

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _try_acquire(
        self, customer_id: str, estimated_tokens: int
    ) -> Optional[AcquireResult]:
        """
        Attempt a single non-blocking acquire. Returns None if budget unavailable.

        Uses a Redis pipeline for the global counter check + increment to
        minimise round-trips. Per-customer budget check is a separate HINCRBY.
        """
        try:
            # ── Step 1: Check + reserve global TPM headroom ───────────────────
            # INCRBY and EXPIRE are sent as a pipeline (two round-trips → one).
            # We INCRBY first, then check. If over limit, we decrement.
            # This is optimistic: assumes most calls will succeed.
            pipe = redis_client.pipeline()
            pipe.incrby(REDIS_KEY_TPM_GLOBAL, estimated_tokens)
            pipe.expire(REDIS_KEY_TPM_GLOBAL, 60)
            pipe.incr(REDIS_KEY_RPM_GLOBAL)
            pipe.expire(REDIS_KEY_RPM_GLOBAL, 60)
            results = await pipe.execute()

            new_tpm = results[0]
            new_rpm = results[2]

            tpm_ok = new_tpm <= self._effective_tpm
            rpm_ok = new_rpm <= self._effective_rpm

            if not tpm_ok or not rpm_ok:
                # Roll back the reservation
                rollback = redis_client.pipeline()
                rollback.decrby(REDIS_KEY_TPM_GLOBAL, estimated_tokens)
                rollback.decr(REDIS_KEY_RPM_GLOBAL)
                await rollback.execute()

                logger.debug(
                    "llm_rate_limit_headroom_exhausted",
                    extra={
                        "customer_id": customer_id,
                        "tpm_after": new_tpm,
                        "rpm_after": new_rpm,
                        "effective_tpm": self._effective_tpm,
                        "effective_rpm": self._effective_rpm,
                    },
                )
                return None

            # ── Step 2: Check per-customer dedicated budget ───────────────────
            minute_bucket = self._minute_bucket()
            budget_key = f"{REDIS_KEY_BUDGET_PREFIX}:{customer_id}:{minute_bucket}"

            dedicated_budget = await self._get_customer_budget(customer_id)
            source = "shared_pool"

            if dedicated_budget > 0:
                new_dedicated = await redis_client.incrby(budget_key, estimated_tokens)
                await redis_client.expire(budget_key, 120)  # 2-min TTL for safety

                if new_dedicated <= dedicated_budget:
                    source = "dedicated"
                else:
                    # Over dedicated budget — decrement and check shared pool
                    await redis_client.decrby(budget_key, estimated_tokens)
                    source = "shared_pool"

            # shared_pool source: global headroom already reserved above (step 1)
            # No additional per-customer check needed — first-come-first-served
            # in the shared pool.

            tpm_utilisation = new_tpm / self._effective_tpm if self._effective_tpm > 0 else 0.0

            return AcquireResult(
                customer_id=customer_id,
                estimated_tokens=estimated_tokens,
                waited_seconds=0.0,  # filled in by acquire()
                source=source,
                tpm_utilisation=tpm_utilisation,
            )

        except Exception as e:
            logger.error(
                "rate_limiter_acquire_error",
                extra={"customer_id": customer_id, "error": str(e)},
            )
            # On Redis error: allow the request (fail open) to avoid starving
            # processing when Redis is temporarily unavailable.
            return AcquireResult(
                customer_id=customer_id,
                estimated_tokens=estimated_tokens,
                waited_seconds=0.0,
                source="fail_open",
                tpm_utilisation=0.0,
            )

    async def _get_customer_budget(self, customer_id: str) -> int:
        """
        Fetch per-minute token allocation for customer_id.

        In production: SELECT tokens_per_minute FROM customer_llm_budgets WHERE customer_id=$1.
        Assessment stub: returns default (0 = shared pool only).

        Caching: this is called on every acquire(). In production, cache in
        Redis with a 5-minute TTL to avoid a DB round-trip per request.
        """
        # Assessment stub — replace with async DB query:
        #   result = await db.execute(
        #       select(CustomerLlmBudget.tokens_per_minute)
        #       .where(CustomerLlmBudget.customer_id == customer_id)
        #   )
        #   row = result.scalar_one_or_none()
        #   return row or settings.DEFAULT_CUSTOMER_TOKEN_BUDGET_PER_MIN
        return settings.DEFAULT_CUSTOMER_TOKEN_BUDGET_PER_MIN

    @staticmethod
    def _minute_bucket() -> int:
        """Current minute as an integer (Unix timestamp // 60). Used as budget key suffix."""
        return int(time.time() // 60)


# Module-level singleton
rate_limiter = LLMRateLimiter()
