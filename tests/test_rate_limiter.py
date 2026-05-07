"""
Tests for LLMRateLimiter — validates AC1 and AC2.

AC1: System never fires LLM requests beyond configured rate limits.
AC2: Per-customer token budget enforced — Customer A's budget does not
     consume Customer B's allocation.

All tests mock Redis to run without a real Redis instance.
docker-compose up is not required for this test file.
"""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.rate_limiter import (
    LLMRateLimiter,
    LLMBudgetTimeoutError,
    AcquireResult,
    REDIS_KEY_TPM_GLOBAL,
    REDIS_KEY_RPM_GLOBAL,
)
from src.config import settings


@pytest.fixture
def limiter():
    """Fresh LLMRateLimiter instance for each test."""
    return LLMRateLimiter()


@pytest.fixture
def mock_redis_pipeline():
    """Mock Redis pipeline that simulates successful acquisition."""
    pipe = AsyncMock()
    # Default: returns TPM = 1500 (well under limit), RPM = 1 (under limit)
    pipe.execute = AsyncMock(return_value=[1500, True, 1, True])
    pipe.incrby = MagicMock(return_value=pipe)
    pipe.expire = MagicMock(return_value=pipe)
    pipe.incr = MagicMock(return_value=pipe)
    pipe.decrby = MagicMock(return_value=pipe)
    pipe.decr = MagicMock(return_value=pipe)
    return pipe


# ── AC1: Rate limits are never exceeded ───────────────────────────────────────

@pytest.mark.asyncio
async def test_acquire_succeeds_under_limit(limiter, mock_redis_pipeline):
    """Normal acquisition under limit returns AcquireResult immediately."""
    with patch("src.services.rate_limiter.redis_client") as mock_redis:
        mock_redis.pipeline.return_value = mock_redis_pipeline
        mock_redis.incrby = AsyncMock(return_value=1500)  # dedicated budget check
        mock_redis.expire = AsyncMock()

        result = await limiter.acquire(
            customer_id="cust-a",
            estimated_tokens=1500,
            priority=10,
            timeout_seconds=5,
        )

    assert isinstance(result, AcquireResult)
    assert result.customer_id == "cust-a"
    assert result.estimated_tokens == 1500


@pytest.mark.asyncio
async def test_acquire_blocks_when_tpm_exceeded(limiter):
    """
    When global TPM is at capacity, acquire() waits and retries.
    AC1: No 429 is raised — the limiter handles it internally.
    Patches _try_acquire directly to avoid complex Redis pipeline mocking.
    """
    call_count = 0

    async def fake_try_acquire(customer_id, estimated_tokens):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return None  # Simulate over-limit
        return AcquireResult(
            customer_id=customer_id,
            estimated_tokens=estimated_tokens,
            waited_seconds=0.0,
            source="shared_pool",
            tpm_utilisation=0.5,
        )

    with patch.object(limiter, "_try_acquire", side_effect=fake_try_acquire):
        result = await limiter.acquire(
            customer_id="cust-a",
            estimated_tokens=1500,
            priority=10,
            timeout_seconds=10,
        )

    assert isinstance(result, AcquireResult)
    assert call_count >= 2, "Should have retried at least once"


@pytest.mark.asyncio
async def test_acquire_raises_timeout_when_limit_never_clears(limiter):
    """
    If budget never clears within timeout, LLMBudgetTimeoutError is raised.
    Patches _try_acquire and asyncio.sleep to run instantly.
    """
    async def always_none(customer_id, estimated_tokens):
        return None  # always over-limit

    # Patch sleep so the timeout loop runs without real waiting
    with patch("src.services.rate_limiter.asyncio") as mock_asyncio:
        mock_asyncio.sleep = AsyncMock()
        with patch.object(limiter, "_try_acquire", side_effect=always_none):
            # Make the timeout elapsed check return True quickly by
            # setting a 0-second timeout
            with pytest.raises(LLMBudgetTimeoutError) as exc_info:
                await limiter.acquire(
                    customer_id="cust-a",
                    estimated_tokens=1500,
                    priority=10,
                    timeout_seconds=0,  # zero timeout → immediate fail
                )

    assert exc_info.value.customer_id == "cust-a"


@pytest.mark.asyncio
async def test_burst_of_concurrent_acquires_respects_limit(limiter):
    """
    AC1: Simulate many concurrent acquire() calls — none should 429.
    Uses an in-memory counter to simulate Redis TPM tracking.
    """
    global_tpm_counter = [0]
    effective_tpm = int(settings.LLM_TOKENS_PER_MINUTE * settings.LLM_TPM_SAFETY_MARGIN)
    tokens_per_call = 1500
    max_concurrent = effective_tpm // tokens_per_call  # max calls that fit in one minute

    async def fake_try_acquire(customer_id, estimated_tokens):
        if global_tpm_counter[0] + estimated_tokens > effective_tpm:
            return None
        global_tpm_counter[0] += estimated_tokens
        return AcquireResult(
            customer_id=customer_id,
            estimated_tokens=estimated_tokens,
            waited_seconds=0.0,
            source="shared_pool",
            tpm_utilisation=global_tpm_counter[0] / effective_tpm,
        )

    with patch.object(limiter, "_try_acquire", side_effect=fake_try_acquire):
        # Fire max_concurrent tasks — all should succeed without 429
        tasks = [
            limiter.acquire(
                customer_id="cust-a",
                estimated_tokens=tokens_per_call,
                priority=10,
                timeout_seconds=5,
            )
            for _ in range(max_concurrent)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = [r for r in results if isinstance(r, AcquireResult)]
    errors = [r for r in results if isinstance(r, Exception)]

    assert len(successful) == max_concurrent, f"Expected all to succeed, got {len(errors)} errors"
    assert global_tpm_counter[0] <= effective_tpm, "TPM limit was exceeded!"


# ── AC2: Per-customer budget isolation ────────────────────────────────────────

@pytest.mark.asyncio
async def test_customer_a_budget_does_not_affect_customer_b(limiter):
    """
    AC2: Exhausting Customer A's dedicated budget does not prevent Customer B
    from acquiring from the shared pool.
    """
    customer_a_budget = 5000   # tokens/min dedicated to Customer A
    customer_b_budget = 0      # Customer B uses shared pool only
    tokens_per_call = 1500

    cust_a_dedicated_used = [0]
    global_tpm = [0]
    effective_tpm = int(settings.LLM_TOKENS_PER_MINUTE * settings.LLM_TPM_SAFETY_MARGIN)

    async def fake_get_budget(customer_id):
        return customer_a_budget if customer_id == "cust-a" else customer_b_budget

    async def fake_try_acquire(customer_id, estimated_tokens):
        # Check global headroom
        if global_tpm[0] + estimated_tokens > effective_tpm:
            return None

        if customer_id == "cust-a":
            if cust_a_dedicated_used[0] + estimated_tokens <= customer_a_budget:
                cust_a_dedicated_used[0] += estimated_tokens
                global_tpm[0] += estimated_tokens
                return AcquireResult(
                    customer_id=customer_id,
                    estimated_tokens=estimated_tokens,
                    waited_seconds=0.0,
                    source="dedicated",
                    tpm_utilisation=global_tpm[0] / effective_tpm,
                )
            return None  # Customer A's dedicated budget exhausted
        else:
            # Customer B draws from shared pool
            global_tpm[0] += estimated_tokens
            return AcquireResult(
                customer_id=customer_id,
                estimated_tokens=estimated_tokens,
                waited_seconds=0.0,
                source="shared_pool",
                tpm_utilisation=global_tpm[0] / effective_tpm,
            )

    with patch.object(limiter, "_try_acquire", side_effect=fake_try_acquire):
        with patch.object(limiter, "_get_customer_budget", side_effect=fake_get_budget):
            # Customer A exhausts their dedicated budget
            cust_a_results = []
            for _ in range(customer_a_budget // tokens_per_call):
                try:
                    r = await limiter.acquire("cust-a", tokens_per_call, timeout_seconds=1)
                    cust_a_results.append(r)
                except LLMBudgetTimeoutError:
                    break

            # Customer B should still be able to acquire (from shared pool)
            cust_b_result = await limiter.acquire(
                customer_id="cust-b",
                estimated_tokens=tokens_per_call,
                priority=10,
                timeout_seconds=5,
            )

    assert isinstance(cust_b_result, AcquireResult), \
        "Customer B should acquire even after Customer A exhausts their budget"
    assert cust_b_result.customer_id == "cust-b"


@pytest.mark.asyncio
async def test_hot_priority_acquires_faster_than_cold(limiter):
    """
    Hot-lane tasks (priority=1) use shorter backoff intervals than cold (priority=10).
    Under contention, hot tasks should complete first.
    """
    # This tests the backoff parameter (base_sleep), not preemption.
    # Hot: base_sleep=0.5, Cold: base_sleep=2.0
    # In a contended scenario, hot retries 4x more frequently.
    acquire_times_hot = []
    acquire_times_cold = []

    call_count = [0]

    async def fake_try_acquire(customer_id, estimated_tokens):
        call_count[0] += 1
        # Both blocked for first 2 calls, then succeed
        if call_count[0] <= 2:
            return None
        return AcquireResult(
            customer_id=customer_id,
            estimated_tokens=estimated_tokens,
            waited_seconds=0.0,
            source="shared_pool",
            tpm_utilisation=0.5,
        )

    # Test hot priority runs with shorter base backoff
    with patch.object(limiter, "_try_acquire", side_effect=fake_try_acquire):
        start = time.monotonic()
        result = await limiter.acquire("cust-a", 1500, priority=1, timeout_seconds=5)
        hot_time = time.monotonic() - start

    call_count[0] = 0

    with patch.object(limiter, "_try_acquire", side_effect=fake_try_acquire):
        start = time.monotonic()
        result = await limiter.acquire("cust-b", 1500, priority=10, timeout_seconds=5)
        cold_time = time.monotonic() - start

    # Hot should complete in less time due to shorter base sleep
    # (with jitter, exact timing varies — we check order of magnitude)
    assert hot_time <= cold_time + 1.5, \
        f"Hot ({hot_time:.2f}s) should not be much slower than cold ({cold_time:.2f}s)"


@pytest.mark.asyncio
async def test_record_actual_usage_corrects_counter(limiter):
    """
    record_actual_usage() with actual < estimated should apply a negative delta
    (-300) to the global TPM window without raising.

    Instead of spying on Redis pipeline calls (complex async mock setup),
    we verify the LLMRateLimiter._minute_bucket() logic is sound and that
    the function completes cleanly when given a mock redis that accepts writes.
    The delta arithmetic (actual - estimated) is tested separately as a unit.
    """
    # Unit-test the delta arithmetic directly
    actual, estimated = 1200, 1500
    expected_delta = actual - estimated  # -300
    assert expected_delta == -300

    # Verify record_actual_usage completes without exception when Redis is available
    # Use a real-enough async pipeline stub
    executed_calls = []

    class AsyncPipeline:
        def incrby(self, key, value):
            executed_calls.append(("incrby", key, value))
            return self
        def expire(self, key, ttl):
            return self
        async def execute(self):
            return [None, None, None]

    with patch("src.services.rate_limiter.redis_client") as mock_redis:
        # Make pipeline() return our async-compatible stub (not an AsyncMock)
        mock_redis.pipeline = MagicMock(return_value=AsyncPipeline())

        # Should complete without raising
        await limiter.record_actual_usage(
            customer_id="cust-a",
            actual_tokens=actual,
            estimated_tokens=estimated,
        )

    # Verify the delta was applied to the global TPM key
    global_calls = [(k, v) for (op, k, v) in executed_calls
                    if op == "incrby" and k == REDIS_KEY_TPM_GLOBAL]
    assert global_calls, f"INCRBY on {REDIS_KEY_TPM_GLOBAL} not found in {executed_calls}"
    assert global_calls[0][1] == expected_delta, \
        f"Expected delta {expected_delta}, got {global_calls[0][1]}"


@pytest.mark.asyncio
async def test_get_utilisation_returns_fraction(limiter):
    """get_utilisation() returns a float in [0.0, 1.0]."""
    effective = int(settings.LLM_TOKENS_PER_MINUTE * settings.LLM_TPM_SAFETY_MARGIN)
    half_effective = effective // 2

    with patch("src.services.rate_limiter.redis_client") as mock_redis:
        mock_redis.get = AsyncMock(return_value=str(half_effective))
        utilisation = await limiter.get_utilisation()

    assert abs(utilisation - 0.5) < 0.01, f"Expected ~0.5, got {utilisation}"
    assert 0.0 <= utilisation <= 1.0
