import os


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/voicebot"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", "redis://localhost:6379/2"
    )

    # ── LLM ───────────────────────────────────────────────────────────────────
    # These limits come straight from the provider's dashboard — they are HARD
    # limits that result in 429 errors when exceeded, not soft suggestions.
    # LLMRateLimiter now actively enforces these before firing any request.
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "sk-mock-key-for-assessment")
    LLM_TOKENS_PER_MINUTE: int = int(os.getenv("LLM_TOKENS_PER_MINUTE", "90000"))
    LLM_REQUESTS_PER_MINUTE: int = int(os.getenv("LLM_REQUESTS_PER_MINUTE", "500"))

    # Average tokens consumed per post-call analysis (measured from prod logs).
    LLM_AVG_TOKENS_PER_CALL: int = int(os.getenv("LLM_AVG_TOKENS_PER_CALL", "1500"))

    # ── Rate limit safety margins ─────────────────────────────────────────────
    # Enforce at 85% of provider cap — headroom for burst and measurement lag.
    LLM_TPM_SAFETY_MARGIN: float = float(os.getenv("LLM_TPM_SAFETY_MARGIN", "0.85"))
    LLM_RPM_SAFETY_MARGIN: float = float(os.getenv("LLM_RPM_SAFETY_MARGIN", "0.85"))

    # Max seconds to wait for budget before dead-lettering the task.
    LLM_ACQUIRE_TIMEOUT_HOT_SECONDS: int = int(
        os.getenv("LLM_ACQUIRE_TIMEOUT_HOT_SECONDS", "120")
    )
    LLM_ACQUIRE_TIMEOUT_COLD_SECONDS: int = int(
        os.getenv("LLM_ACQUIRE_TIMEOUT_COLD_SECONDS", "600")
    )

    # ── Per-customer token budgeting ──────────────────────────────────────────
    # Fraction of total TPM reserved for the shared pool.
    LLM_SHARED_POOL_FRACTION: float = float(
        os.getenv("LLM_SHARED_POOL_FRACTION", "0.20")
    )
    # 0 = draw from shared pool only (no dedicated allocation).
    DEFAULT_CUSTOMER_TOKEN_BUDGET_PER_MIN: int = int(
        os.getenv("DEFAULT_CUSTOMER_TOKEN_BUDGET_PER_MIN", "0")
    )

    # ── Recording ─────────────────────────────────────────────────────────────
    # asyncio.sleep(45) replaced by a polling loop with exponential backoff.
    # total window ≈ POLL_INTERVAL × sum(BACKOFF_FACTOR^i) for i in 0..MAX_ATTEMPTS-1
    RECORDING_WAIT_SECONDS: int = 45  # legacy; no longer used in production path
    RECORDING_POLL_INTERVAL_SECONDS: float = float(
        os.getenv("RECORDING_POLL_INTERVAL_SECONDS", "5")
    )
    RECORDING_POLL_MAX_ATTEMPTS: int = int(
        os.getenv("RECORDING_POLL_MAX_ATTEMPTS", "12")
    )
    RECORDING_POLL_BACKOFF_FACTOR: float = float(
        os.getenv("RECORDING_POLL_BACKOFF_FACTOR", "1.5")
    )
    RECORDING_POLL_MAX_SLEEP_SECONDS: float = float(
        os.getenv("RECORDING_POLL_MAX_SLEEP_SECONDS", "30")
    )
    S3_BUCKET: str = os.getenv("S3_BUCKET", "voicebot-recordings")

    # ── Circuit breaker (gradual backpressure) ────────────────────────────────
    # Binary freeze replaced with proportional dispatch-rate multipliers.
    CIRCUIT_BREAKER_CAPACITY_THRESHOLD: float = 0.90  # kept for compat
    CIRCUIT_BREAKER_FREEZE_SECONDS: int = 1800         # kept for compat
    # TPM fraction thresholds for dispatch rate tiers
    CB_TIER_FULL: float = float(os.getenv("CB_TIER_FULL", "0.70"))   # <70%  → 1.0x
    CB_TIER_75: float = float(os.getenv("CB_TIER_75", "0.80"))       # 70-80%→ 0.75x
    CB_TIER_50: float = float(os.getenv("CB_TIER_50", "0.90"))       # 80-90%→ 0.50x
    CB_TIER_25: float = float(os.getenv("CB_TIER_25", "0.95"))       # 90-95%→ 0.25x
    CB_PAUSE_SECONDS: int = int(os.getenv("CB_PAUSE_SECONDS", "10")) # >95%  → 10s pause

    # ── Post-call queues ──────────────────────────────────────────────────────
    # Two priority lanes replace the single "postcall_processing" queue.
    # hot : rebook_confirmed, demo_booked, escalation_needed → immediate
    # cold: not_interested, callback_requested, etc. → defer under load
    POSTCALL_CELERY_QUEUE: str = "postcall_processing"  # legacy; kept for compat
    POSTCALL_HOT_QUEUE: str = os.getenv("POSTCALL_HOT_QUEUE", "postcall_hot")
    POSTCALL_COLD_QUEUE: str = os.getenv("POSTCALL_COLD_QUEUE", "postcall_cold")
    POSTCALL_MAX_RETRIES: int = 3
    POSTCALL_RETRY_DELAY: int = 60
    POSTCALL_RETRY_MAX_BACKOFF: int = int(os.getenv("POSTCALL_RETRY_MAX_BACKOFF", "300"))

    # ── Dead-letter ───────────────────────────────────────────────────────────
    # Exhausted retries → postcall_dead_letter (Postgres), durable across Redis restarts.
    DEAD_LETTER_RETENTION_DAYS: int = int(os.getenv("DEAD_LETTER_RETENTION_DAYS", "30"))


settings = Settings()
