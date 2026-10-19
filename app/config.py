import os
from dataclasses import dataclass, field


def normalize_database_url(url: str) -> str:
    """Accept provider-style URLs (postgres://, postgresql://) and use the psycopg 3 driver."""
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


def _float(name: str, default: str) -> float:
    return float(os.getenv(name, default))


def _int(name: str, default: str) -> int:
    return int(os.getenv(name, default))


@dataclass(frozen=True)
class Settings:
    database_url: str = field(
        default_factory=lambda: normalize_database_url(
            os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5433/webhooks")
        )
    )
    # Simulated external API latency per transaction.
    processing_delay_seconds: float = field(default_factory=lambda: _float("PROCESSING_DELAY_SECONDS", "30"))
    # Number of jobs one worker process handles in parallel (one thread each).
    worker_concurrency: int = field(default_factory=lambda: _int("WORKER_CONCURRENCY", "10"))
    # How long an idle worker thread waits before polling for jobs again.
    worker_poll_interval_seconds: float = field(default_factory=lambda: _float("WORKER_POLL_INTERVAL_SECONDS", "1"))
    # A RUNNING job not finished within this time is treated as abandoned and reclaimed.
    # Must be comfortably longer than the processing delay.
    job_lease_seconds: float = field(default_factory=lambda: _float("JOB_LEASE_SECONDS", "120"))
    job_max_attempts: int = field(default_factory=lambda: _int("JOB_MAX_ATTEMPTS", "5"))
    # Retry delay after a failed attempt: base * 2^(attempt - 1), capped at 5 minutes.
    retry_backoff_seconds: float = field(default_factory=lambda: _float("RETRY_BACKOFF_SECONDS", "5"))


settings = Settings()
