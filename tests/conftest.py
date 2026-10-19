import os

# Point the app at the test database before any app module creates its engine.
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5433/webhooks_test"
)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

import dataclasses  # noqa: E402

import pytest  # noqa: E402
from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, func, select, text  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402

from app.config import settings  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Job, Transaction  # noqa: E402
from app.schemas import TransactionWebhook  # noqa: E402
from app.services.transaction_service import create_transaction_with_job  # noqa: E402
from app.worker import Worker  # noqa: E402


def _ensure_database_exists(url: str) -> None:
    url_obj = make_url(url)
    admin = create_engine(url_obj.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            exists = conn.scalar(text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": url_obj.database})
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{url_obj.database}"'))
    finally:
        admin.dispose()


@pytest.fixture(scope="session", autouse=True)
def migrated_database():
    try:
        _ensure_database_exists(TEST_DATABASE_URL)
    except Exception as exc:
        pytest.exit(
            f"Cannot reach test Postgres at {make_url(TEST_DATABASE_URL).render_as_string(hide_password=True)}: {exc}\n"
            "Start it with `docker compose up -d db` or set TEST_DATABASE_URL.",
            returncode=2,
        )
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", TEST_DATABASE_URL.replace("%", "%%"))
    command.upgrade(cfg, "head")


@pytest.fixture(autouse=True)
def clean_tables():
    with SessionLocal() as session:
        session.execute(text("TRUNCATE jobs, transactions RESTART IDENTITY CASCADE"))
        session.commit()


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def make_worker():
    def _make(process_fn=None, worker_id="test-worker", **overrides):
        defaults = {"processing_delay_seconds": 0.2, "retry_backoff_seconds": 0}
        test_settings = dataclasses.replace(settings, **{**defaults, **overrides})
        return Worker(test_settings, SessionLocal, process_fn=process_fn, worker_id=worker_id)

    return _make


def webhook_body(txn_id="txn_abc123def456", **overrides) -> dict:
    body = {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
    body.update(overrides)
    return body


def create_transaction(txn_id: str) -> None:
    with SessionLocal() as session:
        create_transaction_with_job(session, TransactionWebhook(**webhook_body(txn_id)))


def load(model, key):
    with SessionLocal() as session:
        return session.get(model, key)


def job_for(txn_id: str) -> Job:
    with SessionLocal() as session:
        return session.scalars(select(Job).where(Job.transaction_id == txn_id)).one()


def row_counts() -> tuple[int, int]:
    with SessionLocal() as session:
        return (
            session.scalar(select(func.count()).select_from(Transaction)),
            session.scalar(select(func.count()).select_from(Job)),
        )
