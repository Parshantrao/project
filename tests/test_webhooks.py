import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.config import settings
from app.database import SessionLocal
from app.models import JobStatus, Transaction, TransactionStatus
from app.schemas import TransactionWebhook
from app.services.transaction_service import create_transaction_with_job
from tests.conftest import job_for, load, row_counts, webhook_body

URL = "/v1/webhooks/transactions"


def test_valid_webhook_returns_202_and_enqueues_job(client):
    response = client.post(URL, json=webhook_body())

    assert response.status_code == 202
    assert response.json() == {"status": "ACCEPTED", "transaction_id": "txn_abc123def456", "duplicate": False}
    job = job_for("txn_abc123def456")
    assert job.status == JobStatus.PENDING
    assert job.attempts == 0


def test_webhook_does_not_wait_for_processing(client):
    assert settings.processing_delay_seconds == 30  # production delay is in effect
    client.get("/")  # warm up the connection pool

    started = time.perf_counter()
    response = client.post(URL, json=webhook_body("txn_fast"))
    elapsed = time.perf_counter() - started

    assert response.status_code == 202
    assert elapsed < 0.5
    assert load(Transaction, "txn_fast").status == TransactionStatus.PROCESSING


def test_duplicate_webhooks_create_one_transaction_and_one_job(client):
    responses = [client.post(URL, json=webhook_body("txn_dup")) for _ in range(5)]

    assert [r.status_code for r in responses] == [202] * 5
    assert [r.json()["duplicate"] for r in responses] == [False, True, True, True, True]
    assert row_counts() == (1, 1)


def test_concurrent_duplicate_requests_create_one_job(client):
    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(lambda _: client.post(URL, json=webhook_body("txn_race")), range(20)))

    assert all(r.status_code == 202 for r in responses)
    assert sum(not r.json()["duplicate"] for r in responses) == 1
    assert row_counts() == (1, 1)


def test_concurrent_inserts_at_database_level_are_safe():
    """Separate sessions released at the same instant, racing on the unique constraint."""
    payload = TransactionWebhook(**webhook_body("txn_db_race"))
    barrier = threading.Barrier(10)

    def insert() -> bool:
        with SessionLocal() as session:
            barrier.wait()
            return create_transaction_with_job(session, payload)

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda _: insert(), range(10)))

    assert results.count(True) == 1
    assert row_counts() == (1, 1)


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": -5},
        {"amount": 0},
        {"amount": 10.555},
        {"currency": "RUPEES"},
        {"transaction_id": ""},
    ],
)
def test_invalid_payload_is_rejected(client, overrides):
    assert client.post(URL, json=webhook_body(**overrides)).status_code == 422
    assert row_counts() == (0, 0)


def test_missing_field_is_rejected(client):
    body = webhook_body()
    del body["destination_account"]
    assert client.post(URL, json=body).status_code == 422
