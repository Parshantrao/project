import threading
import time
from datetime import datetime

from app.models import JobStatus
from tests.conftest import job_for, webhook_body


def test_unknown_transaction_returns_404(client):
    assert client.get("/v1/transactions/does_not_exist").status_code == 404


def test_new_transaction_reports_processing_with_null_processed_at(client):
    client.post("/v1/webhooks/transactions", json=webhook_body(amount=150.50, currency="usd"))

    response = client.get("/v1/transactions/txn_abc123def456")

    assert response.status_code == 200
    [txn] = response.json()
    assert txn == {
        "transaction_id": "txn_abc123def456",
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 150.5,
        "currency": "USD",
        "status": "PROCESSING",
        "created_at": txn["created_at"],
        "processed_at": None,
    }
    assert txn["created_at"].endswith("Z")


def test_transaction_is_processed_after_the_delay(client, make_worker):
    delay = 1.5
    client.post("/v1/webhooks/transactions", json=webhook_body())
    worker = make_worker(processing_delay_seconds=delay)

    thread = threading.Thread(target=worker.run_once)
    thread.start()
    time.sleep(delay / 3)
    [during] = client.get("/v1/transactions/txn_abc123def456").json()
    assert during["status"] == "PROCESSING"
    assert during["processed_at"] is None
    thread.join(timeout=10)

    [after] = client.get("/v1/transactions/txn_abc123def456").json()
    assert after["status"] == "PROCESSED"
    assert after["processed_at"] is not None
    elapsed = datetime.fromisoformat(after["processed_at"]) - datetime.fromisoformat(after["created_at"])
    assert elapsed.total_seconds() >= delay
    assert after["amount"] == 1500
    assert job_for("txn_abc123def456").status == JobStatus.COMPLETED
