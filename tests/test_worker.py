import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from sqlalchemy import update

from app.database import SessionLocal
from app.models import Job, JobStatus, Transaction, TransactionStatus, utcnow
from tests.conftest import create_transaction, job_for, load


def test_skip_locked_prevents_two_workers_claiming_the_same_job(make_worker):
    create_transaction("txn_lock")
    worker = make_worker()

    with SessionLocal() as first, SessionLocal() as second:
        claimed = worker.claim_job(first, "worker-a")  # row lock held, not committed yet
        assert claimed is not None

        started = time.perf_counter()
        assert worker.claim_job(second, "worker-b") is None  # skipped, not blocked
        assert time.perf_counter() - started < 1
        first.commit()


def test_parallel_workers_process_each_job_exactly_once(make_worker):
    for i in range(6):
        create_transaction(f"txn_par_{i}")
    seen: list[str] = []
    lock = threading.Lock()

    def record(txn_id):
        with lock:
            seen.append(txn_id)

    worker = make_worker(process_fn=record)
    with ThreadPoolExecutor(max_workers=4) as pool:
        def drain(n):
            while worker.run_once(f"w{n}"):
                pass
        list(pool.map(drain, range(4)))

    assert sorted(seen) == [f"txn_par_{i}" for i in range(6)]
    for i in range(6):
        assert load(Transaction, f"txn_par_{i}").status == TransactionStatus.PROCESSED
        assert job_for(f"txn_par_{i}").attempts == 1


def test_failed_attempt_is_retried_then_succeeds(make_worker):
    create_transaction("txn_retry")
    calls = []

    def flaky(txn_id):
        calls.append(txn_id)
        if len(calls) == 1:
            raise ConnectionError("external API timed out")

    worker = make_worker(process_fn=flaky)

    assert worker.run_once()
    job = job_for("txn_retry")
    assert job.status == JobStatus.PENDING
    assert job.attempts == 1
    assert "external API timed out" in job.last_error
    txn = load(Transaction, "txn_retry")
    assert txn.status == TransactionStatus.PROCESSING  # a failure is never marked as success
    assert txn.processed_at is None

    assert worker.run_once()
    job = job_for("txn_retry")
    assert job.status == JobStatus.COMPLETED
    assert job.attempts == 2
    assert load(Transaction, "txn_retry").status == TransactionStatus.PROCESSED


def test_retry_waits_for_backoff(make_worker):
    create_transaction("txn_backoff")

    def fail(_):
        raise RuntimeError("boom")

    worker = make_worker(process_fn=fail, retry_backoff_seconds=60)
    assert worker.run_once()
    assert not worker.run_once()  # not runnable again until run_after
    assert job_for("txn_backoff").run_after > utcnow() + timedelta(seconds=50)


def test_retries_are_bounded(make_worker):
    create_transaction("txn_doomed")

    def fail(_):
        raise RuntimeError("provider rejected transaction")

    worker = make_worker(process_fn=fail, job_max_attempts=3)
    for _ in range(3):
        assert worker.run_once()
    assert not worker.run_once()

    job = job_for("txn_doomed")
    assert job.status == JobStatus.FAILED
    assert job.attempts == 3
    txn = load(Transaction, "txn_doomed")
    assert txn.status == TransactionStatus.FAILED
    assert txn.processed_at is None


def test_abandoned_job_is_recovered_after_lease_expires(make_worker):
    create_transaction("txn_crash")
    worker = make_worker(job_lease_seconds=120)

    # Worker A claims the job and then "crashes" without finishing it.
    with SessionLocal() as session:
        worker.claim_job(session, "crashed-worker")
        session.commit()

    # While the lease is still valid, nobody else may take it.
    assert not worker.run_once("worker-b")

    with SessionLocal() as session:
        session.execute(
            update(Job).where(Job.transaction_id == "txn_crash").values(locked_at=utcnow() - timedelta(seconds=121))
        )
        session.commit()

    assert worker.run_once("worker-b")
    job = job_for("txn_crash")
    assert job.status == JobStatus.COMPLETED
    assert job.attempts == 2
    assert load(Transaction, "txn_crash").status == TransactionStatus.PROCESSED


def test_worker_that_lost_its_lease_cannot_overwrite_the_result(make_worker):
    create_transaction("txn_slow")
    worker = make_worker()

    with SessionLocal() as session:
        slow_claim = worker.claim_job(session, "slow-worker")
        session.execute(update(Job).values(locked_at=utcnow() - timedelta(hours=1)))
        session.commit()

    assert worker.run_once("fast-worker")  # recovers and completes the job
    processed_at = load(Transaction, "txn_slow").processed_at

    # The slow worker finally finishes (or fails): both must be no-ops.
    worker.complete_job(slow_claim, "slow-worker")
    worker.record_failure(slow_claim, "slow-worker", RuntimeError("late"))

    assert job_for("txn_slow").status == JobStatus.COMPLETED
    assert load(Transaction, "txn_slow").processed_at == processed_at


def test_graceful_shutdown_releases_in_progress_job(make_worker):
    create_transaction("txn_shutdown")
    worker = make_worker(processing_delay_seconds=30)

    thread = threading.Thread(target=worker.run_once)
    thread.start()
    time.sleep(0.5)
    worker.stop()
    thread.join(timeout=5)

    assert not thread.is_alive()
    job = job_for("txn_shutdown")
    assert job.status == JobStatus.PENDING  # immediately available to the next worker
    assert job.attempts == 0
    assert job.locked_by is None
    assert load(Transaction, "txn_shutdown").status == TransactionStatus.PROCESSING
