"""Background worker: python -m app.worker

Polls the `jobs` table, claims one job at a time per thread with
SELECT ... FOR UPDATE SKIP LOCKED, simulates the external call, then marks the
transaction PROCESSED. No database transaction or row lock is held during the delay.
"""
import logging
import os
import signal
import socket
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings, settings as default_settings
from app.database import SessionLocal, engine
from app.models import Job, JobStatus, Transaction, TransactionStatus, utcnow

log = logging.getLogger("app.worker")


class ShutdownRequested(Exception):
    """Raised when the worker is asked to stop while a job is in progress."""


@dataclass(frozen=True)
class ClaimedJob:
    id: int
    transaction_id: str
    attempts: int


class Worker:
    def __init__(
        self,
        settings: Settings = default_settings,
        session_factory: sessionmaker = SessionLocal,
        process_fn: Callable[[str], None] | None = None,
        worker_id: str | None = None,
    ):
        self.settings = settings
        self.session_factory = session_factory
        self.process_fn = process_fn or self.simulate_external_call
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self.stop_event = threading.Event()

    # --- processing -------------------------------------------------------

    def simulate_external_call(self, transaction_id: str) -> None:
        # Interruptible sleep so shutdown doesn't have to wait out the full delay.
        if self.stop_event.wait(self.settings.processing_delay_seconds):
            raise ShutdownRequested

    def run_once(self, worker_id: str | None = None) -> bool:
        """Claim and process one job. Returns False if there was nothing to do."""
        worker_id = worker_id or self.worker_id
        with self.session_factory() as session:
            job = self.claim_job(session, worker_id)
            session.commit()  # releases the row lock before the long-running work
        if job is None:
            return False

        log.info("Job %s: processing %s (attempt %s)", job.id, job.transaction_id, job.attempts)
        try:
            self.process_fn(job.transaction_id)
        except ShutdownRequested:
            self.release_job(job, worker_id)
            log.info("Job %s: released on shutdown", job.id)
        except Exception as exc:
            log.exception("Job %s: attempt %s failed", job.id, job.attempts)
            self.record_failure(job, worker_id, exc)
        else:
            self.complete_job(job, worker_id)
        return True

    # --- state transitions (each is one short DB transaction) --------------

    def claim_job(self, session: Session, worker_id: str) -> ClaimedJob | None:
        """Lock and claim the next runnable job. The caller commits."""
        now = utcnow()
        stale_before = now - timedelta(seconds=self.settings.job_lease_seconds)
        job = session.scalars(
            select(Job)
            .where(
                or_(
                    and_(Job.status == JobStatus.PENDING, Job.run_after <= now),
                    # Abandoned by a crashed worker: lease expired.
                    and_(Job.status == JobStatus.RUNNING, Job.locked_at < stale_before),
                )
            )
            .order_by(Job.run_after, Job.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        ).first()
        if job is None:
            return None

        if job.status == JobStatus.RUNNING:
            log.warning("Job %s: recovering abandoned job from %s", job.id, job.locked_by)
        if job.attempts >= self.settings.job_max_attempts:
            self._fail(session, job, job.last_error or "Exceeded max attempts (abandoned)")
            return None

        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.locked_at = now
        job.locked_by = worker_id
        session.flush()
        return ClaimedJob(job.id, job.transaction_id, job.attempts)

    def _owned_job(self, session: Session, job: ClaimedJob, worker_id: str) -> Job | None:
        """Re-lock the job and confirm this worker still holds its lease."""
        row = session.get(Job, job.id, with_for_update=True)
        if row is None or row.status != JobStatus.RUNNING or row.locked_by != worker_id or row.attempts != job.attempts:
            log.warning("Job %s: lease lost, discarding this attempt's result", job.id)
            return None
        return row

    def complete_job(self, job: ClaimedJob, worker_id: str) -> None:
        with self.session_factory() as session:
            row = self._owned_job(session, job, worker_id)
            if row is None:
                return
            txn = session.get(Transaction, job.transaction_id, with_for_update=True)
            # Guarded so a retried job can never "process" a transaction twice.
            if txn.status == TransactionStatus.PROCESSING:
                txn.status = TransactionStatus.PROCESSED
                txn.processed_at = utcnow()
            row.status = JobStatus.COMPLETED
            row.completed_at = utcnow()
            row.locked_at = row.locked_by = None
            row.last_error = None
            session.commit()
        log.info("Job %s: transaction %s PROCESSED", job.id, job.transaction_id)

    def record_failure(self, job: ClaimedJob, worker_id: str, exc: Exception) -> None:
        error = f"{type(exc).__name__}: {exc}"[:2000]
        with self.session_factory() as session:
            row = self._owned_job(session, job, worker_id)
            if row is None:
                return
            if row.attempts >= self.settings.job_max_attempts:
                self._fail(session, row, error)
            else:
                backoff = min(self.settings.retry_backoff_seconds * 2 ** (row.attempts - 1), 300)
                row.status = JobStatus.PENDING
                row.run_after = utcnow() + timedelta(seconds=backoff)
                row.locked_at = row.locked_by = None
                row.last_error = error
                log.info("Job %s: retrying in %.0fs", job.id, backoff)
            session.commit()

    def _fail(self, session: Session, row: Job, error: str) -> None:
        row.status = JobStatus.FAILED
        row.last_error = error
        row.locked_at = row.locked_by = None
        txn = session.get(Transaction, row.transaction_id, with_for_update=True)
        if txn is not None and txn.status == TransactionStatus.PROCESSING:
            txn.status = TransactionStatus.FAILED
        session.commit()
        log.error("Job %s: permanently FAILED after %s attempts: %s", row.id, row.attempts, error)

    def release_job(self, job: ClaimedJob, worker_id: str) -> None:
        """Hand an interrupted job back immediately; the attempt doesn't count."""
        try:
            with self.session_factory() as session:
                row = self._owned_job(session, job, worker_id)
                if row is None:
                    return
                row.status = JobStatus.PENDING
                row.attempts -= 1
                row.locked_at = row.locked_by = None
                session.commit()
        except Exception:
            log.exception("Job %s: could not release; it will be recovered after the lease expires", job.id)

    # --- main loop --------------------------------------------------------

    def _loop(self, thread_no: int) -> None:
        worker_id = f"{self.worker_id}:{thread_no}"
        while not self.stop_event.is_set():
            try:
                if not self.run_once(worker_id):
                    self.stop_event.wait(self.settings.worker_poll_interval_seconds)
            except Exception:
                # e.g. database temporarily unreachable: log, back off, keep going.
                log.exception("Worker thread %s error; retrying shortly", worker_id)
                self.stop_event.wait(5)

    def run(self) -> None:
        log.info("Worker %s starting with %s threads", self.worker_id, self.settings.worker_concurrency)
        threads = [
            threading.Thread(target=self._loop, args=(i,), name=f"worker-{i}")
            for i in range(self.settings.worker_concurrency)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        log.info("Worker %s stopped", self.worker_id)

    def stop(self, *_args) -> None:
        log.info("Shutdown requested")
        self.stop_event.set()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s")
    worker = Worker()
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    try:
        worker.run()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
