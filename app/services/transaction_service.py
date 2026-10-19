from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import Job, Transaction, TransactionStatus
from app.schemas import TransactionWebhook


def create_transaction_with_job(session: Session, payload: TransactionWebhook) -> bool:
    """Store the transaction and its job atomically. Returns False if it already existed.

    INSERT ... ON CONFLICT DO NOTHING relies on the primary key, so concurrent duplicate
    webhooks are serialized by Postgres: exactly one insert wins, the others become no-ops.
    """
    inserted_id = session.execute(
        insert(Transaction)
        .values(**payload.model_dump(), status=TransactionStatus.PROCESSING)
        .on_conflict_do_nothing(index_elements=[Transaction.transaction_id])
        .returning(Transaction.transaction_id)
    ).scalar_one_or_none()

    if inserted_id is not None:
        session.execute(
            insert(Job)
            .values(transaction_id=inserted_id)
            .on_conflict_do_nothing(index_elements=[Job.transaction_id])
        )
    session.commit()
    return inserted_id is not None


def get_transaction(session: Session, transaction_id: str) -> Transaction | None:
    return session.get(Transaction, transaction_id)
