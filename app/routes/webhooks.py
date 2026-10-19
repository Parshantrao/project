import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import TransactionWebhook, WebhookAck
from app.services.transaction_service import create_transaction_with_job

log = logging.getLogger(__name__)
router = APIRouter()


@router.post("/v1/webhooks/transactions", status_code=status.HTTP_202_ACCEPTED, response_model=WebhookAck)
def receive_transaction_webhook(payload: TransactionWebhook, db: Session = Depends(get_db)) -> WebhookAck:
    try:
        created = create_transaction_with_job(db, payload)
    except OperationalError:
        log.exception("Database unavailable while storing webhook %s", payload.transaction_id)
        # Non-2xx makes the payment provider redeliver later, so the webhook isn't lost.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Temporarily unavailable, please retry")
    if not created:
        log.info("Duplicate webhook ignored for %s", payload.transaction_id)
    return WebhookAck(transaction_id=payload.transaction_id, duplicate=not created)
