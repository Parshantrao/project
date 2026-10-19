from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import TransactionResponse
from app.services.transaction_service import get_transaction

router = APIRouter()


@router.get(
    "/v1/transactions/{transaction_id}",
    response_model=TransactionResponse,
)
def read_transaction(
    transaction_id: str,
    db: Session = Depends(get_db),
) -> TransactionResponse:

    txn = get_transaction(db, transaction_id)

    if txn is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Transaction not found",
        )

    return TransactionResponse.model_validate(txn)
