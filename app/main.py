import logging

from fastapi import FastAPI

from app.models import utcnow
from app.routes import transactions, webhooks
from app.schemas import HealthResponse, to_utc_iso

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Transaction Webhook Service", version="1.0.0")
app.include_router(webhooks.router)
app.include_router(transactions.router)


@app.get("/", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="HEALTHY", current_time=to_utc_iso(utcnow()))
