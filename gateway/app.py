import os
import time
import logging
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, Field, validator


logger = logging.getLogger("gateway")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

API_KEY = os.getenv("API_KEY", "changeme")
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "600"))  # per client

# In-memory token bucket per client (IP or key). Good enough for demo purposes.
rate_counters = {}


class Transaction(BaseModel):
    TransactionID: str
    TransactionAmt: float
    isFraud: int = Field(ge=0, le=1)
    card1: int
    addr1: int
    addr2: int
    DeviceType: str
    TransactionDT: int
    currency: Optional[str]
    merchant_category: Optional[str]
    account_age_days: Optional[int]

    @validator("TransactionAmt")
    def amount_positive(cls, v):
        if v < 0:
            raise ValueError("TransactionAmt must be non-negative")
        return v


app = FastAPI(title="Fraud Transaction Ingress", version="0.1.0")


def check_api_key(header_key: Optional[str]):
    if header_key is None or header_key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


def rate_limit(client_id: str):
    now = time.time()
    window_start = now - 60
    bucket: List[float] = rate_counters.get(client_id, [])
    bucket = [t for t in bucket if t >= window_start]
    if len(bucket) >= RATE_LIMIT_PER_MIN:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
    bucket.append(now)
    rate_counters[client_id] = bucket


def publish_to_queue(tx: Transaction):
    """
    Placeholder for routing into MQ/Kafka/PubSub. Replace with a real producer.
    """
    logger.info("Enqueue tx %s amount=%.2f", tx.TransactionID, tx.TransactionAmt)
    # TODO: integrate with real message queue


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000
    logger.info(
        "%s %s %s %.1fms",
        request.client.host if request.client else "unknown",
        request.method,
        request.url.path,
        duration_ms,
    )
    return response


@app.get("/healthz")
async def health():
    return {"status": "ok"}


@app.post("/transactions")
async def ingest_transaction(
    tx: Transaction,
    request: Request,
    x_api_key: Optional[str] = Header(None),
):
    check_api_key(x_api_key)
    client_id = x_api_key or (request.client.host if request.client else "unknown")
    rate_limit(client_id)
    publish_to_queue(tx)
    return {"status": "accepted", "id": tx.TransactionID}
