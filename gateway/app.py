import asyncio
import json
import logging
import os
import time
from typing import List, Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, Field, validator


logger = logging.getLogger("gateway")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

API_KEY = os.getenv("API_KEY", "changeme")
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "600"))  # per client
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "transactions-dlq")
KAFKA_CONNECT_TIMEOUT = float(os.getenv("KAFKA_CONNECT_TIMEOUT_SEC", "3.0"))
KAFKA_SEND_TIMEOUT = float(os.getenv("KAFKA_SEND_TIMEOUT_SEC", "3.0"))

# In-memory token bucket per client (IP or key). Good enough for demo purposes.
rate_counters = {}
producer: Optional[AIOKafkaProducer] = None


class Transaction(BaseModel):
    TransactionID: str
    TransactionAmt: float
    isFraud: int = Field(ge=0, le=1)
    card1: int
    addr1: int
    addr2: int
    DeviceType: str
    TransactionDT: int
    currency: Optional[str] = None
    merchant_category: Optional[str] = None
    account_age_days: Optional[int] = None

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


async def get_producer() -> AIOKafkaProducer:
    """
    Lazily create the Kafka producer so the service can start even if Kafka is briefly unavailable.
    """
    global producer
    if producer is None:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
            retry_backoff_ms=250,
        )
        try:
            await asyncio.wait_for(producer.start(), timeout=KAFKA_CONNECT_TIMEOUT)
            logger.info("Kafka producer started for brokers=%s topic=%s", KAFKA_BROKERS, KAFKA_TOPIC)
        except Exception:
            producer = None
            raise
    return producer


async def publish_to_queue(tx: Transaction):
    """
    Publish a transaction to the Kafka topic; on failure, attempt DLQ then surface a 503.
    """
    try:
        producer = await get_producer()
    except Exception as exc:
        logger.exception("Kafka producer unavailable during init")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Message queue unavailable") from exc

    payload = tx.dict()
    try:
        await asyncio.wait_for(producer.send_and_wait(KAFKA_TOPIC, payload), timeout=KAFKA_SEND_TIMEOUT)
        logger.info("Enqueued tx=%s amount=%.2f", tx.TransactionID, tx.TransactionAmt)
    except Exception as exc:
        logger.exception("Failed to publish tx=%s to topic=%s", tx.TransactionID, KAFKA_TOPIC)
        if KAFKA_DLQ_TOPIC:
            try:
                dlq_payload = {"error": str(exc), "tx": payload}
                await asyncio.wait_for(
                    producer.send_and_wait(KAFKA_DLQ_TOPIC, dlq_payload), timeout=KAFKA_SEND_TIMEOUT
                )
                logger.warning("Routed tx=%s to DLQ topic=%s", tx.TransactionID, KAFKA_DLQ_TOPIC)
            except Exception:
                logger.exception("Failed to route tx=%s to DLQ topic=%s", tx.TransactionID, KAFKA_DLQ_TOPIC)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Message queue publish failed") from exc


async def shutdown_producer():
    global producer
    if producer:
        await producer.stop()
        producer = None


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
    return {
        "status": "ok",
        "queue": "ready" if producer else "not_initialized",
    }


@app.on_event("startup")
async def startup_event():
    try:
        await get_producer()
    except Exception:
        # Keep service up; requests will surface 503 until Kafka is reachable.
        logger.exception("Kafka producer failed to start on startup")


@app.post("/transactions")
async def ingest_transaction(
    tx: Transaction,
    request: Request,
    x_api_key: Optional[str] = Header(None),
):
    check_api_key(x_api_key)
    client_id = x_api_key or (request.client.host if request.client else "unknown")
    rate_limit(client_id)
    await publish_to_queue(tx)
    return {"status": "accepted", "id": tx.TransactionID}


@app.on_event("shutdown")
async def shutdown_event():
    await shutdown_producer()
