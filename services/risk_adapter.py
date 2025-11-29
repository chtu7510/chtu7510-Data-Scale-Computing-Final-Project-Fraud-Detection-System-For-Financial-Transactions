import asyncio
import json
import logging
import os
import random
import time
from typing import Any, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("risk-adapter")

# Environment configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "enriched-transactions")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "scored-transactions")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions-dlq")
GROUP_ID = os.getenv("GROUP_ID", "risk-adapter-group")
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))


def _score(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mock hybrid scoring combining simple heuristics and randomness to simulate external APIs/ML.
    """
    base = min(max(float(payload.get("TransactionAmt", 0)) / 500.0, 0), 1)
    boost = 0.15 if payload.get("isFraud") else 0
    noise = random.random() * 0.2  # simple randomness to vary output
    score = min(base + boost + noise, 1.0)
    reason = []
    if payload.get("TransactionAmt", 0) > 200:
        reason.append("high_amount")
    if payload.get("isFraud"):
        reason.append("known_fraud_flag")
    if payload.get("DeviceType") == "desktop":
        reason.append("desktop_risk")
    if not reason:
        reason.append("baseline")
    return {
        **payload,
        "hybrid_score": round(score, 4),
        "risk_reason": reason,
        "scored_at_ms": int(time.time() * 1000),
    }


async def run():
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        retry_backoff_ms=250,
    )

    await consumer.start()
    await producer.start()
    logger.info(
        "Risk adapter consuming from %s and producing to %s (DLQ=%s)",
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        DLQ_TOPIC,
    )
    try:
        while True:
            batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS)
            if not batch:
                continue
            for tp, messages in batch.items():
                for msg in messages:
                    try:
                        scored = _score(msg.value)
                        await producer.send_and_wait(OUTPUT_TOPIC, scored)
                        logger.info(
                            "Scored tx=%s score=%.3f topic=%s->%s",
                            scored.get("TransactionID"),
                            scored.get("hybrid_score", -1.0),
                            tp.topic,
                            OUTPUT_TOPIC,
                        )
                    except Exception as exc:
                        logger.exception("Scoring failed; routing to DLQ")
                        dlq_payload = {"error": str(exc), "tx": msg.value}
                        if DLQ_TOPIC:
                            try:
                                await producer.send_and_wait(DLQ_TOPIC, dlq_payload)
                            except Exception:
                                logger.exception("Failed to send to DLQ; dropping message")
    except asyncio.CancelledError:
        logger.info("Risk adapter shutdown requested")
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
