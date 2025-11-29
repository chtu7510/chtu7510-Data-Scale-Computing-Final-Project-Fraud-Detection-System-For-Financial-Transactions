import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("decision-engine")

# Environment configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "scored-transactions")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "decisions")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions-dlq")
GROUP_ID = os.getenv("GROUP_ID", "decision-engine-group")
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# Simple thresholds for demo purposes.
ALLOW_THRESHOLD = float(os.getenv("ALLOW_THRESHOLD", "0.3"))
HOLD_THRESHOLD = float(os.getenv("HOLD_THRESHOLD", "0.7"))


def _decide(payload: Dict[str, Any]) -> Dict[str, Any]:
    score = float(payload.get("hybrid_score", 0))
    amount = float(payload.get("TransactionAmt", 0))
    amount_bucket = payload.get("amount_bucket", "unknown")
    reasons: List[str] = []

    if score >= HOLD_THRESHOLD or amount_bucket in {"very_high", "high"} or amount > 500:
        decision = "Decline"
        reasons.append("high_score_or_amount")
    elif score >= ALLOW_THRESHOLD:
        decision = "Hold"
        reasons.append("moderate_score")
    else:
        decision = "Allow"
        reasons.append("low_score")

    if payload.get("isFraud"):
        decision = "Decline"
        reasons.append("flagged_fraud")

    return {
        **payload,
        "decision": decision,
        "decision_reasons": reasons,
        "decided_at_ms": int(time.time() * 1000),
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
        "Decision engine consuming from %s and producing to %s (DLQ=%s)",
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
                        decided = _decide(msg.value)
                        await producer.send_and_wait(OUTPUT_TOPIC, decided)
                        logger.info(
                            "Decided tx=%s decision=%s score=%.3f",
                            decided.get("TransactionID"),
                            decided.get("decision"),
                            decided.get("hybrid_score", -1.0),
                        )
                    except Exception as exc:
                        logger.exception("Decision failed; routing to DLQ")
                        dlq_payload = {"error": str(exc), "tx": msg.value}
                        if DLQ_TOPIC:
                            try:
                                await producer.send_and_wait(DLQ_TOPIC, dlq_payload)
                            except Exception:
                                logger.exception("Failed to send to DLQ; dropping message")
    except asyncio.CancelledError:
        logger.info("Decision engine shutdown requested")
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
