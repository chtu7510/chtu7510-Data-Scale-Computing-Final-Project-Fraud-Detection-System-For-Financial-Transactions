import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("enricher")

# Environment configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transactions")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "enriched-transactions")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions-dlq")
GROUP_ID = os.getenv("GROUP_ID", "enricher-group")
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))


def _enrich(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal enrichment stub: add trace_id, ingest timestamp, and simple derived fields.
    """
    now_ms = int(time.time() * 1000)
    return {
        **payload,
        "trace_id": str(uuid.uuid4()),
        "ingested_at_ms": now_ms,
        "amount_bucket": _bucket_amount(payload.get("TransactionAmt")),
        "device_type": payload.get("DeviceType"),
    }


def _bucket_amount(amount: Any) -> str:
    try:
        amt = float(amount)
    except Exception:
        return "unknown"
    if amt < 50:
        return "low"
    if amt < 200:
        return "medium"
    if amt < 1000:
        return "high"
    return "very_high"


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
        "Enricher consuming from %s and producing to %s (DLQ=%s)",
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
                        enriched = _enrich(msg.value)
                        await producer.send_and_wait(OUTPUT_TOPIC, enriched)
                        logger.info(
                            "Enriched tx=%s amount=%.2f topic=%s->%s",
                            enriched.get("TransactionID"),
                            enriched.get("TransactionAmt", -1.0),
                            tp.topic,
                            OUTPUT_TOPIC,
                        )
                    except Exception as exc:
                        logger.exception("Enrichment failed; routing to DLQ")
                        dlq_payload = {"error": str(exc), "tx": msg.value}
                        if DLQ_TOPIC:
                            try:
                                await producer.send_and_wait(DLQ_TOPIC, dlq_payload)
                            except Exception:
                                logger.exception("Failed to send to DLQ; dropping message")
                        # continue processing next messages
    except asyncio.CancelledError:
        logger.info("Enricher shutdown requested")
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
