import json
import os
from collections import deque
from typing import Deque, Dict, List

import kafka
import streamlit as st


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
DECISIONS_TOPIC = os.getenv("DECISIONS_TOPIC", "decisions")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "200"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "2000"))


def fetch_recent() -> List[Dict]:
    """
    Consume up to MAX_RECORDS messages from the beginning and close the consumer.
    Avoids reusing the generator across Streamlit reruns.
    """
    consumer = kafka.KafkaConsumer(
        DECISIONS_TOPIC,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )
    buffer: Deque[Dict] = deque(maxlen=MAX_RECORDS)
    try:
        for msg in consumer:
            buffer.append(msg.value)
            if len(buffer) >= MAX_RECORDS:
                break
    finally:
        consumer.close()
    return list(buffer)


def main():
    st.set_page_config(page_title="Fraud Decisions", layout="wide")
    st.title("Fraud Decisions Stream")
    st.caption(f"Topic: {DECISIONS_TOPIC} | Brokers: {KAFKA_BROKERS}")

    records = fetch_recent()

    st.metric("Records fetched", len(records))
    if not records:
        st.info("No decisions yet. Send transactions to see activity.")
        return

    # Show latest first
    records = list(reversed(records))
    cols = ["TransactionID", "decision", "hybrid_score", "TransactionAmt", "amount_bucket", "risk_reason", "decision_reasons", "trace_id"]
    table = []
    for r in records:
        row = {k: r.get(k) for k in cols}
        table.append(row)

    st.dataframe(table, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
