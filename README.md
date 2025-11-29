# Fraud Detection System - Streaming Ingress

Project description (per proposal): build a datacenter-based fraud detection system that ingests high-throughput payment transactions, validates them at an API gateway, and routes them into a message queue for downstream enrichment/scoring. This repo currently covers producers, the API gateway/ingress, and a working Kafka message queue in Docker Compose.

## Setup
- Python 3.10+ recommended.
- Install dependencies (for local, non-Docker use): `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`

## What’s done so far
- Synthetic transaction producers (TCP/UDP/HTTP/gRPC) with realistic fields (TransactionID, amount, fraud flag, account/location/device, timestamp, optional currency/category/account_age).
- Resilient data loader that auto-generates realistic values if datasets are missing.
- API Gateway / Ingress (FastAPI): schema validation, API key auth, per-client rate limiting, request logging; publishes to Kafka with DLQ fallback on errors.
- Kafka + Zookeeper in Docker Compose; topics auto-created (defaults: `transactions`, `transactions-dlq`).
- Containerization for gateway and producers with a Compose demo.

## Run with Docker Compose (recommended)
```bash
docker-compose up --build
```
Services: `zookeeper`, `kafka`, `api-gateway`, `tcp-server`, `tcp-producer`, `http-producer`.

Health check:
```bash
curl -s localhost:8080/healthz   # queue should be "ready"
```

Send a sample transaction:
```bash
curl -X POST http://localhost:8080/transactions \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: changeme' \
  --data-raw '{
    "TransactionID": "tx-1",
    "TransactionAmt": 10.5,
    "isFraud": 0,
    "card1": 1111,
    "addr1": 100,
    "addr2": 200,
    "DeviceType": "mobile",
    "TransactionDT": 1700000000
  }'
```

Verify in Kafka:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic transactions \
  --from-beginning \
  --timeout-ms 5000
```

Enricher service:
- Runs in Compose as `enricher`, consuming `transactions`, enriching, and publishing to `enriched-transactions`, with DLQ fallback to `transactions-dlq`.
- Inspect enriched output:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic enriched-transactions \
  --from-beginning \
  --timeout-ms 5000
```

Risk adapter:
- Consumes `enriched-transactions`, assigns a mock hybrid score/reason, and produces to `scored-transactions` (DLQ on failure).
- Inspect scored output:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic scored-transactions \
  --from-beginning \
  --timeout-ms 5000
```

Decision engine:
- Consumes `scored-transactions`, applies simple thresholds to classify Allow/Hold/Decline, and emits to `decisions` (DLQ on failure).
- Inspect decisions:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic decisions \
  --from-beginning \
  --timeout-ms 5000
```

Dashboard:
- Streamlit app reading the `decisions` topic; served on http://localhost:8501
- Start with the stack: `docker-compose up -d dashboard`
- View recent decisions in the table; shows TransactionID, decision, score, amount, buckets, reasons.

Environment knobs (see `docker-compose.yml`):
- `API_KEY`, `RATE_LIMIT_PER_MIN`
- `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`
- Producer knobs: `TPS`, `NUM_WORKERS`, `TRANSPORT`, `HTTP_ENDPOINT`, `TX_HOST`, `TX_PORT`
- Enricher knobs: `INPUT_TOPIC`, `OUTPUT_TOPIC`, `DLQ_TOPIC`, `GROUP_ID`
- Risk adapter knobs: `INPUT_TOPIC`, `OUTPUT_TOPIC`, `DLQ_TOPIC`, `GROUP_ID`
- Decision engine knobs: `INPUT_TOPIC`, `OUTPUT_TOPIC`, `DLQ_TOPIC`, `GROUP_ID`, `ALLOW_THRESHOLD`, `HOLD_THRESHOLD`
- Dashboard knobs: `DECISIONS_TOPIC`, `MAX_RECORDS`

## TCP pipeline (manual)
- Start server: `PYTHONPATH=. python Generators/tcp_server.py`
- Start client: `PYTHONPATH=. python Generators/tcp_client.py`
- Client env: `TX_HOST`/`TX_PORT`, `TPS`, `NUM_WORKERS`, `TRANSPORT` (`TCP`/`UDP`/`HTTP`), `HTTP_ENDPOINT`, `API_KEY`.

## gRPC pipeline
- Server: `python grpc/grpc-server.py`
- Client: `python grpc/grpc-client.py`
- Regenerate stubs after proto change: `make -C grpc grpc`

## Data
- If `data/creditcard.csv`, `data/train_transaction.csv`, or `data/train_identity.csv` exist they are used.
- Otherwise synthetic values are generated so producers always work.

## API Gateway / Ingress
- FastAPI at `gateway/app.py`: validation, API key auth, rate limiting, logging, Kafka publishing with DLQ fallback; `/healthz` reports queue readiness.
- Local (non-Docker) run: `PYTHONPATH=. API_KEY=changeme uvicorn gateway.app:app --host 0.0.0.0 --port 8080`
- HTTP producer mode: `PYTHONPATH=. TRANSPORT=HTTP HTTP_ENDPOINT=http://localhost:8080/transactions API_KEY=changeme python Generators/tcp_client.py`

## Next steps (per proposal)
- Add consumers: enricher -> risk adapter -> decision engine, each with DLQ handling.
- Add observability (metrics, structured logs, trace IDs) and a minimal dashboard to view decisions.
- Persist decisions/alerts (DB/BigQuery/Cloud Storage) and add basic alerting. 
