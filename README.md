# Fraud Detection System - Streaming Ingress

Project description (per proposal): build a datacenter-based fraud detection system that ingests high-throughput payment transactions, enriches them with metadata (account, merchant, location, device, timestamp, amount), validates/guards traffic at an API gateway, and routes events into downstream processing for fraud scoring and monitoring. This repo currently covers the ingestion/producers + API gateway/ingress pieces with synthetic data to keep everything runnable.

## Setup
- Python 3.10+ recommended.
- Install dependencies: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`

## What’s done so far
- Synthetic transaction producers (TCP/UDP/HTTP/gRPC) with metadata: TransactionID, amount, fraud flag, account/merchant/location/device, timestamp, optional currency/category/account_age.
- Resilient data loader that auto-generates realistic values if datasets are missing.
- API Gateway / Ingress (FastAPI): schema validation, API key auth, per-client rate limiting, request logging; ready to route into a message queue via `publish_to_queue` stub.
- Containerization and compose demos for producers + gateway.

## TCP pipeline
- Start the server: `PYTHONPATH=. python Generators/tcp_server.py`
- In another shell start the client: `PYTHONPATH=. python Generators/tcp_client.py`
- Environment knobs for the client:
  - `TX_HOST`/`TX_PORT` (default `localhost`/`5050`)
  - `TPS` transactions per second (default `100`)
  - `NUM_WORKERS` sender threads (default `5`)
  - `TRANSPORT` `"TCP"`, `"UDP"`, or `"HTTP"` (default `"TCP"`)
  - `HTTP_ENDPOINT` when `TRANSPORT=HTTP` (default `http://localhost:8080/transactions`)
  - `API_KEY` passed in `x-api-key` header for HTTP

## gRPC pipeline
- Start the server: `python grpc/grpc-server.py`
- In another shell start the client: `python grpc/grpc-client.py`
- Regenerate protobuf stubs (only if you change `grpc/transactions.proto`): run `make -C grpc grpc`

## Data
- If `data/creditcard.csv`, `data/train_transaction.csv`, or `data/train_identity.csv` exist they are used.
- When those files are absent, the loaders generate realistic synthetic values so the streamers always work.

## Containerized producers (for reproducibility / ingress integration)
- Build the image: `docker build -f Dockerfile.producer -t fraud-producer .`
- TCP producer to any host/port (replace with your API gateway):  
  `docker run --rm --network host -e TX_HOST=localhost -e TX_PORT=5050 -e TPS=50 -e NUM_WORKERS=5 fraud-producer`
- gRPC producer:  
  `docker run --rm --network host fraud-producer python grpc/grpc-client.py`
- Local demo with a bundled TCP sink: `docker compose up --build` (spins up `tcp-server` on 5050 and a producer sending to it). Adjust TPS/NUM_WORKERS/TRANSPORT via `docker-compose.yml` or environment overrides.

## API Gateway / Ingress
- FastAPI service at `gateway/app.py` with schema validation, API key auth, simple per-client rate limiting, and logs.
- Local run: `PYTHONPATH=. API_KEY=changeme uvicorn gateway.app:app --host 0.0.0.0 --port 8080`
- HTTP producer mode: `PYTHONPATH=. TRANSPORT=HTTP HTTP_ENDPOINT=http://localhost:8080/transactions API_KEY=changeme python Generators/tcp_client.py`
- Compose demo (gateway + HTTP producer): `docker compose up --build api-gateway http-producer` (edit `API_KEY`, `TPS`, `HTTP_ENDPOINT` as needed). The gateway currently logs/enqueues transactions; swap `publish_to_queue` for your Kafka/PubSub producer.

## Next steps (per proposal)
- Wire `publish_to_queue` to a real message bus (Kafka/Pub/Sub/SQS) with retries/DLQ.
- Add richer auth (mTLS/JWT) if required, plus structured metrics (rate-limit hits, errors).
- Build the feature pipeline and fraud model service; add a scoring endpoint and downstream monitoring for drift/performance.
