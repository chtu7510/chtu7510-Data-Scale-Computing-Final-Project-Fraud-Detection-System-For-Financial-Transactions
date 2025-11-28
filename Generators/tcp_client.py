import socket
import json
import time
import os
import threading
import queue
import numpy as np
from utils.data_loader import (
    train_amounts, overall_fraud_ratio, account_list,
    merchant_list, device_list, location_list
)
from utils.generator import generate_transaction_with_timestamp

# =========================
# Configuration
# =========================
HOST = os.getenv("TX_HOST", "localhost")
PORT = int(os.getenv("TX_PORT", 5050))
TPS = int(os.getenv("TPS", 100))            # Transactions per second
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 5))
TRANSPORT = os.getenv("TRANSPORT", "TCP")   # "TCP" or "UDP"

# =========================
# Socket Setup
# =========================
def create_socket():
    if TRANSPORT == "TCP":
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
    elif TRANSPORT == "UDP":
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    else:
        raise ValueError(f"Unsupported transport: {TRANSPORT}")
    return sock

sock = create_socket()

# =========================
# Transaction Queue
# =========================
TX_QUEUE = queue.Queue()

# =========================
# Transaction Sender
# =========================
def send_tx(tx):
    """
    Send a transaction through the socket (TCP or UDP)
    """
    msg = json.dumps(tx) + "\n"
    if TRANSPORT == "TCP":
        sock.sendall(msg.encode())
    else:
        sock.sendto(msg.encode(), (HOST, PORT))

# =========================
# Worker Thread
# =========================
def worker():
    """
    Worker thread: continuously sends transactions from the queue
    """
    while True:
        tx = TX_QUEUE.get()
        if tx is None:  # Stop signal
            break
        send_tx(tx)
        TX_QUEUE.task_done()

# Start worker threads
threads = [threading.Thread(target=worker, daemon=True) for _ in range(NUM_WORKERS)]
for t in threads:
    t.start()

# =========================
# Transaction Generator
# =========================
def generate_transaction():
    """
    Generate a single transaction with extra metadata
    """
    tx = generate_transaction_with_timestamp(
        train_amounts, overall_fraud_ratio, account_list,
        merchant_list, device_list, location_list
    )
    tx.update({
        "currency": "USD",
        "merchant_category": np.random.choice(["grocery", "electronics", "travel"]),
        "account_age_days": np.random.randint(30, 3650)
    })
    return tx

# =========================
# Main Streaming Loop
# =========================
def stream_transactions():
    """
    Continuously generate and queue transactions based on TPS
    """
    interval = 1 / TPS
    while True:
        tx = generate_transaction()
        TX_QUEUE.put(tx)
        time.sleep(interval)

# =========================
# Entry Point
# =========================
if __name__ == "__main__":
    try:
        stream_transactions()
    except KeyboardInterrupt:
        print("Stopping transaction stream...")
        # Stop worker threads
        for _ in range(NUM_WORKERS):
            TX_QUEUE.put(None)
        for t in threads:
            t.join()
        sock.close()

'''import socket
import json
import time

from utils.data_loader import (
    train_amounts,
    overall_fraud_ratio,
    account_list,
    merchant_list,
    device_list,
    location_list
)

from utils.generator import generate_transaction_with_timestamp


# ------------------------------------------------------------------------------
# TCP Connection Setup
# ------------------------------------------------------------------------------
HOST = "localhost"
PORT = 5050

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))


# ------------------------------------------------------------------------------
# Streaming Function
# ------------------------------------------------------------------------------
def stream_transactions(tps=10):
    interval = 1 / tps    # seconds per transaction

    while True:
        tx = generate_transaction_with_timestamp(
            train_amounts,
            overall_fraud_ratio,
            account_list,
            merchant_list,
            device_list,
            location_list
        )

        message = json.dumps(tx) + "\n"   # newline-delimited JSON
        sock.sendall(message.encode())

        time.sleep(interval)


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting TCP client...")
    stream_transactions(tps=10)

'''