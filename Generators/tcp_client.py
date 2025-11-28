import socket
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
