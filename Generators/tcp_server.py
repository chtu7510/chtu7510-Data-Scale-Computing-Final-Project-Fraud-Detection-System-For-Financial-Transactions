import socket
import json
from datetime import datetime

HOST = 'localhost'
PORT = 5050

# Required transaction fields
REQUIRED_FIELDS = [
    "TransactionID",
    "TransactionAmt",
    "isFraud",
    "card1",
    "addr1",
    "addr2",
    "DeviceType",
    "TransactionDT"
]

def validate_transaction(tx):
    """Check if all required fields exist"""
    missing = [f for f in REQUIRED_FIELDS if f not in tx]
    if missing:
        return False, missing
    return True, []

def handle_client(conn, addr):
    print(f"Connected by {addr}")
    buffer = ""
    while True:
        data = conn.recv(1024)
        if not data:
            break
        buffer += data.decode()
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            try:
                tx = json.loads(line)
            except json.JSONDecodeError:
                print("Malformed JSON:", line)
                continue

            # Schema validation
            valid, missing = validate_transaction(tx)
            if not valid:
                print(f"Invalid transaction, missing fields: {missing}")
                continue

            # Logging / observability
            print(f"[{datetime.now()}] TransactionID: {tx['TransactionID']}, Amount: {tx['TransactionAmt']}, isFraud: {tx['isFraud']}")

            # Placeholder: authentication / rate limiting can go here
            # e.g., check API key or limit number of transactions per second

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Server listening on {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            handle_client(conn, addr)

if __name__ == "__main__":
    main()



''' tcp_server.py - version 1
import socket

HOST = "localhost"
PORT = 5050

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        print(f"Server listening on {HOST}:{PORT}")

        conn, addr = server.accept()
        print(f"Connected by {addr}")

        buffer = ""

        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    print("Client disconnected.")
                    break

                buffer += data.decode()

                # Process newline-delimited messages from client
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    print("Received:", line)

if __name__ == "__main__":
    main()
'''