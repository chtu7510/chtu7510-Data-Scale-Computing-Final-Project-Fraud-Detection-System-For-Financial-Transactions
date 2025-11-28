import grpc
import transactions_pb2
import transactions_pb2_grpc
from utils.data_loader import (
    train_amounts, overall_fraud_ratio, account_list,
    merchant_list, device_list, location_list
)
from utils.generator import generate_transaction_with_timestamp
import numpy as np
import time

channel = grpc.insecure_channel('localhost:50051')
stub = transactions_pb2_grpc.TransactionServiceStub(channel)

def generate_transaction():
    tx = generate_transaction_with_timestamp(
        train_amounts, overall_fraud_ratio, account_list,
        merchant_list, device_list, location_list
    )
    tx.update({
        "currency": "USD",
        "merchant_category": np.random.choice(["grocery","electronics","travel"]),
        "account_age_days": np.random.randint(30,3650)
    })
    return tx

def transaction_stream():
    while True:
        tx = generate_transaction()
        yield transactions_pb2.Transaction(
            TransactionID = tx["TransactionID"],
            TransactionAmt = tx["TransactionAmt"],
            isFraud = tx["isFraud"],
            card1 = tx["card1"],
            addr1 = tx["addr1"],
            addr2 = tx["addr2"],
            DeviceType = tx["DeviceType"],
            TransactionDT = tx["TransactionDT"],
            currency = tx["currency"],
            merchant_category = tx["merchant_category"],
            account_age_days = tx["account_age_days"]
        )
        time.sleep(0.1)

def main():
    response = stub.StreamTransactions(transaction_stream())
    print("Server response:", response.message)

if __name__ == "__main__":
    main()
