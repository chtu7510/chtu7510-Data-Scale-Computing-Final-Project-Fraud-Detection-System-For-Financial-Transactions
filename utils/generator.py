# utils/generator.py
import uuid
import numpy as np
import time

start_time = int(time.time())
tx_count = 0

def generate_transaction(train_amounts, overall_fraud_ratio, account_list, merchant_list, device_list, location_list):
    transaction = {
        "TransactionID": str(uuid.uuid4()),
        "TransactionAmt": float(np.random.choice(train_amounts)),
        "isFraud": int(np.random.rand() < overall_fraud_ratio),
        "card1": int(np.random.choice(account_list)),
        "addr1": int(np.random.choice(merchant_list)),
        "DeviceType": str(np.random.choice(device_list)),
        "addr2": int(np.random.choice([loc[1] for loc in location_list]))
    }
    return transaction

def generate_transaction_with_timestamp(train_amounts, overall_fraud_ratio, account_list, merchant_list, device_list, location_list):
    global tx_count
    tx_count += 1
    tx = generate_transaction(
        train_amounts, overall_fraud_ratio, account_list,
        merchant_list, device_list, location_list
    )
    tx["TransactionDT"] = start_time + tx_count
    return tx
