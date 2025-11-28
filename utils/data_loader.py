import pandas as pd

# Load datasets
cc = pd.read_csv("../data/creditcard.csv")
train_transaction = pd.read_csv("../data/train_transaction.csv")
train_identity = pd.read_csv("../data/train_identity.csv")

# Merge if needed
train_merged = train_transaction.merge(train_identity, on="TransactionID", how="left")

# Compute variables
train_amounts = train_transaction['TransactionAmt'].values
cc_fraud_ratio = cc['Class'].mean()
train_fraud_ratio = train_transaction['isFraud'].mean()
overall_fraud_ratio = (cc_fraud_ratio + train_fraud_ratio) / 2

account_list = train_merged['card1'].dropna().unique().tolist()
merchant_list = train_merged['addr1'].dropna().unique().tolist()
device_list = train_merged['DeviceType'].dropna().unique().tolist()
location_list = train_transaction[['addr1', 'addr2']].dropna().values.tolist()
