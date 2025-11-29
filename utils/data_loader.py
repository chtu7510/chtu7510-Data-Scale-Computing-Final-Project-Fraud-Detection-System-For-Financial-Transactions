import pandas as pd
import numpy as np
from pathlib import Path

# This module is imported by generators/clients; make it resilient so the project
# can run even when the real datasets are not available locally.

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RNG = np.random.default_rng(0)


def _load_or_generate_csv(filename, builder):
    """Load a CSV if present, otherwise return a synthetic DataFrame."""
    path = DATA_DIR / filename
    if path.exists():
        return pd.read_csv(path)
    return builder()


def _synthetic_credit_card(rows: int = 500):
    # Only the Class column is consumed by downstream code.
    return pd.DataFrame({"Class": RNG.binomial(1, 0.03, size=rows)})


def _synthetic_transactions(rows: int = 1_000):
    return pd.DataFrame(
        {
            "TransactionID": np.arange(1, rows + 1),
            "TransactionAmt": RNG.lognormal(mean=3.5, sigma=0.9, size=rows).round(2),
            "isFraud": RNG.binomial(1, 0.03, size=rows),
            "card1": RNG.integers(1000, 6000, size=rows),
            "addr1": RNG.integers(100, 999, size=rows),
            "addr2": RNG.choice([50, 60, 70, 80], size=rows),
            "DeviceType": RNG.choice(["mobile", "desktop", "tablet"], size=rows),
        }
    )


def _synthetic_identity(tx_df: pd.DataFrame):
    # Minimal identity data used for merging DeviceType.
    return pd.DataFrame(
        {
            "TransactionID": tx_df["TransactionID"],
            "DeviceType": tx_df["DeviceType"],
        }
    )


# Load datasets (real if present, synthetic otherwise)
cc = _load_or_generate_csv("creditcard.csv", _synthetic_credit_card)
train_transaction = _load_or_generate_csv("train_transaction.csv", _synthetic_transactions)
train_identity = _load_or_generate_csv(
    "train_identity.csv", lambda: _synthetic_identity(train_transaction)
)

# Ensure DeviceType exists across frames (real files sometimes omit this column)
if "DeviceType" not in train_transaction.columns:
    train_transaction["DeviceType"] = RNG.choice(
        ["mobile", "desktop", "tablet"], size=len(train_transaction)
    )
if "DeviceType" not in train_identity.columns:
    train_identity["DeviceType"] = RNG.choice(
        ["mobile", "desktop", "tablet"], size=len(train_identity)
    )

# Merge if needed
train_merged = train_transaction.merge(train_identity, on="TransactionID", how="left")
if "DeviceType" not in train_merged or train_merged["DeviceType"].isna().all():
    # Fallback if merge dropped DeviceType or produced all-null values
    fallback = train_transaction.get("DeviceType", pd.Series([], dtype=str))
    train_merged["DeviceType"] = fallback.values[: len(train_merged)]

# Compute variables
train_amounts = train_transaction["TransactionAmt"].values
cc_fraud_ratio = cc["Class"].mean()
train_fraud_ratio = train_transaction["isFraud"].mean()
overall_fraud_ratio = (cc_fraud_ratio + train_fraud_ratio) / 2

account_list = train_merged["card1"].dropna().unique().tolist()
merchant_list = train_merged["addr1"].dropna().unique().tolist()
device_list = train_merged["DeviceType"].dropna().unique().tolist()
location_list = train_transaction[["addr1", "addr2"]].dropna().values.tolist()
