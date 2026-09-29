"""Generate reference tables (customers, kyc, watchlist) with Faker + fixed seed.

Reads the PaySim CSV and the injected scenario files, then writes landing/reference/*.csv.
One customer row per unique account (PaySim senders/receivers + injected accounts).
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

HERE = Path(__file__).resolve().parent
OUT = HERE / "output"
REF_DIR = HERE.parent / "landing" / "reference"

EPOCH = pd.Timestamp("2025-01-01")
SEED = 42
IN_DATE = "2025-01-01"  # last_activity_before_window is relative to the start of the window


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paysim", required=True, help="path to the PaySim CSV")
    args = ap.parse_args()

    fake = Faker("en_IN")
    Faker.seed(SEED)
    rng = np.random.default_rng(SEED)

    print("reading PaySim ...")
    pay = pd.read_csv(args.paysim, usecols=["nameOrig", "nameDest"])
    accounts = pd.unique(pd.concat([pay["nameOrig"], pay["nameDest"]]))
    injected = pd.read_csv(OUT / "injected_accounts.csv")["account_id"].tolist()
    all_accounts = sorted(set(accounts) | set(injected))

    n = len(all_accounts)
    merchants = pd.Series(all_accounts).str.startswith("M").to_numpy()

    segment = np.where(merchants, "Merchant", rng.choice(
        ["Retail", "SME", "Corporate"], size=n, p=[0.75, 0.20, 0.05]))
    account_type = np.where(merchants, "Merchant", rng.choice(
        ["Savings", "Current", "Wallet"], size=n, p=[0.60, 0.25, 0.15]))

    open_date = pd.Series(pd.to_datetime("2010-01-01") + pd.to_timedelta(
        rng.integers(0, 5000, size=n), unit="D"))

    # ~90% of accounts were active within 180 days before the window starts;
    # ~10% look dormant. Injected dormant accounts are always dormant.
    dormant_ids = set(pd.read_csv(OUT / "injected_dormant.csv")["account_id"])
    is_dormant = np.array([a in dormant_ids for a in all_accounts])
    random_dormant = (rng.random(n) < 0.10) & ~merchants
    is_dormant = is_dormant | random_dormant
    days_before = np.where(
        is_dormant,
        rng.integers(200, 1200, size=n),   # 200-1200 days before the window
        rng.integers(1, 180, size=n),
    )
    last_activity = pd.Series(EPOCH - pd.to_timedelta(days_before, unit="D"))

    customers = pd.DataFrame({
        "customer_id": [f"C{i:07d}" for i in range(n)],
        "account_id": all_accounts,
        "name": [fake.name() for _ in range(n)],
        "city": [fake.city() for _ in range(n)],
        "state": [fake.state() for _ in range(n)],
        "segment": segment,
        "account_type": account_type,
        "open_date": open_date.dt.strftime("%Y-%m-%d"),
        "last_activity_before_window": last_activity.dt.strftime("%Y-%m-%d"),
        "is_merchant": merchants,
    })

    # KYC: ~5% expired, ~2% missing
    pan_status = rng.choice(["Verified", "Pending", "Missing"], size=n, p=[0.86, 0.12, 0.02])
    kyc_date = pd.Series(pd.to_datetime("2015-01-01") + pd.to_timedelta(rng.integers(0, 3200, size=n), unit="D"))
    kyc_expiry = kyc_date + pd.to_timedelta(3650, unit="D")  # 10-year validity
    # make ~5% of Verified accounts already expired relative to the window start
    expired = (pan_status == "Verified") & (rng.random(n) < 0.055)
    expired_vals = pd.Timestamp(IN_DATE) - pd.to_timedelta(rng.integers(1, 400, size=n), unit="D")
    kyc_expiry = kyc_expiry.where(~expired, expired_vals)
    kyc = pd.DataFrame({
        "customer_id": customers["customer_id"],
        "pan_status": pan_status,
        "kyc_date": kyc_date.dt.strftime("%Y-%m-%d"),
        "kyc_expiry": pd.to_datetime(kyc_expiry).dt.strftime("%Y-%m-%d"),
        "risk_category": rng.choice(["Low", "Medium", "High"], size=n, p=[0.80, 0.17, 0.03]),
    })

    # watchlist: injected watchlist accounts (with a hit scenario behind them)
    # plus ~180 normal accounts sampled at random
    wl_injected = pd.read_csv(OUT / "injected_watchlist.csv")["account_id"].tolist()
    normal_pool = [a for a in all_accounts if a not in set(wl_injected) and not a.startswith("M")]
    wl_normal = rng.choice(normal_pool, size=180, replace=False)
    reasons = rng.choice(["Sanctions", "Fraud history", "Police request"], size=200, p=[0.4, 0.4, 0.2])
    added_on = pd.Series(pd.to_datetime("2024-01-01") + pd.to_timedelta(rng.integers(0, 360, size=200), unit="D"))
    watchlist = pd.DataFrame({
        "account_id": list(wl_injected) + list(wl_normal),
        "reason": reasons,
        "added_on": added_on.dt.strftime("%Y-%m-%d"),
    })

    # one directory per table so the external Hive tables can point at them
    for name, frame in [("customers", customers), ("kyc", kyc), ("watchlist", watchlist)]:
        d = REF_DIR / name
        d.mkdir(parents=True, exist_ok=True)
        frame.to_csv(d / "part.csv", index=False)
    print(f"generate_reference: {len(customers)} customers, {len(kyc)} kyc rows, "
          f"{len(watchlist)} watchlist entries -> {REF_DIR}")


if __name__ == "__main__":
    main()
