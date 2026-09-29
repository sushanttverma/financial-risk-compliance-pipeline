"""Inject synthetic AML scenarios into the PaySim stream.

Run order inside data_gen/ is:  inject_scenarios.py -> generate_reference.py -> split_daily.py

Outputs (data_gen/output/):
  injected_transactions.csv - extra transactions, source='injected'
  injected_accounts.csv     - account ids created by the scenarios
  injected_watchlist.csv    - accounts that must end up on the watchlist
  injected_dormant.csv      - accounts that must look 180+ days dormant

Scenarios (fixed seed, reproducible):
  S1  50 accounts doing 3-5 transfers of INR 9.0-9.9 lakh within a week (structuring)
  S2  40 accounts receiving money and sending 90%+ of it out within 24 hours (mule)
  S3  30 dormant accounts suddenly moving INR 5 lakh+
  S5  10 accounts firing ~25 payments within one hour (velocity)
  S4  20 transactions touching watchlist accounts
"""

import random
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE / "output"
OUT.mkdir(exist_ok=True)

SEED = 42
EPOCH = pd.Timestamp("2025-01-01 00:00:00")  # PaySim step 1


def step_of(ts: pd.Timestamp) -> int:
    return int((ts - EPOCH).total_seconds() // 3600) + 1


def new_account(rng: random.Random) -> str:
    return f"INJ{rng.randint(10_000_000, 99_999_999)}"


def row(sender, receiver, ts, amount, old_org, new_org, old_dst, new_dst,
        ttype="TRANSFER", fraud=0):
    return {
        "step": step_of(ts), "type": ttype, "amount": round(amount, 2),
        "nameOrig": sender,
        "oldbalanceOrg": round(old_org, 2), "newbalanceOrig": round(new_org, 2),
        "nameDest": receiver,
        "oldbalanceDest": round(old_dst, 2), "newbalanceDest": round(new_dst, 2),
        "isFraud": fraud, "isFlaggedFraud": 0, "source": "injected",
    }


def main():
    rng = random.Random(SEED)
    txns, accounts, watchlist, dormant = [], [], [], []

    # S1 structuring: 3-5 transfers just below the 10 lakh CTR threshold within a week
    for _ in range(50):
        acc = new_account(rng)
        accounts.append(acc)
        day = rng.randint(1, 28)
        n = rng.randint(3, 5)
        bal = n * 1_000_000  # funded up front so the balance math stays consistent
        for k in range(n):
            amt = rng.uniform(900_000, 990_000)
            ts = EPOCH + pd.Timedelta(days=day - 1, hours=rng.randint(0, 23)) + pd.Timedelta(hours=k)
            bal -= amt
            txns.append(row(acc, new_account(rng), ts, amt, bal + amt, bal, 0, amt))

    # S2 rapid pass-through: receive, then send >=90% out within 24 hours
    for _ in range(40):
        acc = new_account(rng)
        accounts.append(acc)
        day = rng.randint(2, 29)
        in_ts = EPOCH + pd.Timedelta(days=day - 1, hours=rng.randint(0, 20))
        amt = rng.uniform(200_000, 2_000_000)
        out_ts = in_ts + pd.Timedelta(hours=rng.randint(1, 23))
        out_amt = amt * rng.uniform(0.92, 0.99)
        txns.append(row(new_account(rng), acc, in_ts, amt, amt, 0, 0, amt, ttype="TRANSFER"))
        txns.append(row(acc, new_account(rng), out_ts, out_amt, amt, amt - out_amt, amt, amt - out_amt,
                        ttype="CASH_OUT"))

    # S3 dormant accounts suddenly moving 5 lakh+
    for _ in range(30):
        acc = new_account(rng)
        accounts.append(acc)
        dormant.append(acc)
        day = rng.randint(1, 30)
        ts = EPOCH + pd.Timedelta(days=day - 1, hours=rng.randint(0, 23))
        amt = rng.uniform(500_000, 3_000_000)
        txns.append(row(new_account(rng), acc, ts, amt, amt, 0, 0, amt, ttype="TRANSFER"))

    # S5 velocity: 10 accounts firing ~25 payments within a single hour
    for _ in range(10):
        acc = new_account(rng)
        accounts.append(acc)
        day = rng.randint(1, 30)
        hour = rng.randint(0, 23)
        bal = 100_000.0
        for k in range(rng.randint(21, 26)):
            amt = rng.uniform(500, 2_000)
            ts = EPOCH + pd.Timedelta(days=day - 1, hours=hour) + pd.Timedelta(seconds=k * 120)
            bal -= amt
            txns.append(row(acc, f"M{rng.randint(10_000_000, 99_999_999)}", ts, amt,
                            bal + amt, bal, 0, amt, ttype="PAYMENT"))

    # S4 watchlist hits: first pick the accounts, they are added to the watchlist later
    wl_senders = [new_account(rng) for _ in range(10)]
    wl_receivers = [new_account(rng) for _ in range(10)]
    watchlist.extend(wl_senders + wl_receivers)
    accounts.extend(wl_senders + wl_receivers)
    for _ in range(20):
        day = rng.randint(1, 30)
        ts = EPOCH + pd.Timedelta(days=day - 1, hours=rng.randint(0, 23))
        amt = rng.uniform(50_000, 900_000)
        if rng.random() < 0.5:
            txns.append(row(rng.choice(wl_senders), new_account(rng), ts, amt, amt, 0, 0, amt))
        else:
            txns.append(row(new_account(rng), rng.choice(wl_receivers), ts, amt, amt, 0, 0, amt))

    df = pd.DataFrame(txns)
    df.to_csv(OUT / "injected_transactions.csv", index=False)
    pd.DataFrame({"account_id": accounts}).to_csv(OUT / "injected_accounts.csv", index=False)
    pd.DataFrame({"account_id": watchlist}).to_csv(OUT / "injected_watchlist.csv", index=False)
    pd.DataFrame({"account_id": dormant}).to_csv(OUT / "injected_dormant.csv", index=False)
    print(f"inject_scenarios: {len(df)} injected transactions, {len(accounts)} accounts, "
          f"{len(watchlist)} watchlist, {len(dormant)} dormant")


if __name__ == "__main__":
    main()
