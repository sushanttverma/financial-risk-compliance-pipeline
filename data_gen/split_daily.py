"""Split PaySim + injected rows into one daily CSV per day (bank-extract style),
and sprinkle in dirty rows that the cleaning job must catch.

Output: landing/transactions/txn_date=YYYY-MM-DD/part.csv   (30 partitions)
Dirty rows added on purpose:
  - duplicates (~0.5% of rows, same content -> same txn_id)
  - negative amounts
  - null account ids
  - new balance != old balance - amount (for debit-side transactions)
"""

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
LANDING = HERE.parent / "landing" / "transactions"
SEED = 42

COLS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
        "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud", "source"]
EPOCH = pd.Timestamp("2025-01-01")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paysim", required=True, help="path to the PaySim CSV")
    args = ap.parse_args()
    rng = np.random.default_rng(SEED)

    pay = pd.read_csv(args.paysim)
    pay["source"] = "paysim"
    inj = pd.read_csv(HERE / "output" / "injected_transactions.csv")
    df = pd.concat([pay[COLS], inj[COLS]], ignore_index=True)
    print(f"combined rows: {len(df)} (paysim {len(pay)}, injected {len(inj)})")

    # dirty rows: sample a slice and corrupt it, keep original source tag
    dirty_idx = rng.choice(df.index, size=int(len(df) * 0.001) + 5000, replace=False)
    dirty = df.loc[dirty_idx].copy()
    n = len(dirty)
    cut = n // 4
    dirty.iloc[0:cut, dirty.columns.get_loc("amount")] = -np.abs(
        dirty.iloc[0:cut, dirty.columns.get_loc("amount")].to_numpy())
    dirty.iloc[cut:2 * cut, dirty.columns.get_loc("nameOrig")] = None
    dirty.iloc[2 * cut:3 * cut, dirty.columns.get_loc("nameDest")] = None
    # balance mismatch on the rest (debit-side rows only, else overwrite CASH_IN math)
    mm = dirty.iloc[3 * cut:]
    debit = mm["type"].isin(["CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"])
    mm = mm[debit].copy()
    mm["newbalanceOrig"] = mm["oldbalanceOrg"] - mm["amount"] * 0.5  # wrong on purpose
    dirty = pd.concat([dirty.iloc[:3 * cut], mm])

    df = pd.concat([df, dirty], ignore_index=True)

    # duplicates (~0.5%): append exact copies
    dup = df.sample(frac=0.005, random_state=SEED)
    df = pd.concat([df, dup], ignore_index=True)

    df["txn_date"] = (EPOCH + pd.to_timedelta(df["step"] - 1, unit="h")).dt.strftime("%Y-%m-%d")

    if LANDING.exists():
        shutil.rmtree(LANDING)
    for date, g in df.groupby("txn_date"):
        d = LANDING / f"txn_date={date}"
        d.mkdir(parents=True, exist_ok=True)
        g[COLS].to_csv(d / "part.csv", index=False)

    counts = df.groupby("txn_date").size()
    print(f"split_daily: wrote {len(counts)} daily files to {LANDING}, "
          f"{len(df)} total rows ({len(dirty)} dirty, {len(dup)} duplicates)")


if __name__ == "__main__":
    main()
