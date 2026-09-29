"""Unit tests for compliance rules + cleaning checks on tiny hand-made datasets.

Run from the repo root:  .venv/bin/python -m pytest tests/ -v
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from jobs.clean_transactions import find_reasons  # noqa: E402
from jobs.compliance_rules import (  # noqa: E402
    rule_r1_large_cash,
    rule_r2_structuring,
    rule_r3_rapid_pass_through,
    rule_r5_watchlist,
    rule_r6_kyc_gap,
    rule_r7_velocity,
    rule_r8_balance_drain,
)
import json  # noqa: E402

CFG = json.loads((ROOT / "config" / "rules.json").read_text())
RUN_DATE = "2025-01-15"
EPOCH_TS = "2025-01-01 00:00:00"


@pytest.fixture(scope="module")
def spark():
    import os
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[2]")
         .appName("rule-tests")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def ts(hours_after_epoch: float):
    from datetime import datetime, timedelta
    return datetime(2025, 1, 1) + timedelta(hours=hours_after_epoch)


def txn(txn_id, ttype, amount, orig, dst, ts_hours, old_org=10_000_000.0,
        new_org=None, old_dst=0.0, new_dst=None, fraud=0):
    new_org = old_org - amount if new_org is None else new_org
    new_dst = old_dst + amount if new_dst is None else new_dst
    return (txn_id, ttype, float(amount), orig, float(old_org), float(new_org),
            dst, float(old_dst), float(new_dst), fraud, "paysim", ts(ts_hours))


SCHEMA = ("txn_id string, type string, amount double, nameOrig string, "
          "oldbalanceOrg double, newbalanceOrig double, nameDest string, "
          "oldbalanceDest double, newbalanceDest double, isFraud int, "
          "source string, txn_ts timestamp")


def with_date(df):
    return df.withColumn("txn_date", F.lit(RUN_DATE))


# ---------- cleaning checks ----------

def test_find_reasons_flags_bad_rows(spark):
    rows = [
        txn("t1", "CASH_OUT", -500.0, "A", "B", 24),                    # negative amount
        txn("t2", "CASH_OUT", 500.0, None, "B", 24),                    # null sender
        txn("t3", "CASH_OUT", 500.0, "A", "B", 24, new_org=9_000_000),  # balance mismatch
        txn("t4", "CASH_OUT", 500.0, "A", "B", 24),                     # clean
    ]
    df = spark.createDataFrame(rows, SCHEMA)
    out = find_reasons(df, CFG).select("txn_id", "reasons").collect()
    r = {row["txn_id"]: row["reasons"] for row in out}
    assert r["t1"] == ["bad_amount"]
    assert r["t2"] == ["null_account_id"]
    assert r["t3"] == ["balance_mismatch"]
    assert r["t4"] == []


# ---------- R1 large cash ----------

def test_r1_triggers_at_threshold(spark):
    rows = [
        txn("t1", "CASH_OUT", 1_200_000.0, "A", "B", 24),
        txn("t2", "CASH_OUT", 999_999.0, "C", "D", 24),
        txn("t3", "PAYMENT", 5_000_000.0, "E", "F", 24),  # not cash -> no hit
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r1_large_cash(df, CFG).collect()
    assert len(alerts) == 1
    assert alerts[0]["account_id"] == "A"
    assert alerts[0]["amount"] == 1_200_000.0


# ---------- R2 structuring ----------

def test_r2_three_below_threshold_triggers(spark):
    rows = [txn(f"t{i}", "TRANSFER", 950_000.0, "A", f"D{i}", 24 * (day + 1) + 3)
            for i, day in enumerate(range(3))]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r2_structuring(df, CFG).collect()
    assert len(alerts) == 1
    assert alerts[0]["account_id"] == "A"
    assert len(alerts[0]["txn_ids"]) == 3


def test_r2_two_transactions_do_not_trigger(spark):
    rows = [txn(f"t{i}", "TRANSFER", 950_000.0, "A", f"D{i}", 24 * (day + 1) + 3)
            for i, day in enumerate(range(2))]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    assert rule_r2_structuring(df, CFG).count() == 0


def test_r2_gap_beyond_window_does_not_trigger(spark):
    rows = [txn("t0", "TRANSFER", 950_000.0, "A", "D0", 24 * 1),
            txn("t1", "TRANSFER", 950_000.0, "A", "D1", 24 * 10),
            txn("t2", "TRANSFER", 950_000.0, "A", "D2", 24 * 19)]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    assert rule_r2_structuring(df, CFG).count() == 0


# ---------- R3 rapid pass-through ----------

def test_r3_triggers_on_90pct_outflow(spark):
    rows = [
        txn("in1", "TRANSFER", 1_000_000.0, "X", "A", 24, old_org=0.0, new_org=0.0),
        txn("out1", "CASH_OUT", 950_000.0, "A", "Y", 24 + 10),   # 95% out within 10h
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r3_rapid_pass_through(df, CFG).collect()
    assert len(alerts) == 1
    assert alerts[0]["account_id"] == "A"


def test_r3_no_trigger_on_partial_outflow(spark):
    rows = [
        txn("in1", "TRANSFER", 1_000_000.0, "X", "A", 24, old_org=0.0, new_org=0.0),
        txn("out1", "CASH_OUT", 500_000.0, "A", "Y", 24 + 10),   # only 50%
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    assert rule_r3_rapid_pass_through(df, CFG).count() == 0


def test_r3_no_trigger_after_window(spark):
    rows = [
        txn("in1", "TRANSFER", 1_000_000.0, "X", "A", 24, old_org=0.0, new_org=0.0),
        txn("out1", "CASH_OUT", 990_000.0, "A", "Y", 24 + 30),   # 30h later
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    assert rule_r3_rapid_pass_through(df, CFG).count() == 0


# ---------- R5 watchlist ----------

def test_r5_watchlist_hit_sender_and_receiver(spark):
    wl = spark.createDataFrame([("W1", "Sanctions"), ("W2", "Fraud history")],
                               "account_id string, reason string")
    rows = [
        txn("t1", "TRANSFER", 100_000.0, "W1", "B", 24),
        txn("t2", "TRANSFER", 100_000.0, "C", "W2", 24),
        txn("t3", "TRANSFER", 100_000.0, "D", "E", 24),
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r5_watchlist(df, wl).collect()
    assert {a["account_id"] for a in alerts} == {"W1", "W2"}


# ---------- R6 KYC gap ----------

def test_r6_missing_and_expired_kyc(spark):
    customers = spark.createDataFrame(
        [("C1", "A"), ("C2", "B"), ("C3", "C")], "customer_id string, account_id string")
    kyc = spark.createDataFrame(
        [("C1", "Missing", "2015-01-01", "2025-01-01"),
         ("C2", "Verified", "2015-01-01", "2025-01-10"),   # expired before run date
         ("C3", "Verified", "2015-01-01", "2026-01-01")],  # valid
        "customer_id string, pan_status string, kyc_date string, kyc_expiry string")
    rows = [txn(f"t{i}", "TRANSFER", 100_000.0, orig, "M1", 24)
            for i, orig in enumerate(["A", "B", "C"])]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r6_kyc_gap(df, kyc, customers, RUN_DATE, CFG).collect()
    assert {a["account_id"] for a in alerts} == {"A", "B"}


# ---------- R7 velocity ----------

def test_r7_velocity_over_limit(spark):
    rows = [txn(f"t{i:03d}", "PAYMENT", 1000.0, "A", f"M{i:03d}", 24 + i / 3600.0)
            for i in range(21)]  # 21 txns spread over one hour
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r7_velocity(df, CFG).collect()
    assert len(alerts) == 1
    assert alerts[0]["account_id"] == "A"


def test_r7_velocity_at_limit_is_ok(spark):
    rows = [txn(f"t{i:03d}", "PAYMENT", 1000.0, "A", f"M{i:03d}", 24 + i / 3600.0)
            for i in range(20)]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    assert rule_r7_velocity(df, CFG).count() == 0


# ---------- R8 balance drain ----------

def test_r8_drain_then_cashout(spark):
    rows = [
        txn("drain", "TRANSFER", 9_800_000.0, "V", "A", 24, old_org=10_000_000.0),
        txn("cash", "CASH_OUT", 9_000_000.0, "A", "Y", 24 + 3, old_org=9_800_000.0),
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    alerts = rule_r8_balance_drain(df, CFG).collect()
    assert len(alerts) == 1
    assert alerts[0]["account_id"] == "A"


def test_r8_no_trigger_without_followup_cashout(spark):
    rows = [
        txn("drain", "TRANSFER", 9_800_000.0, "V", "A", 24, old_org=10_000_000.0),
    ]
    df = with_date(spark.createDataFrame(rows, SCHEMA))
    assert rule_r8_balance_drain(df, CFG).count() == 0
