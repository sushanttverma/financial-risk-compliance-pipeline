"""Compliance rules (R1-R8): one PySpark function per rule, thresholds from config/rules.json.

Each rule function returns rows of: account_id, rule_code, txn_ids ARRAY<STRING>,
amount DOUBLE, details STRING. The union goes to alerts.rule_hits, partitioned by
txn_date (dynamic overwrite -> idempotent re-runs).

Lookback: rules that span time (R2, R3, R8) read up to 7 days of clean history,
but every alert is attributed to the run date, so each daily run is self-contained.
"""

from pyspark.sql import DataFrame, functions as F, Window

from jobs.common import get_spark, get_logger, parse_args, init_tables, days_back, overwrite_partition

SECONDS = {
    "hour": 3600,
    "day": 86400,
}


def _unix(df):
    return df.withColumn("ts", F.unix_timestamp("txn_ts"))


def rule_r1_large_cash(day_txns: DataFrame, cfg) -> DataFrame:
    """Large cash transaction: CASH_IN / CASH_OUT at or above the CTR threshold."""
    c = cfg["r1"]
    return (
        day_txns.where(F.col("type").isin(c["types"]) & (F.col("amount") >= c["min_amount"]))
        .select(
            F.col("nameOrig").alias("account_id"),
            F.lit("R1").alias("rule_code"),
            F.array("txn_id").alias("txn_ids"),
            F.col("amount"),
            F.concat_ws(" ", F.col("type"), F.lit("of"),
                        F.col("amount").cast("string")).alias("details"),
        )
    )


def rule_r2_structuring(window_txns: DataFrame, cfg) -> DataFrame:
    """Structuring: >= N transactions of 9-10 lakh per sender inside the lookback window."""
    c = cfg["r2"]
    band = _unix(window_txns.where(
        (F.col("amount") >= c["min_amount"]) & (F.col("amount") < c["max_amount"])))
    w = (Window.partitionBy("nameOrig")
         .orderBy("ts")
         .rangeBetween(-c["window_days"] * SECONDS["day"] + 1, 0))
    flagged = (
        band.withColumn("hit_count", F.count("txn_id").over(w))
        .withColumn("hit_ids", F.collect_list("txn_id").over(w))
        .withColumn("hit_amount", F.sum("amount").over(w))
        .where(F.col("hit_count") >= c["min_count"])
    )
    # one alert per sender: the moment the count threshold is reached last
    last = Window.partitionBy("nameOrig").orderBy(F.col("ts").desc())
    return (
        flagged.withColumn("rn", F.row_number().over(last)).where(F.col("rn") == 1)
        .select(
            F.col("nameOrig").alias("account_id"),
            F.lit("R2").alias("rule_code"),
            F.col("hit_ids").alias("txn_ids"),
            F.col("hit_amount").alias("amount"),
            F.concat_ws("", F.col("hit_count").cast("string"),
                        F.lit(f" txns of {c['min_amount']}-{c['max_amount']} "
                              f"in {c['window_days']} days")).alias("details"),
        )
    )


def rule_r3_rapid_pass_through(window_txns: DataFrame, cfg) -> DataFrame:
    """Rapid pass-through: sends out >= 90% of an amount received within the time window.

    A TRANSFER is incoming for the receiver and outgoing for the sender, so each
    transaction contributes one event per perspective; a single range window per
    account then sums outgoing amounts in [ts, ts + window] for every incoming
    amount - no self-join, so no quadratic blow-up on hot accounts.
    """
    c = cfg["r3"]
    ts = F.unix_timestamp("txn_ts")
    incoming = window_txns.where(F.col("type").isin(["TRANSFER", "CASH_IN"])).select(
        F.col("nameDest").alias("account"), ts.alias("ts"),
        F.col("amount").alias("in_amount"), F.lit(None).cast("double").alias("out_amount"),
        F.lit(None).cast("string").alias("out_id"), F.col("txn_id").alias("in_id"))
    outgoing = window_txns.where(~F.col("type").isin(["TRANSFER", "CASH_IN"])).select(
        F.col("nameOrig").alias("account"), ts.alias("ts"),
        F.lit(None).cast("double").alias("in_amount"),
        F.col("amount").alias("out_amount"), F.col("txn_id").alias("out_id"),
        F.lit(None).cast("string").alias("in_id"))
    events = incoming.unionByName(outgoing)

    w = Window.partitionBy("account").orderBy("ts").rangeBetween(0, c["window_hours"] * SECONDS["hour"])
    flagged = (
        events.withColumn("out_sum", F.sum("out_amount").over(w))
        .where(F.col("in_amount").isNotNull()
               & (F.col("out_sum") >= c["out_pct"] * F.col("in_amount")))
    )
    return flagged.select(
        F.col("account").alias("account_id"),
        F.lit("R3").alias("rule_code"),
        F.array("in_id").alias("txn_ids"),
        F.col("in_amount").alias("amount"),
        F.concat_ws("", F.lit("moved "),
                    F.round(F.col("out_sum") / F.col("in_amount") * 100, 1).cast("string"),
                    F.lit(f"% out within {c['window_hours']}h of receipt")).alias("details"),
    ).dropDuplicates(["account_id", "txn_ids"])


def rule_r4_dormant(txns_history: DataFrame, customers: DataFrame, cfg, run_date: str) -> DataFrame:
    """Dormant reactivation: the account's first-ever transaction happens today, is large,
    and the account sat idle for 180+ days before it."""
    c = cfg["r4"]
    first = (
        txns_history.withColumn("rn", F.row_number().over(
            Window.partitionBy("nameOrig").orderBy("txn_ts", "txn_id")))
        .where(F.col("rn") == 1)
        .join(customers, txns_history["nameOrig"] == customers["account_id"], "inner")
        .where((F.col("amount") >= c["min_amount"])
               & (F.to_date(F.col("txn_ts")) == run_date)
               & (F.datediff(F.col("txn_ts"), F.to_date("last_activity_before_window"))
                  >= c["dormant_days"]))
    )
    return first.select(
        F.col("nameOrig").alias("account_id"),
        F.lit("R4").alias("rule_code"),
        F.array("txn_id").alias("txn_ids"),
        F.col("amount"),
        F.concat_ws("", F.lit(f"dormant {c['dormant_days']}+ days, first txn "),
                    F.col("amount").cast("string")).alias("details"),
    )


def rule_r5_watchlist(day_txns: DataFrame, watchlist: DataFrame) -> DataFrame:
    """Watchlist hit: sender or receiver appears on the watchlist (broadcast join)."""
    wl = F.broadcast(watchlist.select("account_id", "reason"))
    as_sender = day_txns.join(wl, day_txns["nameOrig"] == wl["account_id"]) \
        .select(F.col("nameOrig").alias("account_id"), "txn_id", "amount",
                F.concat_ws(" ", F.lit("sender on watchlist:"), "reason").alias("details"))
    as_receiver = day_txns.join(wl, day_txns["nameDest"] == wl["account_id"]) \
        .select(F.col("nameDest").alias("account_id"), "txn_id", "amount",
                F.concat_ws(" ", F.lit("receiver on watchlist:"), "reason").alias("details"))
    return (as_sender.unionByName(as_receiver)
            .groupBy("account_id")
            .agg(F.lit("R5").alias("rule_code"),
                 F.collect_list("txn_id").alias("txn_ids"),
                 F.max("amount").alias("amount"),
                 F.max("details").alias("details")))


def rule_r6_kyc_gap(day_txns: DataFrame, kyc: DataFrame, customers: DataFrame,
                    run_date: str, cfg) -> DataFrame:
    """KYC gap: transaction on an account with Missing KYC or expired on/before the txn date."""
    accounts = F.broadcast(
        customers.join(kyc, "customer_id")
        .where((F.col("pan_status") == "Missing")
               | (F.col("kyc_expiry") < run_date)
               | (F.col("kyc_expiry").isNull()))
        .select("account_id", "pan_status", "kyc_expiry"))
    as_sender = day_txns.join(accounts, day_txns["nameOrig"] == accounts["account_id"]) \
        .select(F.col("nameOrig").alias("account_id"), "txn_id", "amount",
                F.concat_ws(" ", F.lit("sender kyc"), "pan_status",
                            F.coalesce("kyc_expiry", F.lit("none"))).alias("details"))
    as_receiver = day_txns.join(accounts, day_txns["nameDest"] == accounts["account_id"]) \
        .where(~F.col("nameDest").startswith("M")) \
        .select(F.col("nameDest").alias("account_id"), "txn_id", "amount",
                F.concat_ws(" ", F.lit("receiver kyc"), "pan_status",
                            F.coalesce("kyc_expiry", F.lit("none"))).alias("details"))
    return (as_sender.unionByName(as_receiver)
            .groupBy("account_id")
            .agg(F.lit("R6").alias("rule_code"),
                 F.collect_list("txn_id").alias("txn_ids"),
                 F.max("amount").alias("amount"),
                 F.max("details").alias("details")))


def rule_r7_velocity(window_txns: DataFrame, cfg) -> DataFrame:
    """High velocity: more than N outgoing transactions from one account in one hour."""
    c = cfg["r7"]
    out = _unix(window_txns.where(F.col("type").isin(["TRANSFER", "CASH_OUT", "PAYMENT", "DEBIT"])))
    w = (Window.partitionBy("nameOrig")
         .orderBy("ts")
         .rangeBetween(-(SECONDS["hour"] - 1), 0))
    return (
        out.withColumn("cnt", F.count("txn_id").over(w))
        .withColumn("ids", F.collect_list("txn_id").over(w))
        .where(F.col("cnt") > c["max_txn_per_hour"])
        .withColumn("hr", F.date_trunc("hour", "txn_ts"))
        .withColumn("rk", F.row_number().over(
            Window.partitionBy("nameOrig", "hr").orderBy(F.col("cnt").desc())))
        .where(F.col("rk") == 1)
        .select(
            F.col("nameOrig").alias("account_id"),
            F.lit("R7").alias("rule_code"),
            F.col("ids").alias("txn_ids"),
            F.lit(None).cast("double").alias("amount"),
            F.concat_ws("", F.date_format("hr", "HH:mm"), F.lit(": "),
                        F.col("cnt").cast("string"),
                        F.lit(f" outgoing txns in 1 hour (limit {c['max_txn_per_hour']})"))
            .alias("details"),
        )
    )


def rule_r8_balance_drain(window_txns: DataFrame, cfg) -> DataFrame:
    """Balance drain: transfer emptying >=95% of sender balance, cash-out by the
    receiver within the follow-up window.

    Same perspective-union + range-window trick as R3: for every cash-out, look at
    the drain transfers landing on the same account in the preceding window_hours.
    """
    c = cfg["r8"]
    ts = F.unix_timestamp("txn_ts")
    drains = window_txns.where(
        (F.col("type") == "TRANSFER")
        & (F.col("oldbalanceOrg") > 0)
        & (F.col("amount") >= c["drain_pct"] * F.col("oldbalanceOrg"))).select(
        F.col("nameDest").alias("account"), ts.alias("ts"),
        F.col("amount").alias("drain_amount"), F.col("txn_id").alias("drain_id"),
        F.lit(None).cast("string").alias("cash_id"))
    cashouts = window_txns.where(F.col("type") == "CASH_OUT").select(
        F.col("nameOrig").alias("account"), ts.alias("ts"),
        F.lit(None).cast("double").alias("drain_amount"),
        F.lit(None).cast("string").alias("drain_id"), F.col("txn_id").alias("cash_id"))
    events = drains.unionByName(cashouts)

    w = Window.partitionBy("account").orderBy("ts").rangeBetween(-c["window_hours"] * SECONDS["hour"], 0)
    flagged = (
        events.withColumn("drained", F.max("drain_amount").over(w))
        .where(F.col("cash_id").isNotNull() & F.col("drained").isNotNull())
    )
    return flagged.select(
        F.col("account").alias("account_id"),
        F.lit("R8").alias("rule_code"),
        F.array("drain_id", "cash_id").alias("txn_ids"),
        F.col("drained").alias("amount"),
        F.concat_ws("", F.lit(f"drained >= {int(c['drain_pct']*100)}% balance, "
                              f"cashed out within {c['window_hours']}h")).alias("details"),
    ).dropDuplicates(["account_id", "txn_ids"])


def run(spark, date: str, cfg: dict, log) -> None:
    window = days_back(date, cfg["r2"]["window_days"])
    clean = spark.table("clean.transactions").where(F.col("txn_date").isin(window))
    day_txns = clean.where(F.col("txn_date") == date)
    history = spark.table("clean.transactions").where(F.col("txn_date") <= date)

    customers = spark.table("raw.customers")
    kyc = spark.table("raw.kyc")
    watchlist = spark.table("raw.watchlist").where(F.col("added_on") <= date)

    alerts = [
        rule_r1_large_cash(day_txns, cfg),
        rule_r2_structuring(clean, cfg),
        rule_r3_rapid_pass_through(clean, cfg),
        rule_r4_dormant(history, customers, cfg, date),
        rule_r5_watchlist(day_txns, watchlist),
        rule_r6_kyc_gap(day_txns, kyc, customers, date, cfg),
        rule_r7_velocity(clean, cfg),
        rule_r8_balance_drain(clean, cfg),
    ]

    from functools import reduce
    hits = reduce(DataFrame.unionByName, alerts).cache()
    hits = hits.withColumn("txn_date", F.lit(date)).withColumn(
        "alert_id",
        F.sha2(F.concat_ws("|", "rule_code", "account_id", "txn_date", "details"), 256),
    )
    counts = {r["rule_code"]: r["count"] for r in
              hits.groupBy("rule_code").count().collect()}
    log.info("alert counts: %s (total %d)", counts, hits.count())

    overwrite_partition(spark, hits, "alerts.rule_hits")
    log.info("done: %s", date)


def main():
    a = parse_args(__doc__)
    log = get_logger("compliance_rules", a.date)
    spark = get_spark("compliance_rules")
    init_tables(spark)
    run(spark, a.date, a.cfg, log)


if __name__ == "__main__":
    main()
