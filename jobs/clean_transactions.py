"""Clean one day of raw transactions and enforce data quality.

Reads raw.transactions for --date, applies checks, writes:
  clean.transactions  - good rows (Parquet, partitioned by txn_date)
  clean.quarantine    - one row per failing check, with reason + raw row as JSON
  clean.dq_summary    - one row per day: counts in / clean / quarantined per reason

Re-running the same date overwrites that partition only (dynamic partition overwrite).
"""

from pyspark.sql import SparkSession, functions as F, Window

from jobs.common import get_spark, get_logger, parse_args, init_tables, overwrite_partition

DEBIT_TYPES = ["CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def build_txn_id(df):
    cols = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "source"]
    return df.withColumn("txn_id", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]), 256))


def find_reasons(df, cfg):
    """Add a `reasons` array column: one entry per failing check."""
    tol = cfg["dq"]["balance_tolerance"]
    valid = cfg["dq"]["valid_types"]
    bad_id = F.col("nameOrig").isNull() | (F.trim(F.col("nameOrig")) == "") | \
             F.col("nameDest").isNull() | (F.trim(F.col("nameDest")) == "")
    bad_amount = F.col("amount").isNull() | (F.col("amount") <= 0)
    bad_type = F.col("type").isNull() | (~F.col("type").isin(valid))
    # PaySim rows with oldbalanceOrg == 0 carry no balance info; only compare when
    # the account actually had a balance before the transaction
    bad_bal = F.col("type").isin(DEBIT_TYPES) & \
              (F.col("oldbalanceOrg") > 0) & F.col("newbalanceOrig").isNotNull() & \
              (F.abs(F.col("oldbalanceOrg") - F.col("amount") - F.col("newbalanceOrig")) > tol)
    return df.withColumn(
        "reasons",
        F.array(*[
            F.when(bad_id, F.lit("null_account_id")),
            F.when(bad_amount, F.lit("bad_amount")),
            F.when(bad_type, F.lit("unknown_type")),
            F.when(bad_bal, F.lit("balance_mismatch")),
        ])
    ).withColumn("reasons", F.filter("reasons", lambda x: x.isNotNull()))


def run(spark: SparkSession, date: str, cfg: dict, log) -> None:
    # the day's partition must be registered before reading (new files may have landed)
    raw = spark.table("raw.transactions").where(F.col("txn_date") == date)
    rows_in = raw.count()
    log.info("raw rows in: %d", rows_in)

    df = build_txn_id(raw)
    df = df.withColumn("txn_ts", F.from_unixtime(
        F.unix_timestamp(F.lit("2025-01-01 00:00:00")) + F.col("step") * 3600
    ).cast("timestamp"))

    df = find_reasons(df, cfg)

    # duplicates: first occurrence wins, later ones with no other failure are duplicates
    w = Window.partitionBy("txn_id").orderBy(F.col("txn_ts").asc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w))

    base_clean = F.size("reasons") == 0
    df = df.withColumn(
        "reasons",
        F.when(base_clean & (F.col("rn") > 1), F.array(F.lit("duplicate")))
         .otherwise(F.col("reasons")),
    )

    quarantine = (
        df.where(F.size("reasons") > 0)
        .select("txn_date", "txn_id", F.explode("reasons").alias("reason"),
                F.to_json(F.struct(*[c for c in df.columns if c not in
                                     ("reasons", "rn", "txn_date")])).alias("row_json"))
    )

    clean = df.where(F.size("reasons") == 0).drop("reasons", "rn")

    n_clean = clean.count()
    n_quar_rows = df.where(F.size("reasons") > 0).count()
    reason_counts = quarantine.groupBy("reason").count().collect()
    rc = {r["reason"]: r["count"] for r in reason_counts}
    log.info("clean: %d, quarantined rows: %d, reasons: %s", n_clean, n_quar_rows, rc)

    summary = spark.createDataFrame(
        [(rows_in, n_clean, n_quar_rows,
          rc.get("null_account_id", 0), rc.get("bad_amount", 0), rc.get("unknown_type", 0),
          rc.get("balance_mismatch", 0), rc.get("duplicate", 0))],
        "rows_in long, rows_clean long, rows_quarantined long, cnt_null_id long, "
        "cnt_bad_amount long, cnt_bad_type long, cnt_balance_mismatch long, cnt_duplicate long",
    ).withColumn("txn_date", F.lit(date))

    overwrite_partition(spark, clean, "clean.transactions")
    overwrite_partition(spark, quarantine, "clean.quarantine")
    overwrite_partition(spark, summary, "clean.dq_summary")
    log.info("done: %s", date)


def main():
    a = parse_args(__doc__)
    log = get_logger("clean_transactions", a.date)
    spark: SparkSession = get_spark("clean_transactions")
    init_tables(spark)
    spark.sql(f"ALTER TABLE raw.transactions ADD IF NOT EXISTS PARTITION (txn_date='{a.date}')")
    run(spark, a.date, a.cfg, log)


if __name__ == "__main__":
    main()
