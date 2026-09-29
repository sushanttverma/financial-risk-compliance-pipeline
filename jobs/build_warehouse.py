"""Build the star schema (dimensions + facts) for SQL reporting.

Rebuilt every run:
  wh.dim_date, wh.dim_txn_type, wh.dim_rule   - small dimensions
  wh.dim_customer                             - SCD Type 2 over the risk-band history
                                                (every customer gets a baseline 'Low'
                                                era from the window start)
  wh.fact_transactions, wh.fact_alerts        - facts for the run date, dynamic
                                                partition overwrite
"""

from pyspark.sql import Window, functions as F

from jobs.common import get_spark, get_logger, parse_args, init_tables, overwrite_partition

WINDOW_START = "2025-01-01"
END_OF_TIME = "9999-12-31"


def dim_date(spark):
    return (
        spark.range(0, 365).select(
            F.expr("date_add(to_date('2025-01-01'), cast(id as int))").alias("date"))
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
        .select("date_key", "date", "day_of_week", "week", "month", "month_name", "is_weekend")
    )


def dim_rule(spark, cfg):
    rows = [(i + 1, cfg[k]["code"], cfg[k]["name"], cfg[k]["weight"],
             cfg[k]["regulation_note"])
            for i, k in enumerate(["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"])]
    return spark.createDataFrame(
        rows, "rule_key int, rule_code string, rule_name string, weight int, regulation_note string")


def dim_txn_type(spark):
    types = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    return spark.createDataFrame(
        [(i + 1, t) for i, t in enumerate(types)], "txn_type_key int, type string")


def dim_customer(scd_history, customers):
    """SCD Type 2: one row per risk-band era.

    scd_history: customer_id, txn_date, band  (one row per customer-day with alerts)
    """
    base = customers.select("customer_id", "name", "city", "state", "segment", "account_type")
    hist = scd_history.join(base, "customer_id").select(
        "customer_id", "name", "city", "state", "segment", "account_type",
        F.to_date("txn_date").alias("d"), "band")

    # baseline 'Low' era from the window start for customers whose first alert came later
    first_alert = hist.groupBy("customer_id").agg(F.min("d").alias("first_d"))
    baseline = (
        first_alert.where(F.col("first_d") > F.lit(WINDOW_START))
        .join(base, "customer_id")
        .select("customer_id", "name", "city", "state", "segment", "account_type",
                F.lit(WINDOW_START).cast("date").alias("d"), F.lit("Low").alias("band"))
    )
    all_eras = hist.unionByName(baseline)

    w = Window.partitionBy("customer_id").orderBy("d")
    eras = (
        all_eras.withColumn("prev_band", F.lag("band").over(w))
        .withColumn("era_start", F.when(F.col("prev_band").isNull() |
                                        (F.col("prev_band") != F.col("band")), F.col("d")))
        # forward-fill the era start: last non-null era_start up to this row
        .withColumn("valid_from", F.last("era_start", ignorenulls=True).over(w))
        .groupBy("customer_id", "name", "city", "state", "segment", "account_type",
                 "band", "valid_from")
        .agg(F.max("d").alias("last_day"))
    )
    w2 = Window.partitionBy("customer_id").orderBy("valid_from")
    dims = (
        eras.withColumn("next_from", F.lead("valid_from").over(w2))
        .withColumn("valid_to", F.when(F.col("next_from").isNull(),
                                       F.lit(END_OF_TIME).cast("date"))
                    .otherwise(F.date_sub("next_from", 1)))
        .withColumn("is_current", F.col("next_from").isNull())
        .withColumn("customer_key",
                    F.sha2(F.concat_ws("|", "customer_id",
                                       F.col("valid_from").cast("string")), 256))
        .select("customer_key", "customer_id", "name", "city", "state", "segment",
                "account_type", "band", "valid_from", "valid_to", "is_current")
    )
    return dims


def run(spark, date: str, cfg: dict, log) -> None:

    dd = dim_date(spark)
    dr = dim_rule(spark, cfg)
    dt = dim_txn_type(spark)

    risk_hist = spark.table("risk.customer_risk_daily").where(F.col("txn_date") <= date)
    customers = spark.table("raw.customers")
    dc = dim_customer(
        risk_hist.select("customer_id", "txn_date", "band"), customers).cache()
    log.info("dim_customer eras: %d (current: %d)", dc.count(),
             dc.where(F.col("is_current")).count())

    txn_map = customers.select("account_id", "customer_id")

    alert_counts = (
        spark.table("alerts.rule_hits").where(F.col("txn_date") == date)
        .select(F.explode("txn_ids").alias("txn_id"))
        .groupBy("txn_id").count()
    )

    clean = spark.table("clean.transactions").where(F.col("txn_date") == date)
    fact_txns = (
        clean
        .join(txn_map.select(F.col("account_id").alias("sender_account"),
                             F.col("customer_id").alias("sender_cust")),
              clean["nameOrig"] == F.col("sender_account"), "left")
        .join(txn_map.select(F.col("account_id").alias("receiver_account"),
                             F.col("customer_id").alias("receiver_cust")),
              clean["nameDest"] == F.col("receiver_account"), "left")
        .join(dc.select(F.col("customer_key").alias("sender_key"),
                        F.col("customer_id").alias("sender_cid"),
                        F.col("valid_from").alias("sender_from"),
                        F.col("valid_to").alias("sender_to")),
              (F.col("sender_cust") == F.col("sender_cid"))
              & (F.to_date("txn_date") >= F.col("sender_from"))
              & (F.to_date("txn_date") <= F.col("sender_to")), "left")
        .join(dc.select(F.col("customer_key").alias("receiver_key"),
                        F.col("customer_id").alias("receiver_cid"),
                        F.col("valid_from").alias("receiver_from"),
                        F.col("valid_to").alias("receiver_to")),
              (F.col("receiver_cust") == F.col("receiver_cid"))
              & (F.to_date("txn_date") >= F.col("receiver_from"))
              & (F.to_date("txn_date") <= F.col("receiver_to")), "left")
        .join(dt, clean["type"] == dt["type"])
        .join(dd.select("date_key", "date"),
              F.to_date("txn_date") == F.col("date"), "left")
        .join(alert_counts, "txn_id", "left")
        .select("txn_id", "date_key",
                F.coalesce("sender_key", F.lit("UNKNOWN")).alias("sender_key"),
                F.coalesce("receiver_key", F.lit("UNKNOWN")).alias("receiver_key"),
                "txn_type_key", "amount", F.col("isFraud").alias("is_fraud"), "txn_date",
                F.coalesce("count", F.lit(0)).alias("alert_count"))
    )

    fact_alerts = (
        spark.table("alerts.rule_hits").where(F.col("txn_date") == date)
        .join(txn_map, "account_id", "left")
        .join(
            dc.select(F.col("customer_key"), F.col("customer_id").alias("cid"),
                      "valid_from", "valid_to"),
            (F.col("customer_id") == F.col("cid"))
            & (F.to_date("txn_date") >= F.col("valid_from"))
            & (F.to_date("txn_date") <= F.col("valid_to")),
            "left")
        .join(dr, "rule_code")
        .join(dd.select("date_key", "date"), F.to_date("txn_date") == F.col("date"), "left")
        .select("alert_id", "date_key",
                F.coalesce("customer_key", F.lit("UNKNOWN")).alias("customer_key"),
                "rule_key", "amount", "txn_ids", "txn_date")
    )

    (dd.write.mode("overwrite").format("parquet").saveAsTable("wh.dim_date"))
    (dr.write.mode("overwrite").format("parquet").saveAsTable("wh.dim_rule"))
    (dt.write.mode("overwrite").format("parquet").saveAsTable("wh.dim_txn_type"))
    (dc.write.mode("overwrite").format("parquet").saveAsTable("wh.dim_customer"))
    overwrite_partition(spark, fact_txns, "wh.fact_transactions")
    overwrite_partition(spark, fact_alerts, "wh.fact_alerts")
    log.info("done: %s", date)


def main():
    a = parse_args(__doc__)
    log = get_logger("build_warehouse", a.date)
    spark = get_spark("build_warehouse")
    init_tables(spark)
    run(spark, a.date, a.cfg, log)


if __name__ == "__main__":
    main()
