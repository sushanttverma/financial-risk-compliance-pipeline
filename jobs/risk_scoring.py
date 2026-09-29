"""Daily customer risk scoring.

daily_score  = sum of weights of distinct rules the customer hit that day
               (+ KYC bonus if the customer's KYC risk category is High)
rolling_score = sum of daily scores over the last 30 calendar days
band         = Low < 30 <= Medium < 70 <= High

Output: risk.customer_risk_daily, partitioned by txn_date (dynamic overwrite).
"""

from pyspark.sql import Window, functions as F

from jobs.common import get_spark, get_logger, parse_args, init_tables, shift_date, overwrite_partition


def run(spark, date: str, cfg: dict, log) -> None:
    weights = {cfg[k]["code"]: cfg[k]["weight"] for k in
               ["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"]}

    customers = spark.table("raw.customers").where(F.col("is_merchant") == False)  # noqa: E712
    kyc = spark.table("raw.kyc")
    alerts = spark.table("alerts.rule_hits").where(F.col("txn_date") <= date)

    wdf = spark.createDataFrame([(c, w) for c, w in weights.items()], "rule_code string, weight int")

    # one row per customer-day-rule
    daily = (
        alerts.where(F.col("txn_date") == date)
        .join(customers.select("account_id", "customer_id"), "account_id", "inner")
        .select("customer_id", "txn_date", "rule_code")
        .dropDuplicates()
        .join(F.broadcast(wdf), "rule_code")
        .groupBy("customer_id", "txn_date")
        .agg(F.sum("weight").alias("rule_score"),
             F.collect_set(F.struct("weight", "rule_code")).alias("rules_hit"))
    )

    # KYC high-risk bonus
    high_kyc = (customers.join(kyc, "customer_id")
                .where(F.col("risk_category") == "High")
                .select("customer_id", F.lit(cfg["kyc_high_bonus"]).alias("kyc_bonus")))
    daily = daily.join(high_kyc, "customer_id", "left").na.fill(0, ["kyc_bonus"])
    # collect_set(struct) sorted by weight desc -> top_rules
    daily = daily.withColumn(
        "top_rules",
        F.transform(F.array_sort("rules_hit"), lambda x: x["rule_code"]),
    ).withColumn(
        "daily_score", F.col("rule_score") + F.col("kyc_bonus"),
    )

    # 30-day rolling: sum the per-day scores (including today) over the window
    hist = (
        alerts.join(customers.select("account_id", "customer_id"), "account_id", "inner")
        .select("customer_id", "txn_date", "rule_code")
        .dropDuplicates()
        .join(F.broadcast(wdf), "rule_code")
        .groupBy("customer_id", "txn_date")
        .agg(F.sum("weight").alias("day_total"))
        .withColumn("d", F.unix_timestamp(F.to_date("txn_date")))
    )
    roll_w = Window.partitionBy("customer_id").orderBy("d").rangeBetween(-29 * 86400, 0)
    def day_ts(ds: str):
        return F.unix_timestamp(F.lit(ds).cast("timestamp"))

    rolling = (
        hist.where((F.col("d") >= day_ts(shift_date(date, -29)))
                   & (F.col("d") <= day_ts(date)))
        .withColumn("rolling", F.sum("day_total").over(roll_w))
        .where(F.col("d") == day_ts(date))
        .select("customer_id", "rolling")
    )

    out = (
        daily.join(rolling, "customer_id", "left")
        .fillna(0, ["rolling"])
        .withColumn("rolling_score", F.col("rolling") + F.col("kyc_bonus"))
        .select("customer_id", "daily_score", "rolling_score", "top_rules", "txn_date")
    )
    med, high = cfg["bands"]["medium_min"], cfg["bands"]["high_min"]
    out = out.withColumn(
        "band",
        F.when(F.col("rolling_score") >= high, "High")
        .when(F.col("rolling_score") >= med, "Medium")
        .otherwise("Low"),
    )

    bands = {r["band"]: r["count"] for r in out.groupBy("band").count().collect()}
    log.info("risk bands for %s: %s", date, bands)

    overwrite_partition(spark, out, "risk.customer_risk_daily")
    log.info("done: %s", date)


def main():
    a = parse_args(__doc__)
    log = get_logger("risk_scoring", a.date)
    spark = get_spark("risk_scoring")
    init_tables(spark)
    run(spark, a.date, a.cfg, log)


if __name__ == "__main__":
    main()
