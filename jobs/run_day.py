"""Run the whole day (init -> clean -> rules -> risk -> warehouse) in ONE Spark session.

One JVM per day keeps the embedded Derby metastore (local profile) free of lock
contention between sequential spark-submits, and cuts startup overhead ~4x.
The individual job scripts still work standalone (they create their own session).
"""

from jobs.common import get_spark, get_logger, parse_args, init_tables
from jobs import clean_transactions, compliance_rules, risk_scoring, build_warehouse
from pyspark.sql import functions as F


def main():
    a = parse_args(__doc__)
    log = get_logger("run_day", a.date)
    spark = get_spark("financial-risk-compliance-pipeline")

    init_tables(spark)

    # ---- clean + DQ ----
    spark.sql(f"ALTER TABLE raw.transactions ADD IF NOT EXISTS PARTITION (txn_date='{a.date}')")
    clean_transactions.run(spark, a.date, a.cfg, log)

    # ---- compliance rules ----
    compliance_rules.run(spark, a.date, a.cfg, log)

    # ---- risk scoring ----
    risk_scoring.run(spark, a.date, a.cfg, log)

    # ---- warehouse ----
    build_warehouse.run(spark, a.date, a.cfg, log)

    log.info("pipeline done for %s", a.date)
    spark.stop()


if __name__ == "__main__":
    main()
