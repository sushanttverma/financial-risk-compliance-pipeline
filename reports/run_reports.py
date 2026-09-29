"""Run every report in reports/*.sql and export the result as CSV to docs/sample_outputs/.

Works in both profiles (tables live in the Hive metastore either way).
Usage: spark-submit reports/run_reports.py --date 2025-01-31
"""

from pathlib import Path

from pyspark.sql import functions as F

from jobs.common import get_spark, get_logger, parse_args, ROOT


def main():
    a = parse_args(__doc__)
    log = get_logger("run_reports", a.date)
    spark = get_spark("run_reports")

    out_dir = ROOT / "docs" / "sample_outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    for sql_file in sorted((ROOT / "reports").glob("*.sql")):
        sql = sql_file.read_text().replace("${run_date}", a.date)
        name = sql_file.stem
        try:
            df = spark.sql(sql)
            pdf = df.toPandas()
        except Exception as e:  # noqa: BLE001 - report all failures, keep going
            log.error("report %s failed: %s", name, e)
            continue
        target = out_dir / f"{name}_{a.date}.csv"
        pdf.to_csv(target, index=False)
        log.info("report %s -> %s (%d rows)", name, target.name, len(pdf))
        print(f"\n=== {name} ({len(pdf)} rows) ===")
        print(pdf.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
