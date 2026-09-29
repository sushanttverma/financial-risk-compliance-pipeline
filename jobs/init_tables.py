"""Create databases and tables if missing (idempotent). Run once before the pipeline."""

from jobs.common import get_spark, init_tables

if __name__ == "__main__":
    spark = get_spark("init_tables")
    init_tables(spark)
    print("tables ready")
