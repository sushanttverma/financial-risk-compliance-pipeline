"""Shared helpers: Spark session, paths, config, logging, date utilities.

Profiles (PIPELINE_PROFILE env var, default "local"):
  local  - PySpark local[*] with an embedded Derby Hive metastore.
           Raw files are read straight from the landing folder, warehouse on local disk.
  hadoop - Jobs talk to HDFS (HDFS_URL) and a remote Hive metastore (HIVE_METASTORE_URIS).
           Same code, same tables - only locations change.
"""

import argparse
import json
import logging
import os

import sys
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]

PROFILE = os.environ.get("PIPELINE_PROFILE", "local")
HDFS_URL = os.environ.get("HDFS_URL", "hdfs://localhost:8020")
HIVE_METASTORE_URIS = os.environ.get("HIVE_METASTORE_URIS", "thrift://localhost:9083")

if PROFILE == "hadoop":
    RAW_TXN_LOC = f"{HDFS_URL}/data/raw/transactions"
    REF_LOC = f"{HDFS_URL}/data/raw/reference"
    WAREHOUSE_DIR = f"{HDFS_URL}/user/hive/warehouse"
else:
    RAW_TXN_LOC = f"file://{ROOT}/landing/transactions"
    REF_LOC = f"file://{ROOT}/landing/reference"
    WAREHOUSE_DIR = f"file://{ROOT}/warehouse"

EPOCH = datetime(2025, 1, 1)  # step 1 of PaySim maps to 2025-01-01 00:00


def fs(path: str) -> str:
    return f"{HDFS_URL}{path}" if PROFILE == "hadoop" else f"file://{path}"


def get_spark(app_name: str) -> SparkSession:
    # make sure executors/workers use the same interpreter as the driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if PROFILE == "local":
        # the embedded Derby metastore lives at ./metastore_db, so every entry
        # point (spark-submit, python, pytest) must run from the project root
        os.chdir(ROOT)
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if PROFILE == "hadoop":
        b = b.config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URIS)
    else:
        b = (
            b.master(os.environ.get("SPARK_MASTER", "local[*]"))
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "10g"))
            .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTS", "64"))
            .config("spark.ui.enabled", "false")
        )
    return b.enableHiveSupport().getOrCreate()


def parse_args(description: str):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--date", required=True, help="run date, YYYY-MM-DD")
    p.add_argument("--config", default=str(ROOT / "config" / "rules.json"))
    a = p.parse_args()
    try:
        datetime.strptime(a.date, "%Y-%m-%d")
    except ValueError:
        sys.exit(f"bad --date {a.date!r}: expected YYYY-MM-DD")
    with open(a.config) as f:
        a.cfg = json.load(f)
    return a


def get_logger(name: str, date: str) -> logging.Logger:
    logs = ROOT / "logs"
    logs.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.FileHandler(logs / f"{name}_{date}.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(name)


def days_back(date_str: str, n: int) -> list[str]:
    """The n calendar days ending on date_str (inclusive)."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return [(d - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n - 1, -1, -1)]


def shift_date(date_str: str, days: int) -> str:
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")


def run_sql_file(spark: SparkSession, sql_path: Path, hivevars: dict[str, str]) -> None:
    """Execute a DDL file, substituting ${hivevar:name} placeholders."""
    text = Path(sql_path).read_text()
    for k, v in hivevars.items():
        text = text.replace(f"${{hivevar:{k}}}", v)
    # drop comment lines first (they may contain ';'), then split statements
    text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    for stmt in text.split(";"):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)


def overwrite_partition(spark: SparkSession, df, table: str) -> None:
    """Overwrite only the partitions present in `df` (idempotent daily runs).

    saveAsTable(mode=overwrite) would REPLACE the whole table, so we use
    insertInto: with spark.sql.sources.partitionOverwriteMode=dynamic it
    truncates just the partitions being written. insertInto matches columns by
    POSITION, so we reorder the DataFrame to the table's DDL column order
    (partition columns last).
    """
    names = spark.table(table).limit(0).columns  # DDL order, partition col last
    (df.select(*[F.col(c) for c in names]).write
     .mode("overwrite").insertInto(table))


HIVE_DDL_VARS = {
    "raw_txn_loc": RAW_TXN_LOC,
    "ref_loc": REF_LOC,
}


def init_tables(spark: SparkSession) -> None:
    """Idempotent DDL - creates databases and tables if missing (both profiles)."""
    for f in sorted((ROOT / "hive").glob("*.sql")):
        run_sql_file(spark, f, HIVE_DDL_VARS)
