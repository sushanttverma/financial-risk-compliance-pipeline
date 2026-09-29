-- Clean layer: Parquet tables written by the PySpark cleaning job.
-- These DDLs are the canonical schema; the jobs overwrite partitions
-- dynamically (idempotent re-runs).

CREATE TABLE IF NOT EXISTS clean.transactions (
  txn_id          STRING,
  step            INT,
  type            STRING,
  amount          DOUBLE,
  nameOrig        STRING,
  oldbalanceOrg   DOUBLE,
  newbalanceOrig  DOUBLE,
  nameDest        STRING,
  oldbalanceDest  DOUBLE,
  newbalanceDest  DOUBLE,
  isFraud         INT,
  source          STRING,
  txn_ts          TIMESTAMP
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS clean.quarantine (
  txn_id   STRING,
  reason   STRING,
  row_json STRING
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS clean.dq_summary (
  rows_in             BIGINT,
  rows_clean          BIGINT,
  rows_quarantined    BIGINT,
  cnt_null_id         BIGINT,
  cnt_bad_amount      BIGINT,
  cnt_bad_type        BIGINT,
  cnt_balance_mismatch BIGINT,
  cnt_duplicate       BIGINT
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;
