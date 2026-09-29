-- Star schema: dimension and fact tables for SQL reporting.

CREATE TABLE IF NOT EXISTS wh.dim_date (
  date_key    INT,
  date        DATE,
  day_of_week STRING,
  week        INT,
  month       INT,
  month_name  STRING,
  is_weekend  BOOLEAN
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wh.dim_txn_type (
  txn_type_key INT,
  type         STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wh.dim_rule (
  rule_key        INT,
  rule_code       STRING,
  rule_name       STRING,
  weight          INT,
  regulation_note STRING
)
STORED AS PARQUET;

-- SCD Type 2: one row per risk-band era, history preserved on band change.
CREATE TABLE IF NOT EXISTS wh.dim_customer (
  customer_key STRING,
  customer_id  STRING,
  name         STRING,
  city         STRING,
  state        STRING,
  segment      STRING,
  account_type STRING,
  risk_band    STRING,
  valid_from   DATE,
  valid_to     DATE,
  is_current   BOOLEAN
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wh.fact_transactions (
  txn_id       STRING,
  date_key     INT,
  sender_key   STRING,
  receiver_key STRING,
  txn_type_key INT,
  amount       DOUBLE,
  is_fraud     INT,
  alert_count  INT
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS wh.fact_alerts (
  alert_id     STRING,
  date_key     INT,
  customer_key STRING,
  rule_key     INT,
  amount       DOUBLE,
  txn_ids      ARRAY<STRING>
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;
