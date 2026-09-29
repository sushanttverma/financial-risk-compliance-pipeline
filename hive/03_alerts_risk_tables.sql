-- Alert queue and customer risk scoring.

CREATE TABLE IF NOT EXISTS alerts.rule_hits (
  alert_id   STRING,
  account_id STRING,
  rule_code  STRING,
  txn_ids    ARRAY<STRING>,
  amount     DOUBLE,
  details    STRING
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS risk.customer_risk_daily (
  customer_id   STRING,
  daily_score   INT,
  rolling_score INT,
  band          STRING,
  top_rules     ARRAY<STRING>
)
PARTITIONED BY (txn_date STRING)
STORED AS PARQUET;
