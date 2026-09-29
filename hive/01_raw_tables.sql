-- Raw layer: external CSV tables over the landing / HDFS raw zone.
-- Locations are ${hivevar:...} so the same file works for beeline (hadoop)
-- and jobs/init via Spark SQL (local).

CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS clean;
CREATE DATABASE IF NOT EXISTS alerts;
CREATE DATABASE IF NOT EXISTS risk;
CREATE DATABASE IF NOT EXISTS wh;

CREATE EXTERNAL TABLE IF NOT EXISTS raw.transactions (
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
  isFlaggedFraud  INT,
  source          STRING
)
PARTITIONED BY (txn_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hivevar:raw_txn_loc}'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.customers (
  customer_id                 STRING,
  account_id                  STRING,
  name                        STRING,
  city                        STRING,
  state                       STRING,
  segment                     STRING,
  account_type                STRING,
  open_date                   STRING,
  last_activity_before_window STRING,
  is_merchant                 BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hivevar:ref_loc}/customers'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.kyc (
  customer_id   STRING,
  pan_status    STRING,
  kyc_date      STRING,
  kyc_expiry    STRING,
  risk_category STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hivevar:ref_loc}/kyc'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.watchlist (
  account_id STRING,
  reason     STRING,
  added_on   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hivevar:ref_loc}/watchlist'
TBLPROPERTIES ('skip.header.line.count'='1');
