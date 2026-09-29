-- Rule precision: of all alerts a rule produced, how many touched a transaction
-- PaySim labeled as fraud (isFraud = 1)?
WITH hit_txns AS (
  SELECT f.alert_id, f.rule_key, x.txn_id
  FROM wh.fact_alerts f
  LATERAL VIEW explode(f.txn_ids) x AS txn_id
)
SELECT
  r.rule_code,
  r.rule_name,
  COUNT(DISTINCT h.alert_id)                                   AS alerts,
  COUNT(DISTINCT CASE WHEN t.is_fraud = 1 THEN h.alert_id END) AS fraud_hits,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN t.is_fraud = 1 THEN h.alert_id END)
        / NULLIF(COUNT(DISTINCT h.alert_id), 0), 1)            AS precision_pct
FROM hit_txns h
JOIN wh.dim_rule r ON h.rule_key = r.rule_key
LEFT JOIN wh.fact_transactions t ON h.txn_id = t.txn_id
GROUP BY r.rule_code, r.rule_name
ORDER BY precision_pct DESC
