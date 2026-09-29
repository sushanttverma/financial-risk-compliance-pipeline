-- Today's alert queue: one row per alert, sorted by customer risk score.
SELECT
  r.rule_code,
  r.rule_name,
  c.customer_id,
  c.name            AS customer_name,
  c.state,
  c.segment,
  COALESCE(rs.rolling_score, 0) AS risk_score,
  COALESCE(rs.band, 'Low')      AS risk_band,
  f.amount,
  a.details
FROM wh.fact_alerts f
JOIN wh.dim_rule r        ON f.rule_key = r.rule_key
JOIN alerts.rule_hits a   ON a.alert_id = f.alert_id AND a.txn_date = f.txn_date
LEFT JOIN wh.dim_customer c ON f.customer_key = c.customer_key
LEFT JOIN risk.customer_risk_daily rs
       ON rs.customer_id = c.customer_id AND rs.txn_date = '${run_date}'
WHERE f.txn_date = '${run_date}'
ORDER BY risk_score DESC, f.amount DESC
