-- Rule trends: alerts per rule per day over the window.
SELECT
  d.date,
  r.rule_code,
  r.rule_name,
  COUNT(f.alert_id) AS alerts
FROM wh.fact_alerts f
JOIN wh.dim_rule r ON f.rule_key = r.rule_key
JOIN wh.dim_date d ON f.date_key = d.date_key
WHERE d.date >= date_sub(to_date('${run_date}'), 30)
GROUP BY d.date, r.rule_code, r.rule_name
ORDER BY d.date, r.rule_code
