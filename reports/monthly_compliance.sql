-- Monthly compliance summary: what the bank reports to FIU-IND and reviewers.
SELECT
  r.rule_code,
  r.rule_name,
  COUNT(f.alert_id)     AS alerts,
  SUM(f.amount)         AS total_amount
FROM wh.fact_alerts f
JOIN wh.dim_rule r ON f.rule_key = r.rule_key
WHERE r.rule_code IN ('R1', 'R5', 'R6')   -- large cash, watchlist, KYC gaps
GROUP BY r.rule_code, r.rule_name
ORDER BY r.rule_code
