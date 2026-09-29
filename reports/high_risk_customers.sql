-- High-risk customers (current band = High), grouped by state, segment, account type.
SELECT
  c.state,
  c.segment,
  c.account_type,
  COUNT(DISTINCT c.customer_id) AS customers
FROM wh.dim_customer c
WHERE c.is_current = true
  AND c.band = 'High'
GROUP BY c.state, c.segment, c.account_type
ORDER BY customers DESC
