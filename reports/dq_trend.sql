-- Data quality trend: rows in / clean / quarantined and reasons per day.
SELECT
  txn_date,
  rows_in,
  rows_clean,
  rows_quarantined,
  cnt_null_id,
  cnt_bad_amount,
  cnt_bad_type,
  cnt_balance_mismatch,
  cnt_duplicate
FROM clean.dq_summary
ORDER BY txn_date
