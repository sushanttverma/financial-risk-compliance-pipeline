#!/usr/bin/env bash
# hadoop profile only: copy one day's landing file into HDFS.
# usage: ingest/load_to_hdfs.sh 2025-01-05
set -euo pipefail
DATE=${1:?usage: load_to_hdfs.sh YYYY-MM-DD}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

hdfs dfs -mkdir -p /data/raw/transactions/txn_date=$DATE
hdfs dfs -put -f "$ROOT/landing/transactions/txn_date=$DATE/part.csv" \
      /data/raw/transactions/txn_date=$DATE/

# reference tables go up once; -f keeps this safe to re-run
for t in customers kyc watchlist; do
  hdfs dfs -mkdir -p /data/raw/reference/$t
  hdfs dfs -put -f "$ROOT/landing/reference/$t/part.csv" /data/raw/reference/$t/
done
echo "loaded $DATE to HDFS"
