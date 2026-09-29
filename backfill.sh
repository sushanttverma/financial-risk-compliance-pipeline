#!/usr/bin/env bash
# Re-run the whole window: ./backfill.sh 2025-01-01 2025-01-31
set -euo pipefail
START=${1:?usage: backfill.sh YYYY-MM-DD [END]}
END=${2:-$START}
ROOT="$(cd "$(dirname "$0")" && pwd)"

for d in $(seq "$START" "$END"); do
  echo ">>> backfill day $d"
  "$ROOT/run_pipeline.sh" "$d" || { echo "FAILED at $d"; exit 1; }
done

echo ">>> running reports"
PYTHONPATH="$ROOT" "$ROOT/.venv/bin/spark-submit" reports/run_reports.py --date "$END"
