#!/usr/bin/env bash
# Run the full pipeline for one day. Works in two profiles:
#
#   PIPELINE_PROFILE=local  (default) - PySpark local[*], Derby metastore, landing
#                                       folder read directly from disk
#   PIPELINE_PROFILE=hadoop           - raw files must already be on HDFS
#                                       (ingest/load_to_hdfs.sh), metastore is remote
#
# usage: ./run_pipeline.sh 2025-01-05
set -euo pipefail
DATE=${1:?usage: run_pipeline.sh YYYY-MM-DD}
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT"

SPARK_SUBMIT="$ROOT/.venv/bin/spark-submit"
if [ -x "$SPARK_SUBMIT" ]; then
  export SPARK_HOME="${SPARK_HOME:-$ROOT/.venv/lib/python3.12/site-packages/pyspark}"
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$ROOT/.venv/bin/python}"
  export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$ROOT/.venv/bin/python}"
else
  SPARK_SUBMIT="spark-submit"
fi
# spark-submit starts the driver JVM before it reads SparkConf, so heap must be a CLI flag
DRIVER_MEM="${SPARK_DRIVER_MEM:-10g}"
export PYTHONPATH="$ROOT"

# brew openjdk fallback (no sudo needed for the JDK)
if ! command -v java >/dev/null && [ -x /home/linuxbrew/.linuxbrew/opt/openjdk@17/bin/java ]; then
  export JAVA_HOME=/home/linuxbrew/.linuxbrew/opt/openjdk@17
  export PATH="$JAVA_HOME/bin:$PATH"
fi

if [ "${PIPELINE_PROFILE:-local}" = "hadoop" ]; then
  ./ingest/load_to_hdfs.sh "$DATE"
fi

echo "=== pipeline for $DATE (clean -> rules -> risk -> warehouse, one session) ==="
"$SPARK_SUBMIT" --driver-memory "$DRIVER_MEM" jobs/run_day.py --date "$DATE"

echo "pipeline done for $DATE"
