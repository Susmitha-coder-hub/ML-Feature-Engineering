#!/bin/bash
set -e

JOBMANAGER_URL="${FLINK_JOBMANAGER_URL:-http://flink-jobmanager:8081}"

echo "Waiting for Flink JobManager at $JOBMANAGER_URL ..."
until curl -sf "$JOBMANAGER_URL/overview" > /dev/null; do
    sleep 3
done
echo "Flink JobManager is ready."

echo "Submitting PyFlink job..."
flink run \
    --jobmanager flink-jobmanager:8081 \
    --python /app/job.py \
    --pyFiles /app/job.py

echo "Job submitted successfully."
# Keep container alive so Docker considers it healthy
tail -f /dev/null
