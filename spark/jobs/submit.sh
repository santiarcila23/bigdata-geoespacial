#!/usr/bin/env bash
# Envía un trabajo PySpark al clúster standalone (modo client).
# El driver corre en este contenedor; los executors (workers) se conectan a él
# por su IP dentro de la red de Docker.
set -euo pipefail
JOB="$1"; shift
DRIVER_IP="$(hostname -i | awk '{print $1}')"

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --conf spark.driver.host="${DRIVER_IP}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-900m}" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-16}" \
  --conf spark.executor.processTreeMetrics.enabled=true \
  --conf spark.executor.metrics.pollingInterval=500ms \
  --conf spark.executor.heartbeatInterval=2s \
  --conf spark.jars.ivy=/tmp/ivy \
  "$JOB" "$@"
