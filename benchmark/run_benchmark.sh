#!/usr/bin/env bash
# Benchmark Dask vs Spark: agregación por grilla con dos configuraciones de workers.
# Requisito: haber corrido la ingesta (genera /data/clean en el volumen compartido).
#
#   bash benchmark/run_benchmark.sh
#
# Para comparar en igualdad de núcleos:
#   Spark: 1 worker x 2 cores  y  2 workers x 2 cores   (2 y 4 núcleos)
#   Dask : 2 workers x 1 hilo  y  4 workers x 1 hilo    (2 y 4 núcleos)
set -euo pipefail
cd "$(dirname "$0")/.."

RUNS="${RUNS:-3}"
GRID="${GRID:-0.005}"
OUT="benchmark/results.jsonl"
: > "$OUT"

echo "== Spark =="
docker compose stop dask-worker
for N in 1 2; do
  docker compose up -d --scale spark-worker="$N" spark-master spark-worker
  docker compose run --rm -T --no-deps spark-job bash /opt/jobs/submit.sh /opt/jobs/benchmark_grid.py \
      --runs "$RUNS" --grid "$GRID" --expect-workers "$N" --label "spark-${N}w-$((N*2))c" \
    | tee /dev/stderr | grep '^BENCH ' | sed 's/^BENCH //' >> "$OUT"
done

echo "== Dask =="
docker compose up -d --scale spark-worker=0 spark-worker
for N in 2 4; do
  DASK_WORKER_THREADS=1 docker compose up -d --scale dask-worker="$N" dask-scheduler dask-worker
  docker compose run --rm -T --no-deps ingest python -u benchmark_grid.py \
      --workers "$N" --runs "$RUNS" --grid "$GRID" --label "dask-${N}w-${N}c" \
    | tee /dev/stderr | grep '^BENCH ' | sed 's/^BENCH //' >> "$OUT"
done

echo "== Restaurando configuración por defecto =="
docker compose up -d --scale spark-worker=1 --scale dask-worker=2 spark-worker dask-worker

docker compose run --rm -T --no-deps ingest python summarize_benchmark.py < "$OUT" | tee benchmark/results.md
echo "Resultados en benchmark/results.jsonl y benchmark/results.md"
