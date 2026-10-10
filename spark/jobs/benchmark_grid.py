"""
Benchmark Spark: agregación por grilla sobre los datos limpios (Parquet).

Misma operación que ingest/benchmark_grid.py (Dask):
    celda = (floor(lat / g), floor(lon / g))  ->  conteo por celda

Imprime una línea "BENCH {json}" por repetición. La memoria se toma de la API
REST del driver (métricas pico de los executors): RSS del proceso del executor
(ProcessTreeJVMRSSMemory) cuando está disponible, si no el heap de la JVM.
"""
import argparse
import json
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--runs", type=int, default=3)
ap.add_argument("--grid", type=float, default=0.005)
ap.add_argument("--path", default="/data/clean")
ap.add_argument("--expect-workers", type=int, default=1)
ap.add_argument("--label", default=None)
a = ap.parse_args()


def get_json(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read())


# esperar a que el master tenga registrados los workers de esta configuración
for _ in range(60):
    try:
        alive = get_json("http://spark-master:8080/json/").get("aliveworkers", 0)
        if alive >= a.expect_workers:
            break
    except Exception:
        pass
    time.sleep(2)

spark = SparkSession.builder.appName(a.label or "benchmark-grid").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

# esperar a que arranquen los executors
for _ in range(60):
    if len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1 >= a.expect_workers:
        break
    time.sleep(1)

execs = len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1
cores = int(sc.defaultParallelism)
label = a.label or f"spark-{execs}w"
ui = sc.uiWebUrl.replace(sc.uiWebUrl.split("//")[1].split(":")[0], "localhost")


def executor_peak_mb():
    time.sleep(3)  # dar tiempo a un heartbeat con las métricas pico
    try:
        data = get_json(f"{ui}/api/v1/applications/{sc.applicationId}/executors")
    except Exception:
        return None
    total = 0
    for e in data:
        if e["id"] == "driver":
            continue
        m = e.get("peakMemoryMetrics") or {}
        rss = m.get("ProcessTreeJVMRSSMemory", 0)
        total += rss if rss > 0 else m.get("JVMHeapMemory", 0) + m.get("JVMOffHeapMemory", 0)
    return round(total / 1024 / 1024, 1)


for run in range(1, a.runs + 1):
    t0 = time.perf_counter()
    df = spark.read.parquet(a.path).select("lat", "lon")
    counts = (df.withColumn("cell_r", F.floor(F.col("lat") / a.grid))
                .withColumn("cell_c", F.floor(F.col("lon") / a.grid))
                .groupBy("cell_r", "cell_c").count()
                .collect())
    seconds = time.perf_counter() - t0
    print("BENCH " + json.dumps({
        "engine": "spark", "config": label, "workers": execs, "cores": cores, "run": run,
        "seconds": round(seconds, 3), "cells": len(counts),
        "rows": int(sum(r["count"] for r in counts)),
        "peak_mem_mb_total": executor_peak_mb(),
    }), flush=True)

spark.stop()
