"""
Benchmark Dask: agregación por grilla sobre los datos limpios (Parquet).

Misma operación que spark/jobs/benchmark_grid.py:
    celda = (floor(lat / g), floor(lon / g))  ->  conteo por celda

Imprime una línea "BENCH {json}" por repetición con tiempo y memoria pico.
La memoria es el RSS máximo de cada proceso worker (ru_maxrss) desde que se
reinició el clúster, sumado sobre todos los workers.
"""
import argparse
import json
import os
import resource
import time

import dask.dataframe as dd
from dask.distributed import Client


def peak_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: KB -> MB


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scheduler", default=os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786"))
    ap.add_argument("--workers", type=int, required=True, help="workers esperados")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--grid", type=float, default=0.005)
    ap.add_argument("--path", default=os.environ.get("CLEAN_DIR", "/data/clean"))
    ap.add_argument("--label", default=None)
    a = ap.parse_args()

    client = Client(a.scheduler, timeout="60s")
    client.wait_for_workers(a.workers, timeout=180)
    client.restart()                       # memoria pico limpia para esta configuración
    client.wait_for_workers(a.workers, timeout=180)
    info = client.scheduler_info()["workers"]
    threads = sum(w["nthreads"] for w in info.values())
    label = a.label or f"dask-{len(info)}w"

    for run in range(1, a.runs + 1):
        t0 = time.perf_counter()
        df = dd.read_parquet(a.path, columns=["lat", "lon"])
        df["cell_r"] = (df["lat"] // a.grid).astype("int32")
        df["cell_c"] = (df["lon"] // a.grid).astype("int32")
        counts = df.groupby(["cell_r", "cell_c"]).size().compute()
        seconds = time.perf_counter() - t0

        peaks = client.run(peak_rss_mb)
        print("BENCH " + json.dumps({
            "engine": "dask", "config": label, "workers": len(info), "cores": threads,
            "run": run, "seconds": round(seconds, 3), "cells": int(len(counts)),
            "rows": int(counts.sum()), "peak_mem_mb_total": round(sum(peaks.values()), 1),
            "peak_mem_mb_max_worker": round(max(peaks.values()), 1),
        }), flush=True)
    client.close()


if __name__ == "__main__":
    main()
