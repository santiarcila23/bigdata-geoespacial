"""
Procesamiento distribuido con Spark sobre MongoDB (MongoDB Spark Connector 10.x).

Lee la colección `crimes` y genera colecciones nuevas:
  agg_grid           conteo por celda de una grilla regular (GRID_SIZE grados),
                     con el polígono GeoJSON de la celda, su centro, densidad
                     por km², tasa de arresto y categoría más frecuente
  agg_hotspots       celdas de alta concentración (conteo >= percentil 99 y
                     z-score), ordenadas por ranking
  agg_temporal       comportamiento por hora, día de la semana, mes y año
  agg_category_hour  categoría x hora para las 10 categorías más frecuentes
  agg_types          conteo y tasa de arresto por categoría
  agg_meta           metadatos de la corrida (parámetros, tiempos, totales)

Si no hay ingestas nuevas desde la última corrida, el trabajo no se repite
(FORCE_SPARK=true para forzarlo).
"""
import math
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
DB = os.environ.get("MONGO_DB", "geo")
SOURCE = os.environ.get("MONGO_COLLECTION", "crimes")
G = float(os.environ.get("GRID_SIZE", "0.005"))
FORCE = os.environ.get("FORCE_SPARK", "false").lower() in ("1", "true", "yes")
KM_PER_DEG = 111.32

spark = (SparkSession.builder.appName("geo-aggregations")
         .config("spark.mongodb.read.connection.uri", URI)
         .config("spark.mongodb.write.connection.uri", URI)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def log(msg):
    print(f"[spark {datetime.now():%H:%M:%S}] {msg}", flush=True)


def read(coll):
    return spark.read.format("mongodb").option("database", DB).option("collection", coll).load()


def write(df, coll):
    (df.write.format("mongodb").mode("overwrite")
       .option("database", DB).option("collection", coll).save())
    log(f"  -> {coll}")


def latest_ingest():
    try:
        runs = read("ingest_runs")
        if "finished_at" not in runs.columns:
            return None
        return runs.agg(F.max("finished_at")).first()[0]
    except Exception:
        return None


def already_done(source_ingest):
    try:
        meta = read("agg_meta")
        if "source_ingest" not in meta.columns:
            return False
        last = meta.select("source_ingest").first()
        return last is not None and last[0] == source_ingest
    except Exception:
        return False


t0 = time.time()
source_ingest = latest_ingest()
if not FORCE and source_ingest and already_done(source_ingest):
    log("Las agregaciones ya corresponden a la última ingesta; nada que hacer (FORCE_SPARK=true para forzar).")
    spark.stop()
    sys.exit(0)

# ----------------------------------------------------------------- lectura desde MongoDB
raw = read(SOURCE)
if "location" not in raw.columns:
    sys.exit(f"La colección {SOURCE} está vacía o no tiene el campo location")

has_arrest = "arrest" in raw.columns
pts = raw.select(
    F.col("location.coordinates").getItem(1).cast("double").alias("lat"),
    F.col("location.coordinates").getItem(0).cast("double").alias("lon"),
    F.col("date"),
    F.col("category"),
    (F.col("arrest").cast("int") if has_arrest else F.lit(None).cast("int")).alias("arrest"),
).where(F.col("lat").isNotNull() & F.col("lon").isNotNull()).cache()

total = pts.count()
log(f"Puntos leídos desde MongoDB: {total:,}  (particiones: {pts.rdd.getNumPartitions()})")

# ----------------------------------------------------------------- grilla espacial
cells = (pts.withColumn("cell_r", F.floor(F.col("lat") / G).cast("long"))
            .withColumn("cell_c", F.floor(F.col("lon") / G).cast("long")))

grid = cells.groupBy("cell_r", "cell_c").agg(
    F.count("*").alias("count"),
    F.sum("arrest").alias("arrests"),
    F.min("date").alias("first_date"),
    F.max("date").alias("last_date"),
)

# categoría más frecuente por celda
cat_counts = cells.groupBy("cell_r", "cell_c", "category").count()
w = Window.partitionBy("cell_r", "cell_c").orderBy(F.desc("count"), F.asc("category"))
top_cat = (cat_counts.withColumn("rk", F.row_number().over(w)).where("rk = 1")
           .select("cell_r", "cell_c", F.col("category").alias("top_category"),
                   F.col("count").alias("top_category_count")))

lat0 = F.col("cell_r") * F.lit(G)
lon0 = F.col("cell_c") * F.lit(G)
lat1, lon1 = lat0 + F.lit(G), lon0 + F.lit(G)
r6 = lambda c: F.round(c, 6)
cell_area_km2 = (F.lit(G * KM_PER_DEG) * F.lit(G * KM_PER_DEG)
                 * F.cos(F.radians(lat0 + F.lit(G / 2))))

grid = (grid.join(top_cat, ["cell_r", "cell_c"], "left")
        .withColumn("_id", F.concat_ws("_", F.col("cell_r"), F.col("cell_c")))
        .withColumn("center", F.struct(
            F.lit("Point").alias("type"),
            F.array(r6(lon0 + F.lit(G / 2)), r6(lat0 + F.lit(G / 2))).alias("coordinates")))
        .withColumn("polygon", F.struct(
            F.lit("Polygon").alias("type"),
            F.array(F.array(
                F.array(r6(lon0), r6(lat0)), F.array(r6(lon1), r6(lat0)),
                F.array(r6(lon1), r6(lat1)), F.array(r6(lon0), r6(lat1)),
                F.array(r6(lon0), r6(lat0)))).alias("coordinates")))
        .withColumn("area_km2", F.round(cell_area_km2, 4))
        .withColumn("density_km2", F.round(F.col("count") / cell_area_km2, 1))
        .withColumn("share", F.round(F.col("count") / F.lit(total), 6))
        .withColumn("arrest_rate", F.round(F.col("arrests") / F.col("count"), 4))
        .withColumn("grid_size", F.lit(G))
        .cache())

n_cells = grid.count()
log(f"Celdas con datos: {n_cells:,} (tamaño de celda {G}°)")
write(grid, "agg_grid")

# ----------------------------------------------------------------- zonas de alta concentración
stats = grid.agg(F.mean("count").alias("mean"), F.stddev("count").alias("std")).first()
mean_c, std_c = stats["mean"], (stats["std"] or 1.0)
p99 = grid.approxQuantile("count", [0.99], 0.001)[0]
hotspots = (grid.withColumn("z_score", F.round((F.col("count") - F.lit(mean_c)) / F.lit(std_c), 2))
            .where(F.col("count") >= F.lit(p99))
            .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("count"))))
            .withColumn("threshold_p99", F.lit(p99)))
n_hot = hotspots.count()
log(f"Hotspots (conteo >= p99 = {p99:.0f}): {n_hot}")
write(hotspots, "agg_hotspots")

# ----------------------------------------------------------------- comportamiento temporal
dims = {
    "hour": F.hour("date"),
    "day_of_week": F.dayofweek("date"),   # 1 = domingo ... 7 = sábado
    "month": F.month("date"),
    "year": F.year("date"),
}
temporal = None
for name, expr in dims.items():
    part = (pts.groupBy(expr.alias("value")).count()
            .withColumn("dimension", F.lit(name)))
    temporal = part if temporal is None else temporal.unionByName(part)
temporal = (temporal.withColumn("share", F.round(F.col("count") / F.lit(total), 6))
            .withColumn("_id", F.concat_ws("_", F.col("dimension"), F.col("value"))))
write(temporal, "agg_temporal")

top10 = [r["category"] for r in
         pts.groupBy("category").count().orderBy(F.desc("count")).limit(10).collect()]
cat_hour = (pts.where(F.col("category").isin(top10))
            .groupBy("category", F.hour("date").alias("hour")).count()
            .withColumn("_id", F.concat_ws("_", F.col("category"), F.col("hour"))))
write(cat_hour, "agg_category_hour")

types = (pts.groupBy("category")
         .agg(F.count("*").alias("count"), F.avg("arrest").alias("arrest_rate"))
         .withColumn("arrest_rate", F.round("arrest_rate", 4))
         .withColumn("share", F.round(F.col("count") / F.lit(total), 6))
         .withColumn("_id", F.col("category")))
write(types, "agg_types")

# ----------------------------------------------------------------- metadatos
seconds = round(time.time() - t0, 1)
meta = spark.createDataFrame([{
    "_id": "last_run",
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "source_ingest": source_ingest or "",
    "source_collection": SOURCE,
    "total_points": total,
    "grid_size": G,
    "cells": n_cells,
    "hotspots": n_hot,
    "hotspot_threshold_p99": float(p99),
    "cell_count_mean": float(mean_c),
    "cell_count_std": float(std_c),
    "seconds": seconds,
    "executors": len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1,
}])
write(meta, "agg_meta")
log(f"Listo en {seconds}s")
spark.stop()
