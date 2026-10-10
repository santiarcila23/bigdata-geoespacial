"""
Ingesta: Kaggle -> limpieza distribuida con Dask -> MongoDB (GeoJSON + índice 2dsphere).

Pasos
  1. Descarga automática del dataset con la API de Kaggle (credenciales por
     variables de entorno KAGGLE_USERNAME / KAGGLE_KEY, nunca en el repositorio).
  2. Lectura particionada del CSV con Dask y limpieza:
       - coordenadas nulas o no numéricas
       - coordenadas fuera de rango (lat ±90, lon ±180)
       - coordenadas fuera del área de estudio (BBOX), p. ej. (0,0) o ceros
         de georreferenciación fallida
       - fechas que no se pueden interpretar
       - registros duplicados por ID
  3. Escritura del resultado limpio en Parquet (lo usa también el benchmark).
  4. Carga por lotes a MongoDB: cada partición la escribe un worker de Dask,
     con insert_many(ordered=False) en lotes de BATCH_SIZE documentos.
  5. Creación de índices: 2dsphere sobre `location`, y fecha / categoría.

Todo es configurable por variables de entorno para poder cambiar de dataset.
"""
from __future__ import annotations

import glob
import json
import math
import os
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client
from pymongo import ASCENDING, GEOSPHERE, MongoClient
from pymongo.errors import BulkWriteError


# --------------------------------------------------------------------------- config
def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_list(name: str, default: str) -> list[str]:
    return [c.strip() for c in env(name, default).split(",") if c.strip()]


CFG = {
    "dataset": env("KAGGLE_DATASET", "currie32/crimes-in-chicago"),
    "file": env("KAGGLE_FILE", "Chicago_Crimes_2012_to_2017.csv"),
    "raw_dir": env("RAW_DIR", "/data/raw"),
    "clean_dir": env("CLEAN_DIR", "/data/clean"),
    "report_dir": env("REPORT_DIR", "/data/reports"),
    "mongo_uri": env("MONGO_URI", "mongodb://mongo:27017"),
    "mongo_db": env("MONGO_DB", "geo"),
    "collection": env("MONGO_COLLECTION", "crimes"),
    "scheduler": env("DASK_SCHEDULER", "tcp://dask-scheduler:8786"),
    "force": env("FORCE_INGEST", "false").lower() in ("1", "true", "yes"),
    "sample_rows": int(env("SAMPLE_ROWS", "0") or 0),
    "batch_size": int(env("BATCH_SIZE", "5000")),
    "blocksize": env("DASK_BLOCKSIZE", "64MB"),
    # Mapeo de columnas del CSV (valores por defecto: Chicago Crime)
    "id_col": env("ID_COL", "ID"),
    "lat_col": env("LAT_COL", "Latitude"),
    "lon_col": env("LON_COL", "Longitude"),
    "date_col": env("DATE_COL", "Date"),
    "date_fmt": env("DATE_FORMAT", "%m/%d/%Y %I:%M:%S %p"),
    "category_col": env("CATEGORY_COL", "Primary Type"),
    "extra_cols": env_list(
        "EXTRA_COLS",
        "Description,Location Description,Arrest,Domestic,District,Community Area",
    ),
    "bool_cols": env_list("BOOL_COLS", "Arrest,Domestic"),
    "int_cols": env_list("INT_COLS", "District,Community Area"),
    # Área de estudio: minLon,minLat,maxLon,maxLat (Chicago con margen)
    "bbox": [float(x) for x in env_list("BBOX", "-87.96,41.62,-87.50,42.05")],
}


def snake(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name.strip().lower()).strip("_")


def log(msg: str) -> None:
    print(f"[ingest {datetime.now():%H:%M:%S}] {msg}", flush=True)


# --------------------------------------------------------------------------- 1. descarga
def find_csv(raw_dir: str, wanted: str) -> str | None:
    if wanted:
        exact = os.path.join(raw_dir, wanted)
        if os.path.exists(exact):
            return exact
    csvs = sorted(glob.glob(os.path.join(raw_dir, "**", "*.csv"), recursive=True),
                  key=os.path.getsize, reverse=True)
    return csvs[0] if csvs else None


def extract_archives(raw_dir: str) -> None:
    """Descomprime cualquier ZIP del directorio, aunque tenga extensión .csv.

    Kaggle entrega los archivos grandes comprimidos y, según la versión de la CLI,
    los guarda con el nombre original (p. ej. "datos.csv" siendo en realidad un ZIP).
    Se detecta por contenido (firma PK), no por la extensión.
    """
    for path in glob.glob(os.path.join(raw_dir, "*")):
        if not os.path.isfile(path) or not zipfile.is_zipfile(path):
            continue
        log(f"Descomprimiendo {os.path.basename(path)} ({os.path.getsize(path) / 1e6:.0f} MB comprimido)")
        tmp = os.path.join(raw_dir, "_unzip")
        shutil.rmtree(tmp, ignore_errors=True)
        with zipfile.ZipFile(path) as z:
            z.extractall(tmp)
        os.remove(path)
        for f in glob.glob(os.path.join(tmp, "**", "*"), recursive=True):
            if os.path.isfile(f):
                shutil.move(f, os.path.join(raw_dir, os.path.basename(f)))
        shutil.rmtree(tmp, ignore_errors=True)


def download() -> str:
    os.makedirs(CFG["raw_dir"], exist_ok=True)
    extract_archives(CFG["raw_dir"])
    existing = find_csv(CFG["raw_dir"], CFG["file"])
    if existing and not CFG["force"]:
        log(f"CSV ya descargado: {existing} ({os.path.getsize(existing) / 1e6:.0f} MB; "
            "FORCE_INGEST=true para volver a bajarlo)")
        return existing

    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        sys.exit("ERROR: faltan KAGGLE_USERNAME / KAGGLE_KEY (credencial de Jenkins o archivo .env)")

    if CFG["force"]:
        shutil.rmtree(CFG["raw_dir"], ignore_errors=True)
        os.makedirs(CFG["raw_dir"], exist_ok=True)

    cmd = ["kaggle", "datasets", "download", "-d", CFG["dataset"], "-p", CFG["raw_dir"], "--force"]
    if CFG["file"]:
        cmd += ["-f", CFG["file"]]
    log("Descargando desde Kaggle: " + " ".join(cmd))
    t0 = time.time()
    subprocess.run(cmd, check=True)

    extract_archives(CFG["raw_dir"])
    csv = find_csv(CFG["raw_dir"], CFG["file"])
    if not csv:
        sys.exit("ERROR: la descarga terminó pero no se encontró ningún CSV")
    log(f"Descarga lista en {time.time() - t0:.1f}s -> {csv} ({os.path.getsize(csv) / 1e6:.0f} MB)")
    return csv


# --------------------------------------------------------------------------- 2. limpieza
def _parse(pdf: pd.DataFrame, cfg: dict) -> dict:
    """Convierte columnas crudas y calcula las máscaras de cada regla de limpieza."""
    lat = pd.to_numeric(pdf[cfg["lat_col"]], errors="coerce")
    lon = pd.to_numeric(pdf[cfg["lon_col"]], errors="coerce")
    date = pd.to_datetime(pdf[cfg["date_col"]], format=cfg["date_fmt"], errors="coerce")
    min_lon, min_lat, max_lon, max_lat = cfg["bbox"]

    null_coord = lat.isna() | lon.isna()
    out_of_range = ~null_coord & ~(lat.between(-90, 90) & lon.between(-180, 180))
    out_of_bbox = ~null_coord & ~out_of_range & ~(
        lat.between(min_lat, max_lat) & lon.between(min_lon, max_lon))
    bad_date = date.isna()
    if cfg["id_col"]:
        missing_id = pd.to_numeric(pdf[cfg["id_col"]], errors="coerce").isna()
    else:
        missing_id = pd.Series(False, index=pdf.index)
    return dict(lat=lat, lon=lon, date=date, null_coord=null_coord, out_of_range=out_of_range,
                out_of_bbox=out_of_bbox, bad_date=bad_date, missing_id=missing_id)


def stats_partition(pdf: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    p = _parse(pdf, cfg)
    valid = ~(p["null_coord"] | p["out_of_range"] | p["out_of_bbox"] | p["bad_date"] | p["missing_id"])
    return pd.DataFrame([{
        "raw": len(pdf),
        "null_coord": int(p["null_coord"].sum()),
        "out_of_range": int(p["out_of_range"].sum()),
        "out_of_bbox": int(p["out_of_bbox"].sum()),
        "bad_date": int((p["bad_date"] & ~p["null_coord"]).sum()),
        "missing_id": int(p["missing_id"].sum()),
        "valid": int(valid.sum()),
    }])


def output_meta(cfg: dict) -> pd.DataFrame:
    cols = {"id": "int64", "lat": "float64", "lon": "float64",
            "date": "datetime64[ms]", "category": "object"}
    for c in cfg["extra_cols"]:
        cols[snake(c)] = ("bool" if c in cfg["bool_cols"]
                          else "Int64" if c in cfg["int_cols"] else "object")
    return pd.DataFrame({k: pd.Series(dtype=v) for k, v in cols.items()})


def clean_partition(pdf: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    p = _parse(pdf, cfg)
    keep = ~(p["null_coord"] | p["out_of_range"] | p["out_of_bbox"] | p["bad_date"] | p["missing_id"])
    out = pd.DataFrame(index=pdf.index[keep])
    if cfg["id_col"]:
        out["id"] = pd.to_numeric(pdf.loc[keep, cfg["id_col"]]).astype("int64")
    else:
        out["id"] = np.int64(-1)
    out["lat"] = p["lat"][keep].round(6).astype("float64")
    out["lon"] = p["lon"][keep].round(6).astype("float64")
    out["date"] = p["date"][keep].astype("datetime64[ms]")
    out["category"] = pdf.loc[keep, cfg["category_col"]].astype("object").str.strip()
    for c in cfg["extra_cols"]:
        values = pdf.loc[keep, c]
        if c in cfg["bool_cols"]:
            out[snake(c)] = values.astype(str).str.strip().str.lower().isin(["true", "1", "y", "yes"])
        elif c in cfg["int_cols"]:
            out[snake(c)] = pd.to_numeric(values, errors="coerce").round().astype("Int64")
        else:
            out[snake(c)] = values.astype("object").where(values.notna(), None)
    return out.reset_index(drop=True)


def clean(csv: str, client: Client) -> tuple[dd.DataFrame, dict]:
    cfg = CFG
    usecols = [c for c in [cfg["id_col"], cfg["lat_col"], cfg["lon_col"], cfg["date_col"],
                           cfg["category_col"], *cfg["extra_cols"]] if c]
    log(f"Leyendo {csv} en particiones de {cfg['blocksize']}")
    raw = dd.read_csv(csv, usecols=usecols, dtype=str, blocksize=cfg["blocksize"],
                      on_bad_lines="skip", encoding_errors="replace")
    log(f"Particiones: {raw.npartitions}")

    t0 = time.time()
    stats = raw.map_partitions(stats_partition, cfg,
                               meta=pd.DataFrame({k: pd.Series(dtype="int64") for k in
                                                  ["raw", "null_coord", "out_of_range", "out_of_bbox",
                                                   "bad_date", "missing_id", "valid"]})
                               ).sum().compute().to_dict()
    stats = {k: int(v) for k, v in stats.items()}

    cleaned = raw.map_partitions(clean_partition, cfg, meta=output_meta(cfg))
    if cfg["id_col"]:
        cleaned = cleaned.drop_duplicates(subset=["id"], split_out=cleaned.npartitions)
    if cfg["sample_rows"] and stats["valid"] > cfg["sample_rows"]:
        frac = cfg["sample_rows"] / stats["valid"]
        log(f"Tomando muestra aleatoria del {frac:.1%} (SAMPLE_ROWS={cfg['sample_rows']})")
        cleaned = cleaned.sample(frac=frac, random_state=42)

    shutil.rmtree(cfg["clean_dir"], ignore_errors=True)
    cleaned.to_parquet(cfg["clean_dir"], write_index=False)
    clean_df = dd.read_parquet(cfg["clean_dir"])
    final = int(len(clean_df))
    stats["duplicates"] = stats["valid"] - final if not cfg["sample_rows"] else None
    stats["final"] = final
    stats["clean_seconds"] = round(time.time() - t0, 1)
    log(f"Limpieza lista en {stats['clean_seconds']}s: {json.dumps(stats)}")
    return clean_df, stats


# --------------------------------------------------------------------------- 3. carga
def _native(v):
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if math.isnan(f) else f
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def to_document(rec: dict, with_id: bool) -> dict:
    lat, lon = float(rec.pop("lat")), float(rec.pop("lon"))
    rid = rec.pop("id")
    date = _native(rec.pop("date"))
    doc = {
        "location": {"type": "Point", "coordinates": [lon, lat]},   # GeoJSON: [lon, lat]
        "date": date,
        "year": date.year,
        "category": rec.pop("category"),
    }
    if with_id:
        doc["_id"] = int(rid)
    for k, v in rec.items():
        doc[k] = _native(v)
    return doc


def write_partition(pdf: pd.DataFrame, cfg: dict) -> dict:
    """Se ejecuta en un worker de Dask: inserta su partición en lotes."""
    client = MongoClient(cfg["mongo_uri"])
    coll = client[cfg["mongo_db"]][cfg["collection"]]
    with_id = bool(cfg["id_col"])
    inserted = duplicates = 0
    batch: list[dict] = []

    def flush():
        nonlocal inserted, duplicates, batch
        if not batch:
            return
        try:
            res = coll.insert_many(batch, ordered=False)
            inserted += len(res.inserted_ids)
        except BulkWriteError as e:   # duplicados si se re-ejecuta la carga
            inserted += e.details.get("nInserted", 0)
            duplicates += sum(1 for w in e.details.get("writeErrors", []) if w.get("code") == 11000)
        batch = []

    for rec in pdf.to_dict("records"):
        batch.append(to_document(rec, with_id))
        if len(batch) >= cfg["batch_size"]:
            flush()
    flush()
    client.close()
    return {"inserted": inserted, "duplicates": duplicates}


def load(clean_df: dd.DataFrame) -> dict:
    t0 = time.time()
    parts = clean_df.to_delayed()
    log(f"Cargando {len(parts)} particiones a MongoDB en lotes de {CFG['batch_size']}")
    results = dask.compute(*[dask.delayed(write_partition)(p, CFG) for p in parts])
    out = {
        "inserted": sum(r["inserted"] for r in results),
        "duplicates_skipped": sum(r["duplicates"] for r in results),
        "load_seconds": round(time.time() - t0, 1),
    }
    log(f"Carga lista: {out}")
    return out


def create_indexes(coll) -> list[str]:
    names = [
        coll.create_index([("location", GEOSPHERE)], name="location_2dsphere"),
        coll.create_index([("date", ASCENDING)], name="date_1"),
        coll.create_index([("category", ASCENDING), ("date", ASCENDING)], name="category_date"),
    ]
    log(f"Índices: {names}")
    return names


# --------------------------------------------------------------------------- main
def main() -> None:
    t_start = time.time()
    mongo = MongoClient(CFG["mongo_uri"], serverSelectionTimeoutMS=30000)
    db = mongo[CFG["mongo_db"]]
    coll = db[CFG["collection"]]

    current = coll.estimated_document_count()
    if current > 0 and not CFG["force"]:
        log(f"La colección '{CFG['collection']}' ya tiene {current:,} documentos; "
            "no se recarga (FORCE_INGEST=true para forzar).")
        create_indexes(coll)
        return

    csv = download()

    log(f"Conectando al scheduler de Dask {CFG['scheduler']}")
    client = Client(CFG["scheduler"], timeout="60s")
    client.wait_for_workers(1, timeout=120)
    log(f"Workers activos: {len(client.scheduler_info()['workers'])}")

    clean_df, stats = clean(csv, client)

    if CFG["force"]:
        log("FORCE_INGEST: se borra la colección anterior")
        coll.drop()
    create_indexes(coll)   # el índice se crea antes para no bloquear al final
    load_stats = load(clean_df)

    report = {
        "dataset": CFG["dataset"], "file": os.path.basename(csv),
        "bbox": CFG["bbox"], "finished_at": datetime.now(timezone.utc).isoformat(),
        **stats, **load_stats,
        "documents_in_mongo": coll.count_documents({}),
        "total_seconds": round(time.time() - t_start, 1),
    }
    db["ingest_runs"].insert_one(dict(report))
    os.makedirs(CFG["report_dir"], exist_ok=True)
    with open(os.path.join(CFG["report_dir"], "cleaning.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    log("Reporte de limpieza:\n" + json.dumps(report, indent=2, default=str))
    client.close()


if __name__ == "__main__":
    main()
