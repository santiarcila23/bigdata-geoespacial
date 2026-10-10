"""
API Flask para consultar los datos geoespaciales.

Endpoints principales
  GET  /api/crimes/near          lat, lon, radius -> registros cercanos ($near)
  POST /api/crimes/within        polígono GeoJSON -> registros dentro ($geoWithin)
  GET  /api/crimes/summary       resumen alrededor de un punto (agregación $geoNear)
  GET  /api/stats/grid           resultados de Spark: grilla con conteos
  GET  /api/stats/hotspots       resultados de Spark: zonas de alta concentración
  GET  /api/stats/temporal       resultados de Spark: por hora/día/mes/año
  GET  /api/stats/types          resultados de Spark: por categoría
  GET  /api/stats/meta           resultados de Spark: metadatos de la corrida
Otros: GET /health, GET /api/meta, GET / (mapa Leaflet)
"""
from __future__ import annotations

import os
import time

from flask import Flask, jsonify, request, send_from_directory
from pymongo import MongoClient
from pymongo.errors import OperationFailure

import queries as q

APP_VERSION = os.environ.get("APP_VERSION", "local")


def create_app(db=None) -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")
    app.json.ensure_ascii = False
    app.json.sort_keys = False

    if db is None:
        client = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"),
                             serverSelectionTimeoutMS=5000)
        db = client[os.environ.get("MONGO_DB", "geo")]
    crimes = db[os.environ.get("MONGO_COLLECTION", "crimes")]

    # ------------------------------------------------------------------ utilidades
    @app.errorhandler(q.ValidationError)
    def bad_request(err):
        return jsonify(error=str(err)), 400

    @app.errorhandler(OperationFailure)
    def mongo_rejected(err):
        msg = str(getattr(err, "details", {}) and err.details.get("errmsg", err)) or str(err)
        if "Loop is not valid" in msg or "Edges" in msg or "Duplicate vertices" in msg:
            return jsonify(error="El polígono no es válido: sus lados se cruzan o tiene vértices repetidos. "
                                 "Dibuje los vértices en orden alrededor del área, sin cruzar líneas."), 400
        return jsonify(error=f"MongoDB rechazó la consulta: {msg}"), 400

    @app.errorhandler(404)
    def not_found(_):
        return jsonify(error="Ruta no encontrada"), 404

    @app.before_request
    def start_timer():
        request._t0 = time.perf_counter()

    @app.after_request
    def add_timing(resp):
        t0 = getattr(request, "_t0", None)
        if t0 is not None:
            resp.headers["X-Response-Time-ms"] = f"{(time.perf_counter() - t0) * 1000:.1f}"
        resp.headers["X-App-Version"] = APP_VERSION
        return resp

    # ------------------------------------------------------------------ generales
    @app.get("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.get("/health")
    def health():
        try:
            db.command("ping")
            return jsonify(status="ok", mongo="up", version=APP_VERSION)
        except Exception as e:  # noqa: BLE001
            return jsonify(status="error", mongo="down", detail=str(e), version=APP_VERSION), 503

    @app.get("/api/meta")
    def meta():
        return jsonify(
            version=APP_VERSION,
            dataset=os.environ.get("DATASET_NAME", ""),
            center={"lat": float(os.environ.get("MAP_CENTER_LAT", 41.8781)),
                    "lon": float(os.environ.get("MAP_CENTER_LON", -87.6298))},
            documents=crimes.estimated_document_count(),
            endpoints=sorted(str(r) for r in app.url_map.iter_rules() if str(r).startswith("/api")),
        )

    # ------------------------------------------------------------------ consultas geoespaciales
    @app.get("/api/crimes/near")
    def near():
        """Registros dentro de `radius` metros de (lat, lon), del más cercano al más lejano."""
        lon, lat = q.parse_point(request.args)
        radius = q.parse_radius(request.args)
        limit = q.parse_limit(request.args)
        filters = q.parse_filters(request.args)
        docs = crimes.find(q.near_query(lon, lat, radius, filters)).limit(limit)
        return jsonify(q.feature_collection(docs, operator="$near", lat=lat, lon=lon,
                                            radius_m=radius, limit=limit, filters=filters))

    @app.post("/api/crimes/within")
    def within():
        """Registros dentro de un polígono GeoJSON (Polygon, MultiPolygon o Feature)."""
        body = request.get_json(silent=True)
        if body is None:
            raise q.ValidationError("Envíe un polígono GeoJSON en el cuerpo (Content-Type: application/json)")
        geometry = q.extract_polygon(body)
        args = {**request.args, **{k: str(v) for k, v in body.items()
                                   if k in ("limit", "category", "from", "to", "year", "arrest")}}
        limit = q.parse_limit(args)
        filters = q.parse_filters(args)
        query = q.within_query(geometry, filters)
        docs = list(crimes.find(query).limit(limit))
        result = q.feature_collection(docs, operator="$geoWithin", limit=limit, filters=filters)
        if str(args.get("count", "")).lower() == "true":
            result["total_matching"] = crimes.count_documents(query)
        return jsonify(result)

    @app.get("/api/crimes/summary")
    def summary():
        """Agregación con $geoNear: cuántos registros hay por grupo alrededor del punto."""
        lon, lat = q.parse_point(request.args)
        radius = q.parse_radius(request.args)
        group_by = request.args.get("group_by", "category")
        limit = q.parse_int(request.args, "limit", 1, 500, 50)
        filters = q.parse_filters(request.args)
        pipeline = q.geonear_pipeline(lon, lat, radius, filters, group_by, limit)
        groups = [q.clean_doc(d) for d in crimes.aggregate(pipeline)]
        return jsonify(operator="$geoNear", lat=lat, lon=lon, radius_m=radius,
                       group_by=group_by, total=sum(g["count"] for g in groups), groups=groups)

    # ------------------------------------------------------------------ resultados de Spark
    @app.get("/api/crimes/count")
    def count_near():
        """Cuántos registros hay a menos de `radius` metros de (lat, lon)."""
        lon, lat = q.parse_point(request.args)
        radius = q.parse_radius(request.args)
        filters = q.parse_filters(request.args)
        query = {"location": {"$geoWithin": {"$centerSphere": [[lon, lat], radius / 6378100]}}, **filters}
        return jsonify(lat=lat, lon=lon, radius_m=radius, count=crimes.count_documents(query))

    @app.get("/api/stats/grid")
    def stats_grid():
        min_count = q.parse_int(request.args, "min_count", 0, 10**9, 1)
        limit = q.parse_int(request.args, "limit", 1, 20_000, 5_000)
        docs = (db["agg_grid"].find({"count": {"$gte": min_count}}, {"center": 0})
                .sort("count", -1).limit(limit))
        return jsonify(q.feature_collection(docs, geometry_field="polygon",
                                            source="spark", min_count=min_count, limit=limit))

    @app.get("/api/stats/hotspots")
    def stats_hotspots():
        limit = q.parse_int(request.args, "limit", 1, 1_000, 50)
        docs = db["agg_hotspots"].find({}, {"polygon": 0}).sort("rank", 1).limit(limit)
        return jsonify(q.feature_collection(docs, geometry_field="center", source="spark", limit=limit))

    @app.get("/api/stats/temporal")
    def stats_temporal():
        dimension = request.args.get("dimension", "hour")
        if dimension not in q.TEMPORAL_DIMENSIONS:
            raise q.ValidationError(f"'dimension' debe ser uno de {sorted(q.TEMPORAL_DIMENSIONS)}")
        docs = db["agg_temporal"].find({"dimension": dimension}, {"_id": 0}).sort("value", 1)
        return jsonify(source="spark", dimension=dimension, data=[q.clean_doc(d) for d in docs])

    @app.get("/api/stats/types")
    def stats_types():
        limit = q.parse_int(request.args, "limit", 1, 500, 20)
        docs = db["agg_types"].find({}, {"_id": 0}).sort("count", -1).limit(limit)
        return jsonify(source="spark", data=[q.clean_doc(d) for d in docs])

    @app.get("/api/stats/meta")
    def stats_meta():
        doc = db["agg_meta"].find_one({"_id": "last_run"})
        if doc is None:
            return jsonify(error="Todavía no se ha ejecutado el trabajo de Spark"), 404
        return jsonify(q.clean_doc(doc))

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000, debug=False)
