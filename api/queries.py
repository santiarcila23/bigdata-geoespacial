"""
Validación de parámetros y construcción de consultas geoespaciales de MongoDB.

Se separa de Flask para poder probarlo sin base de datos (pytest unitario).
Las consultas siempre se arman con parámetros recibidos: nada fijo en el código.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping

MAX_RADIUS_M = 50_000
DEFAULT_RADIUS_M = 1_000
MAX_LIMIT = 2_000
DEFAULT_LIMIT = 200
GROUP_FIELDS = {"category", "location_description", "district", "year", "arrest", "domestic"}
TEMPORAL_DIMENSIONS = {"hour", "day_of_week", "month", "year"}


class ValidationError(ValueError):
    """Parámetro inválido -> HTTP 400."""


# --------------------------------------------------------------------------- parámetros
def parse_float(args: Mapping, name: str, lo: float, hi: float,
                default: float | None = None) -> float:
    raw = args.get(name)
    if raw is None or raw == "":
        if default is None:
            raise ValidationError(f"Falta el parámetro '{name}'")
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValidationError(f"'{name}' debe ser numérico") from None
    if not (lo <= value <= hi):
        raise ValidationError(f"'{name}' debe estar entre {lo} y {hi}")
    return value


def parse_int(args: Mapping, name: str, lo: int, hi: int, default: int) -> int:
    raw = args.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValidationError(f"'{name}' debe ser un entero") from None
    if not (lo <= value <= hi):
        raise ValidationError(f"'{name}' debe estar entre {lo} y {hi}")
    return value


def parse_point(args: Mapping) -> tuple[float, float]:
    """Devuelve (lon, lat), el orden que usa GeoJSON."""
    lat = parse_float(args, "lat", -90, 90)
    lon = parse_float(args, "lon", -180, 180)
    return lon, lat


def parse_radius(args: Mapping) -> float:
    return parse_float(args, "radius", 1, MAX_RADIUS_M, DEFAULT_RADIUS_M)


def parse_limit(args: Mapping, default: int = DEFAULT_LIMIT) -> int:
    return parse_int(args, "limit", 1, MAX_LIMIT, default)


def _parse_date(value: str, name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValidationError(f"'{name}' debe tener formato AAAA-MM-DD") from None


def parse_filters(args: Mapping) -> dict:
    """Filtros opcionales comunes: category, from, to, year, arrest."""
    f: dict[str, Any] = {}
    category = (args.get("category") or "").strip()
    if category:
        f["category"] = category.upper()
    date_from, date_to = args.get("from"), args.get("to")
    if date_from or date_to:
        rng = {}
        if date_from:
            rng["$gte"] = _parse_date(date_from, "from")
        if date_to:
            rng["$lt"] = _parse_date(date_to, "to") + timedelta(days=1)
        if "$gte" in rng and "$lt" in rng and rng["$gte"] >= rng["$lt"]:
            raise ValidationError("'from' debe ser anterior a 'to'")
        f["date"] = rng
    if args.get("year"):
        f["year"] = parse_int(args, "year", 1900, 2100, 0)
    arrest = (args.get("arrest") or "").lower()
    if arrest in ("true", "false"):
        f["arrest"] = arrest == "true"
    elif arrest:
        raise ValidationError("'arrest' debe ser true o false")
    return f


# --------------------------------------------------------------------------- GeoJSON
def _check_position(pos: Any) -> list[float]:
    if (not isinstance(pos, (list, tuple)) or len(pos) < 2
            or not all(isinstance(c, (int, float)) and not isinstance(c, bool) for c in pos[:2])):
        raise ValidationError("Cada vértice debe ser [lon, lat] numérico")
    lon, lat = float(pos[0]), float(pos[1])
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        raise ValidationError(f"Vértice fuera de rango: [{lon}, {lat}] (recuerde el orden [lon, lat])")
    return [lon, lat]


def _check_ring(ring: Any) -> list[list[float]]:
    if not isinstance(ring, list) or len(ring) < 3:
        raise ValidationError("Cada anillo del polígono necesita al menos 3 vértices")
    pts = [_check_position(p) for p in ring]
    if pts[0] != pts[-1]:
        pts.append(list(pts[0]))          # se cierra el anillo automáticamente
    if len(pts) < 4:
        raise ValidationError("Un anillo cerrado necesita al menos 4 posiciones")
    return pts


def extract_polygon(body: Any) -> dict:
    """Acepta un Polygon/MultiPolygon, un Feature o un objeto {"geometry": ...}."""
    if not isinstance(body, dict):
        raise ValidationError("El cuerpo debe ser un objeto GeoJSON")
    geom = body
    if body.get("type") == "Feature" or "geometry" in body:
        geom = body.get("geometry")
    if not isinstance(geom, dict):
        raise ValidationError("No se encontró la geometría")
    gtype, coords = geom.get("type"), geom.get("coordinates")
    if gtype == "Polygon":
        if not isinstance(coords, list) or not coords:
            raise ValidationError("Polygon sin coordenadas")
        return {"type": "Polygon", "coordinates": [_check_ring(r) for r in coords]}
    if gtype == "MultiPolygon":
        if not isinstance(coords, list) or not coords:
            raise ValidationError("MultiPolygon sin coordenadas")
        return {"type": "MultiPolygon",
                "coordinates": [[_check_ring(r) for r in poly] for poly in coords]}
    raise ValidationError("La geometría debe ser de tipo Polygon o MultiPolygon")


def point(lon: float, lat: float) -> dict:
    return {"type": "Point", "coordinates": [lon, lat]}


# --------------------------------------------------------------------------- consultas
def near_query(lon: float, lat: float, radius_m: float, filters: dict | None = None) -> dict:
    """$near: documentos a menos de radius_m metros, ordenados del más cercano al más lejano."""
    return {
        "location": {"$near": {"$geometry": point(lon, lat), "$maxDistance": radius_m}},
        **(filters or {}),
    }


def within_query(geometry: dict, filters: dict | None = None) -> dict:
    """$geoWithin: documentos dentro del polígono."""
    return {"location": {"$geoWithin": {"$geometry": geometry}}, **(filters or {})}


def geonear_pipeline(lon: float, lat: float, radius_m: float, filters: dict | None = None,
                     group_by: str = "category", limit: int = 50) -> list[dict]:
    """Agregación con $geoNear: resumen por grupo de lo que hay alrededor del punto."""
    if group_by not in GROUP_FIELDS:
        raise ValidationError(f"'group_by' debe ser uno de {sorted(GROUP_FIELDS)}")
    stage = {
        "near": point(lon, lat),
        "distanceField": "distance_m",
        "maxDistance": radius_m,
        "spherical": True,
        "key": "location",
    }
    if filters:
        stage["query"] = filters
    return [
        {"$geoNear": stage},
        {"$group": {
            "_id": f"${group_by}",
            "count": {"$sum": 1},
            "avg_distance_m": {"$avg": "$distance_m"},
            "min_distance_m": {"$min": "$distance_m"},
            "last_date": {"$max": "$date"},
        }},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "group": "$_id", "count": 1,
                      "avg_distance_m": {"$round": ["$avg_distance_m", 1]},
                      "min_distance_m": {"$round": ["$min_distance_m", 1]},
                      "last_date": 1}},
    ]


# --------------------------------------------------------------------------- serialización
def _json_value(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _json_value(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_value(x) for x in v]
    if type(v).__name__ == "ObjectId":
        return str(v)
    return v


def clean_doc(doc: dict) -> dict:
    return {k: _json_value(v) for k, v in doc.items()}


def to_feature(doc: dict, geometry_field: str = "location") -> dict:
    doc = dict(doc)
    geometry = doc.pop(geometry_field, None)
    props = clean_doc(doc)
    if "_id" in props:
        props["id"] = props.pop("_id")
    return {"type": "Feature", "geometry": geometry, "properties": props}


def feature_collection(docs, geometry_field: str = "location", **meta) -> dict:
    features = [to_feature(d, geometry_field) for d in docs]
    return {"type": "FeatureCollection", "count": len(features),
            "query": clean_doc(meta), "features": features}
