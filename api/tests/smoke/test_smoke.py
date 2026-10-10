"""
Pruebas básicas contra la API desplegada (las corre Jenkins antes de desplegar).

    API_URL=http://localhost:5000 pytest tests/smoke
"""
import os

import pytest
import requests

API = os.environ.get("API_URL", "http://localhost:5000").rstrip("/")
LAT = float(os.environ.get("SMOKE_LAT", "41.8781"))
LON = float(os.environ.get("SMOKE_LON", "-87.6298"))
D = 0.01  # ~1 km


def get(path, **params):
    return requests.get(f"{API}{path}", params=params, timeout=30)


def test_health():
    r = get("/health")
    assert r.status_code == 200
    assert r.json()["mongo"] == "up"


def test_near_returns_close_points():
    r = get("/api/crimes/near", lat=LAT, lon=LON, radius=1000, limit=20)
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    assert 0 < body["count"] <= 20, "no hay datos cerca del punto de prueba: ¿corrió la ingesta?"
    for f in body["features"]:
        lon, lat = f["geometry"]["coordinates"]
        assert abs(lat - LAT) < 0.02 and abs(lon - LON) < 0.02


def test_within_polygon_only_returns_inside_points():
    poly = {"type": "Polygon", "coordinates": [[
        [LON - D, LAT - D], [LON + D, LAT - D], [LON + D, LAT + D], [LON - D, LAT + D], [LON - D, LAT - D]]]}
    r = requests.post(f"{API}/api/crimes/within", json={"geometry": poly, "limit": 50}, timeout=30)
    assert r.status_code == 200
    body = r.json()
    assert body["count"] > 0
    for f in body["features"]:
        lon, lat = f["geometry"]["coordinates"]
        assert LON - D <= lon <= LON + D and LAT - D <= lat <= LAT + D


def test_geonear_summary():
    r = get("/api/crimes/summary", lat=LAT, lon=LON, radius=500)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0
    assert all(g["min_distance_m"] <= 500 for g in body["groups"])


@pytest.mark.parametrize("path", ["/api/stats/grid?limit=10", "/api/stats/hotspots",
                                  "/api/stats/temporal?dimension=hour", "/api/stats/types"])
def test_spark_results_available(path):
    r = requests.get(f"{API}{path}", timeout=30)
    assert r.status_code == 200
    body = r.json()
    items = body.get("features", body.get("data"))
    assert items, f"{path} sin resultados: ¿corrió el trabajo de Spark?"


def test_invalid_parameters_return_400():
    assert get("/api/crimes/near", lat=200, lon=LON).status_code == 400
