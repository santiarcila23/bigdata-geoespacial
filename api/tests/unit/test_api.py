"""Pruebas de los endpoints con una base de datos falsa (no requiere MongoDB)."""
from datetime import datetime

import pytest

from app import create_app

POINT = {"type": "Point", "coordinates": [-87.63, 41.88]}


class FakeCursor(list):
    def limit(self, n):
        return FakeCursor(self[:n])

    def sort(self, *_args, **_kwargs):
        return self


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or []
        self.last_filter = None
        self.last_pipeline = None

    def find(self, flt=None, projection=None):
        self.last_filter = flt
        return FakeCursor(dict(d) for d in self.docs)

    def find_one(self, flt=None):
        return dict(self.docs[0]) if self.docs else None

    def aggregate(self, pipeline):
        self.last_pipeline = pipeline
        return [{"group": "THEFT", "count": 3, "avg_distance_m": 120.5}]

    def count_documents(self, flt):
        return len(self.docs)

    def estimated_document_count(self):
        return len(self.docs)


class FakeDB(dict):
    def __missing__(self, key):
        self[key] = FakeCollection()
        return self[key]

    def command(self, name):
        return {"ok": 1}


@pytest.fixture()
def db():
    d = FakeDB()
    d["crimes"] = FakeCollection([
        {"_id": i, "location": POINT, "date": datetime(2016, 1, 1), "category": "THEFT"}
        for i in range(5)
    ])
    d["agg_grid"] = FakeCollection([{"_id": "1_2", "count": 10, "polygon": {
        "type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}}])
    d["agg_temporal"] = FakeCollection([{"dimension": "hour", "value": 0, "count": 4}])
    return d


@pytest.fixture()
def client(db):
    app = create_app(db=db)
    app.testing = True
    return app.test_client()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json["status"] == "ok"


def test_near_returns_feature_collection(client, db):
    r = client.get("/api/crimes/near?lat=41.88&lon=-87.63&radius=500&limit=3")
    assert r.status_code == 200
    assert r.json["type"] == "FeatureCollection" and r.json["count"] == 3
    assert db["crimes"].last_filter["location"]["$near"]["$maxDistance"] == 500


def test_near_invalid_params(client):
    assert client.get("/api/crimes/near?lat=100&lon=-87.63").status_code == 400
    assert client.get("/api/crimes/near?lon=-87.63").status_code == 400


def test_within_polygon(client, db):
    poly = {"type": "Polygon", "coordinates": [[[-87.7, 41.8], [-87.6, 41.8],
                                                [-87.6, 41.9], [-87.7, 41.9], [-87.7, 41.8]]]}
    r = client.post("/api/crimes/within?count=true", json=poly)
    assert r.status_code == 200 and r.json["total_matching"] == 5
    assert "$geoWithin" in db["crimes"].last_filter["location"]


def test_within_requires_body(client):
    assert client.post("/api/crimes/within", data="x").status_code == 400


def test_summary_uses_geonear(client, db):
    r = client.get("/api/crimes/summary?lat=41.88&lon=-87.63&radius=800")
    assert r.status_code == 200 and r.json["total"] == 3
    assert "$geoNear" in db["crimes"].last_pipeline[0]


def test_spark_grid_and_temporal(client):
    r = client.get("/api/stats/grid?min_count=5")
    assert r.status_code == 200 and r.json["features"][0]["geometry"]["type"] == "Polygon"
    r = client.get("/api/stats/temporal?dimension=hour")
    assert r.status_code == 200 and r.json["data"][0]["count"] == 4
    assert client.get("/api/stats/temporal?dimension=foo").status_code == 400


def test_stats_meta_missing(client):
    assert client.get("/api/stats/meta").status_code == 404
