"""Pruebas unitarias de validación y construcción de consultas (sin MongoDB)."""
from datetime import datetime

import pytest

import queries as q


def test_parse_point_returns_geojson_order():
    assert q.parse_point({"lat": "41.88", "lon": "-87.63"}) == (-87.63, 41.88)


@pytest.mark.parametrize("args", [
    {"lat": "91", "lon": "0"},
    {"lat": "0", "lon": "-181"},
    {"lat": "abc", "lon": "0"},
    {"lon": "0"},
])
def test_parse_point_rejects_invalid(args):
    with pytest.raises(q.ValidationError):
        q.parse_point(args)


def test_radius_default_and_limits():
    assert q.parse_radius({}) == q.DEFAULT_RADIUS_M
    with pytest.raises(q.ValidationError):
        q.parse_radius({"radius": "0"})
    with pytest.raises(q.ValidationError):
        q.parse_radius({"radius": str(q.MAX_RADIUS_M + 1)})


def test_near_query_uses_parameters():
    query = q.near_query(-87.6, 41.8, 750, {"category": "THEFT"})
    near = query["location"]["$near"]
    assert near["$geometry"] == {"type": "Point", "coordinates": [-87.6, 41.8]}
    assert near["$maxDistance"] == 750
    assert query["category"] == "THEFT"


def test_filters_dates_and_category():
    f = q.parse_filters({"category": "theft", "from": "2016-01-01", "to": "2016-01-31"})
    assert f["category"] == "THEFT"
    assert f["date"]["$gte"] == datetime(2016, 1, 1)
    assert f["date"]["$lt"] == datetime(2016, 2, 1)       # "to" es inclusivo
    with pytest.raises(q.ValidationError):
        q.parse_filters({"from": "2016-02-01", "to": "2016-01-01"})
    with pytest.raises(q.ValidationError):
        q.parse_filters({"from": "01/02/2016"})


def test_extract_polygon_closes_ring_and_accepts_feature():
    feature = {"type": "Feature", "properties": {},
               "geometry": {"type": "Polygon",
                            "coordinates": [[[-87.7, 41.8], [-87.6, 41.8], [-87.6, 41.9]]]}}
    geom = q.extract_polygon(feature)
    ring = geom["coordinates"][0]
    assert ring[0] == ring[-1] and len(ring) == 4


@pytest.mark.parametrize("body", [
    {"type": "Point", "coordinates": [0, 0]},
    {"type": "Polygon", "coordinates": [[[0, 0], [1, 1]]]},
    {"type": "Polygon", "coordinates": [[[0, 0], [200, 0], [1, 1], [0, 0]]]},
    {"type": "Polygon", "coordinates": [[["a", 0], [1, 0], [1, 1], [0, 0]]]},
    [1, 2, 3],
])
def test_extract_polygon_rejects_invalid(body):
    with pytest.raises(q.ValidationError):
        q.extract_polygon(body)


def test_within_query():
    poly = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    assert q.within_query(poly) == {"location": {"$geoWithin": {"$geometry": poly}}}


def test_geonear_pipeline_is_first_stage_and_groups():
    p = q.geonear_pipeline(-87.6, 41.8, 500, {"year": 2016}, "category", 10)
    assert list(p[0]) == ["$geoNear"]
    assert p[0]["$geoNear"]["maxDistance"] == 500
    assert p[0]["$geoNear"]["query"] == {"year": 2016}
    assert p[1]["$group"]["_id"] == "$category"
    assert {"$limit": 10} in p
    with pytest.raises(q.ValidationError):
        q.geonear_pipeline(-87.6, 41.8, 500, group_by="password")


def test_feature_collection_serializes_dates():
    docs = [{"_id": 1, "location": {"type": "Point", "coordinates": [0, 0]},
             "date": datetime(2016, 5, 3, 23, 40)}]
    fc = q.feature_collection(docs, operator="$near")
    assert fc["count"] == 1
    assert fc["features"][0]["properties"] == {"id": 1, "date": "2016-05-03T23:40:00"}
