import math
from pathlib import Path

import pytest

from sim_dji_cloud.dashboard.flight_area import load_flight_area, parse_png_bounds

# 合成 XML：2 个区域（1 限制区 + 1 作业区）、2 个 points 块、
# 以及 block_level/block 干扰项（必须被忽略）。
# 坐标用 UTM 51N 锚点：东 500000 / 北 0 -> 经 123.0 / 纬 0.0。
SYNTHETIC_XML = """<?xml version="1.0" encoding="utf-8"?>
<map id="1000">
  <name val="TEST"/>
  <utm_zone val="51R"/>
  <utm_min x="500000" y="0" z="0"/>
  <utm_max x="500100" y="100" z="0"/>
  <restriction_area id="1000">
    <name val="禁飞区A"/>
    <type val="2"/>
    <height_limit val="120"/>
    <extra_points ref="1000"/>
  </restriction_area>
  <ground_area id="1001">
    <name val="作业区B"/>
    <type val="1"/>
    <extra_points ref="1001"/>
  </ground_area>
  <points id="1000">
    <node x="500000" y="0" z="0"/>
    <node x="500100" y="0" z="0"/>
    <node x="500100" y="100" z="0"/>
  </points>
  <points id="1001">
    <node x="500000" y="0" z="0"/>
    <node x="500050" y="50" z="0"/>
    <node x="500000" y="50" z="0"/>
  </points>
  <block_level height="24.4">
    <block id="1_1" x="500010" y="10" z="24.9"/>
    <block id="1_2" x="500020" y="20" z="25.3"/>
  </block_level>
</map>
"""


def _write(tmp_path: Path) -> Path:
    p = tmp_path / "area.xml"
    p.write_text(SYNTHETIC_XML, encoding="utf-8")
    return p


def test_feature_count_and_kinds(tmp_path):
    fa = load_flight_area(_write(tmp_path))
    feats = fa["areas"]["features"]
    assert len(feats) == 2
    kinds = {f["properties"]["id"]: f["properties"]["kind"] for f in feats}
    assert kinds == {1000: "restriction", 1001: "ground"}


def test_properties_carried(tmp_path):
    fa = load_flight_area(_write(tmp_path))
    by_id = {f["properties"]["id"]: f["properties"] for f in fa["areas"]["features"]}
    assert by_id[1000]["height_limit"] == 120
    assert by_id[1000]["name"] == "禁飞区A"
    assert by_id[1001]["name"] == "作业区B"


def test_anchor_coordinate_conversion(tmp_path):
    fa = load_flight_area(_write(tmp_path))
    # 第一个 restriction 的第一个顶点是 UTM(500000,0) -> 经 123.0 / 纬 0.0
    ring = fa["areas"]["features"][0]["geometry"]["coordinates"][0]
    lon, lat = ring[0]
    assert math.isclose(lon, 123.0, abs_tol=1e-6)
    assert math.isclose(lat, 0.0, abs_tol=1e-6)


def test_polygon_ring_closed(tmp_path):
    fa = load_flight_area(_write(tmp_path))
    ring = fa["areas"]["features"][0]["geometry"]["coordinates"][0]
    assert ring[0] == ring[-1]  # GeoJSON 多边形环必须闭合


def test_blocks_ignored(tmp_path):
    # block 的 x/y 属性与 node 同形，必须被忽略：既不产生额外 feature，
    # 也不混入任何多边形顶点。顶点总数 = 各 points 节点数 + 每环闭合 1 点。
    fa = load_flight_area(_write(tmp_path))
    assert len(fa["areas"]["features"]) == 2
    total_vertices = sum(len(f["geometry"]["coordinates"][0]) for f in fa["areas"]["features"])
    assert total_vertices == (3 + 1) + (3 + 1)


def test_area_with_dangling_points_ref_skipped(tmp_path):
    # area 1000 的 extra_points ref=1000 失配（points 改成 9999），应被跳过，仅剩 1001
    xml = SYNTHETIC_XML.replace('<points id="1000">', '<points id="9999">')
    p = tmp_path / "area.xml"
    p.write_text(xml, encoding="utf-8")
    fa = load_flight_area(p)
    ids = {f["properties"]["id"] for f in fa["areas"]["features"]}
    assert ids == {1001}


def test_png_bounds_from_utm_minmax(tmp_path):
    fa = load_flight_area(_write(tmp_path))
    (sw_lat, sw_lng), (ne_lat, ne_lng) = fa["png_bounds"]
    # utm_min(500000,0) -> (lat0,lon123) ; utm_max(500100,100) 略大
    assert math.isclose(sw_lng, 123.0, abs_tol=1e-6)
    assert math.isclose(sw_lat, 0.0, abs_tol=1e-6)
    assert ne_lat > sw_lat and ne_lng > sw_lng


def test_png_bounds_override(tmp_path):
    fa = load_flight_area(_write(tmp_path), png_bounds_utm=(500000, 0, 500000, 0))
    (sw_lat, sw_lng), (ne_lat, ne_lng) = fa["png_bounds"]
    assert math.isclose(sw_lng, 123.0, abs_tol=1e-6)
    assert math.isclose(ne_lng, 123.0, abs_tol=1e-6)


def test_parse_png_bounds():
    assert parse_png_bounds(None) is None
    assert parse_png_bounds("") is None
    assert parse_png_bounds("1,2,3,4") == (1.0, 2.0, 3.0, 4.0)


def test_parse_png_bounds_bad():
    with pytest.raises(ValueError):
        parse_png_bounds("1,2,3")
