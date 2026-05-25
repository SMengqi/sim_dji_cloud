"""解析 DJI 自定义飞行区 XML，输出 WGS84 GeoJSON + PNG 经纬度边界。

XML 坐标为 UTM（本站点 51R = EPSG:32651）。区域多边形由 restriction_area(type2)
/ ground_area(type1) 引用 points 块组成。文件含数十万 <block> 点云体素，本模块
用 iterparse 流式跳过它们。
"""
from __future__ import annotations

from pathlib import Path
from xml.etree.ElementTree import iterparse

from loguru import logger
from pyproj import Transformer

# EPSG:32651 = WGS84 / UTM zone 51N（XML 中 utm_zone 标作军用网格带号 "51R"，同一带）
_UTM_51N_EPSG = "EPSG:32651"
_WGS84_EPSG = "EPSG:4326"

_AREA_TAGS = {"restriction_area": "restriction", "ground_area": "ground"}


def _parse_4floats(s: str | None, fmt: str) -> tuple[float, float, float, float] | None:
    """把逗号分隔的 4 个数值解析为元组；空值返回 None；个数不对则报错。"""
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 4:
        raise ValueError(f"必须是 4 个数值 '{fmt}'")
    return (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))


def parse_png_bounds(s: str | None) -> tuple[float, float, float, float] | None:
    """CLI 'minx,miny,maxx,maxy'(UTM) -> 元组；空值返回 None。"""
    return _parse_4floats(s, "minx,miny,maxx,maxy")


def parse_png_bounds_latlon(s: str | None) -> tuple[float, float, float, float] | None:
    """CLI 'swLat,swLng,neLat,neLng'(WGS84) -> 元组；空值返回 None。"""
    return _parse_4floats(s, "swLat,swLng,neLat,neLng")


def read_sidecar_bounds(png_path: Path | None) -> tuple[float, float, float, float] | None:
    """读取 PNG 旁的 sidecar '<png>.bounds'（单行 swLat,swLng,neLat,neLng）。

    校准模式产出的经纬度边界可写入该文件，启动时自动读取，免去每次传参。
    文件不存在返回 None；内容格式错误则由 parse_png_bounds_latlon 抛出。
    """
    if png_path is None:
        return None
    sidecar = Path(str(png_path) + ".bounds")
    if not sidecar.exists():
        return None
    return parse_png_bounds_latlon(sidecar.read_text(encoding="utf-8").strip())


def load_flight_area(
    xml_path: Path,
    png_bounds_utm: tuple[float, float, float, float] | None = None,
    png_bounds_latlon: tuple[float, float, float, float] | None = None,
) -> dict:
    """解析飞行区 XML，返回 {png_bounds: [[swLat,swLng],[neLat,neLng]], areas: GeoJSON}。"""
    xml_path = Path(xml_path)
    transformer = Transformer.from_crs(_UTM_51N_EPSG, _WGS84_EPSG, always_xy=True)

    utm_min: tuple[float, float] | None = None
    utm_max: tuple[float, float] | None = None
    areas: dict[int, dict] = {}
    area_order: list[int] = []
    points: dict[int, list[tuple[float, float]]] = {}

    current_area: dict | None = None
    current_points_id: int | None = None
    current_points: list[tuple[float, float]] = []

    for event, elem in iterparse(str(xml_path), events=("start", "end")):
        tag = elem.tag
        if event == "start":
            if tag in _AREA_TAGS:
                current_area = {
                    "id": int(elem.get("id")),
                    "kind": _AREA_TAGS[tag],
                    "height_limit": None,
                    "name": None,
                    "points_ref": None,
                }
            elif tag == "points":
                current_points_id = int(elem.get("id"))
                current_points = []
            continue

        # event == "end"
        if tag == "block":
            elem.clear()
            continue
        if tag == "utm_min":
            utm_min = (float(elem.get("x")), float(elem.get("y")))
        elif tag == "utm_max":
            utm_max = (float(elem.get("x")), float(elem.get("y")))
        elif tag == "node" and current_points_id is not None:
            current_points.append((float(elem.get("x")), float(elem.get("y"))))
        elif tag == "name" and current_area is not None:
            current_area["name"] = elem.get("val")
        elif tag == "height_limit" and current_area is not None:
            current_area["height_limit"] = int(float(elem.get("val")))
        elif tag == "extra_points" and current_area is not None:
            current_area["points_ref"] = int(elem.get("ref"))
        elif tag in _AREA_TAGS:
            areas[current_area["id"]] = current_area
            area_order.append(current_area["id"])
            current_area = None
        elif tag == "points":
            points[current_points_id] = current_points
            current_points_id = None
            current_points = []
        elem.clear()

    if utm_min is None or utm_max is None:
        raise ValueError(f"飞行区 XML 缺少 utm_min/utm_max: {xml_path}")

    features = []
    for area_id in area_order:
        area = areas[area_id]
        pts_id = area["points_ref"]
        pts = points.get(pts_id) if pts_id is not None else None
        if not pts:
            if pts_id is not None and pts_id not in points:
                logger.warning("飞行区 area {} 引用了不存在的 points id {}，已跳过", area_id, pts_id)
            continue
        ring = [list(transformer.transform(x, y)) for (x, y) in pts]  # -> [lon, lat]
        if ring and ring[0] != ring[-1]:
            ring.append(ring[0])
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {
                "id": area_id,
                "kind": area["kind"],
                "height_limit": area["height_limit"],
                "name": area["name"],
            },
        })

    if png_bounds_latlon is not None:
        # 直接给定经纬度边界（校准模式产出），跳过 UTM 换算
        sw_lat, sw_lon, ne_lat, ne_lon = png_bounds_latlon
        png_bounds = [[sw_lat, sw_lon], [ne_lat, ne_lon]]
    else:
        minx, miny, maxx, maxy = (
            png_bounds_utm if png_bounds_utm is not None
            else (utm_min[0], utm_min[1], utm_max[0], utm_max[1])
        )
        sw_lon, sw_lat = transformer.transform(minx, miny)
        ne_lon, ne_lat = transformer.transform(maxx, maxy)
        png_bounds = [[sw_lat, sw_lon], [ne_lat, ne_lon]]

    return {
        "png_bounds": png_bounds,
        "areas": {"type": "FeatureCollection", "features": features},
    }
