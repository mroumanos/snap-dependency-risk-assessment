"""Request generator functions for configurable crawl strategies."""

from __future__ import annotations

import csv
import json
import math
import time
from pathlib import Path
import re
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse
from urllib.request import urlopen

import scrapy

from .context import JobContext
from .test_capture import test_input_path_guess


REALISTIC_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
}

MINIMAL_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "curl/7.88.1",
}

US_STATE_BOUNDS: Dict[str, tuple[float, float, float, float]] = {
    # min_lat, max_lat, min_lon, max_lon
    "AL": (30.14, 35.01, -88.47, -84.89),
    "AK": (51.21, 71.39, -179.15, -129.98),
    "AZ": (31.33, 37.00, -114.82, -109.04),
    "AR": (33.00, 36.50, -94.62, -89.64),
    "CA": (32.53, 42.01, -124.48, -114.13),
    "CO": (36.99, 41.00, -109.06, -102.04),
    "CT": (40.98, 42.05, -73.73, -71.78),
    "DC": (38.79, 38.995, -77.12, -76.91),
    "DE": (38.45, 39.84, -75.79, -75.05),
    "FL": (24.39, 31.00, -87.63, -80.03),
    "GA": (30.36, 35.00, -85.61, -80.84),
    "HI": (18.91, 22.24, -160.25, -154.81),
    "IA": (40.37, 43.50, -96.64, -90.14),
    "ID": (42.00, 49.00, -117.24, -111.04),
    "IL": (36.97, 42.51, -91.51, -87.49),
    "IN": (37.77, 41.76, -88.10, -84.78),
    "KS": (36.99, 40.00, -102.06, -94.59),
    "KY": (36.49, 39.15, -89.57, -81.96),
    "LA": (28.93, 33.02, -94.05, -88.82),
    "MA": (41.23, 42.89, -73.51, -69.93),
    "MD": (37.89, 39.72, -79.49, -75.04),
    "ME": (42.98, 47.46, -71.08, -66.89),
    "MI": (41.70, 48.31, -90.42, -82.12),
    "MN": (43.50, 49.38, -97.24, -89.49),
    "MO": (35.99, 40.61, -95.77, -89.10),
    "MS": (30.18, 35.00, -91.66, -88.09),
    "MT": (44.36, 49.00, -116.07, -104.04),
    "NC": (33.84, 36.59, -84.32, -75.46),
    "ND": (45.93, 49.00, -104.05, -96.55),
    "NE": (39.99, 43.00, -104.06, -95.31),
    "NH": (42.70, 45.31, -72.56, -70.57),
    "NJ": (38.93, 41.36, -75.57, -73.89),
    "NM": (31.33, 37.00, -109.05, -103.00),
    "NV": (35.00, 42.00, -120.00, -114.04),
    "NY": (40.49, 45.01, -79.77, -71.85),
    "OH": (38.40, 41.98, -84.82, -80.52),
    "OK": (33.62, 37.00, -103.00, -94.43),
    "OR": (41.99, 46.30, -124.70, -116.46),
    "PA": (39.72, 42.51, -80.52, -74.69),
    "PR": (17.88, 18.52, -67.28, -65.22),
    "RI": (41.09, 42.02, -71.86, -71.12),
    "SC": (32.03, 35.22, -83.36, -78.50),
    "SD": (42.48, 45.95, -104.06, -96.44),
    "TN": (34.98, 36.68, -90.31, -81.64),
    "TX": (25.84, 36.50, -106.65, -93.51),
    "UT": (36.99, 42.00, -114.05, -109.04),
    "VA": (36.54, 39.47, -83.68, -75.24),
    "VT": (42.73, 45.02, -73.44, -71.46),
    "WA": (45.54, 49.00, -124.79, -116.92),
    "WI": (42.49, 47.31, -92.89, -86.25),
    "WV": (37.20, 40.64, -82.65, -77.72),
    "WY": (40.99, 45.01, -111.06, -104.05),
}


def _method_supports_body(method: str) -> bool:
    """Return whether HTTP method conventionally supports request bodies."""
    return str(method).upper() in {"POST", "PUT", "PATCH", "DELETE"}


def _header_value(headers: Optional[Dict[str, str]], key: str) -> Optional[str]:
    if not headers:
        return None
    key_norm = key.lower()
    for k, v in headers.items():
        if str(k).lower() == key_norm:
            return str(v)
    return None


def _wants_json_body(gen_kwargs: Dict[str, object], headers: Optional[Dict[str, str]]) -> bool:
    if bool(gen_kwargs.get("json_body")):
        return True
    body_type = str(gen_kwargs.get("body_type", "")).strip().lower()
    if body_type == "json":
        return True
    content_type = (_header_value(headers, "Content-Type") or "").lower()
    return "application/json" in content_type or content_type.endswith("+json")


def _request_overrides(ctx: JobContext) -> Tuple[Optional[Dict[str, str]], Dict[str, object]]:
    """Collect per-request header/meta overrides from generator kwargs."""
    gen_kwargs = ctx.config.generator_kwargs or {}
    headers = gen_kwargs.get("headers")
    if not headers and gen_kwargs.get("use_minimal_headers"):
        headers = dict(MINIMAL_HEADERS)
    if not headers and gen_kwargs.get("use_realistic_headers"):
        headers = dict(REALISTIC_HEADERS)
    meta: Dict[str, object] = {}
    retry = gen_kwargs.get("max_retry_times", gen_kwargs.get("retry_times"))
    if retry is not None:
        meta["max_retry_times"] = int(retry)
    request_delay = gen_kwargs.get("request_delay", gen_kwargs.get("requestDelay"))
    if request_delay is not None:
        try:
            meta["request_delay"] = float(request_delay)
        except (TypeError, ValueError):
            pass
    download_timeout = gen_kwargs.get("download_timeout", gen_kwargs.get("downloadTimeout"))
    if download_timeout is not None:
        try:
            meta["download_timeout"] = float(download_timeout)
        except (TypeError, ValueError):
            pass
    return headers, meta


def accessfood_url(ctx: JobContext, params: Dict[str, object]) -> str:
    """Build AccessFood API URL with query params."""
    base_url = ctx.config.generator_kwargs.get(
        "base_url", "https://api.accessfood.org/api/MapInformation/LocationSearch"
    )
    return f"{base_url}?{urlencode(params)}"


def _with_query_params(url: str, params: Dict[str, object]) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    for key, value in params.items():
        qs[str(key)] = [str(value)]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


def _resolve_source_urls(gen_kwargs: Dict[str, object], fallback_source: object) -> List[str]:
    """Normalize source URL input to a clean list."""
    source_value = gen_kwargs.get("source")
    if source_value in (None, ""):
        source_value = fallback_source

    if isinstance(source_value, (list, tuple)):
        out: List[str] = []
        for value in source_value:
            text = str(value).strip()
            if text:
                out.append(text)
        return out

    if source_value is None:
        return []
    text = str(source_value).strip()
    return [text] if text else []


def _dedupe(values: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        key = str(value).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def _parse_zipcodes_string(value: str) -> List[str]:
    parts = re.split(r"[\s,;|]+", value.strip())
    return _dedupe([part for part in parts if part])


def _zip_from_row(row: Dict[str, object]) -> str | None:
    candidates = (
        "zip",
        "zipcode",
        "zip_code",
        "postal_code",
        "postalcode",
        "delivery zipcode",
        "physical zip",
    )
    for key in candidates:
        if key in row and row[key] is not None:
            value = str(row[key]).strip()
            if value:
                return value
    return None


def _read_zipcodes_file(path: Path, state: str | None, city: str | None) -> List[str]:
    if not path.exists():
        return []

    def _norm(value: object) -> str:
        return str(value or "").strip().upper()

    target_state = _norm(state)
    target_city = _norm(city)
    suffix = path.suffix.lower()
    rows: List[Dict[str, object]] = []
    if suffix == ".json":
        raw = json.loads(path.read_text())
        if isinstance(raw, list):
            for row in raw:
                if not isinstance(row, dict):
                    continue
                lowered = {str(k).strip().lower(): v for k, v in row.items() if k is not None}
                rows.append(lowered)
    elif suffix in {".xls", ".xlsx"}:
        try:
            import pandas as pd
        except ImportError:
            return []
        converters = {
            "DISTRICT NO": str,
            "DELIVERY ZIPCODE": str,
            "PHYSICAL ZIP": str,
            "PHYSICAL ZIP 4": str,
        }
        frame = pd.read_excel(path, header=0, converters=converters)
        for row in frame.to_dict(orient="records"):
            lowered = {str(k).strip().lower(): v for k, v in row.items() if k is not None}
            rows.append(lowered)
    else:
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                lowered = {str(k).strip().lower(): v for k, v in row.items() if k is not None}
                rows.append(lowered)

    zips: List[str] = []
    for row in rows:
        row_state = _norm(
            row.get("state")
            or row.get("mailaddress_state")
            or row.get("physical state")
        )
        row_city = _norm(
            row.get("city")
            or row.get("mailaddress_city")
            or row.get("physical city")
        )
        if target_state and row_state and row_state != target_state:
            continue
        if target_city and row_city and row_city != target_city:
            continue
        zip_value = _zip_from_row(row)
        if zip_value:
            zips.append(zip_value)
    return _dedupe(zips)


def _fetch_city_zipcodes_zippopotam(state: str, city: str, timeout: float = 10.0) -> List[str]:
    state_norm = state.strip().upper()
    city_slug = quote(city.strip())
    url = f"https://api.zippopotam.us/us/{state_norm}/{city_slug}"
    try:
        with urlopen(url, timeout=timeout) as response:  # nosec - trusted public endpoint
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    places = payload.get("places", []) if isinstance(payload, dict) else []
    zips: List[str] = []
    for place in places:
        if isinstance(place, dict):
            code = place.get("post code")
            if code:
                zips.append(str(code))
    return _dedupe(zips)


def _resolve_zipcodes(gen_kwargs: Dict[str, object], default_state: str | None) -> List[str]:
    """Resolve zipcode list from explicit kwargs, files, or inferred sources."""
    provided = gen_kwargs.get("zipcodes") or gen_kwargs.get("zip_codes")
    if isinstance(provided, list):
        return _dedupe([str(z) for z in provided])
    if isinstance(provided, str):
        parsed = _parse_zipcodes_string(provided)
        if parsed:
            return parsed

    def _norm_state(value: object) -> str | None:
        state_value = str(value or "").strip().upper()
        if not state_value or state_value in {"US", "USA", "UNITED STATES", "N/A", "NA"}:
            return None
        return state_value

    city = str(gen_kwargs.get("city") or "").strip() or None
    if city and city.upper() in {"N/A", "NA", "NONE", "NULL"}:
        city = None

    zip_file = gen_kwargs.get("zipcodes_file") or gen_kwargs.get("zip_file")
    project_root = Path(__file__).resolve().parents[2]
    zip_paths: List[Path] = []
    if zip_file:
        path = Path(str(zip_file))
        if not path.is_absolute():
            path = Path.cwd() / path
            if not path.exists():
                path = project_root / str(zip_file)
        zip_paths.append(path)
    else:
        zip_paths = [
            Path.cwd() / "static/zipcodes.csv",
            Path.cwd() / "static/zipcodes.xls",
            Path.cwd() / "static/zipcodes.xlsx",
            Path.cwd() / "static/zipcodes.json",
            project_root / "static/zipcodes.csv",
            project_root / "static/zipcodes.xls",
            project_root / "static/zipcodes.xlsx",
            project_root / "static/zipcodes.json",
        ]

    def _resolve_for_state(state: str | None) -> List[str]:
        for path in zip_paths:
            zips = _read_zipcodes_file(path, state=state, city=city)
            if zips:
                return zips

        if state and not city:
            try:
                try:
                    from foodbankscrapy.iterator import iter_zip  # type: ignore
                except Exception:
                    from iterator import iter_zip  # type: ignore

                return _dedupe([str(z) for z in iter_zip(state=state)])
            except Exception:
                pass

        if not state and not city:
            try:
                try:
                    from foodbankscrapy.iterator import iter_zip  # type: ignore
                except Exception:
                    from iterator import iter_zip  # type: ignore

                return _dedupe([str(z) for z in iter_zip()])
            except Exception:
                pass

        if state and city:
            return _fetch_city_zipcodes_zippopotam(state=state, city=city)
        return []

    states_value = gen_kwargs.get("states")
    states: List[str] = []
    if isinstance(states_value, list):
        states = [s for s in (_norm_state(v) for v in states_value) if s]
    elif isinstance(states_value, str):
        states = [s for s in (_norm_state(v) for v in _parse_zipcodes_string(states_value)) if s]

    if states:
        aggregated: List[str] = []
        for state in states:
            aggregated.extend(_resolve_for_state(state))
        return _dedupe(aggregated)

    state = _norm_state(gen_kwargs.get("state") or default_state)

    return _resolve_for_state(state)


def _build_rect_centers(
    *,
    start_lat: float,
    start_lng: float,
    width_miles: float,
    height_miles: float,
    spacing_miles: float,
) -> list[tuple[float, float]]:
    if width_miles <= 0 or height_miles <= 0 or spacing_miles <= 0:
        return [(start_lat, start_lng)]

    miles_per_lat_degree = 69.0
    cos_lat = max(0.2, math.cos(math.radians(start_lat)))
    miles_per_lng_degree = 69.0 * cos_lat

    half_w = width_miles / 2.0
    half_h = height_miles / 2.0
    epsilon = 1e-9
    points: list[tuple[float, float]] = []

    y = -half_h
    while y <= half_h + epsilon:
        x = -half_w
        while x <= half_w + epsilon:
            lat = start_lat + (y / miles_per_lat_degree)
            lng = start_lng + (x / miles_per_lng_degree)
            points.append((lat, lng))
            x += spacing_miles
        y += spacing_miles

    points.sort(
        key=lambda p: math.hypot(
            (p[0] - start_lat) * miles_per_lat_degree,
            (p[1] - start_lng) * miles_per_lng_degree,
        )
    )
    return points


def _build_rect_bboxes(
    *,
    start_lat: float,
    start_lng: float,
    coverage_width_miles: float,
    coverage_height_miles: float,
    bbox_width_miles: float,
    bbox_height_miles: float,
    overlap_miles: float,
) -> list[tuple[float, float, float, float]]:
    """Build a center-first grid of bounding boxes over a rectangular region."""
    if coverage_width_miles <= 0 or coverage_height_miles <= 0:
        return []
    if bbox_width_miles <= 0 or bbox_height_miles <= 0:
        return []

    # Keep overlap sane and below box dimensions.
    max_overlap = min(bbox_width_miles, bbox_height_miles) - 0.01
    overlap_miles = min(max(overlap_miles, 0.0), max_overlap if max_overlap > 0 else 0.0)

    step_x = max(0.01, bbox_width_miles - overlap_miles)
    step_y = max(0.01, bbox_height_miles - overlap_miles)

    half_cov_w = coverage_width_miles / 2.0
    half_cov_h = coverage_height_miles / 2.0
    half_box_w = bbox_width_miles / 2.0
    half_box_h = bbox_height_miles / 2.0

    # Build center offsets then sort by distance from the seed point.
    centers: list[tuple[float, float]] = []
    y = -half_cov_h
    eps = 1e-9
    while y <= half_cov_h + eps:
        x = -half_cov_w
        while x <= half_cov_w + eps:
            centers.append((x, y))
            x += step_x
        y += step_y

    centers.sort(key=lambda p: math.hypot(p[0], p[1]))

    miles_per_lat_degree = 69.0
    boxes: list[tuple[float, float, float, float]] = []
    for center_x, center_y in centers:
        center_lat = start_lat + (center_y / miles_per_lat_degree)
        cos_lat = max(0.2, math.cos(math.radians(center_lat)))
        miles_per_lng_degree = 69.0 * cos_lat
        center_lng = start_lng + (center_x / miles_per_lng_degree)

        lat_delta = half_box_h / miles_per_lat_degree
        lng_delta = half_box_w / miles_per_lng_degree
        boxes.append(
            (
                center_lat - lat_delta,  # min_lat
                center_lat + lat_delta,  # max_lat
                center_lng - lng_delta,  # min_lon
                center_lng + lng_delta,  # max_lon
            )
        )

    return boxes


def generate_default(ctx: JobContext) -> Iterable[scrapy.Request]:
    config = ctx.config
    headers, meta = _request_overrides(ctx)
    gen_kwargs = ctx.config.generator_kwargs or {}
    sources = _resolve_source_urls(gen_kwargs, config.source)
    if not sources:
        return []
    method = str(gen_kwargs.get("method", "GET")).upper()
    method_supports_body = _method_supports_body(method)
    params = gen_kwargs.get("params", {})
    if not isinstance(params, dict):
        params = {}
    form_data = gen_kwargs.get("form")
    body = gen_kwargs.get("body")
    json_body = _wants_json_body(gen_kwargs, headers)
    if not method_supports_body:
        form_data = None
        body = None
    elif body is not None and isinstance(body, dict):
        body = {**params, **body}
    elif body is None and params and form_data is None:
        body = dict(params)
    if body is not None and not isinstance(body, (bytes, str)):
        if json_body:
            body = json.dumps(body)
            if headers is None:
                headers = {}
            if _header_value(headers, "Content-Type") is None:
                headers["Content-Type"] = "application/json"
            if _header_value(headers, "Accept") is None:
                headers["Accept"] = "application/json"
        else:
            body = urlencode(body)
    if form_data is not None and not isinstance(form_data, dict):
        form_data = None
    if ctx.test_source_url:
        input_path = test_input_path_guess(ctx.test_source_url)
        if input_path.exists():
            return [
                scrapy.Request(
                    url=f"file://{input_path.as_posix()}",
                    headers=headers,
                    meta=meta,
                    dont_filter=True,
                )
            ]
    requests = []
    for source_url in sources:
        url = source_url
        if isinstance(url, str):
            if url.startswith("file://"):
                path_str = url.replace("file://", "", 1)
                path = Path(path_str)
                if not path.exists():
                    # handle legacy double-encoded test paths
                    from urllib.parse import unquote_plus

                    decoded = Path(unquote_plus(path_str))
                    if decoded.exists():
                        url = decoded.resolve().as_uri()
                    else:
                        parts = path_str.split("/static/tests/", 1)
                        if len(parts) == 2:
                            folder = parts[1].split("/", 1)[0]
                            encoded = unquote_plus(folder)
                            candidate = Path("static/tests") / encoded / "input.data"
                            if candidate.exists():
                                url = candidate.resolve().as_uri()
                    # handle unencoded URL folder (https:/example.com/...)
                    if not path.exists() and "/static/tests/http" in path_str:
                        parts = path_str.split("/static/tests/", 1)
                        if len(parts) == 2:
                            tail = parts[1].split("/input.data", 1)[0]
                            if tail.startswith("http"):
                                from urllib.parse import quote_plus

                                # normalize https:/ -> https:// and ensure trailing slash
                                if tail.startswith("https:/") and not tail.startswith("https://"):
                                    tail = tail.replace("https:/", "https://", 1)
                                if tail.startswith("http:/") and not tail.startswith("http://"):
                                    tail = tail.replace("http:/", "http://", 1)
                                if not tail.endswith("/"):
                                    tail = tail + "/"
                                encoded = quote_plus(tail)
                                candidate = Path("static/tests") / encoded / "input.data"
                                if candidate.exists():
                                    url = candidate.resolve().as_uri()
                requests.append(scrapy.Request(url=url, headers=headers, meta=meta, dont_filter=True))
                continue
            if "://" not in url:
                path_str = url
                if "file%3A%2F%2F%2F" in path_str:
                    from urllib.parse import unquote_plus

                    path_str = unquote_plus(path_str)
                    if path_str.startswith("file:///"):
                        path_str = path_str.replace("file://", "", 1)
                path = Path(path_str)
                if path.exists():
                    requests.append(
                        scrapy.Request(
                            url=path.resolve().as_uri(),
                            headers=headers,
                            meta=meta,
                            dont_filter=True,
                        )
                    )
                    continue

        if method_supports_body:
            url_with_params = _with_query_params(url, params) if (params and isinstance(body, (str, bytes))) else url
            if form_data is not None:
                merged_form = {**params, **form_data}
                requests.append(
                    scrapy.FormRequest(
                        url=url_with_params,
                        method=method,
                        formdata={str(k): str(v) for k, v in merged_form.items()},
                        headers=headers,
                        meta=meta,
                    )
                )
            else:
                requests.append(
                    scrapy.Request(
                        url=url_with_params,
                        method=method,
                        headers=headers,
                        body=body,
                        meta=meta,
                    )
                )
        else:
            final_url = _with_query_params(url, params) if params else url
            requests.append(scrapy.Request(url=final_url, method=method, headers=headers, meta=meta))
    return requests


def generate_accessfood(ctx: JobContext) -> Iterable[scrapy.Request]:
    gen_kwargs = ctx.config.generator_kwargs or {}
    extra_params = gen_kwargs.get("params", {})
    if not isinstance(extra_params, dict):
        extra_params = {}
    params = {**(ctx.config.accessfood_params or {}), **extra_params}
    url = accessfood_url(ctx, params)
    headers, meta = _request_overrides(ctx)
    return [scrapy.Request(url=url, headers=headers, meta=meta)]


def generate_box_search(ctx: JobContext) -> Iterable[scrapy.Request]:
    config = ctx.config
    if not config.source:
        return []
    headers, meta = _request_overrides(ctx)
    gen_kwargs = ctx.config.generator_kwargs or {}
    request_method = str(gen_kwargs.get("request_method", gen_kwargs.get("method", "GET"))).upper()
    method_supports_body = _method_supports_body(request_method)

    lat_val = gen_kwargs.get("start_lat", ctx.config.raw.get("MailAddress_Latitude"))
    lng_val = gen_kwargs.get("start_lng", ctx.config.raw.get("MailAddress_Longitude"))
    try:
        start_lat = float(lat_val)
        start_lng = float(lng_val)
    except (TypeError, ValueError):
        return []

    width_miles = float(gen_kwargs.get("coverage_width_miles", 300))
    height_miles = float(gen_kwargs.get("coverage_height_miles", 300))
    grid_spacing = float(gen_kwargs.get("grid_spacing_miles", 20))
    query_radius = float(gen_kwargs.get("query_radius_miles", 20))

    lat_param = str(gen_kwargs.get("lat_param", "lat"))
    lng_param = str(gen_kwargs.get("lng_param", "lng"))
    radius_param = str(gen_kwargs.get("radius_param", "radius"))
    extra_params = gen_kwargs.get("params", {})
    if not isinstance(extra_params, dict):
        extra_params = {}
    form_template = gen_kwargs.get("form")
    if not isinstance(form_template, dict):
        form_template = {}
    dont_filter = bool(gen_kwargs.get("dont_filter", True))

    centers = _build_rect_centers(
        start_lat=start_lat,
        start_lng=start_lng,
        width_miles=width_miles,
        height_miles=height_miles,
        spacing_miles=grid_spacing,
    )
    requests = []
    for lat, lng in centers:
        request_params = {
            **extra_params,
            lat_param: lat,
            lng_param: lng,
            radius_param: query_radius,
        }
        if request_method == "GET" or not method_supports_body:
            url = _with_query_params(config.source, request_params)
            requests.append(
                scrapy.Request(
                    url=url,
                    method=request_method,
                    headers=headers,
                    meta=meta,
                    dont_filter=dont_filter,
                )
            )
            continue

        merged_form = {**form_template, **request_params}
        requests.append(
            scrapy.FormRequest(
                url=config.source,
                method=request_method,
                formdata={str(k): str(v) for k, v in merged_form.items()},
                headers=headers,
                meta=meta,
                dont_filter=dont_filter,
            )
        )
    return requests


def generate_bbox_search(ctx: JobContext) -> Iterable[scrapy.Request]:
    """Generate requests over min/max-lat/lon bounding boxes."""
    config = ctx.config
    if not config.source:
        return []

    headers, meta = _request_overrides(ctx)
    gen_kwargs = ctx.config.generator_kwargs or {}
    request_method = str(gen_kwargs.get("request_method", gen_kwargs.get("method", "GET"))).upper()
    method_supports_body = _method_supports_body(request_method)

    lat_val = gen_kwargs.get("start_lat", ctx.config.raw.get("MailAddress_Latitude"))
    lng_val = gen_kwargs.get("start_lng", ctx.config.raw.get("MailAddress_Longitude"))
    try:
        start_lat = float(lat_val)
        start_lng = float(lng_val)
    except (TypeError, ValueError):
        start_lat = 39.8283
        start_lng = -98.5795

    coverage_width = float(gen_kwargs.get("coverage_width_miles", 300))
    coverage_height = float(gen_kwargs.get("coverage_height_miles", 300))
    bbox_width = float(gen_kwargs.get("bbox_width_miles", 75))
    bbox_height = float(gen_kwargs.get("bbox_height_miles", 75))
    overlap = float(gen_kwargs.get("bbox_overlap_miles", 10))

    min_lat_param = str(gen_kwargs.get("min_lat_param", "min_lat"))
    max_lat_param = str(gen_kwargs.get("max_lat_param", "max_lat"))
    min_lon_param = str(gen_kwargs.get("min_lon_param", "min_lon"))
    max_lon_param = str(gen_kwargs.get("max_lon_param", "max_lon"))

    extra_params = gen_kwargs.get("params", {})
    if not isinstance(extra_params, dict):
        extra_params = {}
    refresh_time_per_request = bool(gen_kwargs.get("refresh_time_per_request", False))
    time_param = str(gen_kwargs.get("time_param", "_time")).strip() or "_time"
    request_counter = 0
    form_template = gen_kwargs.get("form")
    if not isinstance(form_template, dict):
        form_template = {}
    dont_filter = bool(gen_kwargs.get("dont_filter", True))
    state_param = str(gen_kwargs.get("state_param", "")).strip()

    state_padding_miles = float(gen_kwargs.get("state_padding_miles", 0))

    region_specs: list[tuple[str | None, float, float, float, float]] = []
    requested_states = gen_kwargs.get("states")
    states: list[str] = []
    if isinstance(requested_states, list):
        states = [str(v).strip().upper() for v in requested_states if str(v).strip()]
    elif isinstance(requested_states, str) and requested_states.strip():
        states = [s for s in _parse_zipcodes_string(requested_states.upper()) if s]

    if states:
        for state_code in states:
            bounds = US_STATE_BOUNDS.get(state_code)
            if not bounds:
                continue
            min_lat, max_lat, min_lon, max_lon = bounds
            region_specs.append((state_code, min_lat, max_lat, min_lon, max_lon))

    if not region_specs:
        miles_per_lat_degree = 69.0
        cos_lat = max(0.2, math.cos(math.radians(start_lat)))
        miles_per_lng_degree = 69.0 * cos_lat
        half_lat = (coverage_height / miles_per_lat_degree) / 2.0
        half_lng = (coverage_width / miles_per_lng_degree) / 2.0
        region_specs.append(
            (
                None,
                start_lat - half_lat,
                start_lat + half_lat,
                start_lng - half_lng,
                start_lng + half_lng,
            )
        )

    if not region_specs:
        return []

    requests = []
    for state_code, state_min_lat, state_max_lat, state_min_lon, state_max_lon in region_specs:
        center_lat = (state_min_lat + state_max_lat) / 2.0
        center_lng = (state_min_lon + state_max_lon) / 2.0
        miles_per_lat_degree = 69.0
        cos_lat = max(0.2, math.cos(math.radians(center_lat)))
        miles_per_lng_degree = 69.0 * cos_lat

        state_width_miles = max(
            bbox_width,
            abs(state_max_lon - state_min_lon) * miles_per_lng_degree + (2 * state_padding_miles),
        )
        state_height_miles = max(
            bbox_height,
            abs(state_max_lat - state_min_lat) * miles_per_lat_degree + (2 * state_padding_miles),
        )

        boxes = _build_rect_bboxes(
            start_lat=center_lat,
            start_lng=center_lng,
            coverage_width_miles=state_width_miles,
            coverage_height_miles=state_height_miles,
            bbox_width_miles=bbox_width,
            bbox_height_miles=bbox_height,
            overlap_miles=overlap,
        )

        for min_lat, max_lat, min_lon, max_lon in boxes:
            # Clamp to the region/state bounds to reduce spillover.
            min_lat = max(min_lat, state_min_lat)
            max_lat = min(max_lat, state_max_lat)
            min_lon = max(min_lon, state_min_lon)
            max_lon = min(max_lon, state_max_lon)
            if min_lat >= max_lat or min_lon >= max_lon:
                continue

            request_params = {
                **extra_params,
                min_lat_param: min_lat,
                max_lat_param: max_lat,
                min_lon_param: min_lon,
                max_lon_param: max_lon,
            }
            if refresh_time_per_request:
                request_params[time_param] = str(int(time.time() * 1000) + request_counter)
                request_counter += 1
            if state_param and state_code:
                request_params[state_param] = state_code

            request_meta = dict(meta)
            if state_code:
                request_meta["_bbox_state"] = state_code

            if request_method == "GET" or not method_supports_body:
                url = _with_query_params(config.source, request_params)
                requests.append(
                    scrapy.Request(
                        url=url,
                        method=request_method,
                        headers=headers,
                        meta=request_meta,
                        dont_filter=dont_filter,
                    )
                )
                continue

            merged_form = {**form_template, **request_params}
            requests.append(
                scrapy.FormRequest(
                    url=config.source,
                    method=request_method,
                    formdata={str(k): str(v) for k, v in merged_form.items()},
                    headers=headers,
                    meta=request_meta,
                    dont_filter=dont_filter,
                )
            )
    return requests


def generate_zip_iterator(ctx: JobContext) -> Iterable[scrapy.Request]:
    config = ctx.config
    if not config.source:
        return []

    gen_kwargs = ctx.config.generator_kwargs or {}
    headers, meta = _request_overrides(ctx)

    zipcodes = _resolve_zipcodes(gen_kwargs, default_state=ctx.config.state)
    max_zipcodes = gen_kwargs.get("max_zipcodes")
    if max_zipcodes is not None:
        try:
            zipcodes = zipcodes[: int(max_zipcodes)]
        except (TypeError, ValueError):
            pass
    if not zipcodes:
        return []

    zip_param = str(gen_kwargs.get("zip_param", "zip"))
    request_method = str(gen_kwargs.get("request_method", gen_kwargs.get("method", "GET"))).upper()
    request_method_supports_body = _method_supports_body(request_method)
    request_url = str(gen_kwargs.get("url") or config.source)
    extra_params = gen_kwargs.get("params", {})
    if not isinstance(extra_params, dict):
        extra_params = {}
    form_template = gen_kwargs.get("form")
    if not isinstance(form_template, dict):
        form_template = {}
    dont_filter = bool(gen_kwargs.get("dont_filter", True))

    requests = []
    for zipcode in zipcodes:
        if request_method == "GET" or not request_method_supports_body:
            url = _with_query_params(
                request_url,
                {
                    **extra_params,
                    zip_param: zipcode,
                },
            )
            requests.append(
                scrapy.Request(
                    url=url,
                    method=request_method,
                    headers=headers,
                    meta=meta,
                    dont_filter=dont_filter,
                )
            )
            continue

        form_data: Dict[str, str] = {}
        for key, value in {**extra_params, **form_template}.items():
            if isinstance(value, str):
                form_data[str(key)] = value.replace("{zip}", str(zipcode))
            else:
                form_data[str(key)] = str(value)
        form_data[zip_param] = str(zipcode)
        requests.append(
            scrapy.FormRequest(
                url=request_url,
                method=request_method,
                formdata=form_data,
                headers=headers,
                meta=meta,
                dont_filter=dont_filter,
            )
        )
    return requests


GENERATOR_REGISTRY = {
    "default": generate_default,
    "url": generate_default,
    "accessfood": generate_accessfood,
    "box_search": generate_box_search,
    "bbox_search": generate_bbox_search,
    "zip_iterator": generate_zip_iterator,
}
