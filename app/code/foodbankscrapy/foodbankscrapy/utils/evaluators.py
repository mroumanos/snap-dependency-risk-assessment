"""Evaluator functions that generate follow-up requests from responses."""

from __future__ import annotations

import json
import math
import re
import time
import base64
import hashlib
import zlib
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
try:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ModuleNotFoundError:  # pragma: no cover
    padding = None
    Cipher = None
    algorithms = None
    modes = None

from .context import JobContext
from .generators import MINIMAL_HEADERS, REALISTIC_HEADERS, _method_supports_body
from .test_capture import test_dir_for_url, test_page_inputs


def evaluate_none(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    """Evaluator that intentionally emits no additional requests."""
    return []


def _resolve_headers(eval_kwargs: Dict[str, object], gen_kwargs: Dict[str, object]) -> Dict[str, str] | None:
    headers = eval_kwargs.get("headers")
    if headers is None:
        headers = gen_kwargs.get("headers")
        if headers is None and gen_kwargs.get("use_minimal_headers"):
            headers = dict(MINIMAL_HEADERS)
    if headers is None and gen_kwargs.get("use_realistic_headers"):
        headers = dict(REALISTIC_HEADERS)
    return headers


def _with_query_params(url: str, params: Dict[str, object]) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    for key, value in params.items():
        qs[str(key)] = [str(value)]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


def evaluate_zip_iterator(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    """Deprecated compatibility evaluator retained for old pipeline rows."""
    spider.logger.warning(
        "[deprecation] evaluator 'zip_iterator' is deprecated for org_id=%s; "
        "use generator.name='zip_iterator' with evaluator.name='default'",
        ctx.config.organization_id,
    )
    return []


def _build_box_centers(
    *,
    start_lat: float,
    start_lng: float,
    coverage_radius_miles: float,
    query_radius_miles: float,
    overlap_ratio: float,
) -> List[tuple[float, float]]:
    # For circle coverage with square-grid centers, spacing <= r*sqrt(2) avoids gaps.
    # We apply overlap_ratio to add safety against boundary and rounding errors.
    overlap = max(0.0, min(0.9, overlap_ratio))
    spacing_miles = query_radius_miles * math.sqrt(2.0) * (1.0 - overlap)
    if spacing_miles <= 0:
        spacing_miles = max(query_radius_miles * 0.5, 0.1)

    centers: List[tuple[float, float, float]] = []
    y = -coverage_radius_miles
    epsilon = 1e-9
    while y <= coverage_radius_miles + epsilon:
        x = -coverage_radius_miles
        while x <= coverage_radius_miles + epsilon:
            # Keep only centers inside requested coverage circle.
            if math.hypot(x, y) <= coverage_radius_miles + epsilon:
                lat, lng = _offset_point_miles(start_lat, start_lng, x, y)
                centers.append((math.hypot(x, y), lat, lng))
            x += spacing_miles
        y += spacing_miles

    centers.sort(key=lambda item: item[0])
    return [(lat, lng) for _, lat, lng in centers]


def _build_rect_centers(
    *,
    start_lat: float,
    start_lng: float,
    width_miles: float,
    height_miles: float,
    spacing_miles: float,
) -> List[tuple[float, float]]:
    if width_miles <= 0 or height_miles <= 0 or spacing_miles <= 0:
        return [(start_lat, start_lng)]

    half_w = width_miles / 2.0
    half_h = height_miles / 2.0
    epsilon = 1e-9
    points: List[tuple[float, float]] = []

    y = -half_h
    while y <= half_h + epsilon:
        x = -half_w
        while x <= half_w + epsilon:
            lat, lng = _offset_point_miles(start_lat, start_lng, x, y)
            points.append((lat, lng))
            x += spacing_miles
        y += spacing_miles

    # Start near center first for faster useful early coverage.
    points.sort(
        key=lambda p: _great_circle_miles(start_lat, start_lng, p[0], p[1])
    )
    return points


def _destination_point(lat: float, lng: float, bearing_deg: float, distance_miles: float) -> tuple[float, float]:
    # Great-circle forward geodesic on a spherical earth.
    # This is more accurate than fixed miles-per-degree conversion.
    earth_radius_miles = 3958.7613
    if distance_miles == 0:
        return lat, lng
    lat1 = math.radians(lat)
    lon1 = math.radians(lng)
    brng = math.radians(bearing_deg)
    ang_dist = distance_miles / earth_radius_miles

    sin_lat1 = math.sin(lat1)
    cos_lat1 = math.cos(lat1)
    sin_ang = math.sin(ang_dist)
    cos_ang = math.cos(ang_dist)

    lat2 = math.asin(sin_lat1 * cos_ang + cos_lat1 * sin_ang * math.cos(brng))
    lon2 = lon1 + math.atan2(
        math.sin(brng) * sin_ang * cos_lat1,
        cos_ang - sin_lat1 * math.sin(lat2),
    )
    # normalize longitude to [-180, 180]
    lon2 = (lon2 + 3 * math.pi) % (2 * math.pi) - math.pi
    return math.degrees(lat2), math.degrees(lon2)


def _offset_point_miles(start_lat: float, start_lng: float, east_miles: float, north_miles: float) -> tuple[float, float]:
    lat, lng = start_lat, start_lng
    if north_miles > 0:
        lat, lng = _destination_point(lat, lng, 0.0, north_miles)
    elif north_miles < 0:
        lat, lng = _destination_point(lat, lng, 180.0, abs(north_miles))

    if east_miles > 0:
        lat, lng = _destination_point(lat, lng, 90.0, east_miles)
    elif east_miles < 0:
        lat, lng = _destination_point(lat, lng, 270.0, abs(east_miles))

    return lat, lng


def _great_circle_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth_radius_miles = 3958.7613
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return earth_radius_miles * c


def evaluate_box_search(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    """Fan out a search grid over a region around the configured origin point."""
    if response.meta.get("_box_search_enqueued"):
        return []

    eval_kwargs = ctx.config.evaluator_kwargs or {}
    gen_kwargs = ctx.config.generator_kwargs or {}
    headers = _resolve_headers(eval_kwargs, gen_kwargs)

    lat_val = eval_kwargs.get("start_lat", ctx.config.raw.get("MailAddress_Latitude"))
    lng_val = eval_kwargs.get("start_lng", ctx.config.raw.get("MailAddress_Longitude"))
    try:
        start_lat = float(lat_val)
        start_lng = float(lng_val)
    except (TypeError, ValueError):
        spider.logger.info("[box_search] missing start_lat/start_lng for org_id=%s", ctx.config.organization_id)
        return []

    try:
        query_radius = float(eval_kwargs.get("query_radius_miles", eval_kwargs.get("radius_miles", 10)))
    except (TypeError, ValueError):
        query_radius = 10.0
    try:
        coverage_radius = float(eval_kwargs.get("coverage_radius_miles", query_radius))
    except (TypeError, ValueError):
        coverage_radius = query_radius
    try:
        overlap_ratio = float(eval_kwargs.get("overlap_ratio", 0.10))
    except (TypeError, ValueError):
        overlap_ratio = 0.10

    if query_radius <= 0 or coverage_radius < 0:
        return []

    width_miles = eval_kwargs.get("coverage_width_miles")
    height_miles = eval_kwargs.get("coverage_height_miles")
    grid_spacing = eval_kwargs.get("grid_spacing_miles")
    try:
        width_miles = float(width_miles) if width_miles is not None else None
        height_miles = float(height_miles) if height_miles is not None else None
        grid_spacing = float(grid_spacing) if grid_spacing is not None else None
    except (TypeError, ValueError):
        width_miles = None
        height_miles = None
        grid_spacing = None

    if width_miles and height_miles and grid_spacing:
        centers = _build_rect_centers(
            start_lat=start_lat,
            start_lng=start_lng,
            width_miles=width_miles,
            height_miles=height_miles,
            spacing_miles=grid_spacing,
        )
    else:
        centers = _build_box_centers(
            start_lat=start_lat,
            start_lng=start_lng,
            coverage_radius_miles=coverage_radius,
            query_radius_miles=query_radius,
            overlap_ratio=overlap_ratio,
        )
    if not centers:
        centers = [(start_lat, start_lng)]

    lat_param = str(eval_kwargs.get("lat_param", "lat"))
    lng_param = str(eval_kwargs.get("lng_param", "lng"))
    radius_param = str(eval_kwargs.get("radius_param", "radius"))
    extra_params = eval_kwargs.get("params", {})
    if not isinstance(extra_params, dict):
        extra_params = {}
    dont_filter = bool(eval_kwargs.get("dont_filter", True))

    requests = []
    for lat, lng in centers:
        url = _with_query_params(
            ctx.config.source,
            {
                **extra_params,
                lat_param: lat,
                lng_param: lng,
                radius_param: query_radius,
            },
        )
        requests.append(
            scrapy.Request(
                url=url,
                headers=headers,
                callback=spider.parse,
                dont_filter=dont_filter,
                meta={"_ctx": ctx, "_box_search_enqueued": True},
            )
        )

    spider.logger.info(
        "[box_search] org_id=%s enqueued=%s query_radius_miles=%s coverage_radius_miles=%s",
        ctx.config.organization_id,
        len(requests),
        query_radius,
        coverage_radius,
    )
    return requests


def evaluate_accessfood_pagination(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    """Generate additional AccessFood page requests from page-1 metadata."""
    if response.meta.get("_accessfood_pagination_enqueued"):
        return []

    data = response.json()
    locations = data.get("item1", [])
    current_page = data.get("item3")
    total_items = data.get("item5")
    if not locations:
        return []
    eval_kwargs = ctx.config.evaluator_kwargs or {}
    method = str(eval_kwargs.get("method", getattr(response.request, "method", "GET"))).upper()
    method_supports_body = _method_supports_body(method)
    headers = eval_kwargs.get("headers") or getattr(response.request, "headers", None)
    body = eval_kwargs.get("body")
    if body is None and method_supports_body:
        body = getattr(response.request, "body", b"")
    retry = eval_kwargs.get("max_retry_times", eval_kwargs.get("retry_times"))
    page_param = str(eval_kwargs.get("page_param", "page"))
    dont_filter = bool(eval_kwargs.get("dont_filter", True))

    spider.logger.warning(
        f"AF pagination: current page={current_page}, total={total_items}, locs={str(len(locations))}"
    )
    try:
        current_page_i = int(current_page)
    except (TypeError, ValueError):
        return []
    # Only emit follow-on pagination from page 1 response.
    if current_page_i != 0:
        return []

    try:
        total_pages = int(math.ceil(float(total_items) / float(len(locations))))
    except (TypeError, ValueError, ZeroDivisionError):
        return []
    if total_pages <= 1:
        return []

    requests = []
    for page in range(2, total_pages + 1):
        meta = {"_ctx": ctx, "_accessfood_pagination_enqueued": True}
        if retry is not None:
            meta["max_retry_times"] = int(retry)

        if method == "GET" or not method_supports_body:
            base_url = getattr(response.request, "url", response.url)
            parsed = urlparse(base_url)
            query = parse_qs(parsed.query, keep_blank_values=True)
            query[page_param] = [str(page)]
            next_url = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
            requests.append(
                scrapy.Request(
                    url=next_url,
                    method=method,
                    headers=headers,
                    callback=spider.parse,
                    meta=meta,
                    dont_filter=dont_filter,
                )
            )
            continue

        next_body = body
        if isinstance(body, (bytes, bytearray)):
            decoded = bytes(body).decode("utf-8", errors="replace")
        else:
            decoded = str(body or "")

        updated = False
        if decoded:
            parsed_body = parse_qs(decoded, keep_blank_values=True) if ("=" in decoded or "&" in decoded) else {}
            if parsed_body:
                parsed_body[page_param] = [str(page)]
                next_body = urlencode(parsed_body, doseq=True)
                updated = True
            if not updated:
                try:
                    json_body = json.loads(decoded)
                    if isinstance(json_body, dict):
                        json_body[page_param] = page
                        next_body = json.dumps(json_body)
                        updated = True
                except Exception:
                    pass
        if not updated:
            next_body = urlencode({page_param: page})

        requests.append(
            scrapy.Request(
                url=getattr(response.request, "url", response.url),
                method=method,
                headers=headers,
                body=next_body,
                callback=spider.parse,
                meta=meta,
                dont_filter=dont_filter,
            )
        )
    return requests


def evaluate_fwp_pagination(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    import logging
    logger = logging.getLogger(__name__)
    eval_kwargs = ctx.config.evaluator_kwargs or {}
    param = str(eval_kwargs.get("param", "fwp_paged"))
    headers = eval_kwargs.get("headers")
    if headers is None:
        gen_kwargs = ctx.config.generator_kwargs or {}
        headers = gen_kwargs.get("headers")
        if headers is None and gen_kwargs.get("use_minimal_headers"):
            headers = dict(MINIMAL_HEADERS)
    if headers is None and gen_kwargs.get("use_realistic_headers"):
        headers = dict(REALISTIC_HEADERS)

    if ctx.test_source_url:
        requests = []
        base = test_dir_for_url(ctx.test_source_url)
        page_paths = test_page_inputs(ctx.test_source_url)
        if not page_paths:
            # legacy double-encoded folder
            from urllib.parse import quote_plus

            base = base.parent / quote_plus(base.name)
            page_paths = sorted(base.glob("input_*.data"))
        for path in page_paths:
            requests.append(
                scrapy.Request(
                    url=f"file://{path.as_posix()}",
                    headers=headers,
                    callback=spider.parse,
                    meta={"_ctx": ctx},
                    dont_filter=True,
                )
            )
        return requests

    try:
        # Scan full HTML because the FWP_JSON script is not always the first <script>
        fwp_text = _extract_fwp_json_text(response.text or "")
        fwp_json = json.loads(fwp_text) if fwp_text else {}
        last_page_num = (
            fwp_json.get("preload_data", {})
            .get("settings", {})
            .get("pager", {})
            .get("total_pages")
        )
    except Exception:
        last_page_num = None
    logger.info("[fwp] url=%s total_pages=%s (FWP_JSON)", response.url, last_page_num)

    parsed = urlparse(response.url)
    qs = parse_qs(parsed.query)
    try:
        current = int(qs.get(param, ["1"])[0])
    except (TypeError, ValueError):
        current = 1
    try:
        last_page_num = int(last_page_num)
    except (TypeError, ValueError):
        return []
    if current >= last_page_num:
        return []

    requests = []
    for page in range(current + 1, last_page_num + 1):
        qs[param] = [str(page)]
        next_url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
        requests.append(
            scrapy.Request(
                url=next_url,
                headers=headers,
                callback=spider.parse,
                meta={"_ctx": ctx},
            )
        )
    return requests


def evaluate_dcms_pagination(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    import logging

    logger = logging.getLogger(__name__)
    def _log(msg: str, *args: object) -> None:
        try:
            logger.info(msg, *args)
        except Exception:
            pass
    eval_kwargs = ctx.config.evaluator_kwargs or {}
    page_param = str(eval_kwargs.get("page_param", "number"))

    total_results = None
    text_match = None
    # First try the element text directly
    text_candidate = response.css(".search-result::text").get()
    if text_candidate:
        m = re.search(r"(\d+)", text_candidate)
        if m:
            total_results = int(m.group(1))
            text_match = True
    # Fallback to regex in whole HTML
    if total_results is None:
        total_match = re.search(
            r"(\d+)\s*results",
            response.text or "",
            re.IGNORECASE,
        )
        total_results = int(total_match.group(1)) if total_match else None
        text_match = bool(total_match)
    if not total_results:
        _log("[dcms] total_results missing for url=%s", response.url)
        return []
    per_page = len(response.css("#data-container-find .modal.fade.modal-map"))
    if not per_page:
        per_page = 30
    total_pages = (total_results + per_page - 1) // per_page
    _log(
        "[dcms] url=%s total_results=%s per_page=%s total_pages=%s",
        response.url,
        total_results,
        per_page,
        total_pages,
    )

    current_page = 1
    if response.request and response.request.body:
        try:
            body_qs = parse_qs(response.request.body.decode("utf-8"))
            current_page = int(body_qs.get(page_param, ["1"])[0])
        except Exception:
            current_page = 1

    _log("[dcms] url=%s current_page=%s", response.url, current_page)
    if current_page >= total_pages:
        return []

    gen_kwargs = ctx.config.generator_kwargs or {}
    form_data = gen_kwargs.get("form", {})
    if not isinstance(form_data, dict):
        form_data = {}
    headers = eval_kwargs.get("headers")
    if headers is None:
        headers = gen_kwargs.get("headers")
        if headers is None and gen_kwargs.get("use_minimal_headers"):
            headers = dict(MINIMAL_HEADERS)
    if headers is None and gen_kwargs.get("use_realistic_headers"):
        headers = dict(REALISTIC_HEADERS)

    # Enqueue only the next page to avoid a burst of concurrent POSTs.
    next_page = current_page + 1
    if next_page > total_pages:
        return []
    page_form = {str(k): str(v) for k, v in form_data.items()}
    page_form[page_param] = str(next_page)
    meta = {"_ctx": ctx}
    request_delay = gen_kwargs.get("request_delay", gen_kwargs.get("requestDelay"))
    if request_delay is not None:
        try:
            meta["request_delay"] = float(request_delay)
        except (TypeError, ValueError):
            pass
    _log("[dcms] enqueue page=%s url=%s", next_page, ctx.config.source)
    return [
        scrapy.FormRequest(
            url=ctx.config.source,
            method="POST",
            formdata=page_form,
            headers=headers,
            callback=spider.parse,
            meta=meta,
        )
    ]


def _extract_by_path(data: object, path: List[str]) -> object:
    current = data
    for part in path:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                idx = int(part)
            except (TypeError, ValueError):
                return None
            if idx < 0 or idx >= len(current):
                return None
            current = current[idx]
        else:
            return None
    return current


def _extract_requested_page(response: scrapy.http.Response, page_param: str) -> int | None:
    try:
        if response.request and response.request.method == "GET":
            parsed = urlparse(response.request.url)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            if page_param in qs and qs[page_param]:
                return int(qs[page_param][0])
        if response.request and response.request.body:
            body_qs = parse_qs(response.request.body.decode("utf-8"))
            if page_param in body_qs and body_qs[page_param]:
                return int(body_qs[page_param][0])
    except Exception:
        return None
    return None


def evaluate_json_pagination(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    debug = bool((ctx.config.evaluator_kwargs or {}).get("debug"))
    if response.meta.get("_json_pagination_enqueued"):
        if debug:
            spider.logger.info(
                "[json_pagination] org_id=%s skip re-enqueue for paginated response url=%s",
                ctx.config.organization_id,
                response.url,
            )
        return []

    eval_kwargs = ctx.config.evaluator_kwargs or {}
    gen_kwargs = ctx.config.generator_kwargs or {}
    headers = _resolve_headers(eval_kwargs, gen_kwargs)

    total_path = eval_kwargs.get("total_pages_path", ["data", "pagination", "total_pages"])
    current_path = eval_kwargs.get("current_page_path", ["data", "pagination", "current_page"])
    if not isinstance(total_path, list) or not isinstance(current_path, list):
        if debug:
            spider.logger.info(
                "[json_pagination] org_id=%s invalid paths total_path=%r current_path=%r",
                ctx.config.organization_id,
                total_path,
                current_path,
            )
        return []

    data = response.json()
    total_pages = _extract_by_path(data, [str(v) for v in total_path])
    current_page = _extract_by_path(data, [str(v) for v in current_path])
    if debug:
        spider.logger.info(
            "[json_pagination] org_id=%s extracted total_pages=%r current_page=%r total_path=%s current_path=%s",
            ctx.config.organization_id,
            total_pages,
            current_page,
            total_path,
            current_path,
        )
    try:
        total_pages_i = int(total_pages)
        current_page_i = int(current_page)
    except (TypeError, ValueError):
        if debug:
            spider.logger.info(
                "[json_pagination] org_id=%s could not cast pagination values total_pages=%r current_page=%r",
                ctx.config.organization_id,
                total_pages,
                current_page,
            )
        return []
    page_param = str(eval_kwargs.get("page_param", "page"))
    requested_page_i = _extract_requested_page(response, page_param)
    if debug:
        body_preview = None
        if response.request and response.request.body:
            try:
                body_preview = response.request.body.decode("utf-8")
            except Exception:
                body_preview = "<decode-failed>"
        spider.logger.info(
            "[json_pagination] org_id=%s method=%s page_param=%s requested_page=%r request_url=%s request_body=%r",
            ctx.config.organization_id,
            getattr(response.request, "method", None),
            page_param,
            requested_page_i,
            getattr(response.request, "url", response.url),
            body_preview,
        )
    if requested_page_i is not None and requested_page_i > current_page_i:
        spider.logger.info(
            "[json_pagination] org_id=%s response_current_page=%s requested_page=%s; using requested_page",
            ctx.config.organization_id,
            current_page_i,
            requested_page_i,
        )
        current_page_i = requested_page_i
    if current_page_i >= total_pages_i:
        return []

    method = str(eval_kwargs.get("method", getattr(response.request, "method", "POST"))).upper()
    dont_filter = bool(eval_kwargs.get("dont_filter", True))
    pages_to_enqueue = list(range(current_page_i + 1, total_pages_i + 1))
    if not pages_to_enqueue:
        if debug:
            spider.logger.info(
                "[json_pagination] org_id=%s no pages to enqueue current=%s total=%s",
                ctx.config.organization_id,
                current_page_i,
                total_pages_i,
            )
        return []

    if method == "GET":
        requests = []
        for next_page in pages_to_enqueue:
            url = _with_query_params(response.url, {page_param: next_page})
            requests.append(
                scrapy.Request(
                    url=url,
                    headers=headers,
                    callback=spider.parse,
                    dont_filter=dont_filter,
                    meta={
                        "_ctx": ctx,
                        "_json_pagination_sent_page": next_page,
                        "_json_pagination_enqueued": True,
                    },
                )
            )
        spider.logger.info(
            "[json_pagination] org_id=%s enqueue pages=%s..%s total=%s",
            ctx.config.organization_id,
            pages_to_enqueue[0],
            pages_to_enqueue[-1],
            len(pages_to_enqueue),
        )
        return requests

    form_data: Dict[str, str] = {}
    if response.request and response.request.body:
        try:
            body_qs = parse_qs(response.request.body.decode("utf-8"))
            form_data = {str(k): str(v[0]) for k, v in body_qs.items() if v}
        except Exception:
            form_data = {}
    if not form_data:
        base_form = gen_kwargs.get("form", {})
        if isinstance(base_form, dict):
            form_data = {str(k): str(v) for k, v in base_form.items()}
    meta = {"_ctx": ctx}
    request_delay = gen_kwargs.get("request_delay", gen_kwargs.get("requestDelay"))
    if request_delay is not None:
        try:
            meta["request_delay"] = float(request_delay)
        except (TypeError, ValueError):
            pass
    spider.logger.info(
        "[json_pagination] org_id=%s enqueue pages=%s..%s total=%s",
        ctx.config.organization_id,
        pages_to_enqueue[0],
        pages_to_enqueue[-1],
        len(pages_to_enqueue),
    )
    requests = []
    for next_page in pages_to_enqueue:
        page_form = dict(form_data)
        page_form[page_param] = str(next_page)
        requests.append(
            scrapy.FormRequest(
            url=ctx.config.source or response.url,
            method="POST",
            formdata=page_form,
            headers=headers,
            callback=spider.parse,
            dont_filter=dont_filter,
            meta={
                **meta,
                "_json_pagination_sent_page": next_page,
                "_json_pagination_enqueued": True,
            },
        )
        )
    return requests


def _extract_fwp_json_text(html: str) -> str | None:
    """Extract the JSON text assigned to window.FWP_JSON, if present."""
    marker = "window.FWP_JSON"
    idx = html.find(marker)
    if idx == -1:
        return None
    start = html.find("{", idx)
    if start == -1:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(html)):
        ch = html[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "\"":
                in_string = False
        else:
            if ch == "\"":
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return html[start : i + 1]
    return None


def evaluate_whyhunger_pagination(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    eval_kwargs = ctx.config.evaluator_kwargs or {}
    page_param = str(eval_kwargs.get("page_param", "page"))
    dont_filter = bool(eval_kwargs.get("dont_filter", True))

    # Only enqueue follow-on pages from the first page response.
    current_page = _extract_requested_page(response, page_param) or 1
    if current_page > 1:
        return []

    per_page_raw = eval_kwargs.get("per_page", 15)
    try:
        per_page = int(per_page_raw)
    except (TypeError, ValueError):
        per_page = 15
    if per_page <= 0:
        per_page = 15

    title_text = " ".join(
        t.strip()
        for t in response.xpath(
            "//div[contains(concat(' ', normalize-space(@class), ' '), ' organisations ')]"
            "//div[contains(concat(' ', normalize-space(@class), ' '), ' title ')]//text()"
        ).getall()
        if t and t.strip()
    )
    if not title_text:
        return []

    total_match = re.search(r"(\d[\d,]*)", title_text)
    if not total_match:
        return []
    try:
        total_results = int(total_match.group(1).replace(",", ""))
    except (TypeError, ValueError):
        return []
    if total_results <= per_page:
        return []

    total_pages = int(math.ceil(total_results / float(per_page)))
    if total_pages <= 1:
        return []

    requests: List[scrapy.Request] = []
    for page in range(2, total_pages + 1):
        next_meta = dict(response.request.meta or {})
        next_meta["_whyhunger_page"] = page
        if (response.request.method or "GET").upper() == "GET":
            next_url = _with_query_params(response.request.url, {page_param: page})
            requests.append(
                response.request.replace(
                    url=next_url,
                    meta=next_meta,
                    dont_filter=dont_filter,
                )
            )
            continue

        body_text = ""
        body = response.request.body or b""
        if isinstance(body, bytes):
            body_text = body.decode("utf-8", errors="replace")
        else:
            body_text = str(body)

        updated_body = None
        if body_text:
            body_qs = parse_qs(body_text, keep_blank_values=True)
            if body_qs:
                body_qs[page_param] = [str(page)]
                updated_body = urlencode(body_qs, doseq=True)
            else:
                try:
                    body_json = json.loads(body_text)
                    if isinstance(body_json, dict):
                        body_json[page_param] = page
                        updated_body = json.dumps(body_json)
                except Exception:
                    updated_body = None
        if updated_body is None:
            updated_body = urlencode({page_param: page})
        requests.append(
            response.request.replace(
                body=updated_body,
                meta=next_meta,
                dont_filter=dont_filter,
            )
        )

    return requests


def _foodfinder_md5_hex_timestamp(ts: str) -> str:
    return hashlib.md5(str(ts).encode("utf-8")).hexdigest()


def _foodfinder_evp_bytes_to_key_md5(password: bytes, salt: bytes, key_len: int, iv_len: int):
    d = b""
    prev = b""
    while len(d) < key_len + iv_len:
        prev = hashlib.md5(prev + password + salt).digest()
        d += prev
    return d[:key_len], d[key_len:key_len + iv_len]


def _foodfinder_decrypt_body(response_body: bytes, timestamp: str) -> str:
    if Cipher is None or algorithms is None or modes is None or padding is None:
        raise RuntimeError("FoodFinder split evaluator requires 'cryptography' dependency.")

    passphrase = _foodfinder_md5_hex_timestamp(timestamp)
    ciphertext_b64 = base64.b64encode(response_body).decode("ascii")
    raw = base64.b64decode(ciphertext_b64)

    if raw.startswith(b"Salted__"):
        salt = raw[8:16]
        ct = raw[16:]
        key, iv = _foodfinder_evp_bytes_to_key_md5(passphrase.encode("utf-8"), salt, 32, 16)
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    else:
        key = hashlib.md5(passphrase.encode("utf-8")).digest()
        iv = b"\x00" * 16
        ct = raw
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))

    decryptor = cipher.decryptor()
    padded_bytes = decryptor.update(ct) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    decrypted = unpadder.update(padded_bytes) + unpadder.finalize()

    for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS, zlib.MAX_WBITS | 32):
        try:
            out = zlib.decompress(decrypted, wbits=wbits)
            return out.decode("utf-8", errors="replace")
        except Exception:
            continue
    raise ValueError("Failed to inflate FoodFinder decrypted payload")


def _split_bbox(min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> list[tuple[float, float, float, float]]:
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    center_lat = (min_lat + max_lat) / 2.0
    # Approximate width comparison in miles.
    lon_weight = max(0.2, math.cos(math.radians(center_lat)))
    if lon_span * lon_weight >= lat_span:
        mid = (min_lon + max_lon) / 2.0
        return [(min_lat, max_lat, min_lon, mid), (min_lat, max_lat, mid, max_lon)]
    mid = (min_lat + max_lat) / 2.0
    return [(min_lat, mid, min_lon, max_lon), (mid, max_lat, min_lon, max_lon)]


def evaluate_foodfinder_bbox_split(
    response: scrapy.http.Response, ctx: JobContext, spider: scrapy.Spider
) -> Iterable[scrapy.Request]:
    """
    Adaptive splitter for capped FoodFinder bbox queries.
    If decoded result length == cap_size, split the bbox into two child requests.
    """
    eval_kwargs = ctx.config.evaluator_kwargs or {}
    cap_size = int(eval_kwargs.get("cap_size", 300))
    max_depth = int(eval_kwargs.get("max_split_depth", 12))
    min_lat_span = float(eval_kwargs.get("min_lat_span", 0.01))
    min_lon_span = float(eval_kwargs.get("min_lon_span", 0.01))
    dont_filter = bool(eval_kwargs.get("dont_filter", True))

    min_lat_param = str(eval_kwargs.get("min_lat_param", "min_lat"))
    max_lat_param = str(eval_kwargs.get("max_lat_param", "max_lat"))
    min_lon_param = str(eval_kwargs.get("min_lon_param", "min_lon"))
    max_lon_param = str(eval_kwargs.get("max_lon_param", "max_lon"))
    time_param = str(eval_kwargs.get("time_param", "_time"))
    refresh_time_per_request = bool(eval_kwargs.get("refresh_time_per_request", True))

    parsed = urlparse(response.request.url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    try:
        min_lat = float((qs.get(min_lat_param) or [None])[0])
        max_lat = float((qs.get(max_lat_param) or [None])[0])
        min_lon = float((qs.get(min_lon_param) or [None])[0])
        max_lon = float((qs.get(max_lon_param) or [None])[0])
    except (TypeError, ValueError):
        return []

    ts_vals = qs.get(time_param)
    timestamp = str(ts_vals[0]) if ts_vals and ts_vals[0] else None
    if not timestamp:
        gen_params = (ctx.config.generator_kwargs or {}).get("params", {})
        if isinstance(gen_params, dict):
            value = gen_params.get(time_param)
            if value is not None:
                timestamp = str(value)
    if not timestamp:
        return []

    try:
        decoded_text = _foodfinder_decrypt_body(response.body or b"", timestamp)
        payload = json.loads(decoded_text)
        result_count = len(payload) if isinstance(payload, list) else 0
    except Exception as exc:
        spider.logger.warning(
            "[foodfinder_split] org_id=%s decode/count failed url=%s err=%r",
            ctx.config.organization_id,
            response.request.url,
            exc,
        )
        return []

    if result_count < cap_size:
        return []

    depth = int(response.meta.get("_bbox_split_depth", 0))
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    if depth >= max_depth or lat_span <= min_lat_span or lon_span <= min_lon_span:
        raise ValueError(
            f"FoodFinder cap persists at bbox depth={depth} span=({lat_span:.6f},{lon_span:.6f}) "
            f"for {response.request.url}; cannot split further safely."
        )

    child_boxes = _split_bbox(min_lat, max_lat, min_lon, max_lon)
    requests: list[scrapy.Request] = []
    for idx, (c_min_lat, c_max_lat, c_min_lon, c_max_lon) in enumerate(child_boxes):
        child_qs = dict(qs)
        child_qs[min_lat_param] = [str(c_min_lat)]
        child_qs[max_lat_param] = [str(c_max_lat)]
        child_qs[min_lon_param] = [str(c_min_lon)]
        child_qs[max_lon_param] = [str(c_max_lon)]
        if refresh_time_per_request:
            child_qs[time_param] = [str(int(time.time() * 1000) + idx)]
        next_url = urlunparse(parsed._replace(query=urlencode(child_qs, doseq=True)))
        next_meta = dict(response.meta or {})
        next_meta["_ctx"] = ctx
        next_meta["_bbox_split_depth"] = depth + 1
        # Signal spider to skip parsing/emitting this capped parent response.
        next_meta["_skip_parent_parse"] = True
        requests.append(
            response.request.replace(
                url=next_url,
                meta=next_meta,
                callback=spider.parse,
                dont_filter=dont_filter,
            )
        )

    spider.logger.warning(
        "[foodfinder_split] org_id=%s capped=%s depth=%s split url=%s -> %s children",
        ctx.config.organization_id,
        result_count,
        depth,
        response.request.url,
        len(requests),
    )
    return requests


EVALUATOR_REGISTRY = {
    "default": evaluate_none,
    "none": evaluate_none,
    "accessfood_pagination": evaluate_accessfood_pagination,
    "fwp_pagination": evaluate_fwp_pagination,
    "dcms_pagination": evaluate_dcms_pagination,
    "json_pagination": evaluate_json_pagination,
    "foodfinder_bbox_split": evaluate_foodfinder_bbox_split,
    # "zip_iterator": evaluate_zip_iterator,
    # "box_search": evaluate_box_search,
    "whyhunger_pagination": evaluate_whyhunger_pagination
}
