"""Response parser utilities for the Scrapy ingestion pipeline."""

from __future__ import annotations

import csv
import html
import json
import logging
import os
import re
import base64
import hashlib
import zlib
from typing import Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

import scrapy
from scrapy.selector import Selector
try:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ModuleNotFoundError:  # pragma: no cover
    padding = None
    Cipher = None
    algorithms = None
    modes = None

from .context import JobContext


def parse_json(
    obj: object,
    path: list,
    *,
    dict_key_field: Optional[str] = None,
    dict_value_field: str = "value",
):
    """Traverse a JSON path and yield matching objects."""
    data = obj
    for p in path:
        if isinstance(data, dict):
            data = data.get(p, [])
        elif isinstance(data, list):
            try:
                idx = int(p)
            except (TypeError, ValueError):
                return
            if idx < 0 or idx >= len(data):
                return
            data = data[idx]
        else:
            return

    if isinstance(data, list):
        for d in data:
            yield d
    elif isinstance(data, dict) and dict_key_field:
        for key, value in data.items():
            if isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict):
                        item = dict(entry)
                        item[dict_key_field] = key
                        yield item
                    else:
                        yield {dict_key_field: key, dict_value_field: entry}
            elif isinstance(value, dict):
                item = dict(value)
                item[dict_key_field] = key
                yield item
            else:
                yield {dict_key_field: key, dict_value_field: value}
    else:
        yield data


def _base_meta(ctx: JobContext, source_url: str) -> Dict[str, object]:
    """Build common normalization metadata from config row context."""
    row = ctx.config
    return {
        "source_url": ctx.test_source_url or source_url,
        "generator": row.generator,
        "evaluator": row.evaluator,
        "parser": row.parser,
        "organization_id": row.organization_id,
        "organization_name": row.name,
        "state": row.state,
        "mailAddressState": row.raw.get("MailAddress_State") or row.raw.get("mailAddressState"),
        "mailAddressCity": row.raw.get("MailAddress_City") or row.raw.get("mailAddressCity"),
        "mailAddressZip": row.raw.get("MailAddress_Zip") or row.raw.get("mailAddressZip"),
        "mailAddressLatitude": row.raw.get("MailAddress_Latitude") or row.raw.get("mailAddressLatitude"),
        "mailAddressLongitude": row.raw.get("MailAddress_Longitude") or row.raw.get("mailAddressLongitude"),
        "entityId": row.raw.get("EntityID") or row.raw.get("entityId"),
        "organizationId": row.organization_id,
        "agencyUrl": row.raw.get("AgencyURL") or row.raw.get("agencyUrl"),
        "url": row.raw.get("URL") or row.raw.get("url"),
    }


def _ensure_dict_record(value: object) -> Dict[str, object]:
    if isinstance(value, dict):
        return value
    return {"value": value}


def parse_default(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    """Parse JSON response with configured path semantics."""
    data = response.json()
    path = _coerce_json_path(ctx.config.parser_kwargs)
    dict_key_field = None
    dict_value_field = "value"
    if isinstance(ctx.config.parser_kwargs, dict):
        dict_key_field = ctx.config.parser_kwargs.get("dict_key_field")
        dict_value_field = str(ctx.config.parser_kwargs.get("dict_value_field", "value"))
    for raw in parse_json(
        data,
        path,
        dict_key_field=str(dict_key_field) if dict_key_field else None,
        dict_value_field=dict_value_field,
    ):
        yield _ensure_dict_record(raw)


def parse_whole_json(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    """Emit full JSON payload as one record."""
    data = response.json()
    yield _ensure_dict_record(data)


def parse_csv(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    """Parse CSV response body into row dicts."""
    text = response.text or ""
    reader = csv.DictReader(text.splitlines())
    for row in reader:
        yield row


def parse_arcgis(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    """Parse ArcGIS feature payload and merge geometry into attributes."""
    data = response.json()
    for feature in data.get("features", []):
        attributes = feature.get("attributes", {})
        geometry = feature.get("geometry") or {}
        if geometry:
            attributes = dict(attributes)
            if "x" in geometry:
                attributes["longitude"] = geometry.get("x")
            if "y" in geometry:
                attributes["latitude"] = geometry.get("y")
        yield attributes


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


def _parse_city_state_zip(text: str) -> Dict[str, object]:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return {}
    match = re.match(
        r"^(?P<city>.+?)\s+(?P<state>[A-Za-z]{2,})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
        cleaned,
    )
    if not match:
        return {}
    state = match.group("state")
    if len(state) > 2:
        state_map = {
            "missouri": "MO",
        }
        state = state_map.get(state.lower(), state[:2].upper())
    return {
        "city": match.group("city").strip(),
        "state": state.upper(),
        "postal_code": match.group("zip").strip(),
    }


def parse_locator_html_coordinates(
    response: scrapy.http.Response, ctx: JobContext
) -> Iterable[Dict[str, object]]:
    data = response.json()
    for item in _extract_locator_items(data):
        yield item


def _extract_locator_items(data: object) -> Iterable[Dict[str, object]]:
    payload = _extract_by_path(data, ["data"])
    if not isinstance(payload, dict):
        return []

    html_block = payload.get("html") or ""
    coordinates = payload.get("coordinates") or []
    if not isinstance(coordinates, list):
        coordinates = []

    root = Selector(text=str(html_block))
    rows = root.css("div.agency-row")
    for idx, row in enumerate(rows):
        item: Dict[str, object] = {}
        name = row.css("h4.agency-name::text").get()
        if name:
            item["location_name"] = html.unescape(name.strip())

        detail_id = row.css("detail-button::attr(data-id)").get()
        if detail_id:
            item["id"] = detail_id

        addr_parts = [
            re.sub(r"\s+", " ", part).strip()
            for part in row.css("p.locator-key-text::text").getall()
            if re.sub(r"\s+", " ", part).strip()
        ]
        if addr_parts:
            item["address1"] = html.unescape(addr_parts[0])
        if len(addr_parts) > 1:
            item.update(_parse_city_state_zip(html.unescape(addr_parts[1])))

        href = row.css("detail-button a::attr(href)").get()
        if href and href not in {"http://", "https://"}:
            item["website"] = href.strip()

        icon = row.css("img.agency-icon::attr(src)").get()
        if icon:
            item["icon"] = icon.strip()

        coord = coordinates[idx] if idx < len(coordinates) else {}
        if isinstance(coord, dict):
            if coord.get("lat") is not None:
                item["latitude"] = coord.get("lat")
            if coord.get("lng") is not None:
                item["longitude"] = coord.get("lng")
            if coord.get("image") and not item.get("icon"):
                item["icon"] = coord.get("image")

        yield item


def _debug_enabled(step: Dict[str, object]) -> bool:
    if bool(step.get("debug")):
        return True
    return str(os.getenv("CHAIN_STEP_DEBUG", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _preview_data(value: object, max_chars: int) -> str:
    try:
        if isinstance(value, str):
            text = value
        else:
            text = json.dumps(value, default=str)
    except Exception:
        text = repr(value)
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...[truncated]"


def _extract_json(text: str, *, log_errors: bool = False) -> Optional[object]:
    if not text:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    try:
        data = json.loads(stripped)
        if isinstance(data, str):
            inner = data.strip()
            if inner and inner[0] in "[{":
                try:
                    return json.loads(inner)
                except json.JSONDecodeError:
                    return data
        return data
    except json.JSONDecodeError:
        if log_errors:
            logger = logging.getLogger(__name__)
            logger.info("[chain] json decode error on full text")
        pass

    # Handle JS-assignment string payloads (e.g., var _pageData = "[[...]]")
    # where the captured text is the raw string contents with escape sequences.
    if stripped.startswith("[") or stripped.startswith("{"):
        if "\\u" in stripped or "\\\"" in stripped or "\\n" in stripped or "\\t" in stripped:
            try:
                unescaped = json.loads(f"\"{stripped}\"")
                inner = unescaped.strip()
                if inner and inner[0] in "[{":
                    return json.loads(inner)
                return unescaped
            except json.JSONDecodeError:
                if log_errors:
                    logger = logging.getLogger(__name__)
                    logger.info("[chain] json decode error on unescaped inner")
                pass

    candidates = []
    for open_char, close_char in (("{", "}"), ("[", "]")):
        start = stripped.find(open_char)
        end = stripped.rfind(close_char)
        if start != -1 and end != -1 and end > start:
            candidates.append(stripped[start : end + 1])
    for candidate in sorted(candidates, key=len, reverse=True):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            if log_errors:
                logger = logging.getLogger(__name__)
                logger.info("[chain] json decode error on candidate")
            continue
    return None


def _coerce_json_path(parser_kwargs: object) -> List[str]:
    if isinstance(parser_kwargs, list):
        return [str(v) for v in parser_kwargs]
    if isinstance(parser_kwargs, dict):
        path = parser_kwargs.get("json_path")
        if isinstance(path, list):
            return [str(v) for v in path]
    return []


def _foodfinder_md5_hex_timestamp(ts: str) -> str:
    return hashlib.md5(str(ts).encode("utf-8")).hexdigest()


def _foodfinder_evp_bytes_to_key_md5(password: bytes, salt: bytes, key_len: int, iv_len: int):
    """OpenSSL EVP_BytesToKey (MD5) derivation used by CryptoJS passphrase mode."""
    if len(salt) != 8:
        raise ValueError("Invalid FoodFinder salt length")
    d = b""
    prev = b""
    while len(d) < key_len + iv_len:
        prev = hashlib.md5(prev + password + salt).digest()
        d += prev
    key = d[:key_len]
    iv = d[key_len:key_len + iv_len]
    return key, iv


def _foodfinder_decrypt_response_body(response_body: bytes, timestamp: str) -> str:
    """
    Decode FoodFinder encrypted payload:
    - base64(response bytes)
    - CryptoJS AES passphrase decrypt with passphrase=md5(timestamp)
    - zlib/gzip/raw inflate
    """
    if Cipher is None or algorithms is None or modes is None or padding is None:
        raise RuntimeError(
            "FoodFinder decrypt parser requires 'cryptography' dependency."
        )

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
    padded = decryptor.update(ct) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    decrypted = unpadder.update(padded) + unpadder.finalize()

    for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS, zlib.MAX_WBITS | 32):
        try:
            out = zlib.decompress(decrypted, wbits=wbits)
            return out.decode("utf-8", errors="replace")
        except Exception:
            continue
    raise ValueError("Failed to inflate FoodFinder decrypted payload")


def _foodfinder_raise_on_cap(decoded_text: str, response: scrapy.http.Response, step: Dict[str, object]) -> None:
    """Raise when FoodFinder appears capped at configured result size."""
    cap_size = int(step.get("cap_size", 300))
    fail_on_cap = bool(step.get("fail_on_cap", True))
    if not fail_on_cap:
        return
    try:
        payload = json.loads(decoded_text)
    except Exception:
        return
    if isinstance(payload, list) and len(payload) == cap_size:
        raise ValueError(
            f"FoodFinder query hit cap ({cap_size}) for {response.url}; "
            "narrow bounding box to avoid truncation."
        )


def _foodfinder_timestamp_from_response(response: scrapy.http.Response, step: Dict[str, object], ctx: JobContext) -> Optional[str]:
    timestamp_param = str(step.get("timestamp_param", "_time"))
    qs = parse_qs(urlparse(response.url).query, keep_blank_values=True)
    vals = qs.get(timestamp_param)
    if vals and vals[0]:
        return str(vals[0])

    gen_kwargs = ctx.config.generator_kwargs or {}
    params = gen_kwargs.get("params", {})
    if isinstance(params, dict):
        value = params.get(timestamp_param)
        if value is not None and str(value).strip():
            return str(value).strip()

    explicit = step.get("timestamp")
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    return None


def _is_latlng_pair(value: object) -> bool:
    if not isinstance(value, list) or len(value) != 2:
        return False
    try:
        float(value[0])
        float(value[1])
    except (TypeError, ValueError):
        return False
    return True


def _extract_mymaps_features(data: object) -> Iterable[Dict[str, object]]:
    if not isinstance(data, list):
        return []

    features: List[Dict[str, object]] = []
    stack = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            if len(node) >= 6 and isinstance(node[0], str):
                coords = None
                if isinstance(node[1], list) and node[1]:
                    first = node[1][0]
                    if isinstance(first, list) and first and _is_latlng_pair(first[0]):
                        coords = first[0]
                fields_lists: List[List[object]] = []
                for child in node:
                    if isinstance(child, list) and child and isinstance(child[0], list):
                        if child and isinstance(child[0], list) and child[0]:
                            if isinstance(child[0][0], str) and isinstance(child[0][1], list):
                                fields_lists.append(child)
                if coords and fields_lists:
                    item: Dict[str, object] = {
                        "latitude": coords[0],
                        "longitude": coords[1],
                    }
                    key_map = {
                        "agency": "location_name",
                        "address": "address1",
                        "city": "city",
                        "state": "state",
                        "zip": "postal_code",
                        "phone": "phone",
                        "hours of operation": "hours",
                        "hours": "hours",
                    }
                    def _ingest_field(field: object) -> None:
                        if not isinstance(field, list) or len(field) < 2:
                            return
                        key = str(field[0]).strip().lower()
                        value = field[1]
                        if isinstance(value, list) and value:
                            value = value[0]
                        mapped_key = key_map.get(key, key)
                        item[mapped_key] = value

                    for fields in fields_lists:
                        for field in fields:
                            if (
                                isinstance(field, list)
                                and field
                                and isinstance(field[0], list)
                                and field[0]
                                and isinstance(field[0][0], str)
                            ):
                                for subfield in field:
                                    _ingest_field(subfield)
                                continue
                            _ingest_field(field)
                    features.append(item)
        elif isinstance(node, dict):
            stack.extend(node.values())
    return features


def _select_html_text(response: scrapy.http.Response, parser_kwargs: object) -> str:
    selector = None
    attr = None
    if isinstance(parser_kwargs, dict):
        attr = parser_kwargs.get("attr")
        if parser_kwargs.get("xpath"):
            selector = response.xpath(str(parser_kwargs["xpath"]))
        elif parser_kwargs.get("css"):
            selector = response.css(str(parser_kwargs["css"]))
        elif parser_kwargs.get("id"):
            selector = response.css(f"script#{parser_kwargs['id']}::text")
    if selector is None:
        return response.text or ""
    if attr and attr != "text":
        first = selector[0] if selector else None
        if first is None:
            return ""
        return first.attrib.get(attr, "")
    return "\n".join(selector.getall())


def _extract_html_list(response: scrapy.http.Response, step: Dict[str, object]) -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    item_css = step.get("item_css") or step.get("itemCss") or step.get("css")
    item_xpath = step.get("item_xpath") or step.get("itemXpath") or step.get("xpath")
    if item_xpath:
        selectors = response.xpath(str(item_xpath))
    elif item_css:
        selectors = response.css(str(item_css))
    else:
        selectors = []

    fields = step.get("fields") if isinstance(step.get("fields"), dict) else {}

    def _extract_field(sel: scrapy.selector.Selector, selector: object) -> object:
        if not isinstance(selector, str):
            return None
        if selector.startswith("@"):
            return sel.attrib.get(selector[1:])
        if selector.startswith("xpath:"):
            return sel.xpath(selector[len("xpath:") :]).get()
        return sel.css(selector).get()

    for sel in selectors:
        item: Dict[str, object] = {}
        for key, selector in fields.items():
            value = _extract_field(sel, selector)
            if value is not None:
                item[str(key)] = value

        for attr_key, attr_val in sel.attrib.items():
            if not attr_key.startswith("data-"):
                continue
            raw_key = attr_key[5:]
            norm_key = raw_key.lower()
            mapped = None
            if norm_key in {"lat", "latitude"}:
                mapped = "latitude"
            elif norm_key in {"lng", "lon", "long", "longitude"}:
                mapped = "longitude"
            elif norm_key in {"address", "addr1", "addr", "street"}:
                mapped = "address1"
            elif norm_key in {"address2", "addr2", "street2"}:
                mapped = "address2"
            elif norm_key in {"city"}:
                mapped = "city"
            elif norm_key in {"state"}:
                mapped = "state"
            elif norm_key in {"zip", "postal", "postal_code"}:
                mapped = "postal_code"
            elif norm_key in {"name", "title", "location", "location_name", "agencyname"}:
                mapped = "location_name"
            elif norm_key in {"phone", "phone_number"}:
                mapped = "phone"
            elif norm_key in {"website", "web", "url"}:
                mapped = "website"
            item[mapped or raw_key] = attr_val

        if not item:
            text = " ".join([t.strip() for t in sel.css("::text").getall() if t.strip()])
            if text:
                item["text"] = text
        items.append(item)
    return items


def _unpack_nested_list(data: object, step: Dict[str, object]) -> List[Dict[str, object]]:
    if not isinstance(data, dict):
        return []

    key = str(step.get("key", step.get("list_key", "events"))).strip() or "events"
    include_parent = bool(step.get("include_parent", True))
    drop_key_from_parent = bool(step.get("drop_key_from_parent", True))
    include_children = bool(step.get("include_children", True))

    out: List[Dict[str, object]] = []
    nested = data.get(key)

    if include_parent:
        parent = dict(data)
        if drop_key_from_parent:
            parent.pop(key, None)
        out.append(parent)

    if include_children and isinstance(nested, list):
        for child in nested:
            if isinstance(child, dict):
                out.append(dict(child))

    return out


def _apply_regex(text: str, parser_kwargs: object) -> Optional[str]:
    if not isinstance(parser_kwargs, dict):
        return None
    pattern = parser_kwargs.get("regex")
    if not pattern:
        return None
    flags = parser_kwargs.get("regex_flags", "")
    re_flags = 0
    if "i" in flags:
        re_flags |= re.IGNORECASE
    if "m" in flags:
        re_flags |= re.MULTILINE
    if "s" in flags:
        re_flags |= re.DOTALL
    if "x" in flags:
        re_flags |= re.VERBOSE
    match = re.search(pattern, text, re_flags)
    if not match:
        return None
    group = parser_kwargs.get("regex_group")
    if group is None:
        group = 1 if match.lastindex else 0
    try:
        result = match.group(group)
    except (IndexError, KeyError):
        return None
    if not isinstance(result, str):
        return result
    trimmed = _trim_balanced_json_prefix(result)
    return trimmed if trimmed is not None else result


def _trim_balanced_json_prefix(text: str) -> Optional[str]:
    if not text:
        return None
    start = text.lstrip()
    if not start or start[0] not in "[{":
        return None

    opening = start[0]
    expected_closing = "]" if opening == "[" else "}"
    stack: List[str] = [expected_closing]
    in_string = False
    escape = False

    for idx, ch in enumerate(start[1:], start=1):
        if in_string:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == "\"":
                in_string = False
            continue

        if ch == "\"":
            in_string = True
            continue
        if ch == "[":
            stack.append("]")
            continue
        if ch == "{":
            stack.append("}")
            continue
        if ch in "]}":
            if not stack or ch != stack[-1]:
                return None
            stack.pop()
            if not stack:
                return start[: idx + 1]
    return None


def _js_object_to_json(text: str) -> str:
    # Heuristic conversion of JS object literals to JSON.
    # 1) Quote unquoted keys.
    # 2) Convert single quotes to double quotes.
    # 3) Remove trailing commas.
    # This is best-effort and may fail on complex JS.
    if not text:
        return text
    # Quote keys: { key: ... } or , key: ...
    text = re.sub(r'([{\[,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:', r'\1"\2":', text)
    # Convert single quotes to double quotes
    text = re.sub(r"'", r'"', text)
    # Remove trailing commas before } or ]
    text = re.sub(r',\s*([}\]])', r'\1', text)
    return text


def parse_html(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    import logging
    logger = logging.getLogger(__name__)
    text = _select_html_text(response, ctx.config.parser_kwargs)
    logger.info(
        "[html] url=%s selected_text_len=%s",
        response.url,
        len(text or ""),
    )
    regex_text = _apply_regex(text, ctx.config.parser_kwargs)
    if regex_text is not None:
        logger.info("[html] regex matched len=%s", len(regex_text))
        text = regex_text
    else:
        if isinstance(ctx.config.parser_kwargs, dict) and ctx.config.parser_kwargs.get("regex"):
            logger.info("[html] regex did not match")
    data = _extract_json(text)
    if data is None:
        logger.info("[html] json parse failed (no data)")
        return
    logger.info("[html] json type=%s", type(data).__name__)
    if isinstance(ctx.config.parser_kwargs, dict) and ctx.config.parser_kwargs.get("mymaps"):
        items = list(_extract_mymaps_features(data))
        logger.info("[html] mymaps features=%s", len(items))
        for item in items:
            yield _ensure_dict_record(item)
        return
    path = _coerce_json_path(ctx.config.parser_kwargs)
    if path:
        logger.info("[html] json_path=%s", path)
    for raw in parse_json(data, path):
        yield _ensure_dict_record(raw)


def _apply_step(
    data: object,
    step: Dict[str, object],
    response: scrapy.http.Response,
    ctx: JobContext,
) -> object:
    import logging
    logger = logging.getLogger(__name__)
    name = str(step.get("name", "")).strip().lower()
    if name == "html":
        return _select_html_text(response, step)
    if name == "html_list":
        return _extract_html_list(response, step)
    if name == "regex":
        if isinstance(data, str):
            result = _apply_regex(data, step)
            snippet = None
            if result:
                snippet = result if len(result) <= 300 else result[:300] + "..."
            logger.info(
                "[chain] regex pattern=%s matched=%s snippet=%s",
                step.get("regex"),
                bool(result),
                snippet,
            )
            return result or ""
        return ""
    if name == "js_to_json":
        if isinstance(data, str):
            converted = _js_object_to_json(data)
            snippet = converted if len(converted) <= 300 else converted[:300] + "..."
            logger.info("[chain] js_to_json snippet=%s", snippet)
            return converted
        return data
    if name == "json":
        if isinstance(data, str):
            return _extract_json(data, log_errors=True)
        return data
    if name == "json_path":
        path = step.get("path") or step.get("json_path") or []
        if isinstance(path, list):
            dict_key_field = step.get("dict_key_field")
            dict_value_field = str(step.get("dict_value_field", "value"))
            return list(
                parse_json(
                    data,
                    [str(v) for v in path],
                    dict_key_field=str(dict_key_field) if dict_key_field else None,
                    dict_value_field=dict_value_field,
                )
            )
        return []
    if name in {"unpack_json", "unpack"}:
        if isinstance(data, list):
            expanded: List[Dict[str, object]] = []
            for item in data:
                expanded.extend(_unpack_nested_list(item, step))
            return expanded
        return _unpack_nested_list(data, step)
    if name == "mymaps":
        return list(_extract_mymaps_features(data))
    if name == "locator_html_coordinates":
        if isinstance(data, str):
            data = _extract_json(data, log_errors=True)
        if isinstance(data, dict):
            return list(_extract_locator_items(data))
        if isinstance(data, list):
            extracted: List[Dict[str, object]] = []
            for item in data:
                if isinstance(item, dict):
                    extracted.extend(_extract_locator_items(item))
            return extracted
        return []
    if name == "foodfinder_decrypt":
        timestamp = _foodfinder_timestamp_from_response(response, step, ctx)
        if not timestamp:
            raise ValueError("FoodFinder decrypt step could not resolve request timestamp (_time)")
        decoded = _foodfinder_decrypt_response_body(response.body, timestamp)
        _foodfinder_raise_on_cap(decoded, response, step)
        return decoded
    if name == "whyhunger":
        return list(parse_whyhunger(response, ctx))
    return data


def parse_chain(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    import logging
    logger = logging.getLogger(__name__)
    steps = ctx.config.parser_chain or []
    try:
        data: object = response.text or ""
    except AttributeError:
        # Some endpoints return binary payloads; parser steps (e.g. foodfinder_decrypt)
        # can operate directly from response.body.
        data = response.body or b""
    for step in steps:
        if isinstance(data, list):
            next_data = []
            for item in data:
                result = _apply_step(item, step, response, ctx)
                if isinstance(result, list):
                    next_data.extend(result)
                elif result is not None:
                    next_data.append(result)
            data = next_data
        else:
            data = _apply_step(data, step, response, ctx)
        logger.info(
            "[chain] step=%s type=%s",
            step.get("name"),
            type(data).__name__,
        )
        if _debug_enabled(step):
            max_chars = int(
                step.get("debug_max_chars")
                or step.get("debugMaxChars")
                or os.getenv("CHAIN_STEP_DEBUG_MAX_CHARS", "1200")
            )
            logger.info(
                "[chain] step=%s preview=%s",
                step.get("name"),
                _preview_data(data, max_chars=max_chars),
            )

    if isinstance(data, list):
        logger.info("[chain] final list len=%s", len(data))
        skipped = 0
        for item in data:
            if isinstance(item, dict):
                yield item
            else:
                skipped += 1
        if skipped:
            first_type = type(data[0]).__name__ if data else "unknown"
            logger.info(
                "[chain] final list skipped_non_dict=%s first_item_type=%s",
                skipped,
                first_type,
            )
    elif isinstance(data, dict):
        logger.info("[chain] final dict")
        yield data


def _normalized_class_xpath(class_name: str) -> str:
    return f"contains(concat(' ', normalize-space(@class), ' '), ' {class_name} ')"


def _clean_joined_text(values: List[str]) -> str:
    return re.sub(r"\s+", " ", " ".join(v.strip() for v in values if v and v.strip())).strip()


def _request_page_from_url(response: scrapy.http.Response, page_param: str = "page") -> int:
    try:
        parsed = urlparse(response.url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if page_param in qs and qs[page_param]:
            return int(qs[page_param][0])
    except Exception:
        return 1
    return 1


def parse_whyhunger(response: scrapy.http.Response, ctx: JobContext) -> Iterable[Dict[str, object]]:
    parsed = urlparse(response.url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    current_page = response.meta.get("_whyhunger_page")
    try:
        current_page = int(current_page)
    except (TypeError, ValueError):
        current_page = _request_page_from_url(response)

    org_xpath = f"//div[{_normalized_class_xpath('organisations')}]"
    title_text = _clean_joined_text(
        response.xpath(
            f"{org_xpath}//div[{_normalized_class_xpath('title')}]//text()"
        ).getall()
    )
    total_results = None
    total_match = re.search(r"(\d[\d,]*)", title_text)
    if total_match:
        try:
            total_results = int(total_match.group(1).replace(",", ""))
        except (TypeError, ValueError):
            total_results = None

    items_xpath = f"{org_xpath}//div[{_normalized_class_xpath('items')}]"
    geo_nodes = response.xpath(
        f"{items_xpath}//input[{_normalized_class_xpath('location_cords')}]"
    )
    item_nodes = response.xpath(
        f"{items_xpath}//div[{_normalized_class_xpath('item')}]"
    )

    max_items = max(len(geo_nodes), len(item_nodes))
    for idx in range(max_items):
        geo = geo_nodes[idx] if idx < len(geo_nodes) else None
        item_sel = item_nodes[idx] if idx < len(item_nodes) else None
        location: Dict[str, object] = {}

        if geo is not None:
            lat = geo.attrib.get("data-lat")
            lng = geo.attrib.get("data-lng")
            title = geo.attrib.get("data-title")
            location_id = geo.attrib.get("data-id")
            if lat is not None:
                location["latitude"] = lat
            if lng is not None:
                location["longitude"] = lng
            if title:
                location["name"] = title
                location["location_name"] = title
            if location_id:
                location["id"] = location_id

        if item_sel is not None:
            type_text = _clean_joined_text(
                item_sel.xpath(
                    f".//div[{_normalized_class_xpath('type')}]//text()"
                ).getall()
            )
            if type_text:
                type_match = re.match(r"TYPE\s*:\s*(.+)", type_text, re.IGNORECASE)
                location["type"] = type_match.group(1).strip() if type_match else type_text

            for field in item_sel.xpath(f".//div[{_normalized_class_xpath('field')}]"):
                field_text = _clean_joined_text(field.xpath(".//text()").getall())
                if not field_text:
                    continue
                match = re.match(r"([A-Za-z][A-Za-z\s\-/&]+?)\s*:\s*(.+)", field_text)
                if not match:
                    continue
                key = match.group(1).strip()
                value = match.group(2).strip()
                location[key] = value

        if not location:
            continue

        location["page"] = current_page
        if total_results is not None:
            location["total_results"] = total_results
        if query.get("zip"):
            location["zip"] = query["zip"][0]
        elif query.get("center_zip"):
            location["zip"] = query["center_zip"][0]
        if query.get("distance"):
            location["distance"] = query["distance"][0]
        location["source_url"] = response.url

        yield location


PARSER_REGISTRY = {
    "default": parse_default,
    "json_path": parse_default,
    "json": parse_whole_json,
    "storepoint": parse_default,
    "csv": parse_csv,
    "arcgis": parse_arcgis,
    "locator_html_coordinates": parse_locator_html_coordinates,
    "html": parse_html,
    "chain": parse_chain,
    "whyhunger": parse_whyhunger,
}
