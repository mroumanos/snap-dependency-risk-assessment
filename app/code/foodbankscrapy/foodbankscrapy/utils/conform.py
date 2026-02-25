"""Conformance engine for raw crawl envelopes.

Transforms raw per-org JSONL envelopes into standardized EFO-style records
using configurable field mappings, address parsing, and deduplication.
"""

from __future__ import annotations

import json
import ast
import hashlib
import re
from html import unescape
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, unquote_plus, urlparse

import usaddress

from .pipeline import FoodBankConfig


def _default_mapping_path() -> Path:
    return Path(__file__).resolve().parents[2] / "static" / "pipelines" / "conform.json"


STATE_NAME_TO_CODE = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}

COUNTRY_TO_CODE = {
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "USA": "US",
    "US": "US",
    "CANADA": "CA",
    "CA": "CA",
}

ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")

DEFAULT_TYPE_BOW: Dict[str, List[str]] = {
    "CHURCH": [
        "church",
        "methodist",
        "baptist",
        "catholic",
        "episcopal",
        "lutheran",
        "presbyterian",
        "assembly of god",
        "ministry",
        "ministries",
    ],
    "SOUP KITCHEN": [
        "soup kitchen",
        "community kitchen",
        "meal kitchen",
        "hot meals",
    ],
    "MOBILE PANTRY": [
        "mobile pantry",
        "mobile market",
        "food truck",
        "pop up pantry",
        "pop-up pantry",
        "drive thru pantry",
        "drive-through pantry",
    ],
    "FOOD BANK": [
        "food bank",
        "regional food bank",
    ],
    "FOOD PANTRY": [
        "food pantry",
        "pantry",
    ],
    "MEAL SITE": [
        "meal site",
        "meal program",
        "community meal",
        "free meal",
    ],
    "SHELTER": [
        "shelter",
        "rescue mission",
        "homeless",
    ],
    "SENIOR CENTER": [
        "senior center",
        "senior services",
    ],
    "SCHOOL": [
        "school",
        "elementary",
        "middle school",
        "high school",
        "college",
        "university",
    ],
    "COMMUNITY CENTER": [
        "community center",
        "recreation center",
        "rec center",
        "ymca",
    ],
    "CSFP": [
        "csfp"
    ],
    "TEFAP": [
        "tefap"
    ],
}

# USPS Publication 28-style normalization (common street suffixes and unit designators).
USPS_STREET_SUFFIX = {
    "ALLEE": "ALY", "ALLEY": "ALY", "ALLY": "ALY", "ALY": "ALY",
    "AV": "AVE", "AVE": "AVE", "AVEN": "AVE", "AVENU": "AVE", "AVENUE": "AVE", "AVN": "AVE", "AVNUE": "AVE",
    "BLVD": "BLVD", "BOUL": "BLVD", "BOULEVARD": "BLVD", "BOULV": "BLVD",
    "CIR": "CIR", "CIRCLE": "CIR", "CIRC": "CIR", "CIRCL": "CIR", "CRCLE": "CIR",
    "CT": "CT", "COURT": "CT", "CRT": "CT",
    "DR": "DR", "DRIVE": "DR", "DRV": "DR",
    "EXP": "EXPY", "EXPR": "EXPY", "EXPRESS": "EXPY", "EXPRESSWAY": "EXPY", "EXPW": "EXPY", "EXPY": "EXPY",
    "HWY": "HWY", "HIGHWAY": "HWY", "HIWAY": "HWY",
    "JCT": "JCT", "JCTION": "JCT", "JCTN": "JCT", "JUNCTION": "JCT",
    "LN": "LN", "LANE": "LN",
    "PK": "PARK", "PARK": "PARK", "PRK": "PARK",
    "PKWY": "PKWY", "PARKWAY": "PKWY", "PARKWY": "PKWY", "PKWAY": "PKWY", "PKY": "PKWY",
    "PL": "PL", "PLACE": "PL",
    "PLZ": "PLZ", "PLAZA": "PLZ",
    "RD": "RD", "ROAD": "RD",
    "SQ": "SQ", "SQR": "SQ", "SQRE": "SQ", "SQU": "SQ", "SQUARE": "SQ",
    "ST": "ST", "STR": "ST", "STREET": "ST", "STRT": "ST",
    "TER": "TER", "TERR": "TER", "TERRACE": "TER",
    "TR": "TRL", "TRL": "TRL", "TRAIL": "TRL", "TRAILS": "TRL",
    "WAY": "WAY",
}

USPS_UNIT_DESIGNATOR = {
    "APARTMENT": "APT", "APT": "APT",
    "BASEMENT": "BSMT", "BSMT": "BSMT",
    "BUILDING": "BLDG", "BLDG": "BLDG",
    "DEPARTMENT": "DEPT", "DEPT": "DEPT",
    "FLOOR": "FL", "FL": "FL",
    "FRONT": "FRNT", "FRNT": "FRNT",
    "HANGAR": "HNGR", "HNGR": "HNGR",
    "LOBBY": "LBBY", "LBBY": "LBBY",
    "LOT": "LOT",
    "LOWER": "LOWR", "LOWR": "LOWR",
    "OFFICE": "OFC", "OFC": "OFC",
    "PENTHOUSE": "PH", "PH": "PH",
    "PIER": "PIER",
    "REAR": "REAR",
    "ROOM": "RM", "RM": "RM",
    "SIDE": "SIDE",
    "SLIP": "SLIP",
    "SPACE": "SPC", "SPC": "SPC",
    "STOP": "STOP",
    "SUITE": "STE", "STE": "STE",
    "TRAILER": "TRLR", "TRLR": "TRLR",
    "UNIT": "UNIT",
    "UPPER": "UPPR", "UPPR": "UPPR",
}


def _normalize_paths(value: object) -> List[object]:
    if not isinstance(value, list):
        return []
    def _normalize_refs(raw: object) -> List[object]:
        refs: List[object] = []
        if raw is None:
            return refs
        items = raw if isinstance(raw, list) else [raw]
        for item in items:
            if isinstance(item, list) and item:
                refs.append([str(p) for p in item])
            elif isinstance(item, str) and item.strip():
                refs.append(item.strip())
        return refs

    out: List[object] = []
    for item in value:
        if isinstance(item, list) and item:
            out.append([str(p) for p in item])
            continue
        if isinstance(item, str) and item.strip():
            out.append([item.strip()])
            continue
        if not isinstance(item, dict):
            continue
        rule_type = str(item.get("type") or "").strip().lower()
        if rule_type == "append":
            fields = item.get("fields")
            if not isinstance(fields, list) or not fields:
                continue
            norm_fields: List[List[str]] = []
            for field in fields:
                if isinstance(field, list) and field:
                    norm_fields.append([str(p) for p in field])
                elif isinstance(field, str) and field.strip():
                    norm_fields.append([field.strip()])
            if not norm_fields:
                continue
            sep = item.get("separator", " ")
            out.append({"type": "append", "fields": norm_fields, "separator": str(sep)})
            continue
        if rule_type == "bow_classify":
            norm_fields = _normalize_refs(item.get("fields"))
            norm_pre = _normalize_refs(item.get("pre"))
            norm_post = _normalize_refs(item.get("post"))
            categories = item.get("categories")
            norm_categories: Dict[str, List[str]] | None = None
            if isinstance(categories, dict):
                norm_categories = {}
                for cat, keywords in categories.items():
                    if not isinstance(keywords, list):
                        continue
                    kw = [str(v).strip().lower() for v in keywords if str(v).strip()]
                    if not kw:
                        continue
                    norm_categories[str(cat).strip().upper()] = kw
                if not norm_categories:
                    norm_categories = None
            normalized = {"type": "bow_classify"}
            if norm_fields:
                normalized["fields"] = norm_fields
            if norm_pre:
                normalized["pre"] = norm_pre
            if norm_post:
                normalized["post"] = norm_post
            if norm_categories:
                normalized["categories"] = norm_categories
            out.append(normalized)
            continue

        if rule_type == "gmapurl_parser":
            norm_fields = _normalize_refs(item.get("fields"))
            if not norm_fields:
                norm_fields = _normalize_refs(item.get("field", item.get("path")))
            if not norm_fields:
                continue
            normalized = {"type": "gmapurl_parser", "fields": norm_fields}
            component = str(item.get("component", "")).strip().lower()
            if component in {"lat", "latitude", "lng", "lon", "long", "longitude"}:
                normalized["component"] = component
            out.append(normalized)
            continue

        if rule_type == "split":
            norm_fields = _normalize_refs(item.get("fields"))
            if not norm_fields:
                norm_fields = _normalize_refs(item.get("field", item.get("path")))
            if not norm_fields:
                continue
            indices_raw = item.get("indices")
            indices: List[int] = []
            if isinstance(indices_raw, list):
                for idx in indices_raw:
                    try:
                        indices.append(int(idx))
                    except (TypeError, ValueError):
                        continue
            if not indices:
                indices = [0]
            seps_raw = item.get("separators")
            separators: List[str] = []
            if isinstance(seps_raw, list):
                separators = [str(v) for v in seps_raw if str(v)]
            elif isinstance(item.get("separator"), str) and str(item.get("separator")):
                separators = [str(item.get("separator"))]
            if not separators:
                separators = [" : "]
            normalized = {
                "type": "split",
                "fields": norm_fields,
                "indices": indices,
                "separators": separators,
                "join_separator": str(item.get("join_separator", " ")),
            }
            out.append(normalized)
            continue

        if rule_type == "html":
            norm_fields = _normalize_refs(item.get("fields"))
            if not norm_fields:
                norm_fields = _normalize_refs(item.get("field", item.get("path")))
            if not norm_fields:
                continue
            objects_raw = item.get("objects")
            objects: List[str] = []
            if isinstance(objects_raw, list):
                for obj in objects_raw:
                    if isinstance(obj, list) and obj:
                        token = str(obj[0]).strip().lower()
                    elif isinstance(obj, str):
                        token = obj.strip().lower()
                    else:
                        token = ""
                    if token:
                        objects.append(token)
            when = str(item.get("when", "any")).strip().lower()
            if when not in {"pre", "post", "any"}:
                when = "any"
            normalized = {
                "type": "html",
                "fields": norm_fields,
                "objects": objects,
                "when": when,
                "join_separator": str(item.get("join_separator", " ")),
            }
            out.append(normalized)
            continue

        if rule_type in {"html_text", "links", "path"}:
            field = item.get("field", item.get("path"))
            if isinstance(field, list) and field:
                norm_field: List[str] = [str(p) for p in field]
            elif isinstance(field, str) and field.strip():
                norm_field = [field.strip()]
            else:
                continue
            normalized = {"type": rule_type, "field": norm_field}
            if rule_type == "links":
                normalized["separator"] = str(item.get("separator", ", "))
                normalized["mode"] = str(item.get("mode", "all")).strip().lower()
            out.append(normalized)
    return out


def _load_conform_mappings(mapping_file: Path) -> Tuple[Dict[str, List[object]], Dict[str, Dict[str, object]]]:
    defaults: Dict[str, List[object]] = {}
    org_mappings: Dict[str, Dict[str, object]] = {}

    if not mapping_file.exists():
        return defaults, org_mappings

    try:
        raw = json.loads(mapping_file.read_text())
    except json.JSONDecodeError:
        return defaults, org_mappings

    if not isinstance(raw, dict):
        return defaults, org_mappings

    # Support either {"defaultMappings": ..., "orgMappings": ...} or a legacy
    # top-level default-mappings object.
    fm = raw.get("defaultMappings") if ("defaultMappings" in raw or "orgMappings" in raw) else raw
    om = raw.get("orgMappings") if isinstance(raw.get("orgMappings"), dict) else None

    if isinstance(fm, dict):
        for field, paths in fm.items():
            norm = _normalize_paths(paths)
            if norm:
                defaults[str(field)] = norm

    if isinstance(om, dict):
        for org_id, mapping in om.items():
            if isinstance(mapping, dict):
                org_mappings[str(org_id)] = mapping

    return defaults, org_mappings


def _save_conform_mappings(
    mapping_file: Path,
    defaults: Dict[str, List[object]],
    org_mappings: Dict[str, Dict[str, object]],
) -> None:
    payload = {
        "defaultMappings": defaults,
        "orgMappings": org_mappings,
    }
    mapping_file.parent.mkdir(parents=True, exist_ok=True)
    mapping_file.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _ensure_org_mapping_entries(
    mapping_file: Path,
    defaults: Dict[str, List[object]],
    org_mappings: Dict[str, Dict[str, object]],
    org_ids: List[str],
) -> None:
    changed = not mapping_file.exists()
    for org_id in org_ids:
        if org_id not in org_mappings:
            org_mappings[org_id] = {}
            changed = True
    if changed:
        _save_conform_mappings(mapping_file, defaults, org_mappings)


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _created_at_from_run_id(run_id: str) -> str:
    base = run_id[:15]
    try:
        dt = datetime.strptime(base, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return datetime.now(timezone.utc).isoformat()


def _file_created_time_iso(path: Path) -> str:
    stat = path.stat()
    # macOS provides birth time; fall back to mtime where unavailable.
    ts = getattr(stat, "st_birthtime", None)
    if ts is None:
        ts = stat.st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _latest_run_file(org_dir: Path, run_id: str) -> Optional[Path]:
    exact = org_dir / f"{run_id}.jsonl"
    if run_id and exact.exists():
        return exact
    files = sorted(org_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _lookup_key_ci(data: Dict[str, object], key: str) -> Optional[object]:
    if key in data:
        return data[key]
    low = key.lower()
    for k, v in data.items():
        if str(k).lower() == low:
            return v
    return None


def _extract_path_value(data: object, path: List[str]) -> Optional[object]:
    current = data
    for part in path:
        if isinstance(current, dict):
            current = _lookup_key_ci(current, part)
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
        if current is None:
            return None
    return current


def _flatten_json(node: object, prefix: str, out: Dict[str, object]) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            key = str(k)
            child = f"{prefix}.{key}" if prefix else key
            _flatten_json(v, child, out)
        return
    if isinstance(node, list):
        for i, v in enumerate(node):
            idx = str(i)
            dot_child = f"{prefix}.{idx}" if prefix else idx
            bracket_child = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            _flatten_json(v, dot_child, out)
            _flatten_json(v, bracket_child, out)
        return
    if prefix:
        out[prefix] = node


def _flatten_data(data: Dict[str, object]) -> Dict[str, object]:
    flat: Dict[str, object] = {}
    _flatten_json(data, "", flat)
    return flat


def _path_candidates(path: List[str]) -> List[str]:
    if not path:
        return []
    dot_key = ".".join(str(p) for p in path)
    candidates = [dot_key]
    if any(str(p).isdigit() for p in path):
        parts: List[str] = []
        for idx, part in enumerate(path):
            token = str(part)
            if token.isdigit() and idx > 0:
                parts[-1] = f"{parts[-1]}[{token}]"
            else:
                parts.append(token)
        candidates.append(".".join(parts))
    return candidates


def _decode_to_utf8(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    if isinstance(value, dict):
        out: Dict[str, object] = {}
        for k, v in value.items():
            key = _decode_to_utf8(k)
            if not isinstance(key, str):
                key = str(key)
            out[key] = _decode_to_utf8(v)
        return out
    if isinstance(value, list):
        return [_decode_to_utf8(v) for v in value]
    if isinstance(value, tuple):
        return [_decode_to_utf8(v) for v in value]
    return value


def _looks_like_jsonish_container(text: str) -> bool:
    s = text.strip()
    if len(s) < 2:
        return False
    if (s[0], s[-1]) in {("{", "}"), ("[", "]")}:
        return True
    if s[0] in {"'", '"'} and s[-1] == s[0]:
        inner = s[1:-1].strip()
        return len(inner) >= 2 and (inner[0], inner[-1]) in {("{", "}"), ("[", "]")}
    return False


def _parse_jsonish_container(text: str) -> Optional[object]:
    s = text.strip()
    if not s:
        return None

    candidates: List[str] = [s]
    if s[0] in {"'", '"'} and s[-1] == s[0]:
        inner = s[1:-1].strip()
        if inner:
            candidates.insert(0, inner)

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, (dict, list)):
                return parsed
        except json.JSONDecodeError:
            pass

        try:
            parsed = ast.literal_eval(candidate)
        except (SyntaxError, ValueError):
            continue
        if isinstance(parsed, tuple):
            parsed = list(parsed)
        if isinstance(parsed, (dict, list)):
            return parsed

    return None


def _normalize_metadata_value(value: object) -> object:
    if isinstance(value, dict):
        return {str(k): _normalize_metadata_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_metadata_value(v) for v in value]
    if isinstance(value, tuple):
        return [_normalize_metadata_value(v) for v in value]
    if isinstance(value, str):
        cleaned = value.replace("\xa0", " ").replace("\\xa0", " ")
        if _looks_like_jsonish_container(cleaned):
            parsed = _parse_jsonish_container(cleaned)
            if parsed is not None:
                return _normalize_metadata_value(parsed)
        return cleaned
    return value


def _looks_like_html(text: str) -> bool:
    return bool(re.search(r"<[a-zA-Z][^>]*>", text))


def _parse_html_fragment(text: str) -> Dict[str, object]:
    cleaned = unescape(text or "")
    headings = [
        _strip_html(m.group(1))
        for m in re.finditer(r"<h[1-6][^>]*>\s*(.*?)\s*</h[1-6]>", cleaned, re.IGNORECASE | re.DOTALL)
        if _strip_html(m.group(1))
    ]
    strong = [
        _strip_html(m.group(1))
        for m in re.finditer(r"<strong[^>]*>\s*(.*?)\s*</strong>", cleaned, re.IGNORECASE | re.DOTALL)
        if _strip_html(m.group(1))
    ]
    links = [
        unescape(m.group(1)).strip()
        for m in re.finditer(r"""href=["']([^"']+)["']""", cleaned, re.IGNORECASE)
        if m.group(1).strip()
    ]
    return {
        "text": _strip_html(cleaned),
        "headings": headings,
        "strong": strong,
        "links": links,
    }


def _collect_html_fields(node: object, path: str, out: Dict[str, Dict[str, object]]) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            child_path = f"{path}.{k}" if path else str(k)
            _collect_html_fields(v, child_path, out)
        return
    if isinstance(node, list):
        for i, v in enumerate(node):
            child_path = f"{path}[{i}]"
            _collect_html_fields(v, child_path, out)
        return
    if isinstance(node, str) and _looks_like_html(node):
        out[path] = _parse_html_fragment(node)


def _standardize_raw_data(data: Dict[str, object]) -> Dict[str, object]:
    normalized = _decode_to_utf8(data)
    normalized = _normalize_metadata_value(normalized)
    if not isinstance(normalized, dict):
        return {"value": normalized}
    html_fields: Dict[str, Dict[str, object]] = {}
    _collect_html_fields(normalized, "", html_fields)
    if html_fields:
        normalized["_html"] = html_fields
    return normalized


def _value_from_path(
    data: Dict[str, object],
    path: List[str],
    flat_data: Optional[Dict[str, object]] = None,
) -> Optional[object]:
    value = None
    if flat_data:
        for candidate in _path_candidates(path):
            value = _lookup_key_ci(flat_data, candidate)
            if value is not None:
                break
    if value is None:
        value = _extract_path_value(data, path)
    return value


def _first_non_empty(value: object) -> Optional[object]:
    if isinstance(value, list):
        for item in value:
            picked = _first_non_empty(item)
            if picked is not None:
                return picked
        return None
    if value in (None, "", []):
        return None
    return value


def _extract_mapping_value(
    data: Dict[str, object],
    mapping: object,
    flat_data: Optional[Dict[str, object]] = None,
    resolved_values: Optional[Dict[str, object]] = None,
    target_field: Optional[str] = None,
) -> Optional[object]:
    if isinstance(mapping, list):
        return _value_from_path(data, [str(p) for p in mapping], flat_data)
    if not isinstance(mapping, dict):
        return None

    rule_type = str(mapping.get("type") or "").strip().lower()
    if rule_type in {"", "path"}:
        field = mapping.get("field", mapping.get("path"))
        if isinstance(field, list) and field:
            return _value_from_path(data, [str(p) for p in field], flat_data)
        if isinstance(field, str) and field.strip():
            if resolved_values and field.strip() in resolved_values:
                return resolved_values.get(field.strip())
            return _value_from_path(data, [field.strip()], flat_data)
        return None

    if rule_type == "gmapurl_parser":
        def _extract_lat_lng_from_url(raw_url: object) -> Optional[Tuple[str, str]]:
            if raw_url is None:
                return None
            text = str(raw_url).strip()
            if not text:
                return None
            try:
                parsed = urlparse(text)
                qs = parse_qs(parsed.query, keep_blank_values=True)
                q_val = (qs.get("q") or [""])[0].strip()
            except Exception:
                return None
            if not q_val:
                return None
            m = re.search(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)", q_val)
            if not m:
                return None
            return m.group(1), m.group(2)

        refs = mapping.get("fields")
        source_paths: List[object] = []
        if isinstance(refs, list):
            for ref in refs:
                if isinstance(ref, list) and ref:
                    source_paths.append([str(p) for p in ref])
                elif isinstance(ref, str) and ref.strip():
                    source_paths.append(ref.strip())
        if not source_paths:
            return None

        component = str(mapping.get("component", "")).strip().lower()
        if component in {"lat", "latitude"}:
            want = "lat"
        elif component in {"lng", "lon", "long", "longitude"}:
            want = "lng"
        else:
            tf = str(target_field or "").strip().lower()
            if tf in {"longitude", "lng", "lon", "long"}:
                want = "lng"
            else:
                want = "lat"

        for path in source_paths:
            if isinstance(path, str) and resolved_values and path in resolved_values:
                value = resolved_values.get(path)
            elif isinstance(path, str):
                value = _value_from_path(data, [path], flat_data)
            else:
                value = _value_from_path(data, path, flat_data)
            pair = _extract_lat_lng_from_url(value)
            if not pair:
                continue
            return pair[0] if want == "lat" else pair[1]
        return None

    if rule_type == "html":
        refs = mapping.get("fields")
        source_paths: List[object] = []
        if isinstance(refs, list):
            for ref in refs:
                if isinstance(ref, list) and ref:
                    source_paths.append([str(p) for p in ref])
                elif isinstance(ref, str) and ref.strip():
                    source_paths.append(ref.strip())
        if not source_paths:
            return None

        when = str(mapping.get("when", "any")).strip().lower()
        if when not in {"pre", "post", "any"}:
            when = "any"

        def _resolve_source(path: object) -> object:
            # `post` means prefer resolved values (e.g. already-resolved "name").
            if when in {"post", "any"} and resolved_values:
                if isinstance(path, str) and path in resolved_values:
                    return resolved_values.get(path)
                if isinstance(path, list) and len(path) == 1:
                    key = str(path[0]).strip()
                    if key and key in resolved_values:
                        return resolved_values.get(key)

            # `pre` means use raw metadata values.
            if isinstance(path, str):
                return _value_from_path(data, [path], flat_data)
            return _value_from_path(data, path, flat_data)

        html_text = None
        for path in source_paths:
            value = _first_non_empty(_resolve_source(path))
            if value is None:
                continue
            html_text = str(value).strip()
            if html_text:
                break
        if not html_text:
            return None

        objects = mapping.get("objects")
        requested_tags: List[str] = []
        requested_line_indices: List[int] = []
        if isinstance(objects, list):
            for obj in objects:
                token = str(obj).strip().lower()
                if token:
                    if re.fullmatch(r"-?\d+", token):
                        try:
                            requested_line_indices.append(int(token))
                        except (TypeError, ValueError):
                            pass
                    else:
                        requested_tags.append(token)

        if requested_line_indices:
            lines = [
                _strip_html(unescape(seg))
                for seg in re.split(r"<br\s*/?>", html_text, flags=re.IGNORECASE)
            ]
            selected_lines: List[str] = []
            for idx in requested_line_indices:
                pos = idx if idx >= 0 else len(lines) + idx
                if pos < 0 or pos >= len(lines):
                    continue
                value = lines[pos].strip()
                if value:
                    selected_lines.append(value)
            if selected_lines:
                join_separator = str(mapping.get("join_separator", " "))
                return join_separator.join(selected_lines).strip()

        if not requested_tags:
            return _strip_html(html_text)

        extracted: List[str] = []
        for tag in requested_tags:
            pattern = rf"<{tag}\b[^>]*>(.*?)</{tag}>"
            for match in re.finditer(pattern, html_text, re.IGNORECASE | re.DOTALL):
                text = _strip_html(match.group(1))
                if text:
                    extracted.append(text)

        if not extracted:
            return None
        join_separator = str(mapping.get("join_separator", " "))
        return join_separator.join(extracted).strip()

    if rule_type == "split":
        refs = mapping.get("fields")
        source_paths: List[object] = []
        if isinstance(refs, list):
            for ref in refs:
                if isinstance(ref, list) and ref:
                    source_paths.append([str(p) for p in ref])
                elif isinstance(ref, str) and ref.strip():
                    source_paths.append(ref.strip())
        if not source_paths:
            return None

        text_value = None
        for path in source_paths:
            if isinstance(path, str) and resolved_values and path in resolved_values:
                value = resolved_values.get(path)
            elif isinstance(path, str):
                value = _value_from_path(data, [path], flat_data)
            else:
                value = _value_from_path(data, path, flat_data)
            picked = _first_non_empty(value)
            if picked is None:
                continue
            text_value = str(picked).strip()
            if text_value:
                break
        if not text_value:
            return None

        separators = mapping.get("separators")
        sep_pattern = None
        if isinstance(separators, list) and separators:
            escaped = [re.escape(str(s)) for s in separators if str(s)]
            if escaped:
                sep_pattern = "|".join(escaped)
        if not sep_pattern:
            sep_pattern = re.escape(" : ")

        def _clean_part(text: str) -> str:
            part = str(text).strip()
            part = part.strip("\"'`“”")
            return part.strip()

        parts = [_clean_part(p) for p in re.split(sep_pattern, text_value) if _clean_part(p)]
        if not parts:
            return None

        indices_raw = mapping.get("indices")
        indices: List[int] = []
        if isinstance(indices_raw, list):
            for idx in indices_raw:
                try:
                    indices.append(int(idx))
                except (TypeError, ValueError):
                    continue
        if not indices:
            indices = [0]

        selected: List[str] = []
        for idx in indices:
            if idx < 0:
                pos = len(parts) + idx
            else:
                pos = idx
            if pos < 0 or pos >= len(parts):
                continue
            selected.append(parts[pos])
        if not selected:
            # Ignore out-of-range indices by returning the original cleaned text.
            return _clean_part(text_value)

        join_separator = str(mapping.get("join_separator", " "))
        return join_separator.join(selected).strip()

    if rule_type == "append":
        fields = mapping.get("fields")
        if not isinstance(fields, list):
            return None
        sep = str(mapping.get("separator", " "))
        parts: List[str] = []
        for field in fields:
            if isinstance(field, list) and field:
                path = [str(p) for p in field]
            elif isinstance(field, str) and field.strip():
                path = [field.strip()]
            else:
                continue
            value = _first_non_empty(_value_from_path(data, path, flat_data))
            if value is None:
                continue
            text = str(value).strip()
            if text:
                parts.append(text)
        return sep.join(parts) if parts else None

    if rule_type == "html_text":
        field = mapping.get("field", mapping.get("path"))
        if isinstance(field, list) and field:
            path = [str(p) for p in field]
        elif isinstance(field, str) and field.strip():
            path = [field.strip()]
        else:
            return None
        value = _first_non_empty(_value_from_path(data, path, flat_data))
        if value is None:
            return None
        return _strip_html(str(value))

    if rule_type == "links":
        field = mapping.get("field", mapping.get("path"))
        if isinstance(field, list) and field:
            path = [str(p) for p in field]
        elif isinstance(field, str) and field.strip():
            path = [field.strip()]
        else:
            return None
        value = _first_non_empty(_value_from_path(data, path, flat_data))
        if value is None:
            return None
        text = str(value)
        links = [
            unescape(m.group(1)).strip()
            for m in re.finditer(r"""href=["']([^"']+)["']""", text, re.IGNORECASE)
            if m.group(1).strip()
        ]
        if not links:
            return None
        mode = str(mapping.get("mode", "all")).strip().lower()
        if mode == "first":
            return links[0]
        sep = str(mapping.get("separator", ", "))
        return sep.join(links)

    if rule_type == "bow_classify":
        refs: List[object] = []
        for key in ("pre", "fields", "post"):
            raw = mapping.get(key)
            if raw is None:
                continue
            refs.extend(raw if isinstance(raw, list) else [raw])
        source_paths: List[object] = []
        for ref in refs:
            if isinstance(ref, list) and ref:
                source_paths.append([str(p) for p in ref])
            elif isinstance(ref, str) and ref.strip():
                source_paths.append(ref.strip())
        if not source_paths:
            source_paths = [
                ["name"],
                ["Name"],
                ["title"],
                ["location_name"],
                ["locationName"],
                ["store"],
                ["category"],
                ["categories"],
                ["type"],
                ["description"],
                ["content"],
            ]

        def _collect_texts(value: object) -> List[str]:
            out: List[str] = []
            if value is None:
                return out
            if isinstance(value, (list, tuple)):
                for item in value:
                    out.extend(_collect_texts(item))
                return out
            if isinstance(value, dict):
                for item in value.values():
                    out.extend(_collect_texts(item))
                return out
            text = str(value).strip()
            if text:
                out.append(text)
            return out

        texts: List[str] = []
        for path in source_paths:
            if isinstance(path, str) and resolved_values and path in resolved_values:
                val = resolved_values.get(path)
            elif isinstance(path, str):
                val = _value_from_path(data, [path], flat_data)
            else:
                val = _value_from_path(data, path, flat_data)
            texts.extend(_collect_texts(val))
        if not texts:
            return None

        categories_obj = mapping.get("categories")
        categories: Dict[str, List[str]] = {}
        if isinstance(categories_obj, dict):
            for cat, keywords in categories_obj.items():
                if not isinstance(keywords, list):
                    continue
                categories[str(cat).strip().upper()] = [
                    str(v).strip().lower() for v in keywords if str(v).strip()
                ]
        if not categories:
            categories = DEFAULT_TYPE_BOW

        haystack = " ".join(texts).lower()
        haystack = re.sub(r"[^a-z0-9]+", " ", haystack)
        haystack = f" {haystack.strip()} "
        matches: List[str] = []
        for category, keywords in categories.items():
            for kw in keywords:
                needle = re.sub(r"[^a-z0-9]+", " ", str(kw).strip().lower())
                if not needle:
                    continue
                if f" {needle} " in haystack:
                    matches.append(category)
                    break
        return matches or None

    return None


def _first_from_paths(
    data: Dict[str, object],
    paths: List[object],
    flat_data: Optional[Dict[str, object]] = None,
    resolved_values: Optional[Dict[str, object]] = None,
    target_field: Optional[str] = None,
) -> Optional[str]:
    for mapping in paths:
        value = _first_non_empty(
            _extract_mapping_value(
                data,
                mapping,
                flat_data,
                resolved_values,
                target_field,
            )
        )
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _tags_from_raw_value(value: object) -> List[str]:
    tags: List[str] = []
    if value is None:
        return tags
    if isinstance(value, (list, tuple)):
        for item in value:
            tags.extend(_tags_from_raw_value(item))
        return tags

    text = str(value).strip()
    if not text:
        return tags

    parts = [part.strip() for part in text.split(",")] if "," in text else [text]
    for part in parts:
        if not part:
            continue
        upper = _uppercase(part)
        if not upper:
            continue
        # Avoid numeric-only category artifacts.
        if upper.isdigit():
            continue
        tags.append(upper)
    return tags


def _tags_from_paths(
    data: Dict[str, object],
    paths: List[object],
    flat_data: Optional[Dict[str, object]] = None,
    resolved_values: Optional[Dict[str, object]] = None,
) -> List[str]:
    tags: List[str] = []
    seen = set()

    for mapping in paths:
        value = _extract_mapping_value(data, mapping, flat_data, resolved_values, None)
        if value is None:
            continue
        for tag in _tags_from_raw_value(value):
            if tag in seen:
                continue
            seen.add(tag)
            tags.append(tag)
    return tags


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_name_from_content(data: Dict[str, object]) -> Optional[str]:
    for key in ("content", "tooltipContent", "description"):
        raw = _lookup_key_ci(data, key)
        if not raw:
            continue
        txt = str(raw)
        m = re.search(r"<h[34][^>]*>\s*(.*?)\s*</h[34]>", txt, re.IGNORECASE | re.DOTALL)
        if m:
            return _strip_html(m.group(1))
        m = re.search(r"<strong[^>]*>\s*(.*?)\s*</strong>", txt, re.IGNORECASE | re.DOTALL)
        if m:
            return _strip_html(m.group(1))
    return None


def _extract_address_from_content(data: Dict[str, object]) -> Optional[str]:
    for key in ("content", "tooltipContent"):
        raw = _lookup_key_ci(data, key)
        if not raw:
            continue
        txt = str(raw)
        # Common pattern in HTML content: strong block contains address line.
        strong_blocks = re.findall(r"<strong[^>]*>\s*(.*?)\s*</strong>", txt, re.IGNORECASE | re.DOTALL)
        for blk in strong_blocks:
            candidate = _strip_html(blk)
            if ZIP_RE.search(candidate) or re.search(r"\b[A-Z]{2}\b", candidate):
                return candidate

        # Prefer explicit map destination links when present.
        hrefs = [
            m.group(1).strip()
            for m in re.finditer(r"""href=["']([^"']+)["']""", txt, re.IGNORECASE)
            if m.group(1).strip()
        ]
        for href in hrefs:
            try:
                parsed_href = urlparse(unescape(href))
                qs = parse_qs(parsed_href.query, keep_blank_values=True)
            except Exception:
                continue
            for qkey in ("destination", "daddr", "q"):
                vals = qs.get(qkey) or []
                if not vals:
                    continue
                addr = unquote_plus(str(vals[0]).strip())
                addr = _strip_html(addr)
                if not addr:
                    continue
                if ZIP_RE.search(addr):
                    return addr
                if re.match(r"^\s*\d{1,6}\b", addr) and (
                    "," in addr or re.search(r"\b[A-Za-z]{2}\b", addr)
                ):
                    return addr

        # Parse line-like segments split by <br>, preferring lines that start with street number.
        lines = [
            _strip_html(unescape(seg))
            for seg in re.split(r"<br\s*/?>", txt, flags=re.IGNORECASE)
        ]
        numeric_line = next(
            (
                ln
                for ln in lines
                if ln
                and re.match(r"^\s*\d{1,6}\b", ln)
                and (
                    ZIP_RE.search(ln)
                    or "," in ln
                    or re.search(r"\b[A-Za-z]{2}\b", ln)
                )
            ),
            None,
        )
        if numeric_line:
            return numeric_line
        zip_line = next((ln for ln in lines if ln and ZIP_RE.search(ln)), None)
        if zip_line:
            return zip_line

        # fallback to plain-text extraction
        plain = _strip_html(txt)
        m = ZIP_RE.search(plain)
        if m:
            start = max(0, plain.rfind(" ", 0, m.start() - 25))
            snippet = plain[start:m.end() + 1].strip(" ,")
            if len(snippet) >= 10:
                return snippet
    return None


def _empty_parsed_address() -> Dict[str, Optional[str]]:
    return {
        "addressNumber": None,
        "streetName": None,
        "streetNamePostType": None,
        "occupancyType": None,
        "occupancyIdentifier": None,
        "cityName": None,
        "stateCode": None,
        "zipCode": None,
    }


def _parse_address(address_full: str) -> Tuple[Dict[str, Optional[str]], bool]:
    if not address_full:
        return _empty_parsed_address(), False

    try:
        tagged, _ = usaddress.tag(address_full)
    except Exception:
        return _empty_parsed_address(), False

    def _get(key: str) -> Optional[str]:
        value = tagged.get(key)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    return {
        "addressNumber": _get("AddressNumber"),
        "streetName": _get("StreetName"),
        "streetNamePostType": _get("StreetNamePostType"),
        "occupancyType": _get("OccupancyType"),
        "occupancyIdentifier": _get("OccupancyIdentifier"),
        "cityName": _get("PlaceName"),
        "stateCode": _get("StateName"),
        "zipCode": _get("ZipCode"),
    }, True


def _coerce_address_input(address_full: Optional[str]) -> Optional[str]:
    if not address_full:
        return address_full
    candidate = address_full.strip()
    parsed = urlparse(candidate)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        qs = parse_qs(parsed.query, keep_blank_values=True)
        q_value = None
        for key in ("q", "query", "address", "destination"):
            vals = qs.get(key)
            if vals and vals[0].strip():
                q_value = vals[0].strip()
                break
        if q_value:
            return unquote_plus(q_value)
    return candidate


def _build_address_from_parts(
    *,
    address1: Optional[str],
    address2: Optional[str],
    city: Optional[str],
    state: Optional[str],
    zip_code: Optional[str],
    country: Optional[str],
) -> Optional[str]:
    street = " ".join([v.strip() for v in [address1 or "", address2 or ""] if v and str(v).strip()]).strip()
    city_part = str(city).strip() if city is not None else ""
    state_part = str(state).strip() if state is not None else ""
    zip_part = str(zip_code).strip() if zip_code is not None else ""
    country_part = str(country).strip() if country is not None else ""

    locality = " ".join([v for v in [city_part, state_part, zip_part] if v]).strip()
    parts = [v for v in [street, locality, country_part] if v]
    if not parts:
        return None
    return ", ".join(parts)


def _standardize_address_full(
    *,
    parsed_addr: Dict[str, Optional[str]],
    city_name: Optional[str],
    state_code: Optional[str],
    zip_code: Optional[str],
    country_code: Optional[str],
) -> Optional[str]:
    street_parts: List[str] = []
    if parsed_addr.get("addressNumber"):
        street_parts.append(str(parsed_addr["addressNumber"]).strip())
    if parsed_addr.get("streetName"):
        street_parts.append(str(parsed_addr["streetName"]).strip())
    if parsed_addr.get("streetNamePostType"):
        normalized_suffix = _normalize_street_suffix(parsed_addr.get("streetNamePostType"))
        if normalized_suffix:
            street_parts.append(normalized_suffix)
    if parsed_addr.get("occupancyType"):
        normalized_occ = _normalize_occupancy_type(parsed_addr.get("occupancyType"))
        if normalized_occ:
            street_parts.append(normalized_occ)
    if parsed_addr.get("occupancyIdentifier"):
        street_parts.append(str(parsed_addr["occupancyIdentifier"]).strip())

    street = " ".join([v for v in street_parts if v]).strip()
    locality = " ".join([v for v in [city_name, state_code, zip_code] if v]).strip()
    parts = [v for v in [street, locality, country_code] if v]
    if not parts:
        return None
    return ", ".join(parts)


def _uppercase(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    # Decode HTML entities (including double-encoded forms like &amp;#038;).
    for _ in range(3):
        decoded = unescape(text)
        if decoded == text:
            break
        text = decoded
    # Ensure UTF-8-safe string and normalize whitespace in final output values.
    text = text.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    text = re.sub(r"\s+", " ", text).strip()
    return text.upper() if text else None


def _normalize_token(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return re.sub(r"[^A-Z0-9]", "", _uppercase(value) or "")


def _normalize_street_suffix(value: Optional[str]) -> Optional[str]:
    token = _normalize_token(value)
    if not token:
        return None
    return USPS_STREET_SUFFIX.get(token, token)


def _normalize_occupancy_type(value: Optional[str]) -> Optional[str]:
    token = _normalize_token(value)
    if not token:
        return None
    return USPS_UNIT_DESIGNATOR.get(token, token)


def _normalize_state_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = _uppercase(value)
    if not s:
        return None
    if len(s) == 2 and s.isalpha():
        return s
    m = re.match(r"^([A-Z]{2})\b", s)
    if m:
        return m.group(1)
    if s in STATE_NAME_TO_CODE:
        return STATE_NAME_TO_CODE[s]
    return None


def _normalize_country_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return "US"
    s = _uppercase(value)
    if s in COUNTRY_TO_CODE:
        return COUNTRY_TO_CODE[s]
    if len(s) == 2 and s.isalpha():
        return s
    return "US"


def _normalize_city_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = _uppercase(value)
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip(" ,.;:")
    return s or None


def _infer_zip(data: Dict[str, object], address_full: Optional[str]) -> Optional[str]:
    def _looks_like_latlng(value: str) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return False
        # Common coordinate patterns, e.g. "43.94582,-90.8123" or map URLs with ?q=lat,lng
        if re.search(r"-?\d{1,3}\.\d+\s*,\s*-?\d{1,3}\.\d+", text):
            return True
        return False

    if address_full:
        if _looks_like_latlng(address_full):
            m = None
        else:
            m = ZIP_RE.search(address_full)
        if m:
            return m.group(0)
    for value in data.values():
        if isinstance(value, (str, int, float)):
            value_text = str(value)
            if _looks_like_latlng(value_text):
                continue
            m = ZIP_RE.search(value_text)
            if m:
                return m.group(0)
    return None


def _normalize_phone_number(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    digits = re.sub(r"\D", "", text)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return text

    area = digits[0:3]
    first = digits[3:6]
    last = digits[6:10]
    return f"{area}-{first}-{last}"


def _is_valid_reference_id(value: Optional[str]) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    # Keep only strictly alphanumeric ids; reject URLs and other punctuated forms.
    return bool(re.fullmatch(r"[A-Za-z0-9]+", text))


def _reference_id_from_metadata_hash(metadata: object) -> str:
    try:
        serialized = json.dumps(metadata, sort_keys=True, default=str, ensure_ascii=False)
    except TypeError:
        serialized = str(metadata)
    return hashlib.md5(serialized.encode("utf-8", errors="replace")).hexdigest()  # nosec


def _merge_field_mappings(
    defaults: Dict[str, List[object]], org_mapping: Dict[str, object]
) -> Dict[str, List[object]]:
    merged = {k: _normalize_paths(v) for k, v in defaults.items()}

    for field, value in org_mapping.items():
        norm = _normalize_paths(value)
        if norm:
            merged[str(field)] = norm

    return merged


def _conform_record(
    envelope: Dict[str, object],
    created_time: str,
    modified_time: str,
    field_mappings: Dict[str, List[object]],
    source_org_id: str,
) -> Dict[str, object]:
    """Convert one raw envelope into a conformed output record."""
    org = envelope.get("org") if isinstance(envelope.get("org"), dict) else {}
    request = envelope.get("request") if isinstance(envelope.get("request"), dict) else {}
    source_data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
    data = _standardize_raw_data(source_data)
    flat_data = _flatten_data(data)

    # Authoritative org id comes from the raw envelope org context.
    organization_id_value = org.get("organizationId")
    if organization_id_value is None:
        organization_id_value = org.get("OrganizationID")
    organization_id = (
        str(organization_id_value).strip()
        if organization_id_value is not None
        else (str(source_org_id).strip() if source_org_id is not None else None)
    )
    source_url_value = request.get("url")
    source_url = str(source_url_value).strip() if source_url_value is not None else None
    if not source_url:
        source_url = None

    resolved_values: Dict[str, object] = {}

    name = _first_from_paths(data, field_mappings.get("name", []), flat_data, resolved_values)
    if not name:
        name = _extract_name_from_content(data)
    resolved_values["name"] = name

    reference_id = _first_from_paths(
        data, field_mappings.get("referenceId", []), flat_data, resolved_values
    )
    if not _is_valid_reference_id(reference_id):
        reference_id = _reference_id_from_metadata_hash(data)

    latitude = _to_float(
        _first_from_paths(
            data,
            field_mappings.get("latitude", []),
            flat_data,
            resolved_values,
            "latitude",
        )
    )
    longitude = _to_float(
        _first_from_paths(
            data,
            field_mappings.get("longitude", []),
            flat_data,
            resolved_values,
            "longitude",
        )
    )

    mapped_address_full = _first_from_paths(
        data, field_mappings.get("addressFull", []), flat_data, resolved_values
    )
    mapped_address1 = _first_from_paths(
        data, field_mappings.get("address1", []), flat_data, resolved_values
    )
    mapped_address2 = _first_from_paths(
        data, field_mappings.get("address2", []), flat_data, resolved_values
    )
    mapped_city = _first_from_paths(
        data, field_mappings.get("cityName", []), flat_data, resolved_values
    )
    mapped_state = _first_from_paths(
        data, field_mappings.get("stateCode", []), flat_data, resolved_values
    )
    mapped_zip = _first_from_paths(
        data, field_mappings.get("zipCode", []), flat_data, resolved_values
    )
    mapped_country = _first_from_paths(
        data, field_mappings.get("countryCode", []), flat_data, resolved_values
    )
    mapped_phone = _first_from_paths(
        data, field_mappings.get("phoneNumber", []), flat_data, resolved_values
    )
    synthesized = _build_address_from_parts(
        address1=mapped_address1,
        address2=mapped_address2,
        city=mapped_city,
        state=mapped_state,
        zip_code=mapped_zip,
        country=mapped_country,
    )

    # Resolution order:
    # 1) explicit mapped addressFull
    # 2) synthesized from mapped components
    # 3) extracted from HTML content
    address_full = mapped_address_full or synthesized or _extract_address_from_content(data)
    address_full = _coerce_address_input(address_full)
    parsed_addr, _ = _parse_address(address_full or "")

    # If both addressFull and zipCode are explicitly mapped/populated, prefer that zip.
    if mapped_address_full and mapped_zip:
        zip_code = mapped_zip
    else:
        zip_code = parsed_addr["zipCode"] or mapped_zip

    city_name = parsed_addr["cityName"] or mapped_city
    state_code = parsed_addr["stateCode"] or mapped_state

    country_code = mapped_country
    resolved_values["addressFull"] = address_full
    resolved_values["cityName"] = city_name
    resolved_values["stateCode"] = state_code
    resolved_values["zipCode"] = zip_code
    resolved_values["countryCode"] = country_code

    resource_types = _tags_from_paths(
        data, field_mappings.get("type", []), flat_data, resolved_values
    )
    if "EFO" in resource_types:
        resource_types = [tag for tag in resource_types if tag != "EFO"]
    resource_types = ["EFO", *resource_types]

    state_code_norm = _normalize_state_code(state_code)
    country_code_norm = _normalize_country_code(country_code)
    city_name_norm = _normalize_city_name(city_name)
    standardized_full_address = _standardize_address_full(
        parsed_addr=parsed_addr,
        city_name=city_name_norm,
        state_code=state_code_norm,
        zip_code=str(zip_code).strip() if zip_code is not None else None,
        country_code=country_code_norm,
    )

    return {
        "createdTime": created_time,
        "modifiedTime": modified_time,
        "organizationId": organization_id,
        "name": _uppercase(name),
        "referenceId": str(reference_id).strip() if reference_id is not None else None,
        "sourceUrl": source_url,
        "type": resource_types,
        "latitude": latitude,
        "longitude": longitude,
        "addressFull": _uppercase(standardized_full_address),
        "addressNumber": _uppercase(parsed_addr["addressNumber"]),
        "streetName": _uppercase(parsed_addr["streetName"]),
        "streetNamePostType": _normalize_street_suffix(parsed_addr["streetNamePostType"]),
        "occupancyType": _normalize_occupancy_type(parsed_addr["occupancyType"]),
        "occupancyIdentifier": _uppercase(parsed_addr["occupancyIdentifier"]),
        "cityName": city_name_norm,
        "stateCode": state_code_norm,
        "countryCode": country_code_norm,
        "zipCode": str(zip_code).strip() if zip_code is not None else None,
        "phoneNumber": _normalize_phone_number(
            str(mapped_phone).strip() if mapped_phone is not None else None
        ),
        "metadata": data,
    }


def _iter_records_from_file(
    run_file: Path,
    defaults: Dict[str, List[object]],
    org_mapping: Dict[str, object],
    modified_time: str,
    source_org_id: str,
) -> Iterable[Dict[str, object]]:
    created_time = _file_created_time_iso(run_file)
    field_mappings = _merge_field_mappings(defaults, org_mapping)
    with run_file.open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(envelope, dict):
                continue
            yield _conform_record(
                envelope,
                created_time,
                modified_time,
                field_mappings,
                source_org_id,
            )


def _dedupe_by_raw(records: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    seen = set()
    deduped: List[Dict[str, object]] = []
    for rec in records:
        org_id = str(rec.get("organizationId") or "").strip()
        metadata = rec.get("metadata")
        raw_md5 = _reference_id_from_metadata_hash(metadata)
        key = ("org_raw_md5", org_id, raw_md5)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rec)
    return deduped


def _write_conformed(out_path: Path, records: Iterable[Dict[str, object]]) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as handle:
        for rec in records:
            handle.write(json.dumps(_decode_to_utf8(rec), default=str, ensure_ascii=False) + "\n")
    return out_path


def conform_run_outputs(
    *,
    run_id: str,
    food_banks: List[FoodBankConfig],
    output_dir: str,
    mapping_path: Optional[str | Path] = None,
) -> Path:
    raw_root = Path(output_dir) / "raw"
    conformed_root = Path(output_dir) / "conformed"
    out_path = conformed_root / f"{run_id}.jsonl"

    mapping_file = Path(mapping_path) if mapping_path else _default_mapping_path()
    defaults, org_mappings = _load_conform_mappings(mapping_file)
    modified_time = datetime.now(timezone.utc).isoformat()

    org_ids = sorted({str(cfg.organization_id) for cfg in food_banks if cfg.organization_id is not None})
    _ensure_org_mapping_entries(mapping_file, defaults, org_mappings, org_ids)

    records: List[Dict[str, object]] = []
    for org_id in org_ids:
        org_dir = raw_root / org_id
        if not org_dir.exists():
            continue
        run_file = _latest_run_file(org_dir, run_id)
        if run_file is None:
            continue
        records.extend(
            _iter_records_from_file(
                run_file,
                defaults,
                org_mappings.get(org_id, {}),
                modified_time,
                org_id,
            )
        )

    deduped = _dedupe_by_raw(records)
    return _write_conformed(out_path, deduped)


def conform_latest_raw_outputs(
    *,
    output_dir: str,
    mapping_path: Optional[str | Path] = None,
    org_ids: Optional[List[str]] = None,
) -> Path:
    raw_root = Path(output_dir) / "raw"
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(output_dir) / "conformed" / f"{run_stamp}.jsonl"

    mapping_file = Path(mapping_path) if mapping_path else _default_mapping_path()
    defaults, org_mappings = _load_conform_mappings(mapping_file)
    modified_time = datetime.now(timezone.utc).isoformat()

    all_org_dirs = [p for p in raw_root.iterdir() if p.is_dir()] if raw_root.exists() else []
    requested_orgs = {str(v).strip() for v in (org_ids or []) if str(v).strip()}
    if requested_orgs:
        org_dirs = [p for p in all_org_dirs if p.name in requested_orgs]
    else:
        org_dirs = all_org_dirs

    resolved_org_ids = sorted([p.name for p in org_dirs])
    _ensure_org_mapping_entries(mapping_file, defaults, org_mappings, resolved_org_ids)

    records: List[Dict[str, object]] = []
    for org_dir in org_dirs:
        org_id = org_dir.name
        latest = _latest_run_file(org_dir, "")
        if latest is None:
            continue
        records.extend(
            _iter_records_from_file(
                latest,
                defaults,
                org_mappings.get(org_id, {}),
                modified_time,
                org_id,
            )
        )

    deduped = _dedupe_by_raw(records)
    return _write_conformed(out_path, deduped)
