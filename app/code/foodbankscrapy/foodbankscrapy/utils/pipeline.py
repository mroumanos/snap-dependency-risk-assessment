"""Pipeline row parsing and runtime config materialization.

This module converts JSON/CSV pipeline rows into typed `FoodBankConfig`
objects with resolved generator/evaluator/parser callables.
"""

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from .params import build_params


@dataclass(frozen=True)
class Config:
    """Resolved runtime config for one organization crawl row."""
    state: str
    organization_id: Optional[int]
    name: str
    spider: str
    parser: str
    parser_func: Callable[..., object]
    parser_kwargs: Any
    parser_chain: Optional[List[Dict[str, object]]]
    evaluator: str
    evaluator_func: Callable[..., object]
    evaluator_kwargs: Dict[str, object]
    generator: str
    generator_func: Callable[..., object]
    generator_kwargs: Dict[str, object]
    source: str
    params: Dict[str, object]
    raw: Dict[str, str]


@dataclass(frozen=True)
class FoodBankConfig(Config):
    """Semantic alias for `Config` used by spider interfaces."""
    pass


def _parse_int(value: str) -> Optional[int]:
    """Parse optional integer fields from text payloads."""
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_parser_kwargs(value: object) -> Any:
    """Parse parser kwargs from JSON/text into list or dict forms."""
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    value = str(value).strip()
    if not value or value.upper() == "FALSE":
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [str(v) for v in parsed]
    if isinstance(parsed, dict):
        return parsed
    return []


def _parse_json_dict(value: object) -> Dict[str, object]:
    """Parse optional JSON dict payload with permissive fallback."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    value = str(value).strip()
    if not value or value.upper() == "FALSE":
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _parse_named_block(
    value: object, fallback_name: str, fallback_kwargs: Dict[str, object]
) -> tuple[str, Dict[str, object]]:
    """Support shorthand `{name, ...kwargs}` block syntax in pipeline rows."""
    if isinstance(value, dict):
        name = str(value.get("name") or value.get("type") or fallback_name).strip().lower()
        kwargs = {k: v for k, v in value.items() if k not in {"name", "type"}}
        return name, kwargs
    return fallback_name, fallback_kwargs


def _load_rows(path: Path) -> List[Dict[str, str]]:
    """Load pipeline rows from JSON or CSV sources."""
    if path.suffix.lower() == ".json":
        raw = json.loads(path.read_text())
        if isinstance(raw, dict):
            rows = raw.get("rows", [])
        else:
            rows = raw
        if not isinstance(rows, list):
            raise ValueError("JSON pipeline must be a list of objects or {\"rows\": [...]} ")
        return [dict(r) for r in rows]
    rows: List[Dict[str, str]] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
    return rows


_CAMEL_KEYS = {
    "mailAddressState": "MailAddress_State",
    "mailAddressCity": "MailAddress_City",
    "mailAddressZip": "MailAddress_Zip",
    "mailAddressLatitude": "MailAddress_Latitude",
    "mailAddressLongitude": "MailAddress_Longitude",
    "entityId": "EntityID",
    "organizationId": "OrganizationID",
    "fullName": "FullName",
    "agencyUrl": "AgencyURL",
    "url": "URL",
    "generator": "Generator",
    "generatorKwargs": "Generator Kwargs",
    "evaluator": "Evaluator",
    "evaluatorKwargs": "Evaluator Kwargs",
    "parser": "Parser",
    "parserKwargs": "Parser Kwargs",
    "source": "Source",
    "spider": "Spider",
}


def _normalize_row(row: Dict[str, str]) -> Dict[str, str]:
    """Backfill legacy column names from camelCase aliases."""
    if any(k in row for k in _CAMEL_KEYS):
        normalized = dict(row)
        for camel_key, legacy_key in _CAMEL_KEYS.items():
            if camel_key in row and legacy_key not in normalized:
                normalized[legacy_key] = row[camel_key]
        return normalized
    return row


def load_pipeline_rows(path: str | Path) -> List[FoodBankConfig]:
    """Resolve pipeline rows into validated `FoodBankConfig` objects."""
    from .evaluators import EVALUATOR_REGISTRY
    from .generators import GENERATOR_REGISTRY
    from .parsers import PARSER_REGISTRY

    path = Path(path)
    rows: List[FoodBankConfig] = []
    raw_rows = _load_rows(path)
    import logging
    logger = logging.getLogger(__name__)
    logger.info("[pipeline] loaded %s raw rows from %s", len(raw_rows), path)
    if raw_rows:
        logger.info("[pipeline] first row keys: %s", sorted(raw_rows[0].keys()))
    # CSV rows start at line 2 because line 1 is header; JSON is 1-based.
    row_start = 2 if path.suffix.lower() != ".json" else 1
    for idx, row in enumerate(raw_rows, start=row_start):
        row = _normalize_row(row)
        spider = str(row.get("Spider", "")).strip().lower()
        raw_parser = row.get("Parser", "")
        raw_evaluator = row.get("Evaluator", "")
        raw_generator = row.get("Generator", "")
        parser = str(raw_parser).strip() if not isinstance(raw_parser, (list, dict)) else ""
        evaluator = str(raw_evaluator).strip() if not isinstance(raw_evaluator, dict) else ""
        generator = str(raw_generator).strip() if not isinstance(raw_generator, dict) else ""
        if not generator:
            generator = "accessfood" if spider == "accessfood" else "url"
        if not evaluator:
            evaluator = "accessfood_pagination" if spider == "accessfood" else "none"

        if not parser:
            parser = "json_path"
        parser = parser.lower()
        if parser == "jsonparser":
            parser = "json_path"
        evaluator = evaluator.lower()
        generator = generator.lower()

        parser_chain = None
        if isinstance(raw_parser, list):
            parser_chain = raw_parser
            parser = "chain"
        elif isinstance(raw_parser, dict):
            parser_chain = [raw_parser]
            parser = "chain"

        parser_kwargs = _parse_parser_kwargs(row.get("Parser Kwargs", ""))
        if not parser_chain:
            if spider == "accessfood" and not parser_kwargs:
                parser_kwargs = ["item1"]
            if parser == "storepoint" and not parser_kwargs:
                parser_kwargs = ["results", "locations"]

        generator_kwargs = _parse_json_dict(row.get("Generator Kwargs", ""))
        generator, generator_kwargs = _parse_named_block(raw_generator, generator, generator_kwargs)
        source = str(row.get("Source", "")).strip()
        if not source and isinstance(generator_kwargs, dict):
            source = str(generator_kwargs.get("source") or "").strip()

        evaluator_kwargs = _parse_json_dict(row.get("Evaluator Kwargs", ""))
        evaluator, evaluator_kwargs = _parse_named_block(raw_evaluator, evaluator, evaluator_kwargs)

        generator_func = GENERATOR_REGISTRY.get(generator)
        if generator_func is None:
            raise ValueError(
                f"Row {idx} org_id={row.get('OrganizationID')} invalid generator "
                f"'{generator}'. Allowed: {sorted(GENERATOR_REGISTRY.keys())}"
            )
        evaluator_func = EVALUATOR_REGISTRY.get(evaluator)
        if evaluator_func is None:
            raise ValueError(
                f"Row {idx} org_id={row.get('OrganizationID')} invalid evaluator "
                f"'{evaluator}'. Allowed: {sorted(EVALUATOR_REGISTRY.keys())}"
            )
        parser_func = PARSER_REGISTRY.get(parser)
        if parser_func is None:
            raise ValueError(
                f"Row {idx} org_id={row.get('OrganizationID')} invalid parser "
                f"'{parser}'. Allowed: {sorted(PARSER_REGISTRY.keys())}"
            )

        rows.append(
            FoodBankConfig(
                state=str(row.get("MailAddress_State", "")).strip(),
                organization_id=_parse_int(row.get("OrganizationID")),
                name=str(row.get("FullName", "")).strip(),
                spider=spider,
                parser=parser,
                parser_func=parser_func,
                parser_kwargs=parser_kwargs,
                parser_chain=parser_chain,
                evaluator=evaluator,
                evaluator_func=evaluator_func,
                evaluator_kwargs=evaluator_kwargs,
                generator=generator,
                generator_func=generator_func,
                generator_kwargs=generator_kwargs,
                source=source,
                params=build_params(row, generator, generator_kwargs),
                raw=row,
            )
        )
    return rows


def filter_rows(
    rows: Iterable[FoodBankConfig],
    *,
    state: Optional[str] = None,
    organization_id: Optional[int] = None,
) -> List[FoodBankConfig]:
    """Filter configs by optional state and organization id selectors."""
    state_norm = state.strip().upper() if state else None
    filtered = []
    for row in rows:
        if state_norm and row.state.upper() != state_norm:
            continue
        if organization_id and row.organization_id != organization_id:
            continue
        filtered.append(row)
    return filtered
