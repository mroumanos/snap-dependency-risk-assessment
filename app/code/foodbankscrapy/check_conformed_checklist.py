"""Generate a Markdown quality checklist for conformed JSONL output."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List


ADDRESS_COMPONENTS = [
    "addressNumber",
    "streetName",
    "streetNamePostType",
    "occupancyType",
    "occupancyIdentifier",
    "cityName",
    "stateCode",
    "countryCode",
    "zipCode",
]


def _is_populated(value: object) -> bool:
    """Return whether a field should be treated as populated for QA checks."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) > 0
    return True


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _iter_rows(path: Path) -> Iterable[Dict[str, object]]:
    """Yield valid JSON-object rows from a JSONL file."""
    with path.open() as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def _safe_sort_org_ids(values: Iterable[str]) -> List[str]:
    """Sort org IDs numerically when possible, lexicographically otherwise."""
    def key_fn(v: str):
        try:
            return (0, int(v))
        except Exception:
            return (1, v)

    return sorted(values, key=key_fn)


def build_report(path: Path) -> str:
    """Build checklist markdown covering completeness/quality by organization."""
    by_org: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in _iter_rows(path):
        org_id = str(row.get("organizationId") or "unknown")
        by_org[org_id].append(row)

    lines: List[str] = []
    lines.append(f"# Conformed Output Checklist")
    lines.append("")
    lines.append(f"- Source: `{path}`")
    lines.append(f"- Org count: `{len(by_org)}`")
    lines.append("")

    for org_id in _safe_sort_org_ids(by_org.keys()):
        rows = by_org[org_id]
        total = len(rows)

        names = [str(r.get("name") or "").strip() for r in rows if _is_populated(r.get("name"))]
        name_lengths = [len(n) for n in names]
        avg_len = round(sum(name_lengths) / len(name_lengths), 2) if name_lengths else 0.0
        max_len = max(name_lengths) if name_lengths else 0
        letters_only = sum(1 for n in names if re.fullmatch(r"[A-Za-z ]+", n))
        letters_only_pct = _pct(letters_only, len(names))

        lat_pop = sum(1 for r in rows if _is_populated(r.get("latitude")))
        lng_pop = sum(1 for r in rows if _is_populated(r.get("longitude")))
        latlng_both = sum(
            1
            for r in rows
            if _is_populated(r.get("latitude")) and _is_populated(r.get("longitude"))
        )

        type_values = set()
        for r in rows:
            t = r.get("type")
            if isinstance(t, list):
                type_values.update(str(v).strip() for v in t if _is_populated(v))
            elif _is_populated(t):
                type_values.add(str(t).strip())

        address_full_pop = sum(1 for r in rows if _is_populated(r.get("addressFull")))
        components_pop = {
            comp: sum(1 for r in rows if _is_populated(r.get(comp))) for comp in ADDRESS_COMPONENTS
        }

        phone_pop = sum(1 for r in rows if _is_populated(r.get("phoneNumber")))

        lines.append(f"## Org `{org_id}` ({total} rows)")
        lines.append("")
        lines.append(
            f"- [ ] Name quality: avg_len=`{avg_len}`, max_len=`{max_len}`, letters_only_pct=`{letters_only_pct}%`"
        )
        lines.append(
            f"- [{'x' if latlng_both == total else ' '}] Latitude/Longitude populated together: `{latlng_both}/{total}` ({_pct(latlng_both, total)}%)"
        )
        lines.append(f"- [ ] Distinct `type` values: `{sorted(type_values)}`")
        lines.append(
            f"- [{'x' if address_full_pop == total else ' '}] `addressFull` populated: `{address_full_pop}/{total}` ({_pct(address_full_pop, total)}%)"
        )
        lines.append("- [ ] Address component coverage:")
        for comp in ADDRESS_COMPONENTS:
            pop = components_pop[comp]
            lines.append(
                f"  - [{'x' if pop == total else ' '}] `{comp}`: `{pop}/{total}` ({_pct(pop, total)}%)"
            )
        lines.append(
            f"- [{'x' if phone_pop == total else ' '}] `phoneNumber` populated: `{phone_pop}/{total}` ({_pct(phone_pop, total)}%)"
        )
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> None:
    """CLI entrypoint for checklist generation."""
    parser = argparse.ArgumentParser(description="Build a markdown checklist for conformed org output.")
    parser.add_argument("input", help="Path to conformed JSONL file")
    parser.add_argument(
        "--output",
        help="Output markdown file path (default: same folder with _checklist.md suffix)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input does not exist: {input_path}")

    out_path = (
        Path(args.output)
        if args.output
        else input_path.with_name(f"{input_path.stem}_checklist.md")
    )
    out_path.write_text(build_report(input_path), encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    main()
