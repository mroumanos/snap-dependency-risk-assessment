from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, unquote_plus, urlparse

from .pipeline import Config


def _static_tests_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "static" / "tests"


def capture_enabled(config: Config) -> bool:
    return False


def _folder_for_url(url: str) -> str:
    parsed = urlparse(url)
    netloc = parsed.netloc
    if not netloc:
        # If no scheme provided, treat leading segment as netloc
        netloc = url.split("/", 1)[0]
    return netloc


def test_dir_for_url(url: str) -> Path:
    folder = _folder_for_url(url)
    return _static_tests_dir() / folder


def test_input_path(url: str) -> Path:
    return test_dir_for_url(url) / "input.data"


def test_input_path_guess(url: str) -> Path:
    # New layout: domain-only folder
    folder = _folder_for_url(url)
    candidate = _static_tests_dir() / folder / "input.data"
    if candidate.exists():
        return candidate
    # Legacy layouts: encoded URL folders
    encoded = quote_plus(url)
    legacy = _static_tests_dir() / encoded / "input.data"
    if legacy.exists():
        return legacy
    double_encoded = quote_plus(encoded)
    legacy = _static_tests_dir() / double_encoded / "input.data"
    if legacy.exists():
        return legacy
    decoded = unquote_plus(url)
    if decoded != url:
        return test_input_path_guess(decoded)
    return candidate


def test_page_inputs(url: str) -> list[Path]:
    base = test_dir_for_url(url)
    page_paths = sorted(base.glob("input_*.data"))
    return page_paths


def write_input(url: str, body: bytes) -> Path:
    out_dir = test_dir_for_url(url)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "input.data"
    path.write_bytes(body)
    return path


def write_output(url: str, items: Iterable[object]) -> Path:
    out_dir = test_dir_for_url(url)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "output.jsonl"
    with path.open("w") as handle:
        for item in items:
            if isinstance(item, dict):
                line = json.dumps(item, default=str)
            elif hasattr(item, "model_dump_json"):
                line = item.model_dump_json()
            else:
                line = item.json()
            handle.write(line + "\n")
    return path


def upsert_test_config(config: Config, input_path: Path) -> Path:
    pipelines_dir = Path(__file__).resolve().parents[2] / "static" / "pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    config_path = pipelines_dir / "test_config.json"
    rows = []
    if config_path.exists():
        rows = json.loads(config_path.read_text())
        if not isinstance(rows, list):
            rows = []

    source = f"file://{input_path.as_posix()}"
    generator_block = {
        "name": config.generator,
        **(config.generator_kwargs or {}),
        "source": source,
        "testSourceUrl": config.source,
    }
    evaluator_block = {"name": config.evaluator, **(config.evaluator_kwargs or {})}
    parser_block: object
    if config.parser_chain:
        parser_block = config.parser_chain
    else:
        parser_block = [{"name": config.parser, **(config.parser_kwargs or {})}]

    row = {
        "mailAddressState": config.state,
        "organizationId": str(config.organization_id) if config.organization_id is not None else "",
        "fullName": config.name,
        "generator": generator_block,
        "evaluator": evaluator_block,
        "parser": parser_block,
    }

    replaced = False
    for idx, existing in enumerate(rows):
        if existing.get("source") == source:
            rows[idx] = row
            replaced = True
            break
    if not replaced:
        rows.append(row)

    config_path.write_text(json.dumps(rows, indent=2))
    return config_path
