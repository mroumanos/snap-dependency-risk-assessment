"""Context object passed between generator/evaluator/parser stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .pipeline import Config


@dataclass
class JobContext:
    """Per-request execution context for one pipeline configuration row."""

    config: Config
    test_source_url: Optional[str] = None
