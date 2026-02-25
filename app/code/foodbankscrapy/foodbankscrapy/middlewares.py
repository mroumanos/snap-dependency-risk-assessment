"""Downloader middleware helpers for per-request pacing overrides."""

from __future__ import annotations

from typing import Optional

from twisted.internet import reactor
from twisted.internet.task import deferLater


class RequestDelayMiddleware:
    """Honor request-level delay hints from request metadata."""

    def process_request(self, request, spider=None):
        """Apply deferred processing when `request_delay` meta is present."""
        delay = request.meta.get("request_delay")
        if delay is None:
            return None
        try:
            delay_value: Optional[float] = float(delay)
        except (TypeError, ValueError):
            return None
        if not delay_value or delay_value <= 0:
            return None
        return deferLater(reactor, delay_value, lambda: None)
