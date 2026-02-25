"""Primary Scrapy spider for provider-specific food bank location crawls."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError

from ..utils.context import JobContext
from ..utils.pipeline import FoodBankConfig
from ..utils.test_capture import upsert_test_config, write_input, write_output


class FoodBankSpider(scrapy.Spider):
    """Drive requests from pipeline config and emit standardized raw envelopes."""

    name = "food_bank"

    def __init__(self, food_banks: list[FoodBankConfig], **kwargs):
        super().__init__(**kwargs)
        self.food_banks = food_banks

    def start_requests(self):
        """Generate initial requests from each configured organization row."""
        for food_bank in self.food_banks:
            test_mode = self.settings.getbool("TEST_MODE", False)
            ctx = JobContext(
                config=food_bank,
                test_source_url=food_bank.source if test_mode else None,
            )
            request_count = 0
            self.logger.info(
                "[start] org_id=%s source=%s generator=%s evaluator=%s parser=%s",
                food_bank.organization_id,
                food_bank.source,
                food_bank.generator,
                food_bank.evaluator,
                food_bank.parser,
            )
            for req in food_bank.generator_func(ctx):
                req.meta["_ctx"] = ctx
                req.errback = self.errback_log
                if self.settings.getbool("RAW_RESPONSE_DEBUG", False):
                    req.meta["handle_httpstatus_all"] = True
                self.logger.info(
                    "[enqueue] org_id=%s method=%s url=%s",
                    food_bank.organization_id,
                    getattr(req, "method", "GET"),
                    req.url,
                )
                if self.settings.getbool("RAW_RESPONSE_DEBUG", False):
                    body = getattr(req, "body", b"") or b""
                    if isinstance(body, bytes):
                        body_text = body.decode("utf-8", errors="replace")
                    else:
                        body_text = str(body)
                    self.logger.info(
                        "[enqueue_raw] org_id=%s method=%s url=%s headers=%s body=%s",
                        food_bank.organization_id,
                        getattr(req, "method", "GET"),
                        req.url,
                        dict(getattr(req, "headers", {}) or {}),
                        body_text,
                    )
                request_count += 1
                yield req
            if request_count == 0:
                self.logger.warning(
                    "[start] org_id=%s yielded 0 requests (source=%r, generator=%s)",
                    food_bank.organization_id,
                    food_bank.source,
                    food_bank.generator,
                )

    async def start(self):
        """Async-compatible start hook for newer Scrapy runtimes."""
        for req in self.start_requests():
            yield req

    @staticmethod
    def _decode_headers(headers) -> dict:
        """Decode Scrapy header objects into JSON-serializable dicts."""
        def _decode(value):
            if isinstance(value, bytes):
                return value.decode("latin-1", errors="replace")
            return str(value)

        out = {}
        if not headers:
            return out
        for key, value in dict(headers).items():
            decoded_key = _decode(key)
            if isinstance(value, (list, tuple)):
                decoded_vals = [_decode(v) for v in value]
                out[decoded_key] = decoded_vals[0] if len(decoded_vals) == 1 else decoded_vals
            else:
                out[decoded_key] = _decode(value)
        return out

    @staticmethod
    def _request_payload(request: scrapy.Request) -> dict:
        """Serialize request context for raw output provenance."""
        parsed = urlparse(request.url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        params = {k: (v[0] if len(v) == 1 else v) for k, v in query.items()}

        body_text = ""
        body_data = None
        body = getattr(request, "body", b"") or b""
        if isinstance(body, bytes):
            body_text = body.decode("utf-8", errors="replace")
        else:
            body_text = str(body)
        if body_text:
            parsed_body = parse_qs(body_text, keep_blank_values=True)
            body_data = {k: (v[0] if len(v) == 1 else v) for k, v in parsed_body.items()}

        return {
            "url": request.url,
            "method": getattr(request, "method", "GET"),
            "params": params,
            "body": body_data if body_data is not None else body_text,
            "headers": FoodBankSpider._decode_headers(getattr(request, "headers", None)),
        }

    @staticmethod
    def _org_payload(ctx: JobContext) -> dict:
        """Build stable organization metadata payload for each output row."""
        cfg = ctx.config
        out = dict(cfg.raw or {})
        # Keep only org/profile context; drop crawl wiring fields.
        for key in (
            "Generator",
            "Generator Kwargs",
            "Evaluator",
            "Evaluator Kwargs",
            "Parser",
            "Parser Kwargs",
            "Source",
            "Spider",
            "generator",
            "evaluator",
            "parser",
            "source",
            "spider",
            "generatorKwargs",
            "evaluatorKwargs",
            "parserKwargs",
        ):
            out.pop(key, None)
        out.update(
            {
                "state": cfg.state,
                "organizationId": cfg.organization_id,
                "fullName": cfg.name,
            }
        )
        return out

    @staticmethod
    def _config_payload(ctx: JobContext) -> dict:
        """Persist generator/evaluator/parser config with each emitted item."""
        cfg = ctx.config
        return {
            "source": cfg.source,
            "generator": cfg.generator,
            "evaluator": cfg.evaluator,
            "parser": cfg.parser,
            "generatorKwargs": cfg.generator_kwargs,
            "evaluatorKwargs": cfg.evaluator_kwargs,
            "parserKwargs": cfg.parser_kwargs,
        }

    @staticmethod
    def _data_payload(item) -> dict:
        """Extract parser payload body, preferring nested `raw` dict if present."""
        def _raw_only(payload: dict):
            raw = payload.get("raw")
            if isinstance(raw, dict):
                return raw
            return payload

        if hasattr(item, "model_dump"):
            return _raw_only(item.model_dump())
        elif hasattr(item, "dict"):
            return _raw_only(item.dict())
        else:
            payload = dict(item)
            if "raw" in payload and isinstance(payload["raw"], dict):
                return payload["raw"]
            return payload

    def parse(self, response):
        """Handle response: optional fixture capture, evaluator fan-out, parser emit."""
        ctx: JobContext = response.meta["_ctx"]
        config = ctx.config
        if self.settings.getbool("RAW_RESPONSE_DEBUG", False):
            max_chars = self.settings.getint("RAW_RESPONSE_DEBUG_MAX_CHARS", 4000)
            content_type = (response.headers.get(b"Content-Type") or b"").decode(
                "latin-1", errors="replace"
            )
            text = response.text or ""
            if max_chars <= 0:
                snippet = text
            else:
                snippet = text[:max_chars]
                if len(text) > max_chars:
                    snippet += "\n...[truncated]..."
            self.logger.info(
                "[raw] org_id=%s status=%s url=%s content_type=%s body_preview:\n%s",
                config.organization_id,
                response.status,
                response.url,
                content_type,
                snippet,
            )
            # Persist full body previews to debug hostile/anti-bot endpoints.
            out_dir = Path(self.settings.get("OUTPUT_DIR", "output")) / "raw_responses"
            out_dir.mkdir(parents=True, exist_ok=True)
            run_id = str(self.settings.get("RUN_ID") or "run")
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            safe_org = config.organization_id if config.organization_id is not None else "unknown"
            dump_path = out_dir / f"{run_id}_org{safe_org}_{ts}.txt"
            dump_path.write_text(text, encoding="utf-8", errors="replace")
            self.logger.info(
                "[raw] wrote full response body to %s",
                dump_path.as_posix(),
            )
        do_capture = self.settings.getbool("TEST_RECORD", False)
        if do_capture:
            input_path = write_input(response.url, response.body or b"")
            upsert_test_config(config, input_path)

        next_requests = list(config.evaluator_func(response, ctx, self))
        for req in next_requests:
            yield req

        # If evaluator split this capped response into child queries, skip
        # parsing/emitting the capped parent payload to avoid 300-cap artifacts.
        if any(bool((req.meta or {}).get("_skip_parent_parse")) for req in next_requests):
            self.logger.warning(
                "[parse] skipping capped parent payload org_id=%s url=%s",
                config.organization_id,
                response.url,
            )
            return

        items = list(config.parser_func(response, ctx))
        self.logger.info(
            "[parse] org_id=%s url=%s items=%s",
            config.organization_id,
            response.url,
            len(items),
        )
        if do_capture:
            write_output(response.url, items)
        request_payload = self._request_payload(response.request)
        org_payload = self._org_payload(ctx)
        config_payload = self._config_payload(ctx)
        for item in items:
            data_payload = self._data_payload(item)
            yield {
                "request": request_payload,
                "org": org_payload,
                "config": config_payload,
                "data": data_payload,
            }

    def errback_log(self, failure):
        """Log rich request/response context for failed requests."""
        request = getattr(failure, "request", None)
        url = request.url if request else "(unknown)"
        method = getattr(request, "method", "GET") if request else "GET"
        if failure.check(HttpError):
            response = failure.value.response
            content_type = (response.headers.get(b"Content-Type") or b"").decode(
                "latin-1", errors="replace"
            )
            location = (response.headers.get(b"Location") or b"").decode(
                "latin-1", errors="replace"
            )
            preview = (response.text or "")[:1200]
            self.logger.error(
                "[error] http status=%s method=%s url=%s content_type=%s location=%s body_preview=%r",
                response.status,
                method,
                response.url,
                content_type,
                location,
                preview,
            )
            return
        self.logger.error(
            "[error] request failed method=%s url=%s err=%r",
            method,
            url,
            failure.value,
        )
