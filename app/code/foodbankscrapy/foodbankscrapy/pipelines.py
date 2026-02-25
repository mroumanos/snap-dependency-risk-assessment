"""Scrapy pipelines for raw JSONL output and fixture-based smoke testing."""

import json
import os
from uuid import uuid4

from .schemas import CommonLocation
from .utils.test_capture import test_dir_for_url


def _normalize_utf8(value):
    """Recursively coerce nested payload values to UTF-8-safe strings."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            nk = _normalize_utf8(k)
            if not isinstance(nk, str):
                nk = str(nk)
            out[nk] = _normalize_utf8(v)
        return out
    if isinstance(value, list):
        return [_normalize_utf8(v) for v in value]
    if isinstance(value, tuple):
        return [_normalize_utf8(v) for v in value]
    return value


class RunJsonlPipeline:
    """Write one raw JSONL output file per organization for the current run."""

    def __init__(self):
        self.spider = None
        self._seen_keys_by_org = {}
        self._dropped_dupes_by_org = {}
        self._files_by_org = {}
        self._run_id = None
        self._base_out_dir = None
        self._schema_version = 1

    @classmethod
    def from_crawler(cls, crawler):
        inst = cls()
        inst.spider = crawler.spider
        return inst

    def open_spider(self):
        """Initialize per-run output folders and dedupe state."""
        out_dir = self.spider.settings.get("OUTPUT_DIR", "output")
        self._base_out_dir = os.path.join(out_dir, "raw")
        os.makedirs(self._base_out_dir, exist_ok=True)
        self._run_id = str(self.spider.settings.get("RUN_ID") or uuid4().hex)
        self._schema_version = int(self.spider.settings.get("OUTPUT_SCHEMA_VERSION", 1))
        self._seen_keys_by_org = {}
        self._dropped_dupes_by_org = {}
        self._files_by_org = {}

    def close_spider(self):
        """Close all file handles and log duplicate-drop counters."""
        for handle in self._files_by_org.values():
            handle.close()
        if self.spider is not None:
            for org_key, dropped in self._dropped_dupes_by_org.items():
                if dropped:
                    self.spider.logger.info(
                        "[dedupe] org_id=%s dropped duplicate items=%s",
                        org_key,
                        dropped,
                    )

    def process_item(self, item):
        """Wrap item in envelope, dedupe, and append to org-specific JSONL file."""
        envelope = self._to_envelope(item)
        org_key = self._org_key(envelope["org"], envelope["data"])
        file_handle = self._get_file_for_org(org_key)
        seen_keys = self._seen_keys_by_org.setdefault(org_key, set())
        dedupe_key = self._build_dedupe_key(envelope)
        if dedupe_key in seen_keys:
            self._dropped_dupes_by_org[org_key] = self._dropped_dupes_by_org.get(org_key, 0) + 1
            return item
        seen_keys.add(dedupe_key)
        envelope = _normalize_utf8(envelope)
        line = json.dumps(envelope, default=str, ensure_ascii=False)
        file_handle.write(line + "\n")
        return item

    def _to_envelope(self, item):
        """Normalize incoming item into a stable envelope schema."""
        if isinstance(item, dict) and {"request", "org", "data"}.issubset(item.keys()):
            request = item.get("request") or {}
            org = item.get("org") or {}
            config = item.get("config") or {}
            data = item.get("data") or {}
            if (
                isinstance(request, dict)
                and isinstance(org, dict)
                and isinstance(config, dict)
                and isinstance(data, dict)
            ):
                return {
                    "schemaVersion": int(item.get("schemaVersion") or self._schema_version),
                    "request": request,
                    "org": org,
                    "config": config,
                    "data": data,
                }

        if isinstance(item, CommonLocation):
            model = item
        elif isinstance(item, dict):
            model = CommonLocation(**item)
        else:
            model = CommonLocation(**dict(item))
        data = model.model_dump() if hasattr(model, "model_dump") else model.dict()
        return {
            "schemaVersion": self._schema_version,
            "request": {},
            "org": {},
            "config": {},
            "data": data,
        }

    def _org_key(self, org, data):
        """Resolve organization key used for output file partitioning."""
        org_id = None
        if isinstance(org, dict):
            org_id = org.get("organizationId") or org.get("OrganizationID") or org.get("entityId")
        if org_id is None:
            org_id = data.get("orgId")
        if org_id is None:
            org_id = data.get("organizationId")
        if org_id is None:
            return "unknown"
        return str(org_id)

    def _get_file_for_org(self, org_key):
        """Lazily create file handle for an organization run file."""
        if org_key in self._files_by_org:
            return self._files_by_org[org_key]

        org_dir = os.path.join(self._base_out_dir, org_key)
        os.makedirs(org_dir, exist_ok=True)
        path = os.path.join(org_dir, f"{self._run_id}.jsonl")
        handle = open(path, "w")
        self._files_by_org[org_key] = handle
        return handle

    @staticmethod
    def _build_dedupe_key(envelope):
        """Build dedupe key from canonical org+data payloads."""
        org = envelope.get("org") if isinstance(envelope, dict) else {}
        data = envelope.get("data") if isinstance(envelope, dict) else {}
        try:
            org_key = json.dumps(org or {}, sort_keys=True, default=str)
        except TypeError:
            org_key = str(org)

        try:
            data_key = json.dumps(data or {}, sort_keys=True, default=str)
        except TypeError:
            data_key = str(data)

        return ("org_data", org_key, data_key)

    @staticmethod
    def _build_dedupe_key_legacy(data):
        raw = data.get("raw") if isinstance(data.get("raw"), dict) else {}
        raw_id = raw.get("id") or raw.get("sl_id") or raw.get("store_id")
        if raw_id is not None:
            return ("raw_id", data.get("orgId"), str(raw_id))
        strong_id = data.get("organizationId") or data.get("entityId")
        if strong_id is not None and (data.get("locationName") or data.get("address1")):
            return (
                "org_loc",
                str(strong_id),
                str(data.get("locationName") or "").strip().lower(),
                str(data.get("address1") or "").strip().lower(),
                str(data.get("postalCode") or "").strip().lower(),
            )
        return (
            "fallback",
            str(data.get("orgId") or data.get("organizationId") or "").strip(),
            str(data.get("locationName") or "").strip().lower(),
            str(data.get("address1") or "").strip().lower(),
            str(data.get("city") or "").strip().lower(),
            str(data.get("state") or "").strip().lower(),
            str(data.get("postalCode") or "").strip().lower(),
            str(data.get("latitude") or "").strip(),
            str(data.get("longitude") or "").strip(),
        )


class SmokeTestPipeline:
    """Compare current scrape output against stored expected fixture outputs."""

    def __init__(self):
        self.actual_by_source = {}
        self.spider = None

    @classmethod
    def from_crawler(cls, crawler):
        inst = cls()
        inst.spider = crawler.spider
        return inst

    def open_spider(self):
        """Reset in-memory observed item store for the run."""
        self.actual_by_source = {}

    def process_item(self, item):
        """Capture canonicalized item grouped by source URL."""
        if isinstance(item, dict) and {"request", "org", "data"}.issubset(item.keys()):
            item = item.get("data") or {}
        if isinstance(item, CommonLocation):
            model = item
        elif isinstance(item, dict):
            model = CommonLocation(**item)
        else:
            model = CommonLocation(**dict(item))
        source_url = model.sourceUrl or "unknown"
        self.actual_by_source.setdefault(source_url, []).append(self._canonical(model))
        return item

    def close_spider(self):
        """Validate captured output against expected fixture files."""
        failures = []
        for source_url, actual_items in self.actual_by_source.items():
            expected_path = test_dir_for_url(source_url) / "output.jsonl"
            if not expected_path.exists():
                failures.append(f"Missing expected output for {source_url}")
                continue
            expected_items = []
            with expected_path.open() as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    expected_items.append(self._canonical(json.loads(line)))
            expected_sorted = self._sorted(expected_items)
            actual_sorted = self._sorted(actual_items)
            if expected_sorted != actual_sorted:
                failures.append(f"Mismatch for {source_url}: expected {len(expected_items)} got {len(actual_items)}")
            else:
                if self.spider is not None:
                    self.spider.logger.info(
                        "[smoke] match for %s: %s items",
                        source_url,
                        len(expected_items),
                    )
        if failures:
            raise RuntimeError("Smoke test failures:\\n" + "\\n".join(failures))
        # use spider logger if available
        if self.spider is not None:
            self.spider.logger.info(
                "[smoke] all tests passed (%s sources)",
                len(self.actual_by_source),
            )

    @staticmethod
    def _canonical(item):
        """Return reduced canonical projection for deterministic comparisons."""
        if isinstance(item, CommonLocation):
            data = item.model_dump() if hasattr(item, "model_dump") else item.dict()
        else:
            data = dict(item)
        return {
            "sourceUrl": data.get("sourceUrl"),
            "orgId": data.get("orgId"),
            "organizationName": data.get("organizationName"),
            "locationName": data.get("locationName"),
            "address1": data.get("address1"),
            "address2": data.get("address2"),
            "city": data.get("city"),
            "state": data.get("state"),
            "postalCode": data.get("postalCode"),
            "country": data.get("country"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "phone": data.get("phone"),
            "website": data.get("website"),
            "mailAddressState": data.get("mailAddressState"),
            "mailAddressCity": data.get("mailAddressCity"),
            "mailAddressZip": data.get("mailAddressZip"),
            "mailAddressLatitude": data.get("mailAddressLatitude"),
            "mailAddressLongitude": data.get("mailAddressLongitude"),
            "entityId": data.get("entityId"),
            "organizationId": data.get("organizationId"),
            "agencyUrl": data.get("agencyUrl"),
            "url": data.get("url"),
            "generator": data.get("generator"),
            "evaluator": data.get("evaluator"),
            "parser": data.get("parser"),
        }

    @staticmethod
    def _sorted(items):
        """Sort canonical records in a deterministic order."""
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True))
