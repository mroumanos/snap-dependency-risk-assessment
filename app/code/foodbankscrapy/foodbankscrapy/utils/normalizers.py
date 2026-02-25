"""Normalize provider-specific parser payloads into `CommonLocation`."""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..schemas import CommonLocation


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_accessfood(item: Dict[str, Any], meta: Dict[str, Any]) -> CommonLocation:
    """Map AccessFood payload fields into canonical schema."""
    return CommonLocation(
        source="accessfood",
        sourceUrl=meta.get("source_url"),
        orgId=_safe_int(item.get("organizationId") or meta.get("organization_id")),
        organizationName=meta.get("organization_name"),
        locationName=item.get("locationName"),
        address1=item.get("address1"),
        address2=item.get("address2"),
        city=item.get("city"),
        state=item.get("state"),
        postalCode=_strip(item.get("zipCode")),
        country=item.get("country") or "US",
        latitude=_to_float(item.get("latitude")),
        longitude=_to_float(item.get("longitude")),
        phone=_strip(item.get("phone")),
        website=_strip(item.get("website")),
        mailAddressState=meta.get("mailAddressState"),
        mailAddressCity=meta.get("mailAddressCity"),
        mailAddressZip=meta.get("mailAddressZip"),
        mailAddressLatitude=_to_float(meta.get("mailAddressLatitude")),
        mailAddressLongitude=_to_float(meta.get("mailAddressLongitude")),
        entityId=_safe_int(meta.get("entityId")),
        organizationId=_safe_int(meta.get("organizationId") or meta.get("organization_id")),
        agencyUrl=_strip(meta.get("agencyUrl")),
        url=_strip(meta.get("url")),
        generator=meta.get("generator"),
        evaluator=meta.get("evaluator"),
        parser=meta.get("parser"),
        raw=item,
    )


def normalize_storepoint(item: Dict[str, Any], meta: Dict[str, Any]) -> CommonLocation:
    """Map Storepoint-like payload fields into canonical schema."""
    return CommonLocation(
        source="storepoint",
        sourceUrl=meta.get("source_url"),
        orgId=_safe_int(meta.get("organization_id")),
        organizationName=meta.get("organization_name"),
        locationName=item.get("name") or item.get("location_name"),
        address1=item.get("address") or item.get("address1"),
        address2=item.get("address2"),
        city=item.get("city"),
        state=item.get("state") or meta.get("state"),
        postalCode=_strip(item.get("postal_code") or item.get("zip") or item.get("zip_code")),
        country=item.get("country") or "US",
        latitude=_to_float(item.get("lat") or item.get("latitude")),
        longitude=_to_float(item.get("lng") or item.get("longitude")),
        phone=_strip(item.get("phone") or item.get("phone_number")),
        website=_strip(item.get("website") or meta.get("homepage_url")),
        mailAddressState=meta.get("mailAddressState"),
        mailAddressCity=meta.get("mailAddressCity"),
        mailAddressZip=meta.get("mailAddressZip"),
        mailAddressLatitude=_to_float(meta.get("mailAddressLatitude")),
        mailAddressLongitude=_to_float(meta.get("mailAddressLongitude")),
        entityId=_safe_int(meta.get("entityId")),
        organizationId=_safe_int(meta.get("organizationId") or meta.get("organization_id")),
        agencyUrl=_strip(meta.get("agencyUrl")),
        url=_strip(meta.get("url")),
        generator=meta.get("generator"),
        evaluator=meta.get("evaluator"),
        parser=meta.get("parser"),
        raw=item,
    )


def normalize_fallback(item: Dict[str, Any], meta: Dict[str, Any]) -> CommonLocation:
    """Best-effort normalizer for providers without dedicated mapping."""
    return CommonLocation(
        source=meta.get("source", "unknown"),
        sourceUrl=meta.get("source_url"),
        orgId=_safe_int(meta.get("organization_id")),
        organizationName=meta.get("organization_name"),
        locationName=item.get("name") or item.get("location_name"),
        address1=item.get("address") or item.get("address1"),
        address2=item.get("address2"),
        city=item.get("city"),
        state=item.get("state") or meta.get("state"),
        postalCode=_strip(item.get("postal_code") or item.get("zip") or item.get("zip_code")),
        country=item.get("country") or "US",
        latitude=_to_float(item.get("lat") or item.get("latitude")),
        longitude=_to_float(item.get("lng") or item.get("longitude")),
        phone=_strip(item.get("phone") or item.get("phone_number")),
        website=_strip(item.get("website")),
        mailAddressState=meta.get("mailAddressState"),
        mailAddressCity=meta.get("mailAddressCity"),
        mailAddressZip=meta.get("mailAddressZip"),
        mailAddressLatitude=_to_float(meta.get("mailAddressLatitude")),
        mailAddressLongitude=_to_float(meta.get("mailAddressLongitude")),
        entityId=_safe_int(meta.get("entityId")),
        organizationId=_safe_int(meta.get("organizationId") or meta.get("organization_id")),
        agencyUrl=_strip(meta.get("agencyUrl")),
        url=_strip(meta.get("url")),
        generator=meta.get("generator"),
        evaluator=meta.get("evaluator"),
        parser=meta.get("parser"),
        raw=item,
    )


def normalize_item(item: Dict[str, Any], meta: Dict[str, Any]) -> CommonLocation:
    """Dispatch to source-specific normalizer based on metadata."""
    source_type = (meta.get("source") or meta.get("source_type") or "").lower()
    if source_type == "accessfood":
        return normalize_accessfood(item, meta)
    if source_type == "storepoint":
        return normalize_storepoint(item, meta)
    return normalize_fallback(item, meta)


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _strip(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None
