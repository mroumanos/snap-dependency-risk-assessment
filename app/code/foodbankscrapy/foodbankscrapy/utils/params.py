"""Parameter builders used by request generators."""

from __future__ import annotations

from typing import Dict

def _accessfood_params(row: Dict[str, str], generator_kwargs: Dict[str, object]) -> Dict[str, object]:
    """Build default AccessFood query params and apply row/kwargs overrides."""
    params = {
        "radius": 3900,
        "lat": 39.8283,
        "lng": -98.5795,
        "regionId": 1,
        "regionMapId": 1,
        "showOutOfNetwork": 1,
        "includeLocationOperatingHours": "true",
        "isMapV2": "true",
    }
    if generator_kwargs.get("use_row_latlng"):
        lat = row.get("MailAddress_Latitude")
        lng = row.get("MailAddress_Longitude")
        if lat and lng:
            params["lat"] = float(lat)
            params["lng"] = float(lng)
    params.update(generator_kwargs.get("params", {}))
    return params


PARAMS_REGISTRY = {
    "default": lambda row, generator_kwargs: {},
    "url": lambda row, generator_kwargs: {},
    "accessfood": _accessfood_params,
}


def build_params(row: Dict[str, str], generator: str, generator_kwargs: Dict[str, object]) -> Dict[str, object]:
    """Return generator-specific request params for a pipeline row."""
    builder = PARAMS_REGISTRY.get(generator, PARAMS_REGISTRY["default"])
    return builder(row, generator_kwargs)
