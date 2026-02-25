"""Pydantic schemas used by parser normalization and pipeline output."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class CommonLocation(BaseModel):
    """Canonical intermediate location shape emitted by parsers/normalizers."""
    source: str = Field(..., description="System or provider name.")
    sourceUrl: Optional[str] = Field(None, description="URL used to fetch the data.")
    orgId: Optional[int] = Field(None, description="Organization id from the CSV.")
    organizationName: Optional[str] = None
    locationName: Optional[str] = None

    address1: Optional[str] = None
    address2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postalCode: Optional[str] = None
    country: Optional[str] = "US"

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    phone: Optional[str] = None
    website: Optional[str] = None

    mailAddressState: Optional[str] = None
    mailAddressCity: Optional[str] = None
    mailAddressZip: Optional[str] = None
    mailAddressLatitude: Optional[float] = None
    mailAddressLongitude: Optional[float] = None
    entityId: Optional[int] = None
    organizationId: Optional[int] = None
    agencyUrl: Optional[str] = None
    url: Optional[str] = None
    generator: Optional[str] = None
    evaluator: Optional[str] = None
    parser: Optional[str] = None

    updatedAt: datetime = Field(default_factory=datetime.utcnow)
    raw: Dict[str, Any] = Field(default_factory=dict)
