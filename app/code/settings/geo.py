"""
"""
from pydantic_settings import BaseSettings
from pydantic import Field, SecretStr


class GeoSettings(BaseSettings):
    """ TBD """
    states: list = Field(["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"], description="US States to pull from")
    crs: int = Field(4326, description="Coordinate Reference System")

    class Config:
        env_prefix = "geo_"


geo_settings = GeoSettings()