"""Shared database utilities for ETL writers."""

from datetime import datetime
import logging
from sqlalchemy import create_engine
import geopandas as gpd
from settings.db import postgres_settings
from settings.geo import geo_settings

logger = logging.getLogger(__name__)

def get_conn():
    """Build and return a SQLAlchemy engine for Postgres."""
    return create_engine(postgres_settings.sqlalchemy_url, future=True)

def load_into_pg(
        gdf: gpd.GeoDataFrame,
        table_name: str,
        if_exists: str = 'append',
        chunksize: int = 5000,
        geometry: bool = True) -> None:
    """Load dataframe content into Postgres/PostGIS.

    Args:
        gdf: Input dataframe (GeoDataFrame for spatial writes).
        table_name: Destination table name.
        if_exists: Caller-preferred write mode; retained for API compatibility.
        chunksize: Batch size for geospatial writes.
        geometry: Whether dataframe has geometry and should be reprojected.
        pandas: When `True`, use `to_sql`; otherwise use `to_postgis`.
    """

    engine = get_conn()

    if geometry:
        # Keep all spatial tables in a consistent CRS.
        gdf.to_crs(
            epsg=geo_settings.crs,
            inplace=True
        )

    try:
        # Add load timestamp so downstream models can reason about freshness.
        timestamp = datetime.utcnow()
        gdf['created_at'] = timestamp

        if not geometry:
            gdf.to_sql(
                table_name,
                con=engine,
                if_exists='replace',
                index=False
            )
        else:
            gdf.to_postgis(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=chunksize
            )
        logger.info(f"Successfully loaded data into {table_name} table in PostgreSQL PostGIS database")
    except Exception as e:
        logger.error(f"Error loading data into {table_name} table in PostgreSQL PostGIS database: {e}")
        raise
