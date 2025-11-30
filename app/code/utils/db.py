from datetime import datetime
import logging
from sqlalchemy import create_engine
import geopandas as gpd
from settings.db import postgres_settings
from settings.geo import geo_settings

logger = logging.getLogger(__name__)

def get_conn():
    """Get connection to postgres"""
    return create_engine(postgres_settings.sqlalchemy_url, future=True)

def load_into_pg(
        gdf: gpd.GeoDataFrame,
        table_name: str,
        if_exists: str = 'append',
        chunksize: int = 5000,
        geometry: bool = True,
        pandas: bool = False) -> None:
    """Load dataframe into PostgreSQL PostGIS database"""

    engine = get_conn()

    if geometry:
        gdf.to_crs(
            epsg=geo_settings.crs,
            inplace=True
        )

    try:
        timestamp = datetime.utcnow()
        gdf['created_at'] = timestamp

        if pandas:
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