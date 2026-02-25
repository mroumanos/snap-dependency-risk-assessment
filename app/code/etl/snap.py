"""SNAP retailer ETL.

Downloads SNAP retailer features from ArcGIS Hub as GeoJSON and loads
the result into `snap_retailers_raw`.
"""

from settings.geo import geo_settings
import logging
import aiohttp
import geopandas as gpd
from utils.db import load_into_pg


logger = logging.getLogger(__name__)

async def fetch_snap_retailers(
        fmt: str = 'geojson',
        crs: int = geo_settings.crs
):
    """Fetch SNAP retailer features and return a GeoDataFrame."""
    url = f'https://hub.arcgis.com/api/v3/datasets/8b260f9a10b0459aa441ad8588c2251c_0/downloads/data?format={fmt}&spatialRefId={crs}&where=1=1'

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url) as response:
                data = await response.json()
                gdf = gpd.GeoDataFrame.from_features(data["features"])
                gdf.set_crs(epsg=crs, inplace=True)
                return gdf
    except Exception as e:
        logger.error(f"Error fetching SNAP retailers: {e}")
        return gpd.GeoDataFrame()


async def run():
    """Execute full SNAP retailer ingestion into PostGIS."""
    logger.info("Starting SNAP ETL process...")
    
    # Fetch all SNAP retailers from API
    logger.info("Fetching SNAP retailers from all states...")
    all_snap_retailers = await fetch_snap_retailers()
    logger.info(f"Successfully fetched {len(all_snap_retailers)} SNAP retailers")
    
    logger.info("Loading SNAP retailers into PostgreSQL PostGIS database...")
    load_into_pg(all_snap_retailers, table_name='snap_retailers_raw')
    logger.info("Successfully loaded SNAP retailers into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")

    return all_snap_retailers
