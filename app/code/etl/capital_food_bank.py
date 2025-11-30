
from settings.geo import geo_settings
from settings.db import postgres_settings
from sqlalchemy import create_engine
from datetime import datetime
import logging
import aiohttp
import geopandas as gpd
from utils.db import load_into_pg


logger = logging.getLogger(__name__)

async def fetch_capital_food_bank_centers():
    url = 'https://services.arcgis.com/oCjyzxNy34f0pJCV/arcgis/rest/services/Active_Agencies_Last_45_Days/FeatureServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&where=1%3D1'

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url) as response:
                data = await response.json()
                return [ feature["attributes"] for feature in data["features"] ]
    except Exception as e:
        logger.error(f"Error fetching Capital Food Bank centers: {e}")
        return gpd.GeoDataFrame()

def process_all_foodbanks(foodbanks: List[Dict]) -> gpd.GeoDataFrame:
    """Post-process the foodbank data into a GeoDataFrame and expand nested fields"""
    foodbanks_df = gpd.GeoDataFrame(foodbanks)

    logger.info(foodbanks_df)
    foodbanks_gdf = gpd.GeoDataFrame(foodbanks_df, geometry=gpd.points_from_xy(foodbanks_df["longitude"].astype(float), foodbanks_df["latitude"].astype(float)))
    foodbanks_gdf.set_crs(epsg=geo_settings.crs, inplace=True)
    
    return foodbanks_gdf

async def run():
    """Run the full ETL process for Capital Food Bank centers"""
    logger.info("Starting Capital Food Bank ETL process...")
    
    # Fetch all Capital Food Bank centers from API
    logger.info("Fetching Capital Food Bank centers...")
    all_capital_food_bank_centers = await fetch_capital_food_bank_centers()
    logger.info(f"Successfully fetched {len(all_capital_food_bank_centers)} Capital Food Bank centers")
    
    logger.info("Processing Capital Food Bank centers into GeoDataFrame...")
    processed_capital_food_bank_centers = process_all_foodbanks(all_capital_food_bank_centers)
    logger.info(f"Successfully processed {len(processed_capital_food_bank_centers)} Capital Food Bank centers into GeoDataFrame")
    
    logger.info("Loading Capital Food Bank centers into PostgreSQL PostGIS database...")
    load_into_pg(processed_capital_food_bank_centers, table_name='capital_food_bank_raw')
    logger.info("Successfully loaded Capital Food Bank centers into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")