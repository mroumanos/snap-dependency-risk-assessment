"""Feeding America ETL.

This module calls Feeding America's state-based organization endpoint,
normalizes nested payload fields, builds point geometries, and writes to
`feeding_america_foodbanks_raw`.
"""

import asyncio
import logging
from typing import List, Dict

import aiohttp
import pandas as pd
import geopandas as gpd

from settings.geo import geo_settings
from utils.db import load_into_pg 

logger = logging.getLogger(__name__)

async def fetch_foodbanks_for_state(session: aiohttp.ClientSession, state: str) -> List[Dict]:
    """
    Fetch food bank payloads for a single state from Feeding America.
    """
    foodbanks = []
    url = f"https://www.feedingamerica.org/ws-api/GetOrganizationsByState?state={state}"
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status == 200:
                result = await response.json()
                if "Organization" in result:
                    orgs = result["Organization"]
                    if isinstance(orgs, dict):
                        foodbanks.append(orgs)
                    elif isinstance(orgs, list):
                        for org in orgs:
                            foodbanks.append(org)
                    else:
                        logger.warning(f"Unexpected data format for {state}: {type(orgs)}")
                else:
                    logger.warning(f"No organizations found for {state}")
                logger.info(f"Successfully scraped {len(foodbanks)} foodbanks for {state}")
            else:
                logger.warning(f"Failed to fetch {state}: status {response.status}")
    
    except asyncio.TimeoutError:
        logger.error(f"Timeout while fetching {state}")
    except Exception as e:
        logger.error(f"Error fetching {state}: {e}")

    logger.info(f"Found {len(foodbanks)} foodbanks for {state}")
    return foodbanks

async def fetch_all_foodbanks() -> List[Dict]:
    """Fetch food bank payloads for all configured states concurrently."""
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_foodbanks_for_state(session, state) for state in geo_settings.states]
        results = await asyncio.gather(*tasks)
    
    all_foodbanks = [fb for state_foodbanks in results for fb in state_foodbanks]

    return all_foodbanks

def process_all_foodbanks(all_foodbanks: List[Dict]) -> List[Dict]:
    """Normalize nested fields and return a geometry-enabled dataframe."""
    foodbanks_df = gpd.GeoDataFrame(all_foodbanks)

    # Expand nested dict blobs into flat columns so they are SQL-friendly.
    fields_to_expand = ['MailAddress', 'PoundageStats', 'ED', 'MediaContact']
    for field in fields_to_expand:
        if field in foodbanks_df.columns:
            field_expanded = foodbanks_df[field].apply(lambda x: pd.Series(x) if isinstance(x, dict) else {})
            field_expanded.columns = [f"{field}_" + col for col in field_expanded.columns]
            foodbanks_df = pd.concat([foodbanks_df, field_expanded], axis=1)
        else:
            logger.warning(f"Field {field} not found in DataFrame columns")

    foodbanks_df = foodbanks_df.drop_duplicates(subset=['EntityID', 'OrganizationID'], keep='first')

    foodbanks_gdf = gpd.GeoDataFrame(foodbanks_df, geometry=gpd.points_from_xy(foodbanks_df["MailAddress_Longitude"].astype(float), foodbanks_df["MailAddress_Latitude"].astype(float)))
    foodbanks_gdf.set_crs(epsg=geo_settings.crs, inplace=True)

    return foodbanks_gdf


async def run():
    """Execute full Feeding America extraction, transformation, and load."""
    logger.info("Starting Feeding America ETL process...")
    
    # Fetch all foodbanks from Feeding America API
    logger.info("Fetching foodbanks from all states...")
    all_foodbanks = await fetch_all_foodbanks()
    logger.info(f"Successfully fetched {len(all_foodbanks)} foodbanks")
    
    # Process and expand the foodbank data
    logger.info("Processing foodbank data...")
    foodbanks_df = process_all_foodbanks(all_foodbanks)
    logger.info(f"Successfully processed {len(foodbanks_df)} foodbanks into GeoDataFrame")
    
    logger.info("Loading foodbanks into PostgreSQL PostGIS database...")
    load_into_pg(foodbanks_df, table_name='feeding_america_foodbanks_raw', if_exists='replace')
    logger.info("Successfully loaded foodbanks into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")
