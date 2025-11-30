import asyncio
import logging
from typing import List, Dict
import aiohttp
import geopandas as gpd
from settings.db import postgres_settings
from settings.geo import geo_settings
from utils.db import load_into_pg

logger = logging.getLogger(__name__)


async def fetch_foodbanks_for_area(
        lat: float,
        lng: float,
        radius: int,
        region_id: int,
        region_map_id: int,
        page: int = 0) -> List[Dict]:
    url = 'https://api.accessfood.org/api/MapInformation/LocationSearch'
    params = dict(
        radius=radius,
        lat=lat,
        lng=lng,
        regionId=region_id,
        regionMapId=region_map_id,
        showOutOfNetwork=1,
        page=page,
        includeLocationOperatingHours='true',
        isMapV2='true'
    )

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
                locations = data['item1']
                metadata = {
                    'prior_page': data['item2'],
                    'current_page': data['item3'],
                    'next_page': data['item4'],
                    'total_items': data['item5']
                }
                logger.info(f"Fetched {len(locations)} locations for region {region_id}, map {region_map_id}, page {page}")
                return locations, metadata
    except Exception as e:
        logger.error(f"Error fetching foodbanks for area: {e}")
        return [], {}


async def fetch_foodbanks_for_area_paginated(
        num_workers: int = 5) -> List[Dict]:
    """Fetch foodbanks for specified areas with pagination using multiple workers"""
    all_region_ids = [1]
    all_region_map_ids = [1]
    all_lat_lng_radius = [(39.8283, -98.5795, 3900)]  # Center of the US

    in_q = asyncio.Queue()
    out_q = asyncio.Queue()

    async def fetch(in_q: asyncio.Queue, out_q: asyncio.Queue):
        while True:
            lat, lng, radius, region_id, region_map_id, page = in_q.get_nowait()
            try:
                locs, meta = await fetch_foodbanks_for_area(lat, lng, radius, region_id, region_map_id, page)
                for loc in locs:
                    out_q.put_nowait(loc)
            except asyncio.QueueEmpty as qe:
                logger.error(f"Queue is empty ({qe}), closing worker")
            except Exception as e:
                logger.error(f"Error fetching foodbanks for area: {e}")
            finally:
                in_q.task_done()
    
    for region_id in all_region_ids:
        for region_map_id in all_region_map_ids:
            for lat, lng, radius in all_lat_lng_radius:
                initial_locations, intial_metadata = await fetch_foodbanks_for_area(lat, lng, radius, region_id, region_map_id, 0)
                for loc in initial_locations:
                    out_q.put_nowait(loc)  # add initial locations to output queue

                total_pages = (intial_metadata['total_items'] // len(initial_locations)) + 1
                logger.info(f"Total pages to fetch for region {region_id}, map {region_map_id}: {total_pages}")
                for page in range(1, total_pages + 1):
                    in_q.put_nowait((lat, lng, radius, region_id, region_map_id, page))
                
                workers = [asyncio.create_task(fetch(in_q, out_q)) for _ in range(num_workers)]
                await in_q.join()
                for w in workers:
                    w.cancel()

                logger.info(f"Fetched {out_q.qsize()} additional for region {region_id}, map {region_map_id}")
    
    # return list of results
    return [out_q.get_nowait() for _ in range(out_q.qsize())]

def process_all_foodbanks(foodbanks: List[Dict]) -> gpd.GeoDataFrame:
    """Post-process the foodbank data into a GeoDataFrame and expand nested fields"""
    foodbanks_df = gpd.GeoDataFrame(foodbanks)

    # Remove duplicates based on unique identifiers
    foodbanks_df = foodbanks_df.drop_duplicates(subset=['locationId'], keep='first')

    foodbanks_gdf = gpd.GeoDataFrame(foodbanks_df, geometry=gpd.points_from_xy(foodbanks_df["longitude"].astype(float), foodbanks_df["latitude"].astype(float)))
    foodbanks_gdf.set_crs(epsg=geo_settings.crs, inplace=True)
    
    return foodbanks_gdf

async def run():
    """Run the full ETL process for Access Food foodbanks"""
    logger.info("Starting Access Food ETL process...")
    
    # Fetch all foodbanks from Access Food API
    logger.info("Fetching foodbanks from all areas...")
    all_foodbanks = await fetch_foodbanks_for_area_paginated()
    logger.info(f"Successfully fetched {len(all_foodbanks)} foodbanks")
    
    # Process and expand the foodbank data
    logger.info("Processing foodbank data...")
    foodbanks_gdf = process_all_foodbanks(all_foodbanks)
    logger.info(f"Successfully processed {len(foodbanks_gdf)} foodbanks into GeoDataFrame")
    
    logger.info("Loading foodbanks into PostgreSQL PostGIS database...")
    load_into_pg(foodbanks_gdf, table_name='access_food_foodbanks_raw')
    logger.info("Successfully loaded foodbanks into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")
