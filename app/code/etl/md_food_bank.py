from settings.geo import geo_settings
from settings.db import postgres_settings
from sqlalchemy import create_engine
from datetime import datetime
import logging
import aiohttp
import geopandas as gpd
from utils.db import load_into_pg
from aiohttp import FormData
import asyncio
from typing import List, Dict

logger = logging.getLogger(__name__)

async def fetch_md_food_bank_centers(
        page: int,
        latitude: float,
        longitude: float,
        distance: int,
        per_page: int = 10
):
    url = 'https://mdfoodbank.org/wp-admin/admin-ajax.php'
    data = FormData()
    data.add_field('action', 'gmw_form_ajax_submission')
    data.add_field('form_submitted', 'true')
    data.add_field('form_values', f'address%5B%5D=Baltimore%2C%20MD&distance={distance}&units=imperial&post%5B%5D=partner_locations&page={page}&per_page={per_page}&lat={latitude}&lng={longitude}&swlatlng=&nelatlng=&form=2&action=fs')
    data.add_field('filters', f'per_page={per_page}')
    data.add_field('page', str(page))
    data.add_field('form_id', '2')
    data.add_field('per_page_triggered', '0')

    locations = []
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(url, data=data) as response:
                data = await response.json()
                return data['map_locations'], data
    except Exception as e:
        logger.error(f"Error fetching MD Food Bank centers: {e}")
        return []

async def fetch_md_food_bank_centers_paginated(
        distance: int = 100,
        per_page: int = 10,
        search_points: list = [(39.290502, -76.610407)],
        num_workers: int = 5
) -> List[Dict]:
    in_q = asyncio.Queue()
    out_q = asyncio.Queue()

    async def fetch(in_q: asyncio.Queue, out_q: asyncio.Queue):
        while True:
            page, latitude, longitude, distance, per_page = in_q.get_nowait()
            try:
                locs, meta = await fetch_md_food_bank_centers(page, latitude, longitude, distance, per_page)
                for loc in locs:
                    out_q.put_nowait(loc)
            except asyncio.QueueEmpty as qe:
                logger.error(f"Queue is empty ({qe}), closing worker")
            except Exception as e:
                logger.error(f"Error fetching foodbanks for area: {e}")
            finally:
                in_q.task_done()
    
    for point in search_points:
        latitude, longitude = point
        initial_locations, initial_metadata = await fetch_md_food_bank_centers(1, latitude, longitude, distance, per_page)
        for loc in initial_locations:
            out_q.put_nowait(loc)  # add initial locations to output queue

        total_pages = initial_metadata['max_pages']
        logger.info(f"Fetching a total of {total_pages} pages for point ({latitude}, {longitude})")
        for page in range(1, total_pages + 1):
            in_q.put_nowait((page, latitude, longitude, distance, per_page))
        
        workers = [asyncio.create_task(fetch(in_q, out_q)) for _ in range(num_workers)]
        await in_q.join()
        for w in workers:
            w.cancel()

        logger.info(f"Fetched a total of {total_pages} pages for point ({latitude}, {longitude})")

    # return list of results
    return [out_q.get_nowait() for _ in range(out_q.qsize())]


def process_all_foodbanks(foodbanks: List[Dict]) -> gpd.GeoDataFrame:
    """Post-process the foodbank data into a GeoDataFrame and expand nested fields"""
    logger.info(foodbanks[-1])
    foodbanks_df = gpd.GeoDataFrame(foodbanks)

    logger.info(foodbanks_df)
    foodbanks_gdf = gpd.GeoDataFrame(foodbanks_df, geometry=gpd.points_from_xy(foodbanks_df["lng"].astype(float), foodbanks_df["lat"].astype(float)))
    foodbanks_gdf.set_crs(epsg=geo_settings.crs, inplace=True)
    
    return foodbanks_gdf

async def run():
    """Run the full ETL process for MD Food Bank centers"""
    logger.info("Starting MD Food Bank ETL process...")
    
    # Fetch all MD Food Bank centers from API
    logger.info("Fetching MD Food Bank centers...")
    all_md_food_bank_centers = await fetch_md_food_bank_centers_paginated()
    logger.info(f"Successfully fetched {len(all_md_food_bank_centers)} MD Food Bank centers")
    
    logger.info("Processing MD Food Bank centers into GeoDataFrame...")
    processed_md_food_bank_centers = process_all_foodbanks(all_md_food_bank_centers)
    logger.info(f"Successfully processed {len(processed_md_food_bank_centers)} MD Food Bank centers into GeoDataFrame")
    
    logger.info("Loading MD Food Bank centers into PostgreSQL PostGIS database...")
    load_into_pg(processed_md_food_bank_centers, table_name='md_food_bank_raw')
    logger.info("Successfully loaded MD Food Bank centers into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")