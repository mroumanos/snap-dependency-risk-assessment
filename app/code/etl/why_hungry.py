from bs4 import BeautifulSoup
import re
from settings.geo import geo_settings
import logging
import asyncio
import aiohttp
from typing import List, Dict
import geopandas as gpd
from utils.db import load_into_pg

logger = logging.getLogger(__name__)


async def fetch_foodbanks_for_zip(
        zip_code: str,
        radius: int = 100,
        page: int = 1) -> List[Dict]:
    url = "https://networks.whyhunger.org"
    params = dict(
        s=1,
        s_f='id',
        s_o='desc',
        center_zip=zip_code,
        distance=radius,
        radius_quantity=3959,
        country_id=2,
        zip=zip_code,
        page=page
    )
    full_url = f"{url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url, params=params) as response:
                logger.info(f"{response.status} response from {response.url}")
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                title = soup \
                        .find('div', class_='organisations') \
                        .find('div', class_='title')
                totals = int(re.match(r"FIND FOOD\: (\d+)", title.text.strip()).group(1))

                items = soup \
                        .find('div', class_='organisations') \
                        .find('div', class_='items')

                geo = items \
                        .find_all("input", class_='location_cords')
                metadata = items \
                        .find_all("div", class_='item')

                locations = []
                for g, m in zip(geo, metadata):
                    location = {
                        "latitude": g['data-lat'],
                        "longitude": g['data-lng'],
                        "name": g['data-title'],
                        "id": g['data-id']
                    }

                    location_type_search = m.find('div', class_='type')
                    if location_type_search:
                        location["type"] = re.match(r"TYPE\:[\s\n]+(.+)", location_type_search.text.strip()).group(1)
                    else:
                        location["type"] = None

                    fields = m.find_all('div', class_='field')
                    for i, f in enumerate(fields):
                        f = f.text.strip()
                        f = f.replace("\n", "")
                        f = re.sub(r"[\s]{2,}", "", f)

                        r = r"([a-zA-Z\s\-]+)[\s\n]*\:[\s\n]*(.+)"
                        m = re.match(r,f)
                        k = m.group(1)
                        v = m.group(2)
                        location[k] = v
                    locations.append(location)
                logger.info(f"Fetched {len(locations)} locations for zip {zip_code}, page {page}")
                return locations, totals
                
    except Exception as e:
        logger.error(f"Issue parsing URL: {full_url} ({e})")
        return {}

async def fetch_foodbanks_for_zip_paginated(
        num_workers: int,
        zip_code: str,
        radius: int) -> List[Dict]:
    in_q = asyncio.Queue()
    out_q = asyncio.Queue()

    async def fetch(in_q: asyncio.Queue, out_q: asyncio.Queue):
        while True:
            z, r, p = in_q.get_nowait()
            try:
                locs, t = await fetch_foodbanks_for_zip(z, r, p)
                for loc in locs:
                    out_q.put_nowait(loc)
            except asyncio.QueueEmpty as qe:
                logger.error(f"Queue is empty ({qe}), closing worker")
            except Exception as e:
                logger.error(f"Error fetching foodbanks for area: {e}")
            finally:
                in_q.task_done()
    
    initial_locations, t = await fetch_foodbanks_for_zip(zip_code, radius, 1)
    for loc in initial_locations:
        out_q.put_nowait(loc)  # add initial locations to output queue
    
    pages = t // 15 + 1
    logger.info(f"Total of {t} locations across {pages} pages")

    for page in range(2, pages + 1):
        in_q.put_nowait((zip_code, radius, page))
    
    workers = [asyncio.create_task(fetch(in_q, out_q)) for _ in range(num_workers)]
    await in_q.join()
    for w in workers:
        w.cancel()

    logger.info(f"Fetched {out_q.qsize()} locations for zip {zip_code}")
    return [out_q.get_nowait() for _ in range(out_q.qsize())]

def process_all_foodbanks(all_foodbanks: List[Dict]) -> gpd.GeoDataFrame:
    """Post-process the foodbank data into a GeoDataFrame and expand nested fields"""
    foodbanks_gdf = gpd.GeoDataFrame(all_foodbanks)
    logger.info(f"Initial foodbanks GeoDataFrame has {len(foodbanks_gdf)} records")
    foodbanks_gdf = foodbanks_gdf.drop_duplicates(subset=['id'], keep='first')
    logger.info(f"After removing duplicates, foodbanks GeoDataFrame has {len(foodbanks_gdf)} records")
    foodbanks_gdf = gpd.GeoDataFrame(foodbanks_gdf, geometry=gpd.points_from_xy(foodbanks_gdf["longitude"].astype(float), foodbanks_gdf["latitude"].astype(float)))
    foodbanks_gdf.set_crs(epsg=geo_settings.crs, inplace=True)

    return foodbanks_gdf

async def run():
    """Run the full ETL process for Why Hungry foodbanks"""
    logger.info("Starting Why Hungry ETL process...")
    
    # Fetch all foodbanks from Why Hungry API
    logger.info("Fetching foodbanks from all areas...")
    all_foodbanks = await fetch_foodbanks_for_zip_paginated(num_workers=20, zip_code="66952", radius=5000)
    logger.info(f"Successfully fetched {len(all_foodbanks)} foodbanks")
    
    # Process and expand the foodbank data
    logger.info("Processing foodbank data...")
    foodbanks_gdf = process_all_foodbanks(all_foodbanks)
    logger.info(f"Successfully processed {len(foodbanks_gdf)} foodbanks into GeoDataFrame")
    
    logger.info("Loading foodbanks into PostgreSQL PostGIS database...")
    load_into_pg(foodbanks_gdf, table_name='why_hungry_foodbanks_raw', if_exists='replace')
    logger.info("Successfully loaded foodbanks into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")