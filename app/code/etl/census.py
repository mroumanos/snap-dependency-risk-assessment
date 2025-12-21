from settings.geo import geo_settings
from settings.db import postgres_settings
from sqlalchemy import create_engine
from datetime import datetime
import logging
import aiohttp
import pandas as pd
import geopandas as gpd
from utils.db import load_into_pg


logger = logging.getLogger(__name__)

urls = {
    'census_2024_state': {
        'url': 'https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip'
    },
    'census_2024_county': {
        'url': 'https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip'
    },
    # 'census_2024_tract': {
    #     'url': 'https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_{index:02}_tract.zip',
    #     'index': [i for i in range(1, 79)]
    # },
    # 'census_2024_blkgrp': {
    #     'url': 'https://www2.census.gov/geo/tiger/TIGER2024/BG/tl_2024_{index:02}_bg.zip',
    #     'index': [i for i in range(1, 79)],
    # },
    # 'census_cps_fss': {
    #     'url': 'https://www2.census.gov/programs-surveys/cps/datasets/20{index}/supp/dec{index}pub.csv',
    #     'index': ['19', '20', '21', '22', '23'],
    #     'geometry': False,
    #     'pandas': True
    # }
}

async def fetch_data_from_url(url: str, index: list = None) -> gpd.GeoDataFrame:
    try:
        if index:
            gdfs = []
            for i in index:
                logger.info(f"Fetching data from {url.format(index=i)}")
                formatted_url = url.format(index=i)
                try:
                    gdf = gpd.read_file(formatted_url)
                    gdfs.append(gdf)
                    logger.info(f"Successfully fetched {len(gdf)} records from {formatted_url}")
                except Exception as e:
                    logger.warning(f"Warning fetching data from {formatted_url}: {e}")
            return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        else:
            return gpd.read_file(url)
    except Exception as e:
        logger.error(f"Error fetching census data from {url}: {e}")
        return gpd.GeoDataFrame()


async def run():
    """Run the full ETL process for SNAP retailers"""
    logger.info("Starting ETL process for remotely stored data...")
    
    for name, source in urls.items():
        logger.info(f"Fetching data for {name} from {source['url']}...")
        data_gdf = await fetch_data_from_url(source['url'], source.get('index'))
        logger.info(f"Successfully fetched {len(data_gdf)} records for {name}")
        
        table_name = f"{name}_raw"
        logger.info(f"Loading {name} data into PostgreSQL PostGIS database as table {table_name}...")
        load_into_pg(data_gdf, table_name=table_name, if_exists='replace', pandas=source.get('pandas'), geometry=source.get('geometry', True))
        logger.info(f"Successfully loaded {name} data into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")