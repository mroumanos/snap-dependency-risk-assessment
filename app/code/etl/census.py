"""Census ETL loaders.

This module downloads public Census-hosted datasets (TIGER geometries and
CPS FSS tabular files), converts them to dataframe form, and loads them into
raw Postgres tables.
"""

from settings.geo import geo_settings
import logging
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
    'census_2024_tract': {
        'url': 'https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_{index:02}_tract.zip',
        'index': [i for i in range(1, 79)]
    },
    'census_2024_blkgrp': {
        'url': 'https://www2.census.gov/geo/tiger/TIGER2024/BG/tl_2024_{index:02}_bg.zip',
        'index': [i for i in range(1, 79)],
    },
    'census_cps_fss': {
        'url': 'https://www2.census.gov/programs-surveys/cps/datasets/20{index}/supp/dec{index}pub.csv',
        'index': ['19', '20', '21', '22', '23'],
        'geometry': False
    },
    'ers_county_typology': {
        'url': 'https://www.ers.usda.gov/media/6174/ers-county-typology-codes-2025-edition.csv',
        'geometry': False,
    }
}

async def fetch_data_from_url(url: str, index: list = None) -> gpd.GeoDataFrame:
    """Fetch one dataset URL, optionally expanding an index placeholder."""
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
            # Concatenate all per-index partitions into a single table payload.
            return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        else:
            return gpd.read_file(url)
    except Exception as e:
        logger.error(f"Error fetching census data from {url}: {e}")
        return gpd.GeoDataFrame()


async def run():
    """Execute Census ingestion and load each source into `<name>_raw` tables."""
    logger.info("Starting ETL process for remotely stored data...")
    
    for name, source in urls.items():
        logger.info(f"Fetching data for {name} from {source['url']}...")
        data_gdf = await fetch_data_from_url(source['url'], source.get('index'))
        logger.info(f"Successfully fetched {len(data_gdf)} records for {name}")
        
        table_name = f"{name}_raw"
        logger.info(f"Loading {name} data into PostgreSQL PostGIS database as table {table_name}...")
        load_into_pg(data_gdf, table_name=table_name, if_exists='replace', geometry=source.get('geometry', True))
        logger.info(f"Successfully loaded {name} data into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")
