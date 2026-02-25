"""EFO loader.

Reads line-delimited JSON snapshots from `static/efos/latest.jsonl`,
constructs a GeoDataFrame, and writes results to `efos_raw`.
"""

from settings.geo import geo_settings

import logging

import geopandas as gpd

from utils.db import load_into_pg

import json


logger = logging.getLogger(__name__)

async def run():
    """Load EFO records from local JSONL into PostGIS."""
    logger.info("Starting EFO ETL process...")
    
    # Source file is produced upstream and checked into `static/efos`.
    filename = 'static/efos/latest.jsonl'
    logger.info(f"Getting EFOs from {filename}")
    geodata = []
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            # Parse one JSON object per line; malformed lines are logged and skipped.
            try:
                data = json.loads(line)
                geodata.append(data)
            except json.JSONDecodeError as e:
                print(f"Error parsing line: {line}")
                print(e)
    crs = geo_settings.crs
    gdf = gpd.GeoDataFrame(data=geodata)
    # Longitude/latitude are expected in WGS84-compatible numeric fields.
    efo_gdf = gpd.GeoDataFrame(gdf, geometry=gpd.points_from_xy(gdf["longitude"].astype(float), gdf["latitude"].astype(float)))
    efo_gdf.set_crs(epsg=crs, inplace=True)
    efo_gdf.set_geometry('geometry', inplace=True)
    logger.info(f"Successfully fetched {len(gdf)} EFOs")
    
    logger.info("Loading EFOs into PostgreSQL PostGIS database...")
    load_into_pg(efo_gdf, table_name='efos_raw')
    logger.info("Successfully loaded EFOs into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")

    return gdf
