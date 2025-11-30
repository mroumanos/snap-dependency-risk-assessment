import asyncio
import logging

from etl.feeding_america import run as run_feeding_america_etl
from etl.census import run as run_census_etl
from etl.why_hungry import run as run_why_hungry_etl
from etl.access_food import run as run_access_food_etl
from etl.capital_food_bank import run as run_capital_food_bank_etl
from etl.md_food_bank import run as run_md_food_bank_etl
from etl.nhgis import run as run_nhgis_etl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main function to orchestrate ETL operations"""
    try:
        # await run_feeding_america_etl()
        await run_census_etl()
        # await run_why_hungry_etl()
        # await run_access_food_etl()
        # await run_capital_food_bank_etl()
        # await run_md_food_bank_etl()
        # await run_nhgis_etl()
        
    except Exception as e:
        logger.error(f"Error during ETL process: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
