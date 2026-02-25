"""Top-level ETL entrypoint.

This module orchestrates the sequential execution of all ETL jobs under
`app/code/etl`. Jobs are intentionally run in a fixed order so downstream
tables can rely on a predictable refresh sequence during a full run.
"""

import asyncio
import logging

from etl.fa import run as run_feeding_america_etl
from etl.census import run as run_census_etl
from etl.nhgis import run as run_nhgis_etl
from etl.snap import run as run_snap_etl
from etl.efo import run as run_efo_etl
from foodbankscrapy.foodbankscrapy.main import run as run_foodbankscrapy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Run the full ETL workflow for all configured data sources."""
    try:
        # Keep execution order explicit for easier operations/debugging.
        await run_feeding_america_etl()
        await run_census_etl()
        await run_nhgis_etl()
        await run_snap_etl()
        await run_efo_etl()
        
    except Exception as e:
        logger.error(f"Error during ETL process: {e}")
        raise

if __name__ == "__main__":
    
    # run_foodbankscrapy(
    #     pipeline_path="static/pipelines/prod.json",
    #     conform=True,
    #     conform_output_path="/code/static/efos/latest.jsonl",
    # )

    asyncio.run(main())