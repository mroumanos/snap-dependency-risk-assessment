"""Top-level ETL entrypoint.

This module orchestrates the sequential execution of the web scrapes
for EFOs
"""

import logging

from foodbankscrapy.foodbankscrapy.main import run as run_foodbankscrapy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    
    run_foodbankscrapy(
        pipeline_path="static/pipelines/test.json",
        conform=True,
        conform_output_path="/code/static/efos/test.jsonl",
    )