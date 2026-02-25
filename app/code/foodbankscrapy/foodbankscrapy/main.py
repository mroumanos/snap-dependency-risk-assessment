"""CLI and runtime orchestration for the `foodbankscrapy` crawler."""

import argparse
import logging
import logging.config
import os
import shutil
from datetime import datetime
from pathlib import Path

from scrapy.crawler import AsyncCrawlerProcess
from scrapy.utils.project import get_project_settings

from .spiders.food_bank_spider import FoodBankSpider
from .utils.conform import conform_latest_raw_outputs, conform_run_outputs
from .utils.pipeline import filter_rows, load_pipeline_rows
from .utils.test_capture import test_input_path_guess


def _publish_conformed_output(
    *,
    src_path: Path,
    target_path: str | None,
    logger: logging.Logger,
) -> Path | None:
    if not target_path:
        return None
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, target)
    logger.info("[conform] published latest output: %s", target)
    return target


def run(
    state=None,
    organization_id=None,
    pipeline_path="static/pipelines/test.json",
    smoke_test=False,
    test_mode=False,
    test_record=False,
    conform=False,
    conform_only=False,
    conform_output_path=None,
):
    """Run scraping and optional conformance for a selected pipeline slice."""
    logger = logging.getLogger(__name__)
    os.environ.setdefault(
        "SCRAPY_SETTINGS_MODULE",
        "foodbankscrapy.foodbankscrapy.settings",
    )
    settings = get_project_settings()
    logging_config = settings.get("LOGGING")
    if isinstance(logging_config, dict):
        logging.config.dictConfig(logging_config)
    logger = logging.getLogger("foodbankscrapy")
    logger.info("[startup] logger initialized")
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if test_mode:
        run_stamp = f"{run_stamp}_test"
    settings.set("RUN_ID", run_stamp)
    if test_mode:
        settings.set("TEST_MODE", True)
    if test_record:
        settings.set("TEST_RECORD", True)
    if smoke_test:
        settings.set(
            "ITEM_PIPELINES",
            {
                "foodbankscrapy.foodbankscrapy.pipelines.RunJsonlPipeline": 300,
                "foodbankscrapy.foodbankscrapy.pipelines.SmokeTestPipeline": 400,
            },
        )

    process = AsyncCrawlerProcess(settings)
    pipeline_path = Path(pipeline_path)
    if not pipeline_path.is_absolute():
        pipeline_path = Path(__file__).resolve().parent.parent / pipeline_path
    logger.info("[main] pipeline path: %s", pipeline_path)
    food_banks = load_pipeline_rows(pipeline_path)
    logger.info("[main] loaded rows: %s", len(food_banks))
    if test_mode:
        food_banks = [
            row for row in food_banks if row.source and test_input_path_guess(row.source).exists()
        ]
        logger.info("[main] test mode: rows with fixtures=%s", len(food_banks))

    food_banks = filter_rows(food_banks, state=state, organization_id=organization_id)
    logger.info(
        "[main] filtered rows: %s (state=%s, organization_id=%s)",
        len(food_banks),
        state,
        organization_id,
    )

    # Emit explicit run plan to make configuration reviewable in logs.
    logger.info("Run plan:")
    for cfg in food_banks:
        source = cfg.source or "(no source)"
        gen_params = cfg.generator_kwargs or {}
        eval_params = cfg.evaluator_kwargs or {}
        parser_params = cfg.parser_kwargs or []
        logger.info(
            f"- org_id={cfg.organization_id} "
            f"name={cfg.name} "
            f"state={cfg.state} "
            f"generator={cfg.generator} "
            f"generator_kwargs={gen_params} "
            f"evaluator={cfg.evaluator} "
            f"evaluator_kwargs={eval_params} "
            f"parser={cfg.parser} "
            f"parser_kwargs={parser_params} "
            f"source={source}"
        )
    if not food_banks:
        logger.info("- (no matching rows)")

    if conform_only:
        out_dir = settings.get("OUTPUT_DIR", "output")
        org_ids = sorted(
            {str(cfg.organization_id) for cfg in food_banks if cfg.organization_id is not None}
        )
        conformed_path = conform_latest_raw_outputs(output_dir=out_dir, org_ids=org_ids or None)
        published_path = _publish_conformed_output(
            src_path=conformed_path,
            target_path=conform_output_path,
            logger=logger,
        )
        if published_path is not None:
            logger.info(
                "[conform-only] wrote conformed output: %s (published=%s, org_ids=%s)",
                conformed_path,
                published_path,
                org_ids if org_ids else "all",
            )
        else:
            logger.info(
                "[conform-only] wrote conformed output: %s (org_ids=%s)",
                conformed_path,
                org_ids if org_ids else "all",
            )
        return

    logger.info("Run ID: %s", run_stamp)
    process.crawl(FoodBankSpider, food_banks=food_banks)
    process.start()

    if conform:
        out_dir = settings.get("OUTPUT_DIR", "output")
        conformed_path = conform_run_outputs(
            run_id=run_stamp,
            food_banks=food_banks,
            output_dir=out_dir,
        )
        published_path = _publish_conformed_output(
            src_path=conformed_path,
            target_path=conform_output_path,
            logger=logger,
        )
        if published_path is not None:
            logger.info(
                "[conform] wrote conformed output: %s (published=%s)",
                conformed_path,
                published_path,
            )
        else:
            logger.info("[conform] wrote conformed output: %s", conformed_path)


def build_arg_parser():
    """Build command-line parser for local runs and CI automation."""
    parser = argparse.ArgumentParser(description="Run CSV-driven Scrapy MVP")
    parser.add_argument("--state", help="Run for a single state (e.g., TX)")
    parser.add_argument("--org-id", type=int, help="Run for a single organization id")
    parser.add_argument("--pipeline", default="static/pipelines/test.json", help="Path to pipeline JSON/CSV")
    parser.add_argument("--test-record", action="store_true", help="Record test fixtures from live sources")
    parser.add_argument("--test-smoke", action="store_true", help="Run smoke tests against saved fixtures")
    parser.add_argument(
        "--conform",
        action="store_true",
        help="After scrape run completes, write one conformed JSONL for orgs run in this invocation",
    )
    parser.add_argument(
        "--conform-only",
        action="store_true",
        help="Only run conformance using latest raw file per org; do not execute pulls",
    )
    parser.add_argument(
        "--conform-output-path",
        help="Optional publish path for conformed output (default: no publish copy)",
    )
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    pipeline = args.pipeline
    if (args.test_record or args.test_smoke) and args.pipeline == "static/pipelines/test.json":
        pipeline = "static/pipelines/prod.json"
    run(
        state=args.state,
        organization_id=args.org_id,
        pipeline_path=pipeline,
        smoke_test=args.test_smoke,
        test_mode=args.test_smoke,
        test_record=args.test_record,
        conform=args.conform,
        conform_only=args.conform_only,
        conform_output_path=args.conform_output_path,
    )
