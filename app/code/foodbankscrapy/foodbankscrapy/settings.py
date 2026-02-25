"""Scrapy settings for the food bank crawler.

Values are intentionally centralized here so reviewers can audit crawl
policies, retry behavior, output locations, and logging without tracing
runtime overrides.
"""

import os
from pathlib import Path

BOT_NAME = "foodbankscrapy"

SPIDER_MODULES = ["foodbankscrapy.foodbankscrapy.spiders"]
NEWSPIDER_MODULE = "foodbankscrapy.foodbankscrapy.spiders"

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 32
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = float(os.getenv("SCRAPY_DOWNLOAD_DELAY", "0"))

RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429, 520]

ITEM_PIPELINES = {
    "foodbankscrapy.foodbankscrapy.pipelines.RunJsonlPipeline": 300,
}

OUTPUT_DIR = str(Path(__file__).resolve().parents[1] / "output")

FEED_EXPORT_ENCODING = "utf-8"

# Logging defaults: verbose for app code, quieter for Scrapy internals.
CSV_LOG_LEVEL = os.getenv("CSV_LOG_LEVEL", "INFO")
SCRAPY_LOG_LEVEL = os.getenv("SCRAPY_LOG_LEVEL", "WARNING")
ROOT_LOG_LEVEL = os.getenv("ROOT_LOG_LEVEL", "WARNING")
LOG_LEVEL = SCRAPY_LOG_LEVEL
LOG_LEVELS = {
    "foodbankscrapy": CSV_LOG_LEVEL,
    "scrapy": SCRAPY_LOG_LEVEL,
    "scrapy.core": SCRAPY_LOG_LEVEL,
    "scrapy.core.engine": SCRAPY_LOG_LEVEL,
    "scrapy.middleware": SCRAPY_LOG_LEVEL,
    "scrapy.addons": SCRAPY_LOG_LEVEL,
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "loggers": {
        "": {"handlers": ["console"], "level": ROOT_LOG_LEVEL},
        "foodbankscrapy": {
            "handlers": ["console"],
            "level": CSV_LOG_LEVEL,
            "propagate": False,
        },
        "foodbankscrapy.spiders": {
            "handlers": ["console"],
            "level": CSV_LOG_LEVEL,
            "propagate": False,
        },
        "foodbankscrapy.utils": {
            "handlers": ["console"],
            "level": CSV_LOG_LEVEL,
            "propagate": False,
        },
        "scrapy": {"handlers": ["console"], "level": SCRAPY_LOG_LEVEL},
        "scrapy.core": {"handlers": ["console"], "level": SCRAPY_LOG_LEVEL},
        "scrapy.core.engine": {"handlers": ["console"], "level": SCRAPY_LOG_LEVEL},
        "scrapy.middleware": {"handlers": ["console"], "level": SCRAPY_LOG_LEVEL},
        "scrapy.addons": {"handlers": ["console"], "level": SCRAPY_LOG_LEVEL},
    },
}

# Capture stdout prints emitted by third-party libs into structured logs.
LOG_STDOUT = True


# Disable Scrapy's default end-of-run stats noise.
STATS_DUMP = False


EXTENSIONS = {
    "foodbankscrapy.foodbankscrapy.extensions.ProgressLogger": None,
    "scrapy.extensions.logstats.LogStats": None,
}
EXTENSIONS["scrapy.extensions.telnet.TelnetConsole"] = None
EXTENSIONS["foodbankscrapy.foodbankscrapy.extensions.LoggerLevelApplier"] = 100

DOWNLOADER_MIDDLEWARES = {
    "foodbankscrapy.foodbankscrapy.middlewares.RequestDelayMiddleware": 50,
}

# Progress logging cadence in seconds.
PROGRESS_LOG_INTERVAL = int(os.getenv("SCRAPY_PROGRESS_INTERVAL", "30"))


def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


# Enable HTTP/2 downloader for HTTPS requests when explicitly requested.
# Some hosts fail TLS/ALPN negotiation with H2 and raise OpenSSL "bad extension".
if _env_bool("SCRAPY_ENABLE_HTTP2", "0"):
    DOWNLOAD_HANDLERS = {
        "https": "scrapy.core.downloader.handlers.http2.H2DownloadHandler",
    }


# Debug raw response payloads (useful when endpoint behaves differently in scraper vs browser)
RAW_RESPONSE_DEBUG = _env_bool("RAW_RESPONSE_DEBUG", "0")
RAW_RESPONSE_DEBUG_MAX_CHARS = int(os.getenv("RAW_RESPONSE_DEBUG_MAX_CHARS", "4000"))
