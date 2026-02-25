"""Scrapy extensions for progress logging and runtime log-level control."""

import logging
import time

from scrapy import signals


class ProgressLogger:
    """Log request/item progress at a configurable interval."""

    def __init__(self, stats, interval):
        self.stats = stats
        self.interval = interval
        self.last_log = 0.0

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getint("PROGRESS_LOG_INTERVAL", 30)
        ext = cls(crawler.stats, interval)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def item_scraped(self, item, response, spider):
        """Emit periodic progress logs as items are scraped."""
        now = time.time()
        if now - self.last_log < self.interval:
            return
        self.last_log = now
        item_count = self.stats.get_value("item_scraped_count", 0)
        request_count = self.stats.get_value("downloader/request_count", 0)
        spider.logger.info(
            "Progress: items=%s requests=%s", item_count, request_count
        )

    def spider_closed(self, spider, reason):
        """Emit final aggregate counters when crawl ends."""
        item_count = self.stats.get_value("item_scraped_count", 0)
        request_count = self.stats.get_value("downloader/request_count", 0)
        spider.logger.info(
            "Finished (%s). items=%s requests=%s", reason, item_count, request_count
        )


class LoggerLevelApplier:
    """Apply logger levels from settings when spider opens."""

    def __init__(self, levels):
        self.levels = levels

    @classmethod
    def from_crawler(cls, crawler):
        levels = crawler.settings.getdict("LOG_LEVELS", {})
        ext = cls(levels)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        return ext

    def spider_opened(self, spider):
        """Set logging levels defensively for configured logger names."""
        for logger_name, level in self.levels.items():
            logger = logging.getLogger(logger_name)
            try:
                logger.setLevel(level)
            except Exception:
                # Ignore invalid levels
                continue
