from __future__ import annotations

import os
from dataclasses import asdict

import scrapy
from scrapy.crawler import CrawlerProcess

from .config import CrawlConfig, load_config
from .parser import find_next_page, parse_record


class SchemaSpider(scrapy.Spider):
    name = "schema_crawler"

    def __init__(self, config: CrawlConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.start_urls = config.start_urls
        self.page_count = 0

    def parse(self, response):
        self.page_count += 1

        records = response.css(self.config.schema.record_selector.query)
        if self.config.schema.record_selector.type == "xpath":
            records = response.xpath(self.config.schema.record_selector.query)

        for item in records:
            record = parse_record(
                response=response,
                item=item,
                fields=self.config.schema.fields,
                detail_link_selector=self.config.schema.detail_link_selector,
            )
            title = str(record.get("title") or "").lower()
            if self.config.keyword_filters and title:
                if not any(keyword in title for keyword in self.config.keyword_filters):
                    continue
            elif self.config.keyword_filters and not title:
                continue

            record["compartment"] = self.config.compartment
            record["dataset_name"] = self.config.schema.dataset_name
            record["schema_path"] = self.config.schema_path
            yield record

        if self.page_count >= self.config.max_pages:
            self.logger.info("Reached max pages limit: %s", self.config.max_pages)
            return

        next_page = find_next_page(response, self.config.schema.pagination_selector)
        if next_page:
            yield response.follow(next_page, callback=self.parse)


def _feed_fields(cfg: CrawlConfig) -> list[str]:
    field_names = [field.name for field in cfg.schema.fields]
    metadata_fields = ["source_url", "compartment", "dataset_name", "schema_path", "scraped_at"]
    seen = set()
    ordered: list[str] = []
    for name in field_names + metadata_fields:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def run_crawler(config: CrawlConfig | None = None) -> None:
    cfg = config or load_config()
    output_dir = os.path.dirname(cfg.output_file)
    os.makedirs(output_dir, exist_ok=True)

    feed_settings = {
        cfg.output_file: {
            "format": "csv",
            "overwrite": True,
            "fields": _feed_fields(cfg),
        }
    }

    process = CrawlerProcess(
        settings={
            "FEEDS": feed_settings,
            "LOG_ENABLED": False,
            "ROBOTSTXT_OBEY": True,
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "USER_AGENT": "crawler-platform/1.0 (+https://example.local)",
            "RETRY_ENABLED": True,
            "RETRY_TIMES": 3,
            "DOWNLOAD_TIMEOUT": 20,
        }
    )

    process.crawl(SchemaSpider, config=cfg)
    process.start()


if __name__ == "__main__":
    config = load_config()
    print(f"Starting crawler with config: {asdict(config)}")
    run_crawler(config)
