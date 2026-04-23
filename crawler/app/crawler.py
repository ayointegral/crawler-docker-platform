from __future__ import annotations

import os
from dataclasses import asdict

import scrapy
from scrapy.crawler import CrawlerProcess

from .config import CrawlConfig, load_config
from .parser import parse_book_card


class BooksSpider(scrapy.Spider):
    name = "books"

    def __init__(self, config: CrawlConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.start_urls = config.start_urls
        self.page_count = 0

    def parse(self, response):
        self.page_count += 1

        for card in response.css("article.product_pod"):
            record = parse_book_card(response, card)
            record["compartment"] = self.config.compartment
            if self.config.keyword_filters:
                title = (record.get("title") or "").lower()
                if not any(keyword in title for keyword in self.config.keyword_filters):
                    continue
            yield record

        if self.page_count >= self.config.max_pages:
            self.logger.info("Reached max pages limit: %s", self.config.max_pages)
            return

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)


def run_crawler(config: CrawlConfig | None = None) -> None:
    cfg = config or load_config()
    output_dir = os.path.dirname(cfg.output_file)
    os.makedirs(output_dir, exist_ok=True)

    feed_settings = {
        cfg.output_file: {
            "format": "csv",
            "overwrite": True,
            "fields": [
                "title",
                "price",
                "rating",
                "availability_text",
                "source_url",
                "compartment",
                "scraped_at",
            ],
        }
    }

    process = CrawlerProcess(
        settings={
            "FEEDS": feed_settings,
            # Airflow task logging can recurse with Scrapy root handlers in this setup.
            "LOG_ENABLED": False,
            "ROBOTSTXT_OBEY": True,
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "USER_AGENT": "crawler-platform/1.0 (+https://example.local)",
            "RETRY_ENABLED": True,
            "RETRY_TIMES": 3,
            "DOWNLOAD_TIMEOUT": 20,
        }
    )

    process.crawl(BooksSpider, config=cfg)
    process.start()


if __name__ == "__main__":
    config = load_config()
    print(f"Starting crawler with config: {asdict(config)}")
    run_crawler(config)
