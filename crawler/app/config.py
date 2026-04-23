import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CrawlConfig:
    start_urls: list[str]
    max_pages: int = int(os.getenv("CRAWLER_MAX_PAGES", "10"))
    output_file: str = os.getenv("OUTPUT_FILE", "/app/data/raw/books_raw.csv")
    keyword_filters: list[str] | None = None
    compartment: str = os.getenv("CRAWLER_COMPARTMENT", "default")


def load_config() -> CrawlConfig:
    raw_urls = os.getenv("CRAWLER_START_URLS") or os.getenv("CRAWLER_START_URL") or "https://books.toscrape.com/"
    start_urls = [value.strip() for value in raw_urls.split(",") if value.strip()]
    raw_keywords = os.getenv("CRAWLER_KEYWORDS", "")
    keyword_filters = [value.strip().lower() for value in raw_keywords.split(",") if value.strip()]
    compartment = os.getenv("CRAWLER_COMPARTMENT", "default").strip() or "default"
    return CrawlConfig(
        start_urls=start_urls,
        max_pages=int(os.getenv("CRAWLER_MAX_PAGES", "10")),
        output_file=os.getenv("OUTPUT_FILE", "/app/data/raw/books_raw.csv"),
        keyword_filters=keyword_filters or None,
        compartment=compartment,
    )
