from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urljoin


_RATING_MAP = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5,
}


def parse_book_card(response, card):
    title = card.css("h3 a::attr(title)").get(default="").strip()
    raw_price = card.css("p.price_color::text").get(default="").strip()
    availability = card.css("p.instock.availability::text").getall()
    rating_class = card.css("p.star-rating::attr(class)").get(default="")

    price_value = raw_price.replace("\u00a3", "").strip()
    availability_text = " ".join(fragment.strip() for fragment in availability if fragment.strip())
    rating_word = rating_class.split()[-1] if rating_class else ""
    relative_url = card.css("h3 a::attr(href)").get(default="")

    return {
        "title": title,
        "price": price_value,
        "rating": _RATING_MAP.get(rating_word),
        "availability_text": availability_text,
        "source_url": urljoin(response.url, relative_url),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
