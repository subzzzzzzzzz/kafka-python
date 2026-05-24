"""In-memory rolling analytics for consumed aviation reviews."""

from __future__ import annotations

from collections import Counter
from typing import Any

from config import LOW_RATING_THRESHOLD
from schema import entity_name


class RollingAnalytics:
    def __init__(self) -> None:
        self.total_messages = 0
        self.messages_per_category: Counter[str] = Counter()
        self.low_rated_count = 0
        self.rating_distribution: Counter[str] = Counter()
        self.airlines: Counter[str] = Counter()
        self.airports: Counter[str] = Counter()

    def update(self, message: dict[str, Any]) -> None:
        self.total_messages += 1
        category = str(message["source_category"])
        self.messages_per_category[category] += 1

        rating = message.get("overall_rating")
        if isinstance(rating, (int, float)):
            bucket = str(int(rating))
            self.rating_distribution[bucket] += 1
            if rating <= LOW_RATING_THRESHOLD:
                self.low_rated_count += 1

        name = entity_name(message)
        if name and category == "airline":
            self.airlines[name] += 1
        elif name and category == "airport":
            self.airports[name] += 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "total_messages_processed": self.total_messages,
            "messages_per_category": dict(sorted(self.messages_per_category.items())),
            "low_rated_review_count": self.low_rated_count,
            "rating_distribution": dict(sorted(self.rating_distribution.items(), key=lambda item: float(item[0]))),
            "top_5_airlines": self.airlines.most_common(5),
            "top_5_airports": self.airports.most_common(5),
        }
