"""In-memory analytics for the local aviation Kafka consumer."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class AviationAnalytics:
    total_reviews: int = 0
    reviews_by_category: Counter[str] = field(default_factory=Counter)
    country_counts: Counter[str] = field(default_factory=Counter)
    airline_rating_sum: defaultdict[str, float] = field(default_factory=lambda: defaultdict(float))
    airline_rating_count: Counter[str] = field(default_factory=Counter)
    low_rated_count: int = 0

    def update(self, record: dict[str, Any], low_rating_threshold: float) -> None:
        self.total_reviews += 1
        category = str(record.get("source_category") or "unknown")
        self.reviews_by_category[category] += 1

        country = record.get("country") or "unknown"
        self.country_counts[str(country)] += 1

        rating = float(record["rating"])
        if rating < low_rating_threshold:
            self.low_rated_count += 1

        airline = record.get("airline")
        if airline:
            self.airline_rating_sum[str(airline)] += rating
            self.airline_rating_count[str(airline)] += 1

    def average_rating_by_airline(self) -> dict[str, float]:
        return {
            airline: round(self.airline_rating_sum[airline] / count, 2)
            for airline, count in self.airline_rating_count.items()
            if count
        }

    def top_airlines_by_rating(self, limit: int = 5) -> list[tuple[str, float]]:
        averages = self.average_rating_by_airline()
        return sorted(averages.items(), key=lambda item: item[1], reverse=True)[:limit]

    def snapshot(self) -> dict[str, Any]:
        return {
            "total_reviews": self.total_reviews,
            "reviews_by_category": dict(self.reviews_by_category),
            "country_counts": dict(self.country_counts),
            "average_rating_by_airline": self.average_rating_by_airline(),
            "top_airlines_by_rating": self.top_airlines_by_rating(),
            "low_rated_count": self.low_rated_count,
        }

    def to_dataframe(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        rows.append({"metric": "total_reviews", "key": "all", "value": self.total_reviews})
        rows.append({"metric": "low_rated_count", "key": "all", "value": self.low_rated_count})

        for category, count in self.reviews_by_category.items():
            rows.append({"metric": "reviews_by_category", "key": category, "value": count})
        for country, count in self.country_counts.items():
            rows.append({"metric": "country_counts", "key": country, "value": count})
        for airline, average in self.average_rating_by_airline().items():
            rows.append({"metric": "average_rating_by_airline", "key": airline, "value": average})

        return pd.DataFrame(rows, columns=["metric", "key", "value"])
