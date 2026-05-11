# ==========================================
# Project: Parallel Distributed Computing
# Institution: Emerson University Multan
# Class: BSCS 6th (M) (2023-2027)

# Team Members:
# - Muhammad Ali (56)
# - Muhammad Abdullah (54)
# - Muhammad Subhan Safdar (53)
# - Muhammad Hanan (14)
# ==========================================
# Github Link: https://github.com/leonistheczar/distributed_load_balancer

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class Request:
    timestamp: datetime
    arrival_delta_s: float

    client_ip: str
    client_host: str
    client_hash: int

    method: str
    url_path: str
    url_category: str

    status_code: int
    status_class: str

    bytes_sent: int
    request_weight: float

    referrer: str | None = None
    user_agent: str | None = None
    is_bot: bool = False

    assigned_node: str | None = None
    simulated_latency_ms: float | None = None

    @property
    def is_error(self) -> bool:
        return self.status_code >= 400

    @property
    def is_cacheable(self) -> bool:
        """
        Very lightweight cacheability heuristic for this dataset:
        treat static assets as cacheable unless the URL looks dynamic.
        """
        p = (self.url_path or "").lower()
        if "?" in p or "filter" in p:
            return False
        return p.endswith(
            (
                ".jpg",
                ".jpeg",
                ".png",
                ".gif",
                ".webp",
                ".css",
                ".js",
                ".svg",
                ".ico",
            )
        )

