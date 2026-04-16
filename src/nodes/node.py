from __future__ import annotations

from dataclasses import dataclass, field
import random


@dataclass(slots=True)
class Node:
    id: str
    weight: int = 1
    latency_min_ms: float = 2.0
    latency_max_ms: float = 50.0

    is_healthy: bool = True

    _cache: set[str] = field(default_factory=set, repr=False)
    requests: int = 0
    bytes_sent: int = 0
    errors: int = 0
    cache_hits: int = 0
    next_available_s: float = 0.0
    in_flight: int = 0

    def process(self, request_weight: float, rng: random.Random) -> float:
        if not self.is_healthy:
            raise RuntimeError(f"NodeFailureError: Node {self.id} is unhealthy")

        base = rng.uniform(self.latency_min_ms, self.latency_max_ms)
        return float(base) * float(request_weight)

    def check_cache(self, url_path: str) -> bool:
        if url_path in self._cache:
            self.cache_hits += 1
            return True
        self._cache.add(url_path)
        return False

    def record_request(self, bytes_sent: int, is_error: bool) -> None:
        self.requests += 1
        self.bytes_sent += int(bytes_sent)
        if is_error:
            self.errors += 1

