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

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from src.ingestion.models import Request


@dataclass(slots=True)
class MetricsCollector:
    algorithm_name: str

    _global_latencies: list[float] = field(default_factory=list, repr=False)
    _failover_events: int = 0
    _rps_buckets: dict[int, int] = field(default_factory=dict, repr=False)

    # node_id -> stats
    _node_requests: dict[str, int] = field(default_factory=dict, repr=False)
    _node_bytes: dict[str, int] = field(default_factory=dict, repr=False)
    _node_errors: dict[str, int] = field(default_factory=dict, repr=False)
    _node_latencies: dict[str, list[float]] = field(default_factory=dict, repr=False)
    _node_cache_hits: dict[str, int] = field(default_factory=dict, repr=False)

    _bot_requests: int = 0
    _human_requests: int = 0
    _queue_wait_latencies: list[float] = field(default_factory=list, repr=False)
    _service_latencies: list[float] = field(default_factory=list, repr=False)
    _dropped_requests: int = 0
    _backpressure_reroutes: int = 0

    def record(
        self,
        request: Request,
        node_id: str,
        latency_ms: float,
        sim_second: float,
        was_failover: bool,
        queue_wait_ms: float = 0.0,
        service_latency_ms: float | None = None,
    ) -> None:
        self._global_latencies.append(float(latency_ms))
        self._queue_wait_latencies.append(float(queue_wait_ms))
        self._service_latencies.append(
            float(service_latency_ms if service_latency_ms is not None else max(0.0, latency_ms - queue_wait_ms))
        )
        bucket = int(sim_second)
        self._rps_buckets[bucket] = self._rps_buckets.get(bucket, 0) + 1

        self._node_requests[node_id] = self._node_requests.get(node_id, 0) + 1
        self._node_bytes[node_id] = self._node_bytes.get(node_id, 0) + int(request.bytes_sent)
        self._node_errors[node_id] = self._node_errors.get(node_id, 0) + (1 if request.is_error else 0)
        self._node_latencies.setdefault(node_id, []).append(float(latency_ms))

        if was_failover:
            self._failover_events += 1

        if request.is_bot:
            self._bot_requests += 1
        else:
            self._human_requests += 1

    def record_cache_hit(self, node_id: str) -> None:
        self._node_cache_hits[node_id] = self._node_cache_hits.get(node_id, 0) + 1

    def record_drop(self, request: Request, sim_second: float) -> None:
        self._dropped_requests += 1
        bucket = int(sim_second)
        self._rps_buckets[bucket] = self._rps_buckets.get(bucket, 0)
        if request.is_bot:
            self._bot_requests += 1
        else:
            self._human_requests += 1

    def record_backpressure_reroute(self) -> None:
        self._backpressure_reroutes += 1

    def summary(self) -> dict[str, Any]:
        total = len(self._global_latencies)
        lat = np.array(self._global_latencies, dtype=float) if total else np.array([], dtype=float)

        def pct(p: float) -> float:
            if total == 0:
                return 0.0
            return float(np.percentile(lat, p))

        node_counts = list(self._node_requests.values())
        imbalance = float(np.std(np.array(node_counts, dtype=float), ddof=0)) if len(node_counts) >= 2 else 0.0

        errors = sum(self._node_errors.values())
        error_rate = (errors / total) if total else 0.0

        node_summaries: dict[str, Any] = {}
        for node_id, count in self._node_requests.items():
            nl = self._node_latencies.get(node_id, [])
            node_lat = np.array(nl, dtype=float) if nl else np.array([], dtype=float)
            node_summaries[node_id] = {
                "requests": count,
                "bytes_sent": self._node_bytes.get(node_id, 0),
                "errors": self._node_errors.get(node_id, 0),
                "error_rate": (self._node_errors.get(node_id, 0) / count) if count else 0.0,
                "cache_hits": self._node_cache_hits.get(node_id, 0),
                "cache_hit_rate": (self._node_cache_hits.get(node_id, 0) / count) if count else 0.0,
                "latency_p50_ms": float(np.percentile(node_lat, 50)) if len(node_lat) else 0.0,
                "latency_p95_ms": float(np.percentile(node_lat, 95)) if len(node_lat) else 0.0,
                "latency_p99_ms": float(np.percentile(node_lat, 99)) if len(node_lat) else 0.0,
            }

        return {
            "algorithm": self.algorithm_name,
            "dispatched_requests": total + self._dropped_requests,
            "total_requests": total,
            "dropped_requests": self._dropped_requests,
            "drop_rate": (self._dropped_requests / (total + self._dropped_requests)) if (total + self._dropped_requests) else 0.0,
            "backpressure_reroutes": self._backpressure_reroutes,
            "latency_p50_ms": pct(50),
            "latency_p95_ms": pct(95),
            "latency_p99_ms": pct(99),
            "queue_wait_p50_ms": float(np.percentile(self._queue_wait_latencies, 50)) if total else 0.0,
            "queue_wait_p95_ms": float(np.percentile(self._queue_wait_latencies, 95)) if total else 0.0,
            "queue_wait_p99_ms": float(np.percentile(self._queue_wait_latencies, 99)) if total else 0.0,
            "service_latency_p50_ms": float(np.percentile(self._service_latencies, 50)) if total else 0.0,
            "service_latency_p95_ms": float(np.percentile(self._service_latencies, 95)) if total else 0.0,
            "service_latency_p99_ms": float(np.percentile(self._service_latencies, 99)) if total else 0.0,
            "load_imbalance_score": imbalance,
            "errors": errors,
            "error_rate": error_rate,
            "failover_events": self._failover_events,
            "bot_requests": self._bot_requests,
            "human_requests": self._human_requests,
            "rps_by_second": dict(sorted(self._rps_buckets.items())),
            "nodes": node_summaries,
        }

