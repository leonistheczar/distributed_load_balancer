from __future__ import annotations

from dataclasses import dataclass, field

from src.ingestion.models import Request
from src.nodes.node import Node


@dataclass(slots=True)
class WeightedRoundRobin:
    _schedule: list[str] = field(default_factory=list)
    _idx: int = 0

    def select_node(self, request: Request, nodes: list[Node]) -> Node:
        healthy = [n for n in nodes if n.is_healthy]
        if not healthy:
            raise ValueError("No healthy nodes available")

        # Rebuild schedule if node set changed (health/weights).
        schedule = []
        for n in healthy:
            w = int(n.weight) if int(n.weight) > 0 else 1
            schedule.extend([n.id] * w)

        if not schedule:
            raise ValueError("No healthy nodes available")

        self._schedule = schedule

        pick_id = self._schedule[self._idx % len(self._schedule)]
        node = next(n for n in healthy if n.id == pick_id)

        # Heavier requests advance the pointer more, approximating "token" cost.
        step = int(round(max(1.0, float(request.request_weight))))
        self._idx = (self._idx + step) % len(self._schedule)
        return node

    def on_complete(self, node_id: str, latency_ms: float) -> None:
        return

    def on_failure(self, node_id: str) -> None:
        self._idx = 0

