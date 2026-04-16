from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.models import Request
from src.nodes.node import Node


@dataclass(slots=True)
class RoundRobin:
    _idx: int = 0

    def select_node(self, request: Request, nodes: list[Node]) -> Node:
        healthy = [n for n in nodes if n.is_healthy]
        if not healthy:
            raise ValueError("No healthy nodes available")

        node = healthy[self._idx % len(healthy)]
        self._idx = (self._idx + 1) % len(healthy)
        return node

    def on_complete(self, node_id: str, latency_ms: float) -> None:
        return

    def on_failure(self, node_id: str) -> None:
        # Reset index to keep cycling cleanly after failures.
        self._idx = 0

