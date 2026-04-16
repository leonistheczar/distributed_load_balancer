from __future__ import annotations

from dataclasses import dataclass, field

from src.ingestion.models import Request
from src.nodes.node import Node


@dataclass(slots=True)
class LeastConnections:
    _active: dict[str, int] = field(default_factory=dict)

    def select_node(self, request: Request, nodes: list[Node]) -> Node:
        healthy = [n for n in nodes if n.is_healthy]
        if not healthy:
            raise ValueError("No healthy nodes available")

        # Pick healthy node with minimum active connections; tie-break by id.
        best = min(healthy, key=lambda n: (self._active.get(n.id, 0), n.id))
        self._active[best.id] = self._active.get(best.id, 0) + 1
        return best

    def on_complete(self, node_id: str, latency_ms: float) -> None:
        current = self._active.get(node_id, 0)
        self._active[node_id] = max(0, current - 1)

    def on_failure(self, node_id: str) -> None:
        # If a node fails mid-flight, clear its active count to avoid pinning.
        self._active.pop(node_id, None)

