from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.models import Request
from src.nodes.node import Node


@dataclass(slots=True)
class IPHash:
    def select_node(self, request: Request, nodes: list[Node]) -> Node:
        healthy = [n for n in nodes if n.is_healthy]
        if not healthy:
            raise ValueError("No healthy nodes available")

        start = int(request.client_hash) % len(healthy)
        for i in range(len(healthy)):
            n = healthy[(start + i) % len(healthy)]
            if n.is_healthy:
                return n
        raise ValueError("No healthy nodes available")

    def on_complete(self, node_id: str, latency_ms: float) -> None:
        return

    def on_failure(self, node_id: str) -> None:
        return

