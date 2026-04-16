from __future__ import annotations

from typing import Protocol

from src.ingestion.models import Request
from src.nodes.node import Node


class LoadBalancer(Protocol):
    def select_node(self, request: Request, nodes: list[Node]) -> Node: ...

    def on_complete(self, node_id: str, latency_ms: float) -> None: ...

    def on_failure(self, node_id: str) -> None: ...

