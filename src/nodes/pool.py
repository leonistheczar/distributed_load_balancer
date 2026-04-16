from __future__ import annotations

from dataclasses import dataclass, field

from .node import Node


@dataclass(slots=True)
class FailureEvent:
    node_id: str
    at_second: float
    duration_s: float
    triggered: bool = False
    recovered: bool = False


@dataclass(slots=True)
class NodePool:
    nodes: list[Node]
    _failures: list[FailureEvent] = field(default_factory=list, repr=False)

    def schedule_failure(self, node_id: str, at_second: float, duration_s: float) -> None:
        self._failures.append(
            FailureEvent(node_id=node_id, at_second=float(at_second), duration_s=float(duration_s))
        )

    def tick(self, sim_second: float) -> None:
        t = float(sim_second)
        for ev in self._failures:
            if not ev.triggered and t >= ev.at_second:
                n = self._by_id(ev.node_id)
                if n is not None:
                    n.is_healthy = False
                ev.triggered = True
            if ev.triggered and not ev.recovered and t >= (ev.at_second + ev.duration_s):
                n = self._by_id(ev.node_id)
                if n is not None:
                    n.is_healthy = True
                ev.recovered = True

    def healthy_nodes(self) -> list[Node]:
        return [n for n in self.nodes if n.is_healthy]

    def summary(self) -> dict:
        return {
            "total_nodes": len(self.nodes),
            "healthy": sum(1 for n in self.nodes if n.is_healthy),
            "unhealthy": sum(1 for n in self.nodes if not n.is_healthy),
            "failures_scheduled": len(self._failures),
        }

    def _by_id(self, node_id: str) -> Node | None:
        for n in self.nodes:
            if n.id == node_id:
                return n
        return None

