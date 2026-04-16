from __future__ import annotations

from .interface import LoadBalancer
from .ip_hash import IPHash
from .least_connections import LeastConnections
from .round_robin import RoundRobin
from .weighted_rr import WeightedRoundRobin

ALGORITHMS = ["rr", "wrr", "lc", "ip_hash"]


def get_algorithm(name: str) -> LoadBalancer:
    key = (name or "").strip().lower()
    if key in ("rr", "round_robin", "round-robin"):
        return RoundRobin()
    if key in ("wrr", "weighted_rr", "weighted-round-robin"):
        return WeightedRoundRobin()
    if key in ("lc", "least_connections", "least-connections"):
        return LeastConnections()
    if key in ("ip_hash", "iphash", "ip-hash"):
        return IPHash()
    raise ValueError(f"Unknown algorithm: {name}. Choose from: {ALGORITHMS} or 'all'")


__all__ = [
    "ALGORITHMS",
    "get_algorithm",
    "LoadBalancer",
    "RoundRobin",
    "WeightedRoundRobin",
    "LeastConnections",
    "IPHash",
]

