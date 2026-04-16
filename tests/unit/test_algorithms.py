import pytest
from datetime import datetime

from src.ingestion.models import Request
from src.nodes.node import Node
from src.balancer.round_robin import RoundRobin
from src.balancer.least_connections import LeastConnections
from src.balancer.ip_hash import IPHash


def make_request(client_hash=1000, weight=1.0, url="/test", is_bot=False):
    return Request(
        timestamp=datetime.now(),
        arrival_delta_s=1.0,
        client_ip="185.220.101.45",
        client_host="zanbil.ir",
        client_hash=client_hash,
        method="GET",
        url_path=url,
        url_category="html",
        status_code=200,
        status_class="2xx",
        bytes_sent=1197,
        request_weight=weight,
        is_bot=is_bot,
    )


def make_nodes(n=3):
    return [Node(f"node-{i+1}", weight=i + 1) for i in range(n)]


class TestRoundRobin:
    def test_cycles_nodes(self):
        rr = RoundRobin()
        nodes = make_nodes(3)
        selected = [rr.select_node(make_request(), nodes).id for _ in range(6)]
        assert selected == ["node-1", "node-2", "node-3", "node-1", "node-2", "node-3"]

    def test_skips_unhealthy(self):
        rr = RoundRobin()
        nodes = make_nodes(3)
        nodes[0].is_healthy = False
        for _ in range(6):
            node = rr.select_node(make_request(), nodes)
            assert node.id != "node-1"

    def test_raises_when_all_down(self):
        rr = RoundRobin()
        nodes = make_nodes(2)
        for n in nodes:
            n.is_healthy = False
        with pytest.raises(ValueError):
            rr.select_node(make_request(), nodes)


class TestLeastConnections:
    def test_picks_least_busy(self):
        lc = LeastConnections()
        nodes = make_nodes(3)
        lc._active["node-1"] = 5
        lc._active["node-2"] = 0
        lc._active["node-3"] = 3
        selected = lc.select_node(make_request(), nodes)
        assert selected.id == "node-2"

    def test_decrements_on_complete(self):
        lc = LeastConnections()
        nodes = make_nodes(2)
        lc.select_node(make_request(), nodes)
        assert lc._active.get("node-1", 0) == 1
        lc.on_complete("node-1", 10.0)
        assert lc._active.get("node-1", 0) == 0

    def test_never_goes_negative(self):
        lc = LeastConnections()
        lc.on_complete("node-1", 5.0)
        assert lc._active.get("node-1", 0) >= 0


class TestIPHash:
    def test_same_client_same_node(self):
        ih = IPHash()
        nodes = make_nodes(3)
        req = make_request(client_hash=482736190)
        n1 = ih.select_node(req, nodes)
        n2 = ih.select_node(req, nodes)
        n3 = ih.select_node(req, nodes)
        assert n1.id == n2.id == n3.id

    def test_different_clients_may_differ(self):
        ih = IPHash()
        nodes = make_nodes(3)
        selections = set()
        for h in [100, 200, 300, 400, 500, 600]:
            node = ih.select_node(make_request(client_hash=h), nodes)
            selections.add(node.id)
        assert len(selections) > 1

    def test_remaps_on_failure(self):
        ih = IPHash()
        nodes = make_nodes(3)
        req = make_request(client_hash=12345)
        original = ih.select_node(req, nodes)
        original.is_healthy = False
        ih.on_failure(original.id)
        healthy = [n for n in nodes if n.is_healthy]
        new_node = ih.select_node(req, healthy)
        assert new_node.id != original.id

