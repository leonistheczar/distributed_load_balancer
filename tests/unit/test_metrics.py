from datetime import datetime

from src.ingestion.models import Request
from src.metrics.collector import MetricsCollector


def make_request(is_bot=False, status_code=200):
    return Request(
        timestamp=datetime.now(),
        arrival_delta_s=0.1,
        client_ip="1.2.3.4",
        client_host="zanbil.ir",
        client_hash=123,
        method="GET",
        url_path="/index.html",
        url_category="html",
        status_code=status_code,
        status_class=f"{status_code // 100}xx",
        bytes_sent=100,
        request_weight=1.0,
        is_bot=is_bot,
    )


def test_metrics_summary_basic():
    m = MetricsCollector(algorithm_name="rr")
    for i in range(10):
        m.record(
            request=make_request(is_bot=(i % 2 == 0), status_code=(500 if i == 0 else 200)),
            node_id="node-1" if i < 6 else "node-2",
            latency_ms=10 + i,
            sim_second=float(i),
            was_failover=False,
        )
    s = m.summary()
    assert s["total_requests"] == 10
    assert s["errors"] == 1
    assert s["bot_requests"] == 5
    assert s["human_requests"] == 5
    assert s["load_imbalance_score"] >= 0.0

