from datetime import datetime

from src.ingestion.models import Request


def test_request_properties():
    req = Request(
        timestamp=datetime.now(),
        arrival_delta_s=0.5,
        client_ip="185.220.101.45",
        client_host="crawl.example.com",
        client_hash=482736190,
        method="GET",
        url_path="/filter/27|13",
        url_category="query",
        status_code=200,
        status_class="2xx",
        bytes_sent=1197,
        request_weight=2.0,
        is_bot=False,
    )
    assert req.is_error is False
    assert req.is_cacheable is False

