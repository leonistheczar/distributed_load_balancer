from __future__ import annotations

import csv
from pathlib import Path

from src.cli.main import main


def test_simulation_runs_on_tiny_dataset(tmp_path: Path, monkeypatch):
    # Create a tiny cleaned dataset (schema-compatible).
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    dataset = data_dir / "ecommerce_sample.csv"

    rows = [
        {
            "timestamp": "2019-01-22T00:00:00+00:00",
            "arrival_delta_s": "0.0",
            "client_ip": "1.1.1.1",
            "client_host": "a",
            "client_hash": "123",
            "method": "GET",
            "url_path": "/index.html",
            "url_category": "html",
            "status_code": "200",
            "status_class": "2xx",
            "bytes_sent": "100",
            "request_weight": "1.0",
            "referrer": "-",
            "user_agent": "ua",
            "is_bot": "False",
        },
        {
            "timestamp": "2019-01-22T00:00:01+00:00",
            "arrival_delta_s": "1.0",
            "client_ip": "2.2.2.2",
            "client_host": "b",
            "client_hash": "456",
            "method": "GET",
            "url_path": "/img/logo.png",
            "url_category": "static",
            "status_code": "200",
            "status_class": "2xx",
            "bytes_sent": "200",
            "request_weight": "0.2",
            "referrer": "-",
            "user_agent": "ua",
            "is_bot": "False",
        },
    ]

    with dataset.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # Run from tmp_path as "project root"
    monkeypatch.chdir(tmp_path)
    # Make src importable from tmp_path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2]))

    rc = main(
        [
            "--dataset",
            str(dataset),
            "--algorithm",
            "rr",
            "--sample",
            "0",
            "--output",
            str(tmp_path / "results"),
        ]
    )
    assert rc == 0

