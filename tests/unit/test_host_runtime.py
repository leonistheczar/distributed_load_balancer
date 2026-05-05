from __future__ import annotations

import time

import pytest

from src.metrics.host_runtime import HostRuntimeSampler


def test_host_sampler_requires_psutil(monkeypatch):
    monkeypatch.setattr("src.metrics.host_runtime.psutil", None)
    s = HostRuntimeSampler()
    assert s.is_available() is False
    s.start()
    s.maybe_sample()
    s.flush()
    d = s.as_summary_dict(wall_elapsed_s=1.0)
    assert d["available"] is False


@pytest.mark.skipif(not HostRuntimeSampler.is_available(), reason="psutil not installed")
def test_host_sampler_collects_aggregates():
    s = HostRuntimeSampler(sample_interval_s=0.05)
    s.start()
    deadline = time.perf_counter() + 0.35
    while time.perf_counter() < deadline:
        s.maybe_sample()
        time.sleep(0.02)
    s.flush()
    d = s.as_summary_dict(wall_elapsed_s=0.4)
    assert d["available"] is True
    assert d["sample_count"] >= 2
    assert "mean" in d["process_cpu_pct"]
    assert d["process_rss_mb"]["max"] >= d["process_rss_mb"]["min"]
