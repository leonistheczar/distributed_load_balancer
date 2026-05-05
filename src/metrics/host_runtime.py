from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

try:
    import psutil
except ImportError:  # pragma: no cover - exercised when dependency missing
    psutil = None


@dataclass(slots=True)
class HostRuntimeSampler:
    """Samples this process and system memory while a workload runs."""

    sample_interval_s: float = 0.25
    _process: Any = None
    _samples: list[dict[str, float]] = field(default_factory=list)
    _last_mono: float = 0.0
    _wall_start: float = 0.0

    @staticmethod
    def is_available() -> bool:
        return psutil is not None

    def start(self) -> None:
        if psutil is None:
            return
        self._process = psutil.Process()
        self._process.cpu_percent(interval=None)
        self._samples.clear()
        self._wall_start = time.perf_counter()
        self._last_mono = self._wall_start
        self._record()

    def maybe_sample(self) -> None:
        if psutil is None or self._process is None:
            return
        now = time.perf_counter()
        if now - self._last_mono >= self.sample_interval_s:
            self._record()

    def flush(self) -> None:
        if psutil is None or self._process is None:
            return
        self._record()

    def _record(self) -> None:
        self._last_mono = time.perf_counter()
        vm = psutil.virtual_memory()
        rss = float(self._process.memory_info().rss)
        self._samples.append(
            {
                "process_cpu_pct": float(self._process.cpu_percent(interval=None)),
                "process_rss_mb": rss / (1024.0 * 1024.0),
                "process_mem_pct": float(self._process.memory_percent()),
                "system_mem_used_pct": float(vm.percent),
                "system_mem_avail_mb": float(vm.available) / (1024.0 * 1024.0),
            }
        )

    def as_summary_dict(self, wall_elapsed_s: float) -> dict[str, Any]:
        if psutil is None:
            return {"available": False, "reason": "psutil not installed"}
        if not self._samples:
            return {
                "available": True,
                "wall_time_s": round(wall_elapsed_s, 4),
                "sample_count": 0,
                "note": "no host samples collected",
            }

        def agg(key: str) -> dict[str, float]:
            vals = [s[key] for s in self._samples]
            return {
                "mean": round(sum(vals) / len(vals), 4),
                "min": round(min(vals), 4),
                "max": round(max(vals), 4),
            }

        return {
            "available": True,
            "wall_time_s": round(wall_elapsed_s, 4),
            "sample_count": len(self._samples),
            "process_cpu_pct": agg("process_cpu_pct"),
            "process_rss_mb": agg("process_rss_mb"),
            "process_memory_pct": agg("process_mem_pct"),
            "system_memory_used_pct": agg("system_mem_used_pct"),
            "system_memory_available_mb": agg("system_mem_avail_mb"),
        }
