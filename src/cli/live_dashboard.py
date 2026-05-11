# ==========================================
# Project: Parallel Distributed Computing
# Institution: Emerson University Multan
# Class: BSCS 6th (M) (2023-2027)

# Team Members:
# - Muhammad Ali (56)
# - Muhammad Abdullah (54)
# - Muhammad Subhan Safdar (53)
# - Muhammad Hanan (14)
# ==========================================
# Github Link: https://github.com/leonistheczar/distributed_load_balancer


from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def _supports_utf(console: Console) -> bool:
    encoding = getattr(getattr(console, "file", None), "encoding", "") or ""
    return "utf" in encoding.lower()


def _spark(values: list[float], *, utf: bool) -> str:
    if not values:
        return ""
    chars = " ▁▂▃▄▅▆▇█" if utf else " .:-=+*#@"
    hi = max(values) or 1.0
    return "".join(chars[min(int(v / hi * (len(chars) - 1)), len(chars) - 1)] for v in values)


def _bar(ratio: float, *, width: int, fill: str, empty: str) -> Text:
    ratio = max(0.0, min(1.0, ratio))
    filled = round(ratio * width)
    color = "green" if ratio < 0.6 else "yellow" if ratio < 0.85 else "red"
    t = Text()
    t.append(fill * filled, style=color)
    t.append(empty * (width - filled), style="dim")
    return t


def _fmt_s(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    mins, sec = divmod(int(seconds), 60)
    return f"{mins:02d}:{sec:02d}"


@dataclass
class NodeLiveState:
    node_id: str
    healthy: bool = True
    in_flight: int = 0
    total_handled: int = 0
    errors: int = 0
    latency_sum_ms: float = 0.0
    latency_samples: int = 0
    recent_latencies: deque[float] = field(default_factory=lambda: deque(maxlen=200))

    @property
    def avg_latency_ms(self) -> float:
        return self.latency_sum_ms / self.latency_samples if self.latency_samples else 0.0

    @property
    def p95_approx_ms(self) -> float:
        if not self.recent_latencies:
            return 0.0
        arr = sorted(self.recent_latencies)
        idx = max(0, int(len(arr) * 0.95) - 1)
        return arr[idx]

    def record(self, latency_ms: float, is_error: bool) -> None:
        self.total_handled += 1
        self.latency_sum_ms += latency_ms
        self.latency_samples += 1
        self.recent_latencies.append(latency_ms)
        if is_error:
            self.errors += 1


class LiveDashboard:
    REFRESH_HZ = 8
    NARROW_WIDTH_BREAKPOINT = 160

    def __init__(
        self,
        algorithm: str,
        node_ids: list[str],
        queue_capacity: int = 0,
        total_requests: int = 0,
    ) -> None:
        self.algorithm = algorithm.upper()
        self.queue_capacity = queue_capacity
        self.total_requests = total_requests
        self.nodes: dict[str, NodeLiveState] = {nid: NodeLiveState(node_id=nid) for nid in node_ids}

        self._started_at = time.perf_counter()
        self._sim_second = 0.0
        self._total_processed = 0
        self._drops = 0
        self._reroutes = 0
        self._failovers = 0
        self._cache_hits = 0
        self._replay_index = 0
        self._last_request_method = "-"
        self._last_request_path = "-"
        self._last_request_timestamp = "-"
        self._last_arrival_delta_s = 0.0
        self._rps_history: deque[float] = deque(maxlen=30)
        self._last_rps_sample_t = time.perf_counter()
        self._last_rps_sample_n = 0

        self._console = Console()
        self._utf = _supports_utf(self._console)
        self._bar_fill = "━" if self._utf else "#"
        self._bar_empty = "─" if self._utf else "-"
        self._healthy_marker = "●" if self._utf else "OK"
        self._failed_marker = "✖" if self._utf else "X"

        self._live = Live(
            self._render(),
            console=self._console,
            refresh_per_second=self.REFRESH_HZ,
            transient=False,
        )

    def __enter__(self) -> "LiveDashboard":
        self._live.__enter__()
        return self

    def __exit__(self, *args) -> None:
        self._live.update(self._render())
        self._live.__exit__(*args)

    def tick(
        self,
        sim_second: float,
        total_processed: int,
        drops: int = 0,
        reroutes: int = 0,
        failovers: int = 0,
        cache_hits: int = 0,
        replay_index: int = 0,
        request_method: str = "-",
        request_path: str = "-",
        request_timestamp: str = "-",
        arrival_delta_s: float = 0.0,
    ) -> None:
        self._sim_second = sim_second
        self._total_processed = total_processed
        self._drops = drops
        self._reroutes = reroutes
        self._failovers = failovers
        self._cache_hits = cache_hits
        self._replay_index = replay_index
        self._last_request_method = request_method
        self._last_request_path = request_path
        self._last_request_timestamp = request_timestamp
        self._last_arrival_delta_s = arrival_delta_s
        self._maybe_sample_rps()
        self._live.update(self._render())

    def record_completion(self, node_id: str, latency_ms: float, is_error: bool = False) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].record(latency_ms, is_error)

    def set_node_health(self, node_id: str, healthy: bool) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].healthy = healthy

    def set_in_flight(self, node_id: str, count: int) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].in_flight = count

    def _elapsed(self) -> float:
        return time.perf_counter() - self._started_at

    def _current_rps(self) -> float:
        elapsed = self._elapsed()
        return self._total_processed / elapsed if elapsed > 0 else 0.0

    def _progress_ratio(self) -> float:
        if self.total_requests <= 0:
            return 0.0
        return min(1.0, self._total_processed / self.total_requests)

    def _healthy_nodes(self) -> int:
        return sum(1 for n in self.nodes.values() if n.healthy)

    def _total_in_flight(self) -> int:
        return sum(n.in_flight for n in self.nodes.values())

    def _eta_seconds(self) -> float | None:
        if self.total_requests <= 0 or self._total_processed <= 0:
            return None
        rps = self._current_rps()
        if rps <= 1e-9:
            return None
        return max(0.0, (self.total_requests - self._total_processed) / rps)

    def _node_snapshot(self) -> tuple[int, int, str, str]:
        healthy = self._healthy_nodes()
        failed = len(self.nodes) - healthy
        if not self.nodes:
            return healthy, failed, "-", "-"
        busiest = max(self.nodes.values(), key=lambda n: (n.in_flight, n.total_handled, n.node_id))
        slowest = max(self.nodes.values(), key=lambda n: (n.p95_approx_ms, n.avg_latency_ms, n.node_id))
        return healthy, failed, busiest.node_id, slowest.node_id

    def _maybe_sample_rps(self) -> None:
        now = time.perf_counter()
        if now - self._last_rps_sample_t >= 0.5:
            delta_n = self._total_processed - self._last_rps_sample_n
            delta_t = max(1e-9, now - self._last_rps_sample_t)
            self._rps_history.append(delta_n / delta_t)
            self._last_rps_sample_t = now
            self._last_rps_sample_n = self._total_processed

    def _overview_panel(self) -> Panel:
        progress = self._progress_ratio()
        bar = _bar(progress, width=22, fill=self._bar_fill, empty=self._bar_empty)
        eta = self._eta_seconds()
        healthy, failed, busiest_node, slowest_node = self._node_snapshot()
        inflight = self._total_in_flight()

        t = Text()
        t.append(" Algorithm ", style="dim")
        t.append(f"{self.algorithm}", style="bold cyan")
        t.append("   Elapsed ", style="dim")
        t.append(_fmt_s(self._elapsed()), style="white")
        t.append("   Sim Time ", style="dim")
        t.append(f"{self._sim_second:,.1f}s", style="white")
        t.append("   Nodes ", style="dim")
        t.append(f"{healthy}/{len(self.nodes)}", style="white")
        t.append("   Failed ", style="dim")
        t.append(f"{failed}", style="red" if failed else "white")
        t.append("   InFlight ", style="dim")
        t.append(f"{inflight}", style="white")
        t.append("   Busiest ", style="dim")
        t.append(f"{busiest_node}", style="white")
        t.append("   Slowest(p95) ", style="dim")
        t.append(f"{slowest_node}", style="white")
        t.append("   Progress ", style="dim")
        t.append(bar)
        t.append(f" {progress * 100:5.1f}%", style="white")
        if self.total_requests > 0:
            t.append(f" ({self._total_processed:,}/{self.total_requests:,})", style="dim")
        if eta is not None:
            t.append("   ETA ", style="dim")
            t.append(_fmt_s(eta), style="white")
        return Panel(t, title="[bold]Simulation Overview[/bold]", border_style="bright_black", padding=(0, 1))

    def _kpi_panel(self) -> Panel:
        rps = self._current_rps()
        spark = _spark(list(self._rps_history), utf=self._utf)
        remaining = max(0, self.total_requests - self._total_processed) if self.total_requests > 0 else 0
        healthy, failed, busiest_node, slowest_node = self._node_snapshot()

        table = Table.grid(padding=(0, 2))
        table.add_column(justify="right", style="dim", min_width=18)
        table.add_column(justify="left")
        table.add_row("Throughput (req/s)", f"[bold white]{rps:,.0f}[/] [dim]{spark}[/]")
        table.add_row("Requests dispatched", f"[white]{self._total_processed:,}[/]")
        if self.total_requests > 0:
            table.add_row("Requests remaining", f"[white]{remaining:,}[/]")
        table.add_row("In-flight requests", f"[white]{self._total_in_flight():,}[/]")
        table.add_row("Dropped", f"[{'red' if self._drops else 'dim'}]{self._drops:,}[/]")
        table.add_row("Reroutes", f"[{'yellow' if self._reroutes else 'dim'}]{self._reroutes:,}[/]")
        table.add_row("Failovers", f"[{'yellow' if self._failovers else 'dim'}]{self._failovers:,}[/]")
        table.add_row("Cache hits", f"[{'green' if self._cache_hits else 'dim'}]{self._cache_hits:,}[/]")
        table.add_row("Nodes healthy/failed", f"[white]{healthy}[/] / [{'red' if failed else 'dim'}]{failed}[/]")
        table.add_row("Busiest node", f"[white]{busiest_node}[/]")
        table.add_row("Slowest node (p95)", f"[white]{slowest_node}[/]")
        return Panel(table, title="[bold]Traffic & Reliability[/bold]", border_style="bright_black", padding=(0, 1))

    def _replay_panel(self) -> Panel:
        table = Table.grid(padding=(0, 2))
        table.add_column(justify="right", style="dim", min_width=16)
        table.add_column(justify="left")
        table.add_row("Replay source", "[white]dataset stream[/]")
        table.add_row("Dataset cursor", f"[white]#{self._replay_index:,}[/]")
        table.add_row("Dataset time", f"[white]{self._sim_second:,.1f}s[/]")
        table.add_row("Last row time", f"[white]{self._last_request_timestamp}[/]")
        table.add_row("Last request", f"[white]{self._last_request_method} {self._last_request_path}[/]")
        table.add_row("Arrival delta", f"[white]{self._last_arrival_delta_s:.4f}s[/]")
        return Panel(table, title="[bold]Replay Evidence[/bold]", border_style="bright_black", padding=(0, 1))

    def _nodes_panel(self) -> Panel:
        narrow = self._console.width < self.NARROW_WIDTH_BREAKPOINT
        table = Table(show_header=True, header_style="dim", show_edge=False, pad_edge=False, box=None, expand=True)
        table.add_column("Node", style="bold", min_width=8, no_wrap=True)
        table.add_column("Health", min_width=8, no_wrap=True)
        table.add_column("InFlight", justify="right", min_width=8, no_wrap=True)
        table.add_column("Handled", justify="right", min_width=8, no_wrap=True)
        table.add_column("Progress", justify="right", min_width=8, no_wrap=True)
        table.add_column("Errors", justify="right", min_width=7, no_wrap=True)
        table.add_column("Avg ms", justify="right", min_width=7, no_wrap=True)
        table.add_column("p95 ms", justify="right", min_width=7, no_wrap=True)
        if not narrow:
            table.add_column("Load", min_width=12, no_wrap=True)

        cap = self.queue_capacity if self.queue_capacity > 0 else 50
        for node_id, node in self.nodes.items():
            health = (
                Text(f"{self._healthy_marker} up", style="green")
                if node.healthy
                else Text(f"{self._failed_marker} down", style="red bold")
            )
            load_ratio = node.in_flight / cap
            node_share = (node.total_handled / self._total_processed) if self._total_processed else 0.0
            in_flight_style = "red" if node.in_flight >= cap * 0.9 else "yellow" if node.in_flight >= cap * 0.6 else "white"
            error_style = "red" if node.errors > 0 else "dim"

            row = [
                node_id,
                health,
                f"[{in_flight_style}]{node.in_flight:>5}[/]",
                f"{node.total_handled:>8,}",
                f"{node_share * 100:>6.1f}%",
                f"[{error_style}]{node.errors:>6,}[/]",
                f"{node.avg_latency_ms:>7.1f}",
                f"{node.p95_approx_ms:>7.1f}",
            ]
            if not narrow:
                row.append(_bar(load_ratio, width=10, fill=self._bar_fill, empty=self._bar_empty))
            table.add_row(*row)

        return Panel(table, title="[bold]Node Execution Matrix[/bold]", border_style="bright_black", padding=(0, 1))

    def _render(self) -> Layout:
        narrow = self._console.width < self.NARROW_WIDTH_BREAKPOINT
        layout = Layout()
        layout.split_column(
            Layout(self._overview_panel(), name="overview", size=3),
            Layout(name="body"),
        )
        if narrow:
            layout["body"].split_column(
                Layout(self._kpi_panel(), name="kpi", size=9),
                Layout(self._replay_panel(), name="replay", size=8),
                Layout(self._nodes_panel(), name="nodes"),
            )
        else:
            layout["body"].split_row(
                Layout(name="left", ratio=3),
                Layout(self._nodes_panel(), name="nodes", ratio=5),
            )
            layout["left"].split_column(
                Layout(self._kpi_panel(), name="kpi"),
                Layout(self._replay_panel(), name="replay"),
            )
        return layout
