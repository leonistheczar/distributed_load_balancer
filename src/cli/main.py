from __future__ import annotations

import argparse
import heapq
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
import random

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console

from src.balancer import ALGORITHMS, get_algorithm
from src.ingestion.loader import DatasetLoader
from src.metrics.collector import MetricsCollector
from src.metrics.exporter import MetricsExporter
from src.metrics.host_runtime import HostRuntimeSampler
from src.nodes.node import Node
from src.nodes.pool import NodePool
from src.replayer.replayer import TrafficReplayer

try:
    # Module execution: python -m src.cli.main
    from .live_dashboard import LiveDashboard
except ImportError:
    # Script execution: python src/cli/main.py
    from live_dashboard import LiveDashboard


console = Console()


@dataclass(frozen=True, slots=True)
class NodeConfig:
    id: str
    weight: int
    latency_min_ms: float = 2.0
    latency_max_ms: float = 50.0


@dataclass(frozen=True, slots=True)
class CompletionEvent:
    complete_at_s: float
    node_id: str
    request: object
    total_latency_ms: float
    service_latency_ms: float
    queue_wait_ms: float
    was_failover: bool


DEFAULT_NODES: list[NodeConfig] = [
    NodeConfig("node-1", weight=1, latency_min_ms=2.0, latency_max_ms=55.0),
    NodeConfig("node-2", weight=2, latency_min_ms=2.0, latency_max_ms=45.0),
    NodeConfig("node-3", weight=1, latency_min_ms=3.0, latency_max_ms=60.0),
    NodeConfig("node-4", weight=1, latency_min_ms=3.0, latency_max_ms=60.0),
    NodeConfig("node-5", weight=1, latency_min_ms=3.0, latency_max_ms=60.0),
]


def _safe_total_rows(loader: DatasetLoader) -> int:
    """Best-effort total rows hint for dashboard progress."""
    for attr in ("total_rows", "row_count", "rows"):
        value = getattr(loader, attr, None)
        if isinstance(value, int) and value >= 0:
            return value
    return 0


def resolve_output_dir(
    output_arg: str,
    dataset_path: str,
    algos: list[str],
    sample: int,
    queue_capacity: int,
    exclude_bots: bool,
) -> Path:
    base = Path("results")
    output_arg = (output_arg or "").strip()
    dataset_name = Path(dataset_path).stem
    algo_label = "all" if len(algos) > 1 else algos[0]
    sample_label = "all" if sample <= 0 else str(sample)
    queue_label = "unbounded" if queue_capacity <= 0 else str(queue_capacity)
    bots_label = "no_bots" if exclude_bots else "with_bots"

    if output_arg in ("", "results", "."):
        # Classified default layout:
        # results/<dataset>/<algorithms>/<sample>/<queue>/<bots>/
        return (
            base
            / dataset_name
            / f"algorithms_{algo_label}"
            / f"sample_{sample_label}"
            / f"queue_{queue_label}"
            / f"bots_{bots_label}"
        )

    candidate = Path(output_arg)
    if candidate.is_absolute():
        return candidate

    lowered = output_arg.lower()
    if lowered.startswith("results_"):
        output_arg = output_arg[len("results_") :]
    if lowered.startswith("results\\") or lowered.startswith("results/"):
        return Path(output_arg)

    return base / output_arg


def build_node_pool(configs: list[NodeConfig]) -> NodePool:
    nodes = [
        Node(
            id=c.id,
            weight=c.weight,
            latency_min_ms=c.latency_min_ms,
            latency_max_ms=c.latency_max_ms,
        )
        for c in configs
    ]
    pool = NodePool(nodes)
    # Single failure injection (matches guide example).
    pool.schedule_failure("node-1", at_second=120.0, duration_s=30.0)
    return pool


def run_once(
    dataset_path: str,
    algorithm: str,
    speed: float,
    sample_size: int,
    exclude_bots: bool,
    seed: int,
    queue_capacity: int,
) -> dict:
    loader = DatasetLoader(dataset_path, sample_size=sample_size, exclude_bots=exclude_bots)
    replayer = TrafficReplayer(loader, replay_speed=speed)

    pool = build_node_pool(DEFAULT_NODES)
    algo = get_algorithm(algorithm)
    metrics = MetricsCollector(algorithm_name=algorithm)

    rng = random.Random(seed)
    started = time.perf_counter()
    host_sampler = HostRuntimeSampler(sample_interval_s=0.25)
    host_sampler.start()
    in_flight: list[tuple[float, int, CompletionEvent]] = []
    event_seq = 0
    dispatched_count = 0
    dropped_count = 0
    reroute_count = 0
    failover_count = 0
    cache_hits_count = 0

    dash = LiveDashboard(
        algorithm=algorithm,
        node_ids=[n.id for n in pool.nodes],
        queue_capacity=queue_capacity,
        total_requests=_safe_total_rows(loader),
    )

    def flush_completions(up_to_second: float) -> None:
        while in_flight and in_flight[0][0] <= up_to_second:
            _, _, event = heapq.heappop(in_flight)
            node = next((n for n in pool.nodes if n.id == event.node_id), None)
            if node is None:
                continue

            node.in_flight = max(0, node.in_flight - 1)
            node.record_request(bytes_sent=event.request.bytes_sent, is_error=event.request.is_error)
            algo.on_complete(node.id, event.total_latency_ms)
            metrics.record(
                request=event.request,
                node_id=node.id,
                latency_ms=event.total_latency_ms,
                sim_second=event.complete_at_s,
                was_failover=event.was_failover,
                queue_wait_ms=event.queue_wait_ms,
                service_latency_ms=event.service_latency_ms,
            )
            dash.record_completion(
                node_id=node.id,
                latency_ms=event.total_latency_ms,
                is_error=event.request.is_error,
            )
            dash.set_in_flight(node.id, node.in_flight)

    def has_queue_slot(candidate: Node) -> bool:
        return queue_capacity <= 0 or candidate.in_flight < queue_capacity

    def pick_reroute_node(selected: Node) -> Node | None:
        candidates = [n for n in pool.nodes if n.is_healthy and n.id != selected.id and has_queue_slot(n)]
        if not candidates:
            return None
        return min(candidates, key=lambda n: (n.in_flight, n.id))

    with dash:
        for req, sim_second in replayer.replay():
            host_sampler.maybe_sample()
            flush_completions(sim_second)
            pool.tick(sim_second)
            dispatched_count += 1

            for n in pool.nodes:
                dash.set_node_health(n.id, n.is_healthy)

            node = algo.select_node(req, pool.nodes)
            selected_node_id = node.id
            if not has_queue_slot(node):
                reroute = pick_reroute_node(node)
                if reroute is None:
                    metrics.record_drop(req, sim_second=sim_second)
                    dropped_count += 1
                    dash.tick(
                        sim_second=sim_second,
                        total_processed=dispatched_count,
                        drops=dropped_count,
                        reroutes=reroute_count,
                        failovers=failover_count,
                        cache_hits=cache_hits_count,
                        replay_index=dispatched_count,
                        request_method=req.method,
                        request_path=req.url_path,
                        request_timestamp=req.timestamp.isoformat(),
                        arrival_delta_s=req.arrival_delta_s,
                    )
                    continue
                node = reroute
                metrics.record_backpressure_reroute()
                reroute_count += 1

            was_failover = False
            if algorithm == "ip_hash":
                ideal = pool.nodes[int(req.client_hash) % len(pool.nodes)]
                was_failover = not ideal.is_healthy
            if selected_node_id != node.id:
                was_failover = True
            if was_failover:
                failover_count += 1

            req.assigned_node = node.id

            if req.is_cacheable and node.check_cache(req.url_path):
                metrics.record_cache_hit(node.id)
                cache_hits_count += 1

            service_latency_ms = node.process(req.request_weight, rng=rng)
            queue_wait_ms = max(0.0, (node.next_available_s - sim_second) * 1000.0)
            start_at_s = max(sim_second, node.next_available_s)
            complete_at_s = start_at_s + (service_latency_ms / 1000.0)
            node.next_available_s = complete_at_s
            node.in_flight += 1

            total_latency_ms = queue_wait_ms + service_latency_ms
            req.simulated_latency_ms = total_latency_ms

            event_seq += 1
            heapq.heappush(
                in_flight,
                (
                    complete_at_s,
                    event_seq,
                    CompletionEvent(
                        complete_at_s=complete_at_s,
                        node_id=node.id,
                        request=req,
                        total_latency_ms=total_latency_ms,
                        service_latency_ms=service_latency_ms,
                        queue_wait_ms=queue_wait_ms,
                        was_failover=was_failover,
                    ),
                ),
            )
            dash.tick(
                sim_second=sim_second,
                total_processed=dispatched_count,
                drops=dropped_count,
                reroutes=reroute_count,
                failovers=failover_count,
                cache_hits=cache_hits_count,
                replay_index=dispatched_count,
                request_method=req.method,
                request_path=req.url_path,
                request_timestamp=req.timestamp.isoformat(),
                arrival_delta_s=req.arrival_delta_s,
            )

        flush_completions(float("inf"))

    host_sampler.flush()
    summary = metrics.summary()
    elapsed = max(1e-9, time.perf_counter() - started)
    summary["host_runtime"] = host_sampler.as_summary_dict(wall_elapsed_s=elapsed)
    rps = summary["total_requests"] / elapsed if summary["total_requests"] else 0.0
    console.print(
        f"  Done: {summary['total_requests']:,} requests in {elapsed:.2f}s | "
        f"p99={summary['latency_p99_ms']:.1f}ms | imbalance={summary['load_imbalance_score']:.1f} | {rps:.0f} req/s"
    )
    hr = summary["host_runtime"]
    if hr.get("available") and hr.get("sample_count", 0):
        cpu = hr["process_cpu_pct"]
        rss = hr["process_rss_mb"]
        sysm = hr["system_memory_used_pct"]
        console.print(
            f"  Host: CPU {cpu['mean']:.1f}% (max {cpu['max']:.1f}%) | "
            f"process RSS {rss['mean']:.0f} MB (peak {rss['max']:.0f} MB) | "
            f"system RAM used {sysm['mean']:.1f}% (max {sysm['max']:.1f}%)"
        )
    elif hr.get("available") is False:
        console.print(f"  Host metrics: unavailable ({hr.get('reason', 'unknown')})")
    return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MVP Distributed Load Balancer Simulator")
    p.add_argument(
        "--dataset",
        default="dataset/archive/ecommerce_sample.csv",
        help="Path to cleaned CSV",
    )
    p.add_argument("--algorithm", default="all", help="rr, wrr, lc, ip_hash, or all")
    p.add_argument("--speed", type=float, default=0.0, help="Replay speed (0=max throughput, 1=real-time)")
    p.add_argument("--sample", type=int, default=0, help="Limit to first N requests (0=all)")
    p.add_argument("--exclude-bots", action="store_true", help="Exclude rows where is_bot=True")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    p.add_argument("--output", default="results", help="Output directory for reports and charts")
    p.add_argument(
        "--queue-capacity",
        type=int,
        default=0,
        help="Max in-flight requests per node before backpressure (0=unbounded)",
    )
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    dataset_path = args.dataset
    algo_arg = (args.algorithm or "all").strip().lower()
    algos = ALGORITHMS if algo_arg == "all" else [algo_arg]
    output_dir = resolve_output_dir(
        output_arg=args.output,
        dataset_path=dataset_path,
        algos=algos,
        sample=args.sample,
        queue_capacity=args.queue_capacity,
        exclude_bots=args.exclude_bots,
    )

    dataset_name = Path(dataset_path).stem

    console.print("=" * 60)
    console.print("  MVP Distributed Load Balancer Simulator")
    console.print("=" * 60)
    console.print(f"  Dataset    : {dataset_path}")
    console.print(f"  Algorithms : {algos}")
    console.print(f"  Speed      : {args.speed}x  (0 = max throughput)")
    console.print(f"  Sample     : {'all rows' if args.sample == 0 else args.sample}")
    console.print(f"  Bots       : {'excluded' if args.exclude_bots else 'included'}")
    console.print(f"  Queue cap  : {'unbounded' if args.queue_capacity <= 0 else args.queue_capacity}")
    console.print(f"  Seed       : {args.seed}")
    console.print("=" * 60)
    console.print("")

    results: list[dict] = []
    for a in algos:
        console.print(f"  Running: {a.upper():<20} | dataset: {dataset_name}")
        console.print("  " + "-" * 52)
        summary = run_once(
            dataset_path=dataset_path,
            algorithm=a,
            speed=args.speed,
            sample_size=args.sample,
            exclude_bots=args.exclude_bots,
            seed=args.seed,
            queue_capacity=args.queue_capacity,
        )
        results.append(summary)
        console.print("")

    exporter = MetricsExporter(output_dir=output_dir)
    paths = exporter.export(results=results, dataset_name=dataset_name)
    console.print(f"Saved report: {paths['json']}")
    console.print(f"Saved summary: {paths['txt']}")
    if "html_dir" in paths:
        console.print(f"Saved HTML: {paths['html_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

