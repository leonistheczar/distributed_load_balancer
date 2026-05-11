from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class MetricsExporter:
    output_dir: str | Path = "results"

    def export(self, results: list[dict[str, Any]], dataset_name: str) -> dict[str, Path]:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        run_dir = out_dir / "runs" / run_stamp
        # Extremely unlikely, but guard against same-tick collisions.
        while run_dir.exists():
            run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            run_dir = out_dir / "runs" / run_stamp
        reports_dir = run_dir / "reports"
        charts_dir = run_dir / "charts"
        html_dir = run_dir / "html"
        reports_dir.mkdir(parents=True, exist_ok=True)
        charts_dir.mkdir(parents=True, exist_ok=True)
        html_dir.mkdir(parents=True, exist_ok=True)

        safe_dataset = dataset_name.replace("/", "_").replace("\\", "_")

        json_path = reports_dir / f"benchmark_report_{safe_dataset}.json"
        txt_path = reports_dir / f"benchmark_summary_{safe_dataset}.txt"
        dist_png = charts_dir / f"load_distribution_{safe_dataset}.png"
        lat_png = charts_dir / f"latency_comparison_{safe_dataset}.png"
        imb_png = charts_dir / f"imbalance_score_{safe_dataset}.png"
        html_overview = html_dir / "overview.html"
        html_nodes = html_dir / "nodes.html"

        json_path.write_text(json.dumps({"dataset": dataset_name, "results": results}, indent=2), encoding="utf-8")
        txt_path.write_text(self._render_summary(results, dataset_name), encoding="utf-8")

        self._try_write_charts(results, dist_png, lat_png, imb_png)
        html_overview.write_text(self._render_overview_html(results, dataset_name), encoding="utf-8")
        html_nodes.write_text(self._render_nodes_html(results, dataset_name), encoding="utf-8")

        return {
            "json": json_path,
            "txt": txt_path,
            "load_distribution_png": dist_png,
            "latency_comparison_png": lat_png,
            "imbalance_score_png": imb_png,
            "run_dir": run_dir,
            "reports_dir": reports_dir,
            "charts_dir": charts_dir,
            "html_dir": html_dir,
            "html_overview": html_overview,
            "html_nodes": html_nodes,
        }

    def _render_summary(self, results: list[dict[str, Any]], dataset_name: str) -> str:
        rows = []
        for r in results:
            rows.append(
                (
                    r.get("algorithm", ""),
                    float(r.get("latency_p50_ms", 0.0)),
                    float(r.get("latency_p95_ms", 0.0)),
                    float(r.get("latency_p99_ms", 0.0)),
                    float(r.get("load_imbalance_score", 0.0)),
                    float(r.get("error_rate", 0.0)) * 100.0,
                    float(r.get("drop_rate", 0.0)) * 100.0,
                )
            )

        winner = min(rows, key=lambda t: t[3])[0] if rows else "n/a"
        best_balance = min(rows, key=lambda t: t[4])[0] if rows else "n/a"

        lines = []
        lines.append("=" * 65)
        lines.append("  LOAD BALANCER BENCHMARK REPORT")
        lines.append(f"  Dataset : {dataset_name}")
        lines.append("=" * 65)
        lines.append("  Algorithm              p50 ms   p95 ms   p99 ms  Imbalance   Errors   Drops")
        lines.append("  " + "─" * 61)
        for algo, p50, p95, p99, imb, err, drop in rows:
            tag = ""
            if algo == winner:
                tag = "  [BEST LATENCY]"
            if algo == best_balance:
                tag = "  [BEST BALANCE]" if not tag else tag + " + BALANCE"
            lines.append(f"  {algo:<20} {p50:7.1f} {p95:7.1f} {p99:7.1f} {imb:10.1f} {err:7.2f}% {drop:7.2f}%{tag}")
        lines.append("=" * 65)
        lines.append(f"  Winner (lowest p99 latency): {winner}")
        lines.append(f"  Best load balance:           {best_balance}")
        lines.append("=" * 65)
        lines.append("  Host runtime (this machine while each algorithm ran)")
        lines.append("  " + "─" * 61)
        lines.append(
            "  {:<20} {:>8} {:>9} {:>9} {:>11} {:>12} {:>12}".format(
                "Algorithm",
                "Wall s",
                "CPU avg",
                "CPU max",
                "RSS avg MB",
                "RSS peak MB",
                "Sys RAM %",
            )
        )
        lines.append("  " + "─" * 61)
        for r in results:
            algo = str(r.get("algorithm", ""))
            hr = r.get("host_runtime") or {}
            if not hr.get("available"):
                reason = str(hr.get("reason", "n/a"))
                lines.append(f"  {algo:<20}  unavailable: {reason}")
            elif not hr.get("sample_count"):
                lines.append(f"  {algo:<20}  (no host samples)")
            else:
                wall = float(hr.get("wall_time_s", 0.0))
                cpu = hr.get("process_cpu_pct") or {}
                rss = hr.get("process_rss_mb") or {}
                sysm = hr.get("system_memory_used_pct") or {}
                lines.append(
                    "  {:<20} {:>8.2f} {:>9.1f} {:>9.1f} {:>11.0f} {:>12.0f} {:>12.1f}".format(
                        algo,
                        wall,
                        float(cpu.get("mean", 0.0)),
                        float(cpu.get("max", 0.0)),
                        float(rss.get("mean", 0.0)),
                        float(rss.get("max", 0.0)),
                        float(sysm.get("mean", 0.0)),
                    )
                )
        lines.append("=" * 65)
        lines.append("")
        return "\n".join(lines)

    def _try_write_charts(self, results: list[dict[str, Any]], dist_png: Path, lat_png: Path, imb_png: Path) -> None:
        try:
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except Exception:
            return

        # Load distribution chart (per algo: node share)
        try:
            algos = [r["algorithm"] for r in results]
            node_ids = sorted({nid for r in results for nid in (r.get("nodes") or {}).keys()})
            if algos and node_ids:
                fig, ax = plt.subplots(figsize=(10, 5))
                width = 0.8 / max(1, len(algos))
                for i, r in enumerate(results):
                    total = float(r.get("total_requests", 0) or 0)
                    shares = []
                    for nid in node_ids:
                        nreq = float(((r.get("nodes") or {}).get(nid) or {}).get("requests", 0) or 0)
                        shares.append((nreq / total) * 100.0 if total else 0.0)
                    x = [j + i * width for j in range(len(node_ids))]
                    ax.bar(x, shares, width=width, label=r["algorithm"])
                ax.set_xticks([j + (len(algos) - 1) * width / 2 for j in range(len(node_ids))])
                ax.set_xticklabels(node_ids, rotation=0)
                ax.set_ylabel("Request share (%)")
                ax.set_title("Load distribution by node")
                ax.legend()
                fig.tight_layout()
                fig.savefig(dist_png)
                plt.close(fig)
        except Exception:
            pass

    def _render_overview_html(self, results: list[dict[str, Any]], dataset_name: str) -> str:
        winner = min(results, key=lambda r: float(r.get("latency_p99_ms", 0.0))) if results else {}
        best_balance = min(results, key=lambda r: float(r.get("load_imbalance_score", 0.0))) if results else {}

        max_p99 = max((float(r.get("latency_p99_ms", 0.0)) for r in results), default=1.0) or 1.0
        max_imb = max((float(r.get("load_imbalance_score", 0.0)) for r in results), default=1.0) or 1.0

        rows = []
        p99_bars = []
        imb_bars = []
        for r in results:
            algo = str(r.get("algorithm", "unknown"))
            p50 = float(r.get("latency_p50_ms", 0.0))
            p95 = float(r.get("latency_p95_ms", 0.0))
            p99 = float(r.get("latency_p99_ms", 0.0))
            imbalance = float(r.get("load_imbalance_score", 0.0))
            errors = float(r.get("error_rate", 0.0)) * 100.0
            drops = float(r.get("drop_rate", 0.0)) * 100.0
            total = int(r.get("total_requests", 0))
            dispatched = int(r.get("dispatched_requests", total))
            reroutes = int(r.get("backpressure_reroutes", 0))
            hr = r.get("host_runtime") or {}
            if hr.get("available") and hr.get("sample_count"):
                wall = float(hr.get("wall_time_s", 0.0))
                cpu_m = float((hr.get("process_cpu_pct") or {}).get("mean", 0.0))
                cpu_x = float((hr.get("process_cpu_pct") or {}).get("max", 0.0))
                rss_m = float((hr.get("process_rss_mb") or {}).get("mean", 0.0))
                rss_x = float((hr.get("process_rss_mb") or {}).get("max", 0.0))
                ram_m = float((hr.get("system_memory_used_pct") or {}).get("mean", 0.0))
                host_cells = (
                    f"<td>{wall:.2f}</td>"
                    f"<td>{cpu_m:.1f} / {cpu_x:.1f}</td>"
                    f"<td>{rss_m:.0f} / {rss_x:.0f}</td>"
                    f"<td>{ram_m:.1f}</td>"
                )
            elif hr.get("available") is False:
                reason = str(hr.get("reason", "n/a"))
                host_cells = f'<td class="muted">—</td><td class="muted" colspan="3">{reason}</td>'
            else:
                host_cells = '<td class="muted">—</td><td class="muted" colspan="3">—</td>'

            rows.append(
                f"""
                <tr>
                  <td>{algo}</td>
                  <td>{total:,}</td>
                  <td>{dispatched:,}</td>
                  <td>{p50:.1f}</td>
                  <td>{p95:.1f}</td>
                  <td>{p99:.1f}</td>
                  <td>{imbalance:.1f}</td>
                  <td>{errors:.2f}%</td>
                  <td>{drops:.2f}%</td>
                  <td>{reroutes:,}</td>
                  {host_cells}
                </tr>
                """
            )
            p99_width = (p99 / max_p99) * 100.0 if max_p99 > 0 else 0.0
            imb_width = (imbalance / max_imb) * 100.0 if max_imb > 0 else 0.0
            p99_bars.append(
                f'<div class="bar-row"><span>{algo}</span><div class="bar"><div style="width:{p99_width:.2f}%"></div></div><em>{p99:.1f} ms</em></div>'
            )
            imb_bars.append(
                f'<div class="bar-row"><span>{algo}</span><div class="bar"><div style="width:{imb_width:.2f}%"></div></div><em>{imbalance:.1f}</em></div>'
            )

        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Benchmark Overview</title>
  <style>
    :root {{ --bg:#0f0f10; --card:#17171a; --text:#e9e9ea; --muted:#a8a8ac; --line:#2b2b31; --accent:#e9e9ea; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,Segoe UI,Arial,sans-serif; background:var(--bg); color:var(--text); }}
    .wrap {{ max-width:1100px; margin:24px auto; padding:0 16px; }}
    .grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; margin:12px 0 20px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; }}
    .k {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .v {{ font-size:20px; margin-top:4px; }}
    h1,h2 {{ margin:0 0 8px; font-weight:600; }}
    h1 {{ font-size:24px; }}
    h2 {{ font-size:17px; }}
    p {{ color:var(--muted); margin:6px 0 0; }}
    table {{ width:100%; border-collapse:collapse; margin-top:10px; }}
    th,td {{ border-bottom:1px solid var(--line); padding:10px 8px; text-align:left; font-size:13px; }}
    th {{ color:var(--muted); font-weight:600; }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .bars {{ display:grid; gap:10px; }}
    .bar-row {{ display:grid; grid-template-columns:90px 1fr 80px; align-items:center; gap:8px; font-size:13px; }}
    .bar {{ height:8px; border:1px solid var(--line); border-radius:99px; overflow:hidden; background:#111; }}
    .bar > div {{ height:100%; background:var(--accent); }}
    a {{ color:var(--text); }}
    @media (max-width:900px) {{ .grid {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Load Balancer Benchmark</h1>
    <p>Dataset: {dataset_name} · Generated: {generated_at}</p>
    <div class="grid">
      <div class="card"><div class="k">Best p99 Latency</div><div class="v">{winner.get("algorithm", "n/a")}</div></div>
      <div class="card"><div class="k">Best Balance</div><div class="v">{best_balance.get("algorithm", "n/a")}</div></div>
      <div class="card"><div class="k">Algorithms</div><div class="v">{len(results)}</div></div>
    </div>
    <div class="card">
      <h2>Algorithm Comparison</h2>
      <table>
        <thead>
          <tr>
            <th>Algorithm</th><th>Completed</th><th>Dispatched</th><th>p50</th><th>p95</th><th>p99</th>
            <th>Imbalance</th><th>Errors</th><th>Drops</th><th>Reroutes</th>
            <th>Wall s</th><th>Host CPU %<br><span class="muted">mean / max</span></th>
            <th>Proc RSS MB<br><span class="muted">mean / peak</span></th>
            <th>Sys RAM %<br><span class="muted">mean</span></th>
          </tr>
        </thead>
        <tbody>
          {"".join(rows)}
        </tbody>
      </table>
    </div>
    <div class="grid">
      <div class="card">
        <h2>p99 Latency (ms)</h2>
        <div class="bars">{"".join(p99_bars)}</div>
      </div>
      <div class="card">
        <h2>Imbalance Score</h2>
        <div class="bars">{"".join(imb_bars)}</div>
      </div>
      <div class="card">
        <h2>Files</h2>
        <p>See <code>nodes.html</code> for per-node stats.</p>
      </div>
    </div>
  </div>
<footer
  style="
    background:#111827;
    color:#e5e7eb;
    padding:20px;
    text-align:center;
    border-top:1px solid #374151;
    font-family:Arial,sans-serif;
    font-size:14px;
    line-height:1.8;
    margin-top:20px;
  "
>
  <p style="margin:4px 0; font-weight:600; color:#f9fafb;">
    Developed by Muhammad Ali (56), Muhammad Abdullah (54),
    Muhammad Subhan Safdar (53), Muhammad Hanan (14)
  </p>

  <p style="margin:4px 0; color:#9ca3af;">
    BSCS 6th (M) • 2023-2027
  </p>

  <p style="margin:4px 0; color:#6b7280;">
    Parallel Distributed Computing • Emerson University Multan
  </p>
    <a href="https://github.com/leonistheczar/distributed_load_balancer">Github Link: https://github.com/leonistheczar/distributed_load_balancer</a>
</footer>
</body>
</html>
"""

    def _render_nodes_html(self, results: list[dict[str, Any]], dataset_name: str) -> str:
        sections = []
        for r in results:
            algo = str(r.get("algorithm", "unknown"))
            nodes = r.get("nodes", {}) or {}
            node_rows = []
            for node_id, ns in nodes.items():
                node_rows.append(
                    f"""
                    <tr>
                      <td>{node_id}</td>
                      <td>{int(ns.get("requests", 0)):,}</td>
                      <td>{int(ns.get("bytes_sent", 0)):,}</td>
                      <td>{float(ns.get("error_rate", 0.0))*100.0:.2f}%</td>
                      <td>{float(ns.get("cache_hit_rate", 0.0))*100.0:.2f}%</td>
                      <td>{float(ns.get("latency_p50_ms", 0.0)):.1f}</td>
                      <td>{float(ns.get("latency_p95_ms", 0.0)):.1f}</td>
                      <td>{float(ns.get("latency_p99_ms", 0.0)):.1f}</td>
                    </tr>
                    """
                )
            sections.append(
                f"""
                <section class="card">
                  <h2>{algo.upper()} Nodes</h2>
                  <table>
                    <thead>
                      <tr>
                        <th>Node</th><th>Requests</th><th>Bytes</th><th>Error Rate</th>
                        <th>Cache Hit</th><th>p50</th><th>p95</th><th>p99</th>
                      </tr>
                    </thead>
                    <tbody>
                      {"".join(node_rows)}
                    </tbody>
                  </table>
                </section>
                """
            )

        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Per-Node Metrics</title>
  <style>
    :root {{ --bg:#0f0f10; --card:#17171a; --text:#e9e9ea; --muted:#a8a8ac; --line:#2b2b31; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,Segoe UI,Arial,sans-serif; background:var(--bg); color:var(--text); }}
    .wrap {{ max-width:1100px; margin:24px auto; padding:0 16px 24px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; margin-bottom:12px; }}
    h1 {{ margin:0 0 6px; font-size:24px; font-weight:600; }}
    h2 {{ margin:0 0 10px; font-size:16px; font-weight:600; }}
    p {{ margin:0; color:var(--muted); }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ border-bottom:1px solid var(--line); padding:10px 8px; text-align:left; font-size:13px; }}
    th {{ color:var(--muted); font-weight:600; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Per-Node Metrics</h1>
      <p>Dataset: {dataset_name} · Generated: {generated_at}</p>
    </div>
    {"".join(sections)}
  </div>
  <footer
  style="
    background:#111827;
    color:#e5e7eb;
    padding:20px;
    text-align:center;
    border-top:1px solid #374151;
    font-family:Arial,sans-serif;
    font-size:14px;
    line-height:1.8;
    margin-top:20px;
  "
>
  <p style="margin:4px 0; font-weight:600; color:#f9fafb;">
    Developed by Muhammad Ali (56), Muhammad Abdullah (54),
    Muhammad Subhan Safdar (53), Muhammad Hanan (14)
  </p>

  <p style="margin:4px 0; color:#9ca3af;">
    BSCS 6th (M) • 2023-2027
  </p>

  <p style="margin:4px 0; color:#6b7280;">
    Parallel Distributed Computing • Emerson University Multan
  </p>
    <a href="https://github.com/leonistheczar/distributed_load_balancer">Github Link: https://github.com/leonistheczar/distributed_load_balancer</a>
</footer>
</body>
</html>
"""

        # Latency comparison (p50/p95/p99 per algo)
        try:
            labels = [r["algorithm"] for r in results]
            p50 = [float(r.get("latency_p50_ms", 0.0)) for r in results]
            p95 = [float(r.get("latency_p95_ms", 0.0)) for r in results]
            p99 = [float(r.get("latency_p99_ms", 0.0)) for r in results]
            x = list(range(len(labels)))
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.bar([i - 0.25 for i in x], p50, width=0.25, label="p50")
            ax.bar(x, p95, width=0.25, label="p95")
            ax.bar([i + 0.25 for i in x], p99, width=0.25, label="p99")
            ax.set_xticks(x)
            ax.set_xticklabels(labels)
            ax.set_ylabel("Latency (ms)")
            ax.set_title("Latency percentiles by algorithm")
            ax.legend()
            fig.tight_layout()
            fig.savefig(lat_png)
            plt.close(fig)
        except Exception:
            pass

        # Imbalance score
        try:
            labels = [r["algorithm"] for r in results]
            imbalance = [float(r.get("load_imbalance_score", 0.0)) for r in results]
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(labels, imbalance)
            ax.set_ylabel("Std dev of per-node request counts")
            ax.set_title("Load imbalance score (lower is better)")
            fig.tight_layout()
            fig.savefig(imb_png)
            plt.close(fig)
        except Exception:
            pass

