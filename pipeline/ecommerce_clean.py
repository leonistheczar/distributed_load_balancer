from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator


LOG_LINE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<req>[^"]*)" (?P<status>\d{3}) (?P<bytes>\S+) "(?P<ref>[^"]*)" "(?P<ua>[^"]*)"$'
)


def stable_client_hash(value: str) -> int:
    # Stable across runs (unlike Python's built-in hash()).
    h = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()
    return int(h[:12], 16) % 1_000_000_000


def parse_apache_ts(value: str) -> datetime:
    # Example: 22/Jan/2019:03:09:13 +0330
    dt = datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z")
    return dt.astimezone(timezone.utc)


def classify_url(url_path: str) -> tuple[str, float]:
    p = (url_path or "").lower()
    if "?" in p or "/filter/" in p or "filter" in p:
        return "query", 2.0
    if p.endswith((".gz", ".zip", ".mp4", ".mp3", ".pdf")):
        return "download", 3.0
    if p.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".css", ".js", ".svg", ".ico")):
        return "static", 0.2
    return "html", 1.0


def is_bot_ua(user_agent: str) -> bool:
    ua = (user_agent or "").lower()
    return any(k in ua for k in ("bot", "crawl", "spider", "slurp", "bingpreview", "facebookexternalhit"))


def iter_log_rows(log_path: Path) -> Iterator[dict]:
    with log_path.open("r", encoding="latin-1", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            m = LOG_LINE.match(line)
            if not m:
                continue
            gd = m.groupdict()

            req = gd["req"]
            method = ""
            url_path = ""
            if req and req != "-":
                parts = req.split()
                if len(parts) >= 2:
                    method = parts[0]
                    url_path = parts[1]

            bytes_sent_raw = gd["bytes"]
            try:
                bytes_sent = int(bytes_sent_raw) if bytes_sent_raw != "-" else 0
            except Exception:
                bytes_sent = 0

            status_code = int(gd["status"])
            status_class = f"{status_code // 100}xx"

            ts = parse_apache_ts(gd["ts"])
            yield {
                "timestamp": ts,
                "client_ip": gd["ip"],
                "method": method,
                "url_path": url_path,
                "status_code": status_code,
                "status_class": status_class,
                "bytes_sent": bytes_sent,
                "referrer": gd["ref"],
                "user_agent": gd["ua"],
            }


def load_hostnames(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    mapping: dict[str, str] = {}
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ip = (row.get("ip") or row.get("client_ip") or "").strip()
            host = (row.get("hostname") or row.get("client_host") or "").strip()
            if ip and host:
                mapping[ip] = host
    return mapping


def write_cleaned(
    log_path: Path,
    hostnames_path: Path,
    out_cleaned: Path,
    out_sample: Path,
    sample_rows: int = 100_000,
    arrival_cap_s: float = 60.0,
) -> None:
    host_map = load_hostnames(hostnames_path)

    out_cleaned.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "arrival_delta_s",
        "client_ip",
        "client_host",
        "client_hash",
        "method",
        "url_path",
        "url_category",
        "status_code",
        "status_class",
        "bytes_sent",
        "request_weight",
        "referrer",
        "user_agent",
        "is_bot",
    ]

    last_ts: datetime | None = None
    sample_written = 0

    with out_cleaned.open("w", encoding="utf-8", newline="") as f_all, out_sample.open(
        "w", encoding="utf-8", newline=""
    ) as f_sample:
        w_all = csv.DictWriter(f_all, fieldnames=fieldnames)
        w_s = csv.DictWriter(f_sample, fieldnames=fieldnames)
        w_all.writeheader()
        w_s.writeheader()

        for r in iter_log_rows(log_path):
            ts = r["timestamp"]
            if last_ts is None:
                delta = 0.0
            else:
                delta = (ts - last_ts).total_seconds()
                if delta < 0:
                    delta = 0.0
                if delta > arrival_cap_s:
                    delta = arrival_cap_s
            last_ts = ts

            client_ip = r["client_ip"]
            client_host = host_map.get(client_ip, client_ip)
            client_hash = stable_client_hash(client_host)

            url_category, request_weight = classify_url(r["url_path"])
            bot = is_bot_ua(r["user_agent"])

            row = {
                "timestamp": ts.isoformat(),
                "arrival_delta_s": f"{delta:.3f}",
                "client_ip": client_ip,
                "client_host": client_host,
                "client_hash": client_hash,
                "method": r["method"],
                "url_path": r["url_path"],
                "url_category": url_category,
                "status_code": r["status_code"],
                "status_class": r["status_class"],
                "bytes_sent": r["bytes_sent"],
                "request_weight": request_weight,
                "referrer": r["referrer"],
                "user_agent": r["user_agent"],
                "is_bot": bot,
            }

            w_all.writerow(row)
            if sample_written < sample_rows:
                w_s.writerow(row)
                sample_written += 1


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    # Support both layouts:
    # - Build guide layout: data/raw/{access.log,client_hostname.csv}
    # - This repo layout:    dataset/archive/{access.log,client_hostname.csv}
    dataset_archive = root / "dataset" / "archive"
    data_raw = root / "data" / "raw"

    log_path = dataset_archive / "access.log"
    host_path = dataset_archive / "client_hostname.csv"
    if not log_path.exists():
        log_path = data_raw / "access.log"
    if not host_path.exists():
        host_path = data_raw / "client_hostname.csv"

    # Prefer writing next to the dataset if dataset/archive exists,
    # otherwise fall back to the build guide's data/ directory.
    out_dir = dataset_archive if dataset_archive.exists() else (root / "data")
    out_cleaned = out_dir / "ecommerce_cleaned.csv"
    out_sample = out_dir / "ecommerce_sample.csv"

    if not log_path.exists():
        raise FileNotFoundError(
            "Missing access.log. Place it in either "
            f"{dataset_archive} or {data_raw}."
        )

    write_cleaned(
        log_path=log_path,
        hostnames_path=host_path,
        out_cleaned=out_cleaned,
        out_sample=out_sample,
    )

    print(f"Wrote: {out_cleaned}")
    print(f"Wrote: {out_sample}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

