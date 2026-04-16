from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .models import Request


def _status_class(status_code: int) -> str:
    if status_code < 100:
        return "unknown"
    return f"{status_code // 100}xx"


def _parse_timestamp(value: str) -> datetime:
    # Expected output of pipeline: ISO-ish strings parseable by pandas.
    # We keep parsing here small/fast; pipeline should normalize timezones.
    return pd.to_datetime(value, utc=True).to_pydatetime()


@dataclass(slots=True)
class DatasetLoader:
    csv_path: str | Path
    sample_size: int = 0
    exclude_bots: bool = False
    chunksize: int = 200_000
    encoding: str = "utf-8"

    def stream(self) -> Iterable[Request]:
        path = Path(self.csv_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Dataset not found: {path}. "
                "Expected a cleaned CSV like data/ecommerce_sample.csv. "
                "Run: python pipeline/ecommerce_clean.py"
            )

        remaining = self.sample_size if self.sample_size and self.sample_size > 0 else None

        # Some CSVs from this dataset can be latin-1; caller can override.
        reader = pd.read_csv(
            path,
            chunksize=self.chunksize,
            encoding=self.encoding,
            low_memory=False,
        )

        for chunk in reader:
            if remaining is not None and remaining <= 0:
                break

            # Normalize/guard common schema issues.
            if "timestamp" not in chunk.columns:
                raise KeyError(
                    "Missing required column 'timestamp'. "
                    f"Found columns: {chunk.columns.tolist()}"
                )

            if self.exclude_bots and "is_bot" in chunk.columns:
                chunk = chunk[~chunk["is_bot"].astype(bool)]

            if remaining is not None and len(chunk) > remaining:
                chunk = chunk.iloc[:remaining]

            # Fast iteration (avoid iterrows).
            for row in chunk.itertuples(index=False):
                req = self._row_to_request(row)
                if req is not None:
                    yield req

            if remaining is not None:
                remaining -= len(chunk)

    def _row_to_request(self, row) -> Request | None:
        # Access via attribute names (from itertuples).
        try:
            timestamp = _parse_timestamp(getattr(row, "timestamp"))
            arrival_delta_s = float(getattr(row, "arrival_delta_s"))
            client_ip = str(getattr(row, "client_ip"))
            client_host = str(getattr(row, "client_host"))
            client_hash = int(getattr(row, "client_hash"))
            method = str(getattr(row, "method"))
            url_path = str(getattr(row, "url_path"))
            url_category = str(getattr(row, "url_category"))
            status_code = int(getattr(row, "status_code"))
            status_class = str(getattr(row, "status_class")) if hasattr(row, "status_class") else _status_class(status_code)
            bytes_sent = int(getattr(row, "bytes_sent"))
            request_weight = float(getattr(row, "request_weight"))
            referrer = str(getattr(row, "referrer")) if hasattr(row, "referrer") else None
            user_agent = str(getattr(row, "user_agent")) if hasattr(row, "user_agent") else None
            is_bot = bool(getattr(row, "is_bot")) if hasattr(row, "is_bot") else False
        except Exception:
            return None

        return Request(
            timestamp=timestamp,
            arrival_delta_s=arrival_delta_s,
            client_ip=client_ip,
            client_host=client_host,
            client_hash=client_hash,
            method=method,
            url_path=url_path,
            url_category=url_category,
            status_code=status_code,
            status_class=status_class,
            bytes_sent=bytes_sent,
            request_weight=request_weight,
            referrer=referrer,
            user_agent=user_agent,
            is_bot=is_bot,
        )

