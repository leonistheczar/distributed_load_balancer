from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Iterator, Tuple

from src.ingestion.loader import DatasetLoader
from src.ingestion.models import Request


@dataclass(slots=True)
class TrafficReplayer:
    loader: DatasetLoader
    replay_speed: float = 0.0

    _dispatched: int = 0

    def replay(self) -> Iterator[Tuple[Request, float]]:
        """
        Yields (Request, sim_second).

        sim_second is simulated elapsed seconds derived from arrival deltas.
        """
        sim_second = 0.0
        for req in self.loader.stream():
            self._dispatched += 1
            delta = float(req.arrival_delta_s)
            if delta < 0:
                delta = 0.0

            if self.replay_speed and self.replay_speed > 0:
                time.sleep(delta / self.replay_speed)

            sim_second += delta
            yield req, sim_second

