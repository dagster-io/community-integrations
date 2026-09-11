import math
import time
from collections.abc import Callable

from dagster import Failure


def positive(value: float, name: str) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return value


class Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self.end = clock() + positive(seconds, "timeout")

    def remaining(self) -> float:
        remaining = self.end - self.clock()
        if remaining <= 0:
            raise TimeoutError("Nebius invocation deadline exceeded")
        return remaining


def failure(message: str, **metadata: str) -> Failure:
    # Never include an SDK exception/request repr: these may contain secrets.
    return Failure(message, metadata=metadata, allow_retries=False)
