# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Shared test fakes for adapter / storage / sensor / e2e tests."""

from __future__ import annotations

from typing import Any, List

from openlineage.client.event_v2 import RunEvent, RunState
from openlineage.client.transport import Transport


class FakeOpenLineageTransport(Transport):
    """Captures events in memory rather than shipping them anywhere.

    Subclasses ``openlineage.client.transport.Transport`` so it can be
    plugged straight into an ``OpenLineageClient`` constructor argument.
    """

    kind = "fake"

    def __init__(self) -> None:
        self.events: List[Any] = []

    def emit(self, event: Any) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self.events.append(event)

    # Helpers -----------------------------------------------------------------

    def run_events_of_type(self, event_type: RunState) -> List[RunEvent]:
        return [
            e
            for e in self.events
            if isinstance(e, RunEvent) and e.eventType == event_type
        ]

    def dedup_by(
        self, *, run_id: str, event_type: RunState, event_time: str
    ) -> List[RunEvent]:
        seen: set[tuple[str, RunState, str]] = set()
        out: List[RunEvent] = []
        for e in self.events:
            if not isinstance(e, RunEvent) or e.eventType is None:
                continue
            key = (e.run.runId, e.eventType, e.eventTime)
            if key in seen:
                continue
            seen.add(key)
            if (
                e.run.runId == run_id
                and e.eventType == event_type
                and e.eventTime == event_time
            ):
                out.append(e)
        return out

    def reset(self) -> None:
        self.events.clear()
