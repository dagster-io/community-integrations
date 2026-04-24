# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Bounded synchronous emitter for OpenLineage events.

Synchronous, short timeout, retries disabled, per-error-class rate-limited
logging. No background thread, no queue, no circuit breaker. Rationale:
Dagster run-worker / step-worker / Pipes subprocesses exit as soon as user
code completes, so background flush semantics cannot be relied on in K8s
ephemeral pods. Operators needing async/retries configure those via
OpenLineage's own ``openlineage.yml`` / ``OPENLINEAGE_CONFIG``.
"""

import logging
import os
import time
from typing import Any, Dict, Optional, Union

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import DatasetEvent, JobEvent, RunEvent
from openlineage.client.transport.http import HttpConfig, HttpTransport

log = logging.getLogger("dagster_openlineage.emit")

DEFAULT_TIMEOUT_SECONDS = 2.0

# Explicit zero-retry dict. HttpConfig.retry default is
# {'total': 5, 'read': 5, 'connect': 5, 'backoff_factor': 0.3, ...}, which
# multiplies the 2s timeout ~7.5x in the worst case. We pass every field the
# default sets because HttpConfig.from_dict merges user retry over defaults.
_NO_RETRY_CONFIG: Dict[str, Any] = {
    "total": 0,
    "connect": 0,
    "read": 0,
    "backoff_factor": 0,
    "status_forcelist": [],
    "allowed_methods": [],
}

_RATE_LIMIT_WINDOW_SECONDS = 60.0


OLEvent = Union[RunEvent, DatasetEvent, JobEvent]


class OpenLineageEmitter:
    """Wrap an ``OpenLineageClient`` with a bounded, fail-soft ``emit``.

    The emitter:
      * Calls ``client.emit(event)`` synchronously.
      * Swallows ``Exception`` with a rate-limited ERROR log per error class.
      * Re-raises ``BaseException`` (shutdown signals, ``SystemExit``,
        ``KeyboardInterrupt``, ``MemoryError``) so Dagster's shutdown contract
        is preserved.

    Construction either accepts an injected client (tests) or builds a default
    ``OpenLineageClient`` configured with an ``HttpTransport`` whose retries are
    zeroed. Users wanting different transport behavior should construct their
    own ``OpenLineageClient`` via ``openlineage.yml`` / ``OPENLINEAGE_CONFIG``
    and pass it in.
    """

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        client: Optional[OpenLineageClient] = None,
    ) -> None:
        self.timeout = timeout
        self._client = (
            client if client is not None else self._build_default_client(timeout)
        )
        self._last_logged_at: Dict[type, float] = {}

    @staticmethod
    def _build_default_client(timeout: float) -> OpenLineageClient:
        url = os.getenv("OPENLINEAGE_URL")
        if not url:
            # No URL configured — hand back a default client whose transport is
            # whatever OL picks (console/noop). The user cannot expect delivery.
            return OpenLineageClient()
        config = HttpConfig(url=url, timeout=timeout, retry=dict(_NO_RETRY_CONFIG))
        return OpenLineageClient(transport=HttpTransport(config))

    def emit(self, event: OLEvent) -> None:
        try:
            self._client.emit(event)
            log.debug("Emitted OpenLineage event: %s", event)
        except Exception as exc:  # noqa: BLE001
            self._log_rate_limited(exc)

    def _log_rate_limited(self, exc: Exception) -> None:
        now = time.monotonic()
        cls = type(exc)
        last = self._last_logged_at.get(cls)
        if last is None or now - last >= _RATE_LIMIT_WINDOW_SECONDS:
            self._last_logged_at[cls] = now
            log.error(
                "OpenLineage emit failed (%s). Events will be dropped until the backend recovers. url=%s",
                cls.__name__,
                os.getenv("OPENLINEAGE_URL", "<unset>"),
                exc_info=exc,
            )
