# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState

from dagster_openlineage.emitter import (
    DEFAULT_TIMEOUT_SECONDS,
    OpenLineageEmitter,
)


def _sample_event() -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime="2026-04-23T00:00:00.000000Z",
        run=Run(runId="00000000-0000-0000-0000-000000000001"),
        job=Job(namespace="dagster", name="test"),
        producer="urn:test",
    )


def test_emit_returns_on_success():
    client = MagicMock()
    emitter = OpenLineageEmitter(client=client)
    emitter.emit(_sample_event())
    client.emit.assert_called_once()


def test_emit_swallows_exception_and_logs_once(caplog):
    client = MagicMock()
    client.emit.side_effect = ConnectionError("backend down")
    emitter = OpenLineageEmitter(client=client)
    with caplog.at_level("ERROR", logger="dagster_openlineage.emit"):
        emitter.emit(_sample_event())
        # Second call within the rate-limit window should NOT log a new ERROR.
        emitter.emit(_sample_event())
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "ConnectionError" in errors[0].getMessage()


def test_emit_rate_limiter_is_per_error_class(caplog):
    client = MagicMock()
    client.emit.side_effect = [ConnectionError("a"), TimeoutError("b")]
    emitter = OpenLineageEmitter(client=client)
    with caplog.at_level("ERROR", logger="dagster_openlineage.emit"):
        emitter.emit(_sample_event())
        emitter.emit(_sample_event())
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 2


def test_emit_propagates_base_exception():
    client = MagicMock()
    client.emit.side_effect = SystemExit("shutdown")
    emitter = OpenLineageEmitter(client=client)
    with pytest.raises(SystemExit):
        emitter.emit(_sample_event())


def test_emit_propagates_keyboard_interrupt():
    client = MagicMock()
    client.emit.side_effect = KeyboardInterrupt()
    emitter = OpenLineageEmitter(client=client)
    with pytest.raises(KeyboardInterrupt):
        emitter.emit(_sample_event())


def test_default_transport_zeroes_retries():
    with patch.dict(
        os.environ, {"OPENLINEAGE_URL": "http://ol-backend:5000"}, clear=False
    ):
        emitter = OpenLineageEmitter()
    transport = emitter._client.transport
    retry = transport.config.retry
    # Probe 13 guard: the default HttpConfig.retry bakes in total=5 + backoff_factor=0.3,
    # which multiplies our 2s timeout ~7.5x. We must zero every field the default sets.
    assert retry["total"] == 0
    assert retry["connect"] == 0
    assert retry["read"] == 0
    assert retry["backoff_factor"] == 0


def test_default_timeout_applied_to_transport():
    with patch.dict(
        os.environ, {"OPENLINEAGE_URL": "http://ol-backend:5000"}, clear=False
    ):
        emitter = OpenLineageEmitter()
    assert emitter._client.transport.config.timeout == DEFAULT_TIMEOUT_SECONDS


def test_custom_timeout_overrides_default():
    with patch.dict(
        os.environ, {"OPENLINEAGE_URL": "http://ol-backend:5000"}, clear=False
    ):
        emitter = OpenLineageEmitter(timeout=10.0)
    assert emitter._client.transport.config.timeout == 10.0


def test_no_url_env_var_falls_back_to_default_client():
    env = {k: v for k, v in os.environ.items() if k != "OPENLINEAGE_URL"}
    with patch.dict(os.environ, env, clear=True):
        emitter = OpenLineageEmitter()
    assert emitter._client is not None
