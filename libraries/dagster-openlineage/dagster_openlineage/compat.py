# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import importlib.util
from typing import TYPE_CHECKING, Any

import dagster

from dagster import SensorDefinition, DagsterEventType  # noqa: F401,E402

DEFAULT_SENSOR_DAEMON_INTERVAL = getattr(dagster, "DEFAULT_SENSOR_DAEMON_INTERVAL", 300)

PIPELINE_EVENTS = getattr(dagster, "PIPELINE_EVENTS", None)
STEP_EVENTS = getattr(dagster, "STEP_EVENTS", None)

if PIPELINE_EVENTS is None or STEP_EVENTS is None:
    _core_events_spec = importlib.util.find_spec("dagster.core.events")
    if _core_events_spec is not None:
        _core_events_module = importlib.import_module("dagster.core.events")
        PIPELINE_EVENTS = getattr(_core_events_module, "PIPELINE_EVENTS", set())
        STEP_EVENTS = getattr(_core_events_module, "STEP_EVENTS", set())
    else:
        PIPELINE_EVENTS = set()
        STEP_EVENTS = set()


def get_pipeline_origin(run: Any) -> Any:
    """Return pipeline origin. Floor is Dagster >= 1.11.6; remote_pipeline_origin is stable."""
    return getattr(run, "remote_pipeline_origin", None)


def get_job_origin(run: Any) -> Any:
    """Return job origin. Floor is Dagster >= 1.11.6; prefer remote_job_origin, fall back to
    external_job_origin for the narrow 1.11.x window where both attrs may coexist."""
    origin = getattr(run, "remote_job_origin", None)
    if origin is not None:
        return origin
    return getattr(run, "external_job_origin", None)  # pyright: ignore[reportAttributeAccessIssue]


def get_repository_origin(origin: Any) -> Any:
    """Return repository origin across Dagster versions.

    RemoteJobOrigin (1.7.0+) uses "repository_origin";
    ExternalJobOrigin uses "external_repository_origin".
    """
    if origin is None:
        return None
    if hasattr(origin, "repository_origin"):
        return getattr(origin, "repository_origin", None)
    return getattr(origin, "external_repository_origin", None)  # pyright: ignore[reportAttributeAccessIssue]


__all__ = [
    "SensorDefinition",
    "DagsterEventType",
    "DEFAULT_SENSOR_DAEMON_INTERVAL",
    "PIPELINE_EVENTS",
    "STEP_EVENTS",
    "get_pipeline_origin",
    "get_job_origin",
    "get_repository_origin",
]

if TYPE_CHECKING:
    from dagster import DagsterRun  # noqa: F401
