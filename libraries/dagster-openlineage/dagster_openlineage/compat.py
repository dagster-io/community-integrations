# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import dagster
from typing import TYPE_CHECKING, Any

from packaging import version as version_parser

DAGSTER_VERSION = version_parser.parse(dagster.__version__)
IS_LE_1_11_5 = DAGSTER_VERSION <= version_parser.parse("1.11.5")
IS_GE_1_11_6 = DAGSTER_VERSION >= version_parser.parse("1.11.6")
IS_GE_1_8_0 = DAGSTER_VERSION >= version_parser.parse("1.8.0")

if IS_LE_1_11_5:
    # Dagster <= 1.11.5: imports from dagster.core.*
    from dagster.core.definitions import sensor_definition
    from dagster.core.definitions.sensor_definition import SensorDefinition  # noqa: F401
    from dagster.core import events
    from dagster.core.events import DagsterEventType  # noqa: F401

    DEFAULT_SENSOR_DAEMON_INTERVAL = getattr(
        sensor_definition, "DEFAULT_SENSOR_DAEMON_INTERVAL", 300
    )

    PIPELINE_EVENTS = getattr(events, "PIPELINE_EVENTS", set())
    STEP_EVENTS = getattr(events, "STEP_EVENTS", set())
else:
    # Dagster >= 1.11.6: imports from main dagster module
    from dagster import SensorDefinition, DagsterEventType  # noqa: F401

    DEFAULT_SENSOR_DAEMON_INTERVAL = getattr(
        dagster, "DEFAULT_SENSOR_DAEMON_INTERVAL", 300
    )

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
    """
    Return pipeline origin across Dagster versions
    """
    if IS_GE_1_11_6:
        return getattr(run, "remote_pipeline_origin", None)
    return getattr(run, "external_pipeline_origin", None)  # type: ignore[attr-defined]


def get_job_origin(run: Any) -> Any:
    """Return job origin across Dagster versions.

    Dagster 1.8.0+ uses remote_job_origin, but DagsterRun constructor
    may still use external_job_origin in some versions.
    """
    if IS_GE_1_8_0:
        origin = getattr(run, "remote_job_origin", None)
        if origin is not None:
            return origin
        return getattr(run, "external_job_origin", None)  # type: ignore[attr-defined]
    return getattr(run, "external_job_origin", None)  # type: ignore[attr-defined]


def get_repository_origin(origin: Any) -> Any:
    """Return repository origin across Dagster versions.

    RemoteJobOrigin (1.7.0+) uses "repository_origin",
    ExternalJobOrigin uses "external_repository_origin".
    """
    if origin is None:
        return None
    if hasattr(origin, "repository_origin"):
        return getattr(origin, "repository_origin", None)
    return getattr(origin, "external_repository_origin", None)  # type: ignore[attr-defined]


__all__ = [
    "SensorDefinition",
    "DagsterEventType",
    "DEFAULT_SENSOR_DAEMON_INTERVAL",
    "PIPELINE_EVENTS",
    "STEP_EVENTS",
    "IS_LE_1_11_5",
    "IS_GE_1_11_6",
    "IS_GE_1_8_0",
    "get_pipeline_origin",
    "get_job_origin",
    "get_repository_origin",
]

if TYPE_CHECKING:
    from dagster import DagsterRun  # noqa: F401
