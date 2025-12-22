# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import dagster
from typing import TYPE_CHECKING, Any

# Version parsing
from packaging import version as version_parser

DAGSTER_VERSION = version_parser.parse(dagster.__version__)
IS_LE_1_11_5 = DAGSTER_VERSION <= version_parser.parse("1.11.5")
IS_GE_1_11_6 = DAGSTER_VERSION >= version_parser.parse("1.11.6")
IS_GE_1_8_0 = DAGSTER_VERSION >= version_parser.parse("1.8.0")

# Conditional imports
if IS_LE_1_11_5:
    # Old API imports (Dagster <= 1.11.5)
    # Note: For versions < 1.7.0, these may need to come from dagster.core.*
    # For versions 1.7.0-1.11.5, they're in dagster.core.*
    from dagster.core.definitions.sensor_definition import SensorDefinition  # noqa: F401
    from dagster.core.events import DagsterEventType  # noqa: F401

    try:
        from dagster.core.definitions.sensor_definition import (
            DEFAULT_SENSOR_DAEMON_INTERVAL,
        )  # noqa: F401
    except ImportError:
        DEFAULT_SENSOR_DAEMON_INTERVAL = 300  # Fallback value
    try:
        from dagster.core.events import PIPELINE_EVENTS, STEP_EVENTS  # noqa: F401
    except ImportError:
        PIPELINE_EVENTS = set()
        STEP_EVENTS = set()
else:
    # New API imports (Dagster >= 1.11.6)
    from dagster import SensorDefinition, DagsterEventType  # noqa: F401

    try:
        from dagster import DEFAULT_SENSOR_DAEMON_INTERVAL  # noqa: F401
    except ImportError:
        DEFAULT_SENSOR_DAEMON_INTERVAL = 300  # Fallback value
    try:
        from dagster import PIPELINE_EVENTS, STEP_EVENTS  # noqa: F401
    except ImportError:
        # Fallback to core.events for versions where they're not in main dagster module
        try:
            from dagster.core.events import PIPELINE_EVENTS, STEP_EVENTS  # noqa: F401
        except ImportError:
            PIPELINE_EVENTS = set()
            STEP_EVENTS = set()


# Attribute helpers
def get_pipeline_origin(run: Any) -> Any:
    """
    Return pipeline origin across Dagster versions
    """
    if IS_GE_1_11_6:
        return getattr(run, "remote_pipeline_origin", None)
    return getattr(run, "external_pipeline_origin", None)  # type: ignore[attr-defined]


def get_job_origin(run: Any) -> Any:
    # Versions 1.8.0+ use remote_job_origin, but DagsterRun constructor
    # may still use external_job_origin in some versions. Try both.
    if IS_GE_1_8_0:
        # Try remote_job_origin first (for newer versions)
        origin = getattr(run, "remote_job_origin", None)
        if origin is not None:
            return origin
        # Fallback to external_job_origin (for 1.8.0 which may still use it)
        return getattr(run, "external_job_origin", None)  # type: ignore[attr-defined]
    return getattr(run, "external_job_origin", None)  # type: ignore[attr-defined]


def get_repository_origin(origin: Any) -> Any:
    # For RemoteJobOrigin (1.7.0+), the attribute is just "repository_origin"
    # For older ExternalJobOrigin, it's "external_repository_origin"
    if origin is None:
        return None
    # Try repository_origin first (for RemoteJobOrigin)
    if hasattr(origin, "repository_origin"):
        return getattr(origin, "repository_origin", None)
    return getattr(origin, "external_repository_origin", None)  # type: ignore[attr-defined]


# Export version flags for use in other modules
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
