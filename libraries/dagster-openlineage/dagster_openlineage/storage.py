# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Mechanism A: ``OpenLineageEventLogStorage`` generic wrapper.

The wrapper composes any ``EventLogStorage`` at ``instance.yaml`` load time
via ``ConfigurableClassData.rehydrate()`` and intercepts ``store_event`` /
``store_event_batch`` to emit OpenLineage events for asset activity. All
other abstract methods delegate to the wrapped storage via a class-body
``setattr`` loop so the wrapper survives Dagster upstream adding new
abstract methods.

Ordering invariant: the wrapped storage write happens *before* OL
emission, per event. A mid-batch failure leaves the prefix of events
durable AND emitted, and events from the failure point onward neither
durable nor emitted — consistent per-event.

Failure synthesis: the wrapper tracks planned assets per run in memory
and emits synthesized ``asset_failed_to_materialize`` OL events when a
run ends with planned assets that never materialized. State is
process-local; reconciliation across restarts is deferred to v0.2.1.
Users needing crash-tolerant failure reporting configure the sensor-based
Mechanism B instead (cursor-persisted state).
"""

from __future__ import annotations

import inspect
import logging
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

import yaml
from dagster import AssetKey, Field
from dagster._core.storage.event_log.base import EventLogStorage
from dagster._serdes import ConfigurableClass, ConfigurableClassData

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.compat import DagsterEventType
from dagster_openlineage.emitter import DEFAULT_TIMEOUT_SECONDS

log = logging.getLogger(__name__)


_OVERRIDDEN = frozenset(
    {
        "store_event",
        "store_event_batch",
        "inst_data",
        "config_type",
        "from_config_value",
    }
)


def _make_method_delegate(name: str):
    def _delegate(self, *args, **kwargs):
        return getattr(self._wrapped, name)(*args, **kwargs)

    _delegate.__name__ = name
    return _delegate


def _make_property_delegate(name: str):
    def _getter(self):
        return getattr(self._wrapped, name)

    _getter.__name__ = name
    return property(_getter)


def _resolve_abstracts() -> frozenset[str]:
    return frozenset(EventLogStorage.__abstractmethods__) | frozenset(
        ConfigurableClass.__abstractmethods__
    )


class OpenLineageEventLogStorage(EventLogStorage, ConfigurableClass):
    """Wrap an inner ``EventLogStorage`` to emit OpenLineage events."""

    def __init__(
        self,
        wrapped: EventLogStorage,
        *,
        namespace: Optional[str] = None,
        namespace_template: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        strict_assertion_mapping: bool = False,
        adapter: Optional[OpenLineageAdapter] = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ) -> None:
        self._wrapped = wrapped
        self._inst_data = inst_data
        self._adapter = adapter or OpenLineageAdapter(
            namespace=namespace,
            namespace_template=namespace_template,
            timeout=timeout,
            strict_assertion_mapping=strict_assertion_mapping,
        )
        self._synthesis_lock = threading.Lock()
        # run_id -> planned but not-yet-materialized asset keys.
        self._planned_assets: Dict[str, Set[AssetKey]] = {}
        super().__init__()

    # ------------------------------------------------------------------
    # ConfigurableClass surface
    # ------------------------------------------------------------------

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "wrapped": {
                "module": Field(str),
                "class": Field(str),
                "config": Field(dict, is_required=False, default_value={}),
            },
            "namespace": Field(str, is_required=False),
            "namespace_template": Field(str, is_required=False),
            "timeout": Field(
                float, is_required=False, default_value=DEFAULT_TIMEOUT_SECONDS
            ),
            "strict_assertion_mapping": Field(
                bool, is_required=False, default_value=False
            ),
        }

    @classmethod
    def from_config_value(
        cls,
        inst_data: Optional[ConfigurableClassData],
        config_value: Mapping[str, Any],
    ) -> "OpenLineageEventLogStorage":
        wrapped_cfg = config_value["wrapped"]
        wrapped_config_dict = wrapped_cfg.get("config") or {}
        inner = ConfigurableClassData(
            module_name=wrapped_cfg["module"],
            class_name=wrapped_cfg["class"],
            config_yaml=yaml.safe_dump(wrapped_config_dict),
        ).rehydrate(as_type=EventLogStorage)
        return cls(
            wrapped=inner,
            namespace=config_value.get("namespace"),
            namespace_template=config_value.get("namespace_template"),
            timeout=config_value.get("timeout", DEFAULT_TIMEOUT_SECONDS),
            strict_assertion_mapping=config_value.get(
                "strict_assertion_mapping", False
            ),
            inst_data=inst_data,
        )

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def store_event(self, event) -> None:
        # Durability invariant: storage write happens first. If it raises,
        # OL emission never happens for this event — matches the contract
        # that OL backends only see events the event log accepted.
        self._wrapped.store_event(event)
        try:
            self._dispatch(event)
        except Exception:
            # Emitter already swallows; this catch covers dispatcher bugs
            # and metadata-extraction failures. BaseException propagates.
            log.exception("OpenLineage dispatch failed for event")

    def store_event_batch(self, events) -> None:
        # Per-event loop. Delegating wholesale to the wrapped backend's
        # store_event_batch would hide partial failures from OL — the
        # default EventLogStorage.store_event_batch already iterates
        # self.store_event, so this is no worse than the default for
        # backends without a native batch override.
        for event in events:
            self.store_event(event)

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def _dispatch(self, event) -> None:
        if not getattr(event, "is_dagster_event", False):
            return
        de = event.get_dagster_event()
        etype = de.event_type
        run_id = event.run_id
        ts = event.timestamp
        run_tags = _run_tags_for(self._wrapped, run_id)

        if etype == DagsterEventType.ASSET_MATERIALIZATION_PLANNED:
            data = de.event_specific_data
            asset_key = data.asset_key
            with self._synthesis_lock:
                self._planned_assets.setdefault(run_id, set()).add(asset_key)
            self._adapter.asset_materialization_planned(
                asset_key, run_id, ts, run_tags=run_tags
            )

        elif etype == DagsterEventType.ASSET_MATERIALIZATION:
            mat = de.event_specific_data.materialization
            asset_key = mat.asset_key
            with self._synthesis_lock:
                planned = self._planned_assets.get(run_id)
                if planned is not None:
                    planned.discard(asset_key)
            self._adapter.asset_materialization(
                asset_key,
                run_id,
                ts,
                metadata=mat.metadata,
                partition_key=mat.partition,
                run_tags=run_tags,
            )

        elif etype == DagsterEventType.ASSET_FAILED_TO_MATERIALIZE:
            data = de.event_specific_data
            with self._synthesis_lock:
                planned = self._planned_assets.get(run_id)
                if planned is not None:
                    planned.discard(data.asset_key)
            self._adapter.asset_failed_to_materialize(
                data.asset_key,
                run_id,
                ts,
                error_message=_format_error_message(data.error),
                partition_key=data.partition,
                run_tags=run_tags,
            )

        elif etype == DagsterEventType.ASSET_OBSERVATION:
            obs = de.event_specific_data.asset_observation
            self._adapter.asset_observation(
                obs.asset_key,
                run_id,
                ts,
                metadata=obs.metadata,
                partition_key=obs.partition,
                run_tags=run_tags,
            )

        elif etype == DagsterEventType.ASSET_CHECK_EVALUATION_PLANNED:
            data = de.event_specific_data
            self._adapter.asset_check_evaluation_planned(
                data.asset_key,
                check_name=data.check_name,
                run_id=run_id,
                timestamp=ts,
                # Standalone detection requires cross-event context the
                # wrapper does not hold; dispatch-time heuristic treats a
                # PLANNED without a same-run materialization planned as
                # standalone. Cheap approximation: always emit the start.
                standalone=True,
                run_tags=run_tags,
            )

        elif etype == DagsterEventType.ASSET_CHECK_EVALUATION:
            evaluation = de.event_specific_data
            self._adapter.asset_check_evaluation(
                evaluation.asset_key,
                [evaluation],
                run_id=run_id,
                timestamp=ts,
                run_tags=run_tags,
            )

        elif etype in (
            DagsterEventType.RUN_FAILURE,
            DagsterEventType.STEP_FAILURE,
        ):
            self._drain_planned_as_failures(run_id, ts, run_tags)

        elif etype in (
            DagsterEventType.RUN_SUCCESS,
            DagsterEventType.RUN_CANCELED,
        ):
            with self._synthesis_lock:
                self._planned_assets.pop(run_id, None)

    def _drain_planned_as_failures(
        self,
        run_id: str,
        timestamp: float,
        run_tags: Mapping[str, str],
    ) -> None:
        with self._synthesis_lock:
            planned = self._planned_assets.pop(run_id, None)
        if not planned:
            return
        for asset_key in planned:
            self._adapter.asset_failed_to_materialize(
                asset_key,
                run_id,
                timestamp,
                error_message="asset planned for run, run failed before materialization",
                run_tags=run_tags,
            )


def _format_error_message(error: Any) -> Optional[str]:
    if error is None:
        return None
    message = getattr(error, "message", None)
    if isinstance(message, str):
        return message
    return str(error)


def _run_tags_for(wrapped: EventLogStorage, run_id: str) -> Dict[str, str]:
    # The wrapper does not hold a direct RunStorage reference; the tags are
    # only needed for namespace template splitting, so return empty when we
    # cannot resolve — the adapter falls back to the configured namespace.
    del wrapped, run_id
    return {}


# Auto-delegation: cover every EventLogStorage and ConfigurableClass abstract
# we did NOT explicitly override. Factory form avoids the classic
# lambda-in-loop late-binding trap.
for _name in _resolve_abstracts():
    if _name in _OVERRIDDEN:
        continue
    _attr = inspect.getattr_static(EventLogStorage, _name, None)
    if isinstance(_attr, property):
        setattr(OpenLineageEventLogStorage, _name, _make_property_delegate(_name))
    else:
        setattr(OpenLineageEventLogStorage, _name, _make_method_delegate(_name))

# Clear the abstract flag on the concrete class; delegate implementations
# cover every remaining name.
OpenLineageEventLogStorage.__abstractmethods__ = frozenset()


__all__ = ["OpenLineageEventLogStorage"]

# Quiet unused-import noise for static checkers.
_ = (Iterable, List)
