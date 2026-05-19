# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# pyright: reportAbstractUsage=false, reportOptionalSubscript=false
# Auto-delegation (class-body setattr loop) satisfies EventLogStorage's
# abstractmethods at runtime; pyright can't see that.

"""End-to-end Pipes-style runless asset event (SC-9).

Pipes externally-generated materializations arrive via
``instance.report_runless_asset_event`` and flow through the same
``store_event`` seam as in-process materializations. The wrapper must emit
an OL event for them.
"""

import tempfile

from dagster import AssetKey, AssetMaterialization
from dagster._core.instance import DagsterInstance
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunEvent, RunState

from dagster_openlineage.adapter import OpenLineageAdapter
from dagster_openlineage.emitter import OpenLineageEmitter
from dagster_openlineage.storage import OpenLineageEventLogStorage

from .fakes import FakeOpenLineageTransport


def _wrapper_for(instance_storage, transport: FakeOpenLineageTransport):
    client = OpenLineageClient(transport=transport)
    adapter = OpenLineageAdapter(emitter=OpenLineageEmitter(client=client))
    return OpenLineageEventLogStorage(wrapped=instance_storage, adapter=adapter)


def test_pipes_runless_event_flows_through_wrapper():
    transport = FakeOpenLineageTransport()
    with tempfile.TemporaryDirectory() as tmp:
        instance = DagsterInstance.ephemeral()
        # Swap the event log storage on the instance with our wrapper around
        # its own default storage. DagsterInstance.ephemeral uses in-memory
        # storage we can wrap directly.
        wrapped_storage = _wrapper_for(instance.event_log_storage, transport)
        # Instance holds a reference; for this test we bypass the instance's
        # store_event plumbing and invoke the wrapper directly with a
        # runless AssetMaterialization event log entry.
        from dagster import DagsterEvent, DagsterEventType, EventLogEntry
        from dagster._core.events import StepMaterializationData
        from openlineage.client.uuid import generate_new_uuid

        run_id = str(generate_new_uuid())
        entry = EventLogEntry(
            error_info=None,
            level="debug",
            user_message="pipes-originated",
            run_id=run_id,
            timestamp=0.0,
            step_key=None,
            job_name=None,
            dagster_event=DagsterEvent(
                event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
                job_name="pipes_runless",
                event_specific_data=StepMaterializationData(
                    materialization=AssetMaterialization(
                        asset_key=AssetKey(["orders"]),
                        metadata={"path": "/data/orders.parquet"},
                    ),
                ),
            ),
        )

        wrapped_storage.store_event(entry)

        completes = transport.run_events_of_type(RunState.COMPLETE)
        assert len(completes) == 1
        event: RunEvent = completes[0]
        assert event.job.name == "orders"
        assert event.outputs is not None
        assert event.outputs[0].name == "orders"
        del tmp
