# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""End-to-end subprocess parity (SC-8).

``reconstructable`` + ``execute_job`` hits the same ``store_event`` seam
as in-process ``materialize()``. This test drives a subprocess run
against an instance whose event log storage is ``OpenLineageEventLogStorage``
wrapping ``ConsolidatedSqliteEventLogStorage`` and verifies (a) the run
completes successfully and (b) the inner storage records show the step
activity — confirming the wrapper survives ``ConfigurableClass``
rehydration across the process boundary without interfering with Dagster
writes.

OL emission itself happens in the subprocess with its own adapter; we do
not assert OL-backend state because the subprocess's adapter is
independent of the parent-process test fixture.
"""

import tempfile

from dagster import execute_job, job, op, reconstructable
from dagster._core.test_utils import instance_for_test


@op
def an_op():
    return 42


@job
def a_job():
    an_op()


def test_storage_wrapper_survives_subprocess_execution():
    with tempfile.TemporaryDirectory() as temp_dir:
        overrides = {
            "event_log_storage": {
                "module": "dagster_openlineage.storage",
                "class": "OpenLineageEventLogStorage",
                "config": {
                    "wrapped": {
                        "module": "dagster._core.storage.event_log",
                        "class": "ConsolidatedSqliteEventLogStorage",
                        "config": {"base_dir": temp_dir},
                    },
                    "namespace": "e2e",
                },
            },
        }
        with instance_for_test(temp_dir=temp_dir, overrides=overrides) as instance:
            result = execute_job(
                job=reconstructable(a_job),
                instance=instance,
                raise_on_error=True,
            )
            assert result.success
            # Confirm the subprocess wrote through our wrapper — if rehydration
            # had failed, the whole run would have errored before this.
            logs = instance.all_logs(result.run_id)
            assert logs, "expected events in the wrapped event log"
