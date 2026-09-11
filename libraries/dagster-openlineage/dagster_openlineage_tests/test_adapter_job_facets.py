# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
from unittest.mock import patch

from openlineage.client.facet_v2 import job_type_job, ownership_job
from openlineage.client.uuid import generate_new_uuid

from dagster import AssetKey

from dagster_openlineage.adapter import OpenLineageAdapter


def test_jobtype_facet_always_present():
    facets = OpenLineageAdapter()._job_facets()

    assert facets["jobType"] == job_type_job.JobTypeJobFacet(
        processingType="BATCH", integration="DAGSTER", jobType="JOB"
    )


def test_no_ownership_without_team_or_owners():
    assert "ownership" not in OpenLineageAdapter()._job_facets()


def test_default_team_ownership_when_no_per_asset_owners():
    facets = OpenLineageAdapter(team="lakehouse")._job_facets()

    assert facets["ownership"] == ownership_job.OwnershipJobFacet(
        owners=[ownership_job.Owner(name="team:lakehouse")]
    )


def test_per_asset_owners_override_the_default_team():
    facets = OpenLineageAdapter(team="lakehouse")._job_facets(
        owners=["team:calculator", "dan@example.com"]
    )

    assert facets["ownership"] == ownership_job.OwnershipJobFacet(
        owners=[
            ownership_job.Owner(name="team:calculator"),
            ownership_job.Owner(name="dan@example.com"),
        ]
    )


def test_per_asset_owners_without_a_default_team():
    facets = OpenLineageAdapter()._job_facets(owners=["team:calculator"])

    assert facets["ownership"] == ownership_job.OwnershipJobFacet(
        owners=[ownership_job.Owner(name="team:calculator")]
    )


def test_empty_owners_falls_back_to_default_team():
    facets = OpenLineageAdapter(team="lakehouse")._job_facets(owners=[])

    assert facets["ownership"].owners[0].name == "team:lakehouse"


def test_team_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("OPENLINEAGE_TEAM", "calculator")

    facets = OpenLineageAdapter()._job_facets()

    assert facets["ownership"].owners[0].name == "team:calculator"


def test_team_argument_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("OPENLINEAGE_TEAM", "from-env")

    facets = OpenLineageAdapter(team="from-arg")._job_facets()

    assert facets["ownership"].owners[0].name == "team:from-arg"


@patch("dagster_openlineage.adapter.OpenLineageClient.emit")
def test_asset_materialization_threads_owners_onto_the_job(mock_emit):
    # the owners argument on an asset method reaches the emitted job's ownership facet
    adapter = OpenLineageAdapter()
    adapter.asset_materialization(
        AssetKey(["db.table"]),
        str(generate_new_uuid()),
        time.time(),
        owners=["team:lakehouse"],
    )

    emitted = mock_emit.call_args[0][0]
    assert emitted.job.facets == adapter._job_facets(["team:lakehouse"])
