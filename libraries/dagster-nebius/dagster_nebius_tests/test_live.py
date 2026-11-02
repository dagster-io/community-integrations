"""Explicitly opt-in, billable tests. Never enable these in fork PR CI."""

import importlib.util
import os
from pathlib import Path
from urllib.parse import urlsplit

import dagster as dg
import pytest

from dagster_nebius import NebiusEndpointResource

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("NEBIUS_RUN_LIVE_TESTS") != "1",
        reason="Live cloud tests require explicit opt-in",
    ),
]


def example():
    path = Path(__file__).parents[1] / "examples/gpu_asset/definitions.py"
    spec = importlib.util.spec_from_file_location("live_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_live_gpu_output():
    module = example()
    result = dg.materialize([module.gpu_result], resources=module.defs.resources)
    materialization = result.asset_materializations_for_node("gpu_result")[0]
    uri = urlsplit(materialization.metadata["output_uri"].value)
    client = module.s3_factory()
    try:
        assert (
            client.head_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))[
                "ContentLength"
            ]
            > 0
        )
    finally:
        client.close()
    # Retain exact test-owned Job ID/output URI for inspection and manual cleanup.
    print("Job:", materialization.metadata["nebius/job_id"].value)
    print("Output:", materialization.metadata["output_uri"].value)


@pytest.mark.skipif(
    not os.environ.get("NEBIUS_TEST_ENDPOINT_ID"),
    reason="No existing test Endpoint configured",
)
def test_live_existing_endpoint():
    module = example()
    resource = NebiusEndpointResource(
        sdk_factory=module.sdk_factory,
        endpoint_id=os.environ["NEBIUS_TEST_ENDPOINT_ID"],
        project_id=os.environ["NEBIUS_PROJECT_ID"],
        endpoint_token=os.environ["NEBIUS_TEST_ENDPOINT_TOKEN"],
        health_path=os.environ["NEBIUS_TEST_ENDPOINT_HEALTH_PATH"],
        endpoint_url=os.environ.get("NEBIUS_TEST_ENDPOINT_URL"),
    )
    assert (
        resource.wait_until_ready(timeout=120)["id"]
        == os.environ["NEBIUS_TEST_ENDPOINT_ID"]
    )
