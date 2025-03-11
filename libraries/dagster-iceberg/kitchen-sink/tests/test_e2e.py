import os
import subprocess
from typing import Optional

import pytest
from click.testing import CliRunner
from dagster import AssetKey
from dagster._cli.asset import asset_materialize_command
from dagster._core.test_utils import instance_for_test
from dagster._utils import file_relative_path


MAKEFILE_DIR = file_relative_path(__file__, "../")

os.environ["SPARK_REMOTE"] = "sc://localhost"


@pytest.fixture(autouse=True)
def catalog():
    subprocess.run(["make", "catalog"], cwd=MAKEFILE_DIR, check=True)
    yield
    subprocess.run(["make", "down"], cwd=MAKEFILE_DIR, check=True)


def invoke_materialize(
    select: str,
    partition: Optional[str] = None,
    partition_range: Optional[str] = None,
):
    runner = CliRunner()
    options = [
        "-f",
        file_relative_path(__file__, "../kitchen_sink.py"),
        "--select",
        select,
    ]
    if partition:
        options.extend(["--partition", partition])
    if partition_range:
        options.extend(["--partition-range", partition_range])
    return runner.invoke(asset_materialize_command, options)


def test_polars():
    with instance_for_test() as instance:
        result = invoke_materialize("*reloaded_nyc_taxi_data")
        assert "RUN_SUCCESS" in result.output
        for asset_key in [
            AssetKey("combined_nyc_taxi_data"),
            AssetKey("reloaded_nyc_taxi_data"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None


def test_spark():
    with instance_for_test() as instance:
        result = invoke_materialize("*reloaded_nyc_taxi_data_spark")
        assert "RUN_SUCCESS" in result.output
        for asset_key in [
            AssetKey("combined_nyc_taxi_data_spark"),
            AssetKey("reloaded_nyc_taxi_data_spark"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
