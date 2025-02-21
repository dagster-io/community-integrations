import subprocess
from typing import Optional

import pytest
from click.testing import CliRunner
from dagster import AssetKey
from dagster._cli.asset import asset_materialize_command
from dagster._core.test_utils import instance_for_test
from dagster._utils import file_relative_path


MAKEFILE_DIR = file_relative_path(__file__, "../")


@pytest.fixture(autouse=True)
def catalog():
    subprocess.run(["make", "catalog"], cwd=MAKEFILE_DIR, check=True)
    yield
    subprocess.run(["make", "clean"], cwd=MAKEFILE_DIR, check=True)


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


def test_single_asset():
    with instance_for_test() as instance:
        result = invoke_materialize("clean_nyc_taxi_data")
        assert "RUN_SUCCESS" in result.output
        assert (
            instance.get_latest_materialization_event(AssetKey("clean_nyc_taxi_data"))
            is not None
        )
        assert result.exit_code == 0
