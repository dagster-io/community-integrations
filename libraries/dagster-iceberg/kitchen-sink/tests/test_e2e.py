import subprocess

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
    subprocess.run(["sleep", "10"])
    yield
    subprocess.run(["make", "down"], cwd=MAKEFILE_DIR, check=True)


def invoke_materialize(
    select: str,
    partition: str | None = None,
    partition_range: str | None = None,
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


def test_polars_static_partitioned():
    with instance_for_test() as instance:
        for partition in ["part.0", "part.1"]:
            result = invoke_materialize(
                "*reloaded_static_partitioned_nyc_taxi_data", partition=partition
            )
            assert "RUN_SUCCESS" in result.output

        for asset_key in [
            AssetKey("static_partitioned_nyc_taxi_data"),
            AssetKey("reloaded_static_partitioned_nyc_taxi_data"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
            partitions = instance.get_materialized_partitions(asset_key)
            for partition in ["part.0", "part.1"]:
                assert partition in partitions


def test_polars_multi_partitioned():
    with instance_for_test() as instance:
        for partition in ["2015-01-01|part.0", "2015-01-01|part.1"]:
            result = invoke_materialize(
                "*reloaded_multi_partitioned_nyc_taxi_data", partition=partition
            )
            assert "RUN_SUCCESS" in result.output

        for asset_key in [
            AssetKey("multi_partitioned_nyc_taxi_data"),
            AssetKey("reloaded_multi_partitioned_nyc_taxi_data"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
            partitions = instance.get_materialized_partitions(asset_key)
            for partition in ["2015-01-01|part.0", "2015-01-01|part.1"]:
                assert partition in partitions


def test_polars_daily_partitioned():
    with instance_for_test() as instance:
        for partition in [
            "2015-01-01|part.0",
            "2015-01-01|part.1",
            "2015-01-02|part.0",
            "2015-01-02|part.1",
        ]:
            result = invoke_materialize(
                "multi_partitioned_nyc_taxi_data", partition=partition
            )
            assert "RUN_SUCCESS" in result.output

        for partition in ["2015-01-01", "2015-01-02"]:
            result = invoke_materialize(
                "daily_partitioned_nyc_taxi_data,"
                "reloaded_daily_partitioned_nyc_taxi_data",
                partition=partition,
            )
            assert "RUN_SUCCESS" in result.output

        for asset_key in [
            AssetKey("daily_partitioned_nyc_taxi_data"),
            AssetKey("reloaded_daily_partitioned_nyc_taxi_data"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
            partitions = instance.get_materialized_partitions(asset_key)
            for partition in ["2015-01-01", "2015-01-02"]:
                assert partition in partitions


def test_spark():
    with instance_for_test() as instance:
        result = invoke_materialize("*reloaded_nyc_taxi_data_spark")
        assert "RUN_SUCCESS" in result.output
        for asset_key in [
            AssetKey("combined_nyc_taxi_data_spark"),
            AssetKey("reloaded_nyc_taxi_data_spark"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None


def test_spark_static_partitioned():
    with instance_for_test() as instance:
        for partition in ["part.0", "part.1"]:
            result = invoke_materialize(
                "*reloaded_static_partitioned_nyc_taxi_data_spark", partition=partition
            )
            assert "RUN_SUCCESS" in result.output

        for asset_key in [
            AssetKey("static_partitioned_nyc_taxi_data_spark"),
            AssetKey("reloaded_static_partitioned_nyc_taxi_data_spark"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
            partitions = instance.get_materialized_partitions(asset_key)
            for partition in ["part.0", "part.1"]:
                assert partition in partitions


def test_spark_multi_partitioned():
    with instance_for_test() as instance:
        for partition in ["2015-01-01|part.0", "2015-01-01|part.1"]:
            result = invoke_materialize(
                "*reloaded_multi_partitioned_nyc_taxi_data_spark", partition=partition
            )
            assert "RUN_SUCCESS" in result.output

        for asset_key in [
            AssetKey("multi_partitioned_nyc_taxi_data_spark"),
            AssetKey("reloaded_multi_partitioned_nyc_taxi_data_spark"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
            partitions = instance.get_materialized_partitions(asset_key)
            for partition in ["2015-01-01|part.0", "2015-01-01|part.1"]:
                assert partition in partitions


def test_spark_daily_partitioned():
    with instance_for_test() as instance:
        for partition in [
            "2015-01-01|part.0",
            "2015-01-01|part.1",
            "2015-01-02|part.0",
            "2015-01-02|part.1",
        ]:
            result = invoke_materialize(
                "multi_partitioned_nyc_taxi_data", partition=partition
            )
            assert "RUN_SUCCESS" in result.output

        for partition in ["2015-01-01", "2015-01-02"]:
            result = invoke_materialize(
                "daily_partitioned_nyc_taxi_data,"
                "daily_partitioned_nyc_taxi_data_spark,"
                "reloaded_daily_partitioned_nyc_taxi_data_spark",
                partition=partition,
            )
            assert "RUN_SUCCESS" in result.output

        for asset_key in [
            AssetKey("daily_partitioned_nyc_taxi_data_spark"),
            AssetKey("reloaded_daily_partitioned_nyc_taxi_data_spark"),
        ]:
            assert instance.get_latest_materialization_event(asset_key) is not None
            partitions = instance.get_materialized_partitions(asset_key)
            for partition in ["2015-01-01", "2015-01-02"]:
                assert partition in partitions
