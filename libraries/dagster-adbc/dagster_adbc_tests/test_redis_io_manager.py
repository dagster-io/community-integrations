"""Opt-in integration tests against the Redis ADBC driver.

Set REDIS_ADBC_DRIVER to the shared-library path and REDIS_ADBC_URI to a
Redis endpoint with Search support. Every test uses a unique schema.
"""

import os
from collections.abc import Iterator
from uuid import uuid4

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from dagster import (
    AssetExecutionContext,
    StaticPartitionsDefinition,
    asset,
    build_init_resource_context,
    build_output_context,
    materialize,
)

from dagster_adbc import ADBCIOManager, ADBCResource
from dagster_adbc_tests import test_io_manager as shared

pytestmark = pytest.mark.skipif(
    not (os.environ.get("REDIS_ADBC_DRIVER") and os.environ.get("REDIS_ADBC_URI")),
    reason="Set REDIS_ADBC_DRIVER and REDIS_ADBC_URI to run Redis integration tests",
)


@pytest.fixture
def redis_manager() -> Iterator[ADBCIOManager]:
    schema = f"dagster_test_{uuid4().hex}"
    manager = ADBCIOManager(
        driver=os.environ["REDIS_ADBC_DRIVER"],
        uri=os.environ["REDIS_ADBC_URI"],
        db_kwargs={"adbc.redis.default_schema": schema},
        schema=schema,
        autocommit=True,
    )
    try:
        yield manager
    finally:
        resource = ADBCResource(
            driver=manager.driver, uri=manager.uri, db_kwargs=manager.db_kwargs, autocommit=True
        )
        with resource.get_connection() as connection, connection.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.mark.parametrize("frame_type", [pa.Table, pd.DataFrame, pl.DataFrame])
def test_redis_round_trip_and_columns(redis_manager: ADBCIOManager, frame_type: type) -> None:
    shared.test_round_trip_and_columns(redis_manager, frame_type)


@pytest.mark.parametrize("time_partition", [False, True])
def test_redis_partition_replacement(redis_manager: ADBCIOManager, time_partition: bool) -> None:
    shared.test_partition_replacement(redis_manager, time_partition)


def test_redis_multi_partitions(redis_manager: ADBCIOManager) -> None:
    shared.test_multi_partitions(redis_manager)


def test_redis_failed_staging_preserves_output(
    redis_manager: ADBCIOManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared.test_failed_staging_preserves_output(redis_manager, monkeypatch, autocommit=True)


def test_redis_swap_failure_cleanup(
    redis_manager: ADBCIOManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared.test_swap_failure_and_autocommit_cleanup(redis_manager, monkeypatch, autocommit=True)


def test_redis_effective_autocommit(redis_manager: ADBCIOManager) -> None:
    manager = redis_manager.model_copy(update={"autocommit": False})
    io_manager = manager.create_io_manager(build_init_resource_context())
    with build_output_context(name="data") as context:
        with pytest.warns(Warning, match="Cannot disable autocommit"):
            io_manager.handle_output(context, pa.table({"value": [1]}))
    assert shared.read(redis_manager, 'SELECT * FROM "data"').to_pydict() == {"value": [1]}
    assert shared.staging_tables(redis_manager) == []


def test_redis_failed_partition_ingestion_is_not_rolled_back(
    redis_manager: ADBCIOManager,
) -> None:
    bad = False

    @asset(
        partitions_def=StaticPartitionsDefinition(["a", "b"]),
        metadata={"partition_expr": '"partition"'},
    )
    def partitioned(context: AssetExecutionContext) -> pa.Table:
        return pa.table({"partition": [context.partition_key], "wrong" if bad else "value": [1]})

    for key in ["a", "b"]:
        assert materialize(
            [partitioned], resources={"io_manager": redis_manager}, partition_key=key
        ).success
    bad = True
    result = materialize(
        [partitioned],
        resources={"io_manager": redis_manager},
        partition_key="a",
        raise_on_error=False,
    )
    assert not result.success
    # Redis cannot roll back the DELETE when the later append fails.
    assert shared.read(redis_manager, 'SELECT "partition" FROM "partitioned"').to_pydict() == {
        "partition": ["b"]
    }
