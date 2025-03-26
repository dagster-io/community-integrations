import datetime as dt
import pathlib as plb

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension, TableSlice
from pyiceberg import expressions as E
from pyiceberg.catalog import Catalog

from dagster_iceberg._utils import io


def test_table_writer(namespace: str, catalog: Catalog, data: pa.Table):
    table_ = "handler_data_table_writer"
    identifier_ = f"{namespace}.{table_}"
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            # In assets that are not partitioned, this value is not None but an empty list.
            #  bit confusing since the type is optional and default value is None
            partition_dimensions=[],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    assert catalog.table_exists(identifier_)
    table = catalog.load_table(identifier_)
    assert (
        table.current_snapshot().summary.additional_properties["dagster-run-id"]
        == "hfkghdgsh467374828"
    )
    assert (
        table.current_snapshot().summary.additional_properties["created-by"]
        == "dagster"
    )


def test_table_writer_partitioned(namespace: str, catalog: Catalog, data: pa.Table):
    # Works similar to # https://docs.dagster.io/integrations/deltalake/reference#storing-multi-partitioned-assets
    # Need to subset the data.
    table_ = "handler_data_table_writer_partitioned"
    identifier_ = f"{namespace}.{table_}"
    data = data.filter(
        (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1)),
    )
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table(identifier_)
    partition_field_names = [f.name for f in table.spec().fields]
    assert partition_field_names == ["timestamp"]
    assert len(table.scan().to_arrow().to_pydict()["value"]) == 60


def test_table_writer_multi_partitioned(
    namespace: str,
    catalog: Catalog,
    data: pa.Table,
):
    # Works similar to # https://docs.dagster.io/integrations/deltalake/reference#storing-multi-partitioned-assets
    # Need to subset the data.
    table_ = "handler_data_table_writer_multi_partitioned"
    identifier_ = f"{namespace}.{table_}"
    data = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1)),
    )
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
                TablePartitionDimension(
                    "category",
                    ["A"],
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table(identifier_)
    partition_field_names = [f.name for f in table.spec().fields]
    assert partition_field_names == ["timestamp", "category"]
    assert len(table.scan().to_arrow().to_pydict()["value"]) == 23


def test_table_writer_multi_partitioned_update(
    namespace: str,
    catalog: Catalog,
    data: pa.Table,
):
    # Works similar to # https://docs.dagster.io/integrations/deltalake/reference#storing-multi-partitioned-assets
    # Need to subset the data.
    table_ = "handler_data_table_writer_multi_partitioned_update"
    identifier_ = f"{namespace}.{table_}"
    data = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1)),
    ).to_pydict()
    data["value"] = pa.array([10.0] * len(data["value"]))
    data = pa.Table.from_pydict(data)
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
                TablePartitionDimension(
                    "category",
                    ["A"],
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table(identifier_)
    data_out = (
        table.scan(
            E.And(
                E.And(
                    E.GreaterThanOrEqual("timestamp", "2023-01-01T00:00:00"),
                    E.LessThan("timestamp", "2023-01-01T01:00:00"),
                ),
                E.EqualTo("category", "A"),
            ),
        )
        .to_arrow()
        .to_pydict()
    )
    assert all(v == 10 for v in data_out["value"])


def test_table_writer_multi_partitioned_update_partition_spec_change(
    namespace: str,
    warehouse_path: str,
    catalog: Catalog,
    data: pa.Table,
):
    table_ = "handler_data_table_writer_multi_partitioned_update_partition_spec_change"
    identifier_ = f"{namespace}.{table_}"
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1)),
    )
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
                TablePartitionDimension(
                    "category",
                    ["A"],
                ),
            ],
        ),
        data=data_,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    path_to_dwh = (
        plb.Path(warehouse_path)
        / f"{namespace}.db"
        / table_
        / "data"
        / "timestamp=2023-01-01-00"
    )
    categories = sorted([p.name for p in path_to_dwh.glob("*") if p.is_dir()])
    assert categories == ["category=A", "category=B", "category=C"]
    assert (
        len(catalog.load_table(identifier_).scan().to_arrow().to_pydict()["value"])
        == 1440
    )


def test_table_writer_multi_partitioned_update_partition_spec_error(
    namespace: str,
    catalog: Catalog,
    data: pa.Table,
):
    table_ = "handler_data_multi_partitioned_update_partition_spec_error"
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= dt.datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < dt.datetime(2023, 1, 1, 1)),
    )
    with pytest.raises(
        ValueError,
        match="Partition spec update mode is set to 'error' but there",
    ):
        io.table_writer(
            table_slice=TableSlice(
                table=table_,
                schema=namespace,
                partition_dimensions=[
                    TablePartitionDimension(
                        "timestamp",
                        TimeWindow(
                            dt.datetime(2023, 1, 1, 0),
                            dt.datetime(2023, 1, 1, 1),
                        ),
                    ),
                    TablePartitionDimension(
                        "category",
                        ["A"],
                    ),
                ],
            ),
            data=data_,
            catalog=catalog,
            schema_update_mode="update",
            partition_spec_update_mode="error",
            dagster_run_id="hfkghdgsh467374828",
        )


def test_iceberg_table_writer_with_table_properties(
    namespace: str,
    catalog: Catalog,
    data: pa.Table,
):
    table_ = "handler_data_iceberg_table_writer_with_table_properties"
    identifier_ = f"{namespace}.{table_}"
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        table_properties={
            "write.parquet.page-size-bytes": "2048",  # 2MB
            "write.parquet.page-row-limit": "10000",
        },
        dagster_run_id="hfkghdgsh467374828",
    )
    table = catalog.load_table(identifier_)
    assert table.properties["write.parquet.page-size-bytes"] == "2048"
    assert table.properties["write.parquet.page-row-limit"] == "10000"


def test_iceberg_table_writer_drop_partition_spec_column(
    namespace: str,
    catalog: Catalog,
    data: pa.Table,
):
    table_ = "handler_data_iceberg_table_writer_drop_partition_spec"
    # First write
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="update",
        partition_spec_update_mode="update",
        dagster_run_id="hfkghdgsh467374828",
    )
    # Second write: user drops partition column but keeps the partition spec
    data = data.drop("timestamp")
    with pytest.raises(ValueError, match="Could not find field"):
        io.table_writer(
            table_slice=TableSlice(
                table=table_,
                schema=namespace,
                partition_dimensions=[
                    TablePartitionDimension(
                        "timestamp",
                        TimeWindow(
                            dt.datetime(2023, 1, 1, 0),
                            dt.datetime(2023, 1, 1, 1),
                        ),
                    ),
                ],
            ),
            data=data,
            catalog=catalog,
            schema_update_mode="update",
            partition_spec_update_mode="update",
            dagster_run_id="gfgd744445dfhgfgfg",
        )


def test_write_from_any_to_zero_partition_spec_fields(
    namespace: str,
    catalog: Catalog,
    data: pa.Table,
):
    table_ = "handler_data_write_from_any_to_zero_partition_spec_fields"
    # First write
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[
                TablePartitionDimension(
                    "timestamp",
                    TimeWindow(dt.datetime(2023, 1, 1, 0), dt.datetime(2023, 1, 1, 1)),
                ),
            ],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="error",
        partition_spec_update_mode="error",
        dagster_run_id="hfkghdgsh467374828",
    )
    # Second write: user drops the partition spec
    io.table_writer(
        table_slice=TableSlice(
            table=table_,
            schema=namespace,
            partition_dimensions=[],
        ),
        data=data,
        catalog=catalog,
        schema_update_mode="error",
        partition_spec_update_mode="update",
        dagster_run_id="gfgd744445dfhgfgfg",
    )
    table = catalog.load_table(f"{namespace}.{table_}")
    assert len(table.specs()) == 2
    # Spec from the first write
    assert table.specs()[1].fields[0].name == "timestamp"
    # Spec from the second write (no partition spec)
    assert len(table.spec().fields) == 0
