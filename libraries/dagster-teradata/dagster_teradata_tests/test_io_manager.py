from collections.abc import Sequence
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
import teradatasql
from dagster import (
    AssetIn,
    AssetKey,
    DailyPartitionsDefinition,
    Definitions,
    InputContext,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    Out,
    OutputContext,
    PartitionKeyRange,
    StaticPartitionsDefinition,
    asset,
    job,
    materialize,
    op,
)
from dagster._core.errors import DagsterInvalidConfigError
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
    TimeWindow,
)

from dagster_teradata import (
    TeradataDbClient,
    TeradataIOManager,
    TeradataResource,
    build_teradata_io_manager,
)


class FakeDataFrame:
    def __init__(self, rows=None):
        self.rows = rows or []


class FakeDataFrameTypeHandler(DbTypeHandler[FakeDataFrame]):
    def __init__(self):
        self.handled_outputs = []
        self.loaded_inputs = []

    def handle_output(self, context: OutputContext, table_slice, obj, connection):
        self.handled_outputs.append((table_slice, obj))
        return {"rows": len(obj.rows)}

    def load_input(self, context: InputContext, table_slice, connection):
        self.loaded_inputs.append(table_slice)
        return FakeDataFrame([1, 2, 3])

    @property
    def supported_types(self) -> Sequence[type[object]]:
        return [FakeDataFrame]


def make_table_slice(**kwargs) -> TableSlice:
    defaults = {"table": "my_table", "schema": "my_db", "database": None}
    defaults.update(kwargs)
    return TableSlice(**defaults)


@pytest.fixture
def teradata_resource() -> TeradataResource:
    return TeradataResource(
        host="localhost", user="dbc", password="dbc", database="my_db"
    )


def test_get_select_statement():
    assert (
        TeradataDbClient.get_select_statement(make_table_slice())
        == 'SELECT * FROM "my_db"."my_table"'
    )


def test_get_select_statement_columns():
    assert (
        TeradataDbClient.get_select_statement(make_table_slice(columns=["a", "b"]))
        == 'SELECT "a", "b" FROM "my_db"."my_table"'
    )


def test_get_select_statement_time_window():
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(
                partition_expr="ts",
                partitions=TimeWindow(
                    DailyPartitionsDefinition(start_date="2024-01-01")
                    .time_window_for_partition_key("2024-01-02")
                    .start,
                    DailyPartitionsDefinition(start_date="2024-01-01")
                    .time_window_for_partition_key("2024-01-02")
                    .end,
                ),
            )
        ]
    )
    assert TeradataDbClient.get_select_statement(table_slice) == (
        'SELECT * FROM "my_db"."my_table" WHERE\n'
        "(ts >= CAST('2024-01-02 00:00:00' AS TIMESTAMP(6)) AND "
        "ts < CAST('2024-01-03 00:00:00' AS TIMESTAMP(6)))"
    )


def test_get_select_statement_static_partitions():
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(partition_expr="color", partitions=["red", "blue"])
        ]
    )
    assert TeradataDbClient.get_select_statement(table_slice) == (
        "SELECT * FROM \"my_db\".\"my_table\" WHERE\n(color IN ('red', 'blue'))"
    )


def test_multiple_partition_dimensions_are_parenthesized_and_anded():
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(partition_expr="color", partitions=["red"]),
            TablePartitionDimension(partition_expr="size", partitions=["s", "m"]),
        ]
    )
    assert TeradataDbClient.get_select_statement(table_slice) == (
        'SELECT * FROM "my_db"."my_table" WHERE\n'
        "(color IN ('red')) AND\n(size IN ('s', 'm'))"
    )


def test_empty_partition_list_matches_no_rows():
    """Dagster passes an empty partition list when no partition keys are selected."""
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(partition_expr="ts", partitions=[])
        ]
    )
    assert TeradataDbClient.get_select_statement(table_slice) == (
        'SELECT * FROM "my_db"."my_table" WHERE\n(1 = 0)'
    )
    assert TeradataDbClient.get_cleanup_statement(table_slice) == (
        'DELETE FROM "my_db"."my_table" WHERE\n(1 = 0)'
    )


def test_partition_values_are_escaped():
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(partition_expr="name", partitions=["o'brien"])
        ]
    )
    assert "'o''brien'" in TeradataDbClient.get_select_statement(table_slice)


def test_partition_expr_is_treated_as_a_sql_expression():
    """`partition_expr` is author-supplied SQL, so expressions must survive intact."""
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(
                partition_expr="CAST(ts AS DATE)", partitions=["2024-01-01"]
            )
        ]
    )
    assert TeradataDbClient.get_select_statement(table_slice) == (
        'SELECT * FROM "my_db"."my_table" WHERE\n(CAST(ts AS DATE) IN (\'2024-01-01\'))'
    )


def test_identifiers_are_quoted():
    table_slice = make_table_slice(schema='ev"il', table='ta"ble')
    assert (
        TeradataDbClient.get_select_statement(table_slice)
        == 'SELECT * FROM "ev""il"."ta""ble"'
    )


def test_get_table_name_is_two_part():
    assert TeradataDbClient.get_table_name(make_table_slice()) == "my_db.my_table"


def test_cleanup_statement_unpartitioned():
    assert (
        TeradataDbClient.get_cleanup_statement(make_table_slice())
        == 'DELETE FROM "my_db"."my_table"'
    )


def test_cleanup_statement_partitioned():
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(partition_expr="color", partitions=["red"])
        ]
    )
    assert TeradataDbClient.get_cleanup_statement(table_slice) == (
        'DELETE FROM "my_db"."my_table" WHERE\n(color IN (\'red\'))'
    )


def test_delete_table_slice_swallows_missing_table(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        teradatasql.OperationalError(
            "[Version 20.0] [Session 1] [Teradata Database] [Error 3807] "
            "[SQLState 42S02] Object 'my_table' does not exist."
        )
    )
    client.delete_table_slice(MagicMock(), make_table_slice(), connection)


def test_delete_table_slice_does_not_match_bare_error_digits(teradata_resource):
    """'3807' appearing outside an error code must not be swallowed."""
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        teradatasql.OperationalError("[Error 3523] no DELETE access to table_3807")
    )
    with pytest.raises(Exception, match="3523"):
        client.delete_table_slice(MagicMock(), make_table_slice(), connection)


def test_delete_table_slice_reraises_other_errors(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        teradatasql.OperationalError("[Error 3523] user does not have DELETE access")
    )
    with pytest.raises(Exception, match="3523"):
        client.delete_table_slice(MagicMock(), make_table_slice(), connection)


def test_delete_table_slice_propagates_non_database_errors(teradata_resource):
    """Bugs (non-driver exceptions) must never be swallowed as 'table missing'."""
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        AttributeError("[Error 3807] not really a database error")
    )
    with pytest.raises(AttributeError):
        client.delete_table_slice(MagicMock(), make_table_slice(), connection)


def test_ensure_schema_exists_raises_when_missing(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = None
    with pytest.raises(ValueError, match="does not exist"):
        client.ensure_schema_exists(MagicMock(), make_table_slice(), connection)


def test_ensure_schema_exists_passes_when_present(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (1,)
    client.ensure_schema_exists(MagicMock(), make_table_slice(), connection)


def test_ensure_schema_exists_is_advisory_without_dbc_access(teradata_resource):
    """Users without SELECT rights on DBC views must not be blocked."""
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        teradatasql.OperationalError(
            "[Error 3523] user does not have SELECT access to DBC.DatabasesV"
        )
    )
    client.ensure_schema_exists(MagicMock(), make_table_slice(), connection)


def test_ensure_schema_exists_lookup_is_case_insensitive(teradata_resource):
    """In ANSI transaction mode string comparisons are CASESPECIFIC, so the DBC
    lookup must fold case on both sides or it silently misses the database.
    """
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = [1]

    client.ensure_schema_exists(MagicMock(), make_table_slice(), connection)

    sql, params = cursor.execute.call_args[0]
    assert "UPPER(DatabaseName) = UPPER(?)" in sql
    assert params == ["my_db"]


def test_ensure_schema_exists_propagates_non_database_errors(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = (
        AttributeError("not a database error")
    )
    with pytest.raises(AttributeError):
        client.ensure_schema_exists(MagicMock(), make_table_slice(), connection)


def test_ensure_schema_exists_queries_with_bound_parameter(teradata_resource):
    """The database name must be bound, never interpolated into the SQL."""
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (1,)
    client.ensure_schema_exists(MagicMock(), make_table_slice(schema="db1"), connection)
    sql, params = cursor.execute.call_args[0]
    assert "?" in sql
    assert "db1" not in sql
    assert params == ["db1"]


def test_connection_is_closed_after_use(teradata_resource):
    """TeradataResource.get_connection() never closes, so the client must."""
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()

    @contextmanager
    def fake_get_connection(self):
        yield connection

    with patch.object(TeradataResource, "get_connection", fake_get_connection):
        with client.connect(MagicMock(), make_table_slice()) as conn:
            assert conn is connection
            connection.close.assert_not_called()
    connection.close.assert_called_once()


def test_connection_is_closed_when_body_raises(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()

    @contextmanager
    def fake_get_connection(self):
        yield connection

    with patch.object(TeradataResource, "get_connection", fake_get_connection):
        with (
            pytest.raises(RuntimeError),
            client.connect(MagicMock(), make_table_slice()),
        ):
            raise RuntimeError("boom")
    connection.close.assert_called_once()


def test_connect_disables_autocommit_and_commits_on_success(teradata_resource):
    """delete_table_slice + handle_output share this connection, so the whole
    materialization must commit as a single transaction, not statement-by-statement."""
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()

    @contextmanager
    def fake_get_connection(self):
        yield connection

    with patch.object(TeradataResource, "get_connection", fake_get_connection):
        with client.connect(MagicMock(), make_table_slice()):
            assert connection.autocommit is False
            connection.commit.assert_not_called()
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_connect_rolls_back_on_failure(teradata_resource):
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()

    @contextmanager
    def fake_get_connection(self):
        yield connection

    with patch.object(TeradataResource, "get_connection", fake_get_connection):
        with (
            pytest.raises(RuntimeError),
            client.connect(MagicMock(), make_table_slice()),
        ):
            raise RuntimeError("boom")
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    connection.close.assert_called_once()


def test_connect_rejects_non_ansi_transaction_mode():
    """BTET mode requires DDL to be the transaction's final statement, and a failed
    DDL there aborts the whole transaction - both break the explicit ANSI-mode
    transaction wrapping used to make delete+insert atomic, so it must be rejected."""
    teradata_resource = TeradataResource(
        host="localhost", user="dbc", password="dbc", tmode="TERA"
    )
    client = TeradataDbClient(teradata_resource)
    with pytest.raises(ValueError, match="requires the resource's tmode to be 'ANSI'"):
        with client.connect(MagicMock(), make_table_slice()):
            pass


def test_connect_rejects_none_transaction_mode():
    """tmode=None makes TeradataResource.get_connection() omit the tmode connection
    parameter entirely, leaving the session mode up to the driver/server default,
    which is not guaranteed to be ANSI - so it must be rejected just like an
    explicit non-ANSI mode."""
    teradata_resource = TeradataResource(
        host="localhost", user="dbc", password="dbc", tmode=None
    )
    client = TeradataDbClient(teradata_resource)
    with pytest.raises(ValueError, match="requires the resource's tmode to be 'ANSI'"):
        with client.connect(MagicMock(), make_table_slice()):
            pass


def test_connect_allows_ansi_transaction_mode_case_insensitively():
    teradata_resource = TeradataResource(
        host="localhost", user="dbc", password="dbc", tmode="ansi"
    )
    client = TeradataDbClient(teradata_resource)
    connection = MagicMock()

    @contextmanager
    def fake_get_connection(self):
        yield connection

    with patch.object(TeradataResource, "get_connection", fake_get_connection):
        with client.connect(MagicMock(), make_table_slice()):
            pass
    connection.commit.assert_called_once()


def _patch_connection(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (1,)

    @contextmanager
    def fake_get_connection(self):
        yield connection

    monkeypatch.setattr(TeradataResource, "get_connection", fake_get_connection)
    return connection


def test_handle_output_and_load_input_round_trip(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset(key_prefix=["other_db"])
    def upstream() -> FakeDataFrame:
        return FakeDataFrame([1, 2])

    @asset
    def downstream(upstream: FakeDataFrame) -> FakeDataFrame:
        return FakeDataFrame(upstream.rows)

    result = materialize(
        [upstream, downstream],
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
    )
    assert result.success

    # asset key prefix wins over the resource database for `upstream`
    output_slices = {ts.table: ts for ts, _ in handler.handled_outputs}
    assert output_slices["upstream"].schema == "other_db"
    assert output_slices["upstream"].database is None
    # `downstream` has no prefix, so it falls back to the resource's database
    assert output_slices["downstream"].schema == "my_db"

    assert [ts.table for ts in handler.loaded_inputs] == ["upstream"]
    # load_input must resolve the same database the upstream asset was written to
    assert handler.loaded_inputs[0].schema == "other_db"
    assert handler.loaded_inputs[0].database is None


def test_missing_database_configuration_raises_actionable_error(
    monkeypatch, teradata_resource
):
    """Dagster's generic 'public' fallback is meaningless in Teradata."""
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset
    def my_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    no_database = TeradataResource(host="localhost", user="dbc", password="dbc")
    result = materialize(
        [my_asset],
        resources={"io_manager": MyIOManager(teradata=no_database)},
        raise_on_error=False,
    )
    assert not result.success
    failure = result.filter_events(lambda event: event.is_step_failure)[0]
    assert "Could not determine which Teradata database" in str(
        failure.step_failure_data.error
    )
    assert not handler.handled_outputs


def test_op_output_uses_resource_database(monkeypatch, teradata_resource):
    """Non-asset outputs have no asset key to derive a database from."""
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @op(out=Out(io_manager_key="io_manager"))
    def my_op() -> FakeDataFrame:
        return FakeDataFrame([1])

    @job(resource_defs={"io_manager": MyIOManager(teradata=teradata_resource)})
    def my_job():
        my_op()

    assert my_job.execute_in_process().success
    assert handler.handled_outputs[0][0].schema == "my_db"
    assert handler.handled_outputs[0][0].table == "result"


def test_multi_partitioned_asset_builds_all_dimensions(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    partitions_def = MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2024-01-01"),
            "color": StaticPartitionsDefinition(["red", "blue"]),
        }
    )

    @asset(
        partitions_def=partitions_def,
        metadata={"partition_expr": {"date": "ts", "color": "color_code"}},
    )
    def multi_asset_table() -> FakeDataFrame:
        return FakeDataFrame([1])

    assert materialize(
        [multi_asset_table],
        partition_key=MultiPartitionKey({"date": "2024-01-02", "color": "red"}),
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
    ).success

    exprs = {
        dim.partition_expr for dim in handler.handled_outputs[0][0].partition_dimensions
    }
    assert exprs == {"ts", "color_code"}


def test_multi_partitioned_asset_with_missing_expr_raises(
    monkeypatch, teradata_resource
):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    partitions_def = MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2024-01-01"),
            "color": StaticPartitionsDefinition(["red", "blue"]),
        }
    )

    @asset(
        partitions_def=partitions_def,
        metadata={"partition_expr": {"date": "ts"}},
    )
    def multi_asset_table() -> FakeDataFrame:
        return FakeDataFrame([1])

    result = materialize(
        [multi_asset_table],
        partition_key=MultiPartitionKey({"date": "2024-01-02", "color": "red"}),
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
        raise_on_error=False,
    )
    assert not result.success
    failure = result.filter_events(lambda event: event.is_step_failure)[0]
    assert "does not provide a column for every partition dimension" in str(
        failure.step_failure_data.error
    )


def test_time_window_partitioned_asset_generates_valid_sql(
    monkeypatch, teradata_resource
):
    connection = _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset(
        partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
        metadata={"partition_expr": "ts"},
    )
    def daily_table() -> FakeDataFrame:
        return FakeDataFrame([1])

    assert materialize(
        [daily_table],
        partition_key="2024-01-02",
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
    ).success

    cleanup_sql = (
        connection.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
    )
    assert cleanup_sql == (
        'DELETE FROM "my_db"."daily_table" WHERE\n'
        "(ts >= CAST('2024-01-02 00:00:00' AS TIMESTAMP(6)) AND "
        "ts < CAST('2024-01-03 00:00:00' AS TIMESTAMP(6)))"
    )


def test_partition_validation_applies_to_load_input(monkeypatch, teradata_resource):
    """The input path must be validated too, not just handle_output."""
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    io_manager = MyIOManager(teradata=teradata_resource).create_io_manager(MagicMock())

    partitions_def = MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2024-01-01"),
            "color": StaticPartitionsDefinition(["red", "blue"]),
        }
    )
    partition_key = MultiPartitionKey({"date": "2024-01-02", "color": "red"})

    input_context = MagicMock(spec=InputContext)
    input_context.dagster_type.typing_type = FakeDataFrame
    input_context.has_asset_key = True
    input_context.asset_key = AssetKey(["my_db", "upstream"])
    input_context.has_asset_partitions = True
    input_context.asset_partitions_def = partitions_def
    input_context.asset_partition_keys = [partition_key]
    input_context.asset_partition_key_range = PartitionKeyRange(
        start=partition_key, end=partition_key
    )
    input_context.definition_metadata = {}
    # the upstream mapping is missing an entry for the "color" dimension
    input_context.upstream_output.definition_metadata = {
        "partition_expr": {"date": "ts"}
    }

    with pytest.raises(
        ValueError, match="does not provide a column for every partition dimension"
    ):
        io_manager.load_input(input_context)
    assert not handler.loaded_inputs


def test_op_output_metadata_overrides_schema_and_table(monkeypatch, teradata_resource):
    """Ops select the database/table via output metadata, as in the Snowflake I/O manager."""
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @op(
        out=Out(
            io_manager_key="io_manager",
            metadata={"schema": "op_db", "table": "op_table"},
        )
    )
    def my_op() -> FakeDataFrame:
        return FakeDataFrame([1])

    @job(resource_defs={"io_manager": MyIOManager(teradata=teradata_resource)})
    def my_job():
        my_op()

    assert my_job.execute_in_process().success
    assert handler.handled_outputs[0][0].schema == "op_db"
    assert handler.handled_outputs[0][0].table == "op_table"


def test_columns_metadata_restricts_selected_columns(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset(key_prefix=["my_db"])
    def upstream() -> FakeDataFrame:
        return FakeDataFrame([1, 2])

    @asset(ins={"upstream": AssetIn("upstream", metadata={"columns": ["a"]})})
    def downstream(upstream: FakeDataFrame) -> FakeDataFrame:
        return FakeDataFrame(upstream.rows)

    assert materialize(
        [upstream, downstream],
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
    ).success

    loaded = handler.loaded_inputs[0]
    assert loaded.columns == ["a"]
    assert TeradataDbClient.get_select_statement(loaded) == (
        'SELECT "a" FROM "my_db"."upstream"'
    )


def test_io_manager_schema_config_takes_precedence(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset
    def my_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    assert materialize(
        [my_asset],
        resources={
            "io_manager": MyIOManager(teradata=teradata_resource, schema="config_db")
        },
    ).success
    assert handler.handled_outputs[0][0].schema == "config_db"


def test_metadata_schema_takes_precedence(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset(metadata={"schema": "metadata_db"})
    def my_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    assert materialize(
        [my_asset],
        resources={
            "io_manager": MyIOManager(teradata=teradata_resource, schema="config_db")
        },
    ).success
    assert handler.handled_outputs[0][0].schema == "metadata_db"


def test_partitioned_asset_builds_partition_dimensions(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset(
        partitions_def=StaticPartitionsDefinition(["red", "blue"]),
        metadata={"partition_expr": "color"},
    )
    def partitioned_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    assert materialize(
        [partitioned_asset],
        partition_key="red",
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
    ).success

    table_slice = handler.handled_outputs[0][0]
    assert table_slice.partition_dimensions[0].partition_expr == "color"
    assert table_slice.partition_dimensions[0].partitions == ["red"]


def test_unsupported_type_raises(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [FakeDataFrameTypeHandler()]

    @asset
    def string_asset() -> str:
        return "hello"

    result = materialize(
        [string_asset],
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
        raise_on_error=False,
    )
    assert not result.success

    failure = next(
        event for event in result.all_events if event.event_type_value == "STEP_FAILURE"
    )
    message = failure.event_specific_data.error.cause.message
    assert "TeradataIOManager does not have a handler for type" in message


def test_build_teradata_io_manager(monkeypatch):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()
    io_manager_def = build_teradata_io_manager([handler])

    @asset
    def my_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    defs = Definitions(
        assets=[my_asset],
        resources={
            "io_manager": io_manager_def.configured(
                {
                    "teradata": {
                        "host": "localhost",
                        "user": "dbc",
                        "password": "dbc",
                        "database": "my_db",
                    },
                    "schema": "configured_db",
                }
            )
        },
    )

    assert defs.resolve_implicit_global_asset_job_def().execute_in_process().success
    assert handler.handled_outputs[0][0].schema == "configured_db"
    assert handler.handled_outputs[0][0].table == "my_asset"


def test_build_teradata_io_manager_requires_teradata_config():
    """Omitting the nested connection config must fail config validation, not at runtime."""
    io_manager_def = build_teradata_io_manager([FakeDataFrameTypeHandler()])

    @asset
    def my_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    defs = Definitions(
        assets=[my_asset],
        resources={"io_manager": io_manager_def.configured({"schema": "some_db"})},
    )
    with pytest.raises(DagsterInvalidConfigError):
        defs.resolve_implicit_global_asset_job_def().execute_in_process()


def test_asset_key_resolution_uses_last_component(monkeypatch, teradata_resource):
    _patch_connection(monkeypatch)
    handler = FakeDataFrameTypeHandler()

    class MyIOManager(TeradataIOManager):
        @staticmethod
        def type_handlers():
            return [handler]

    @asset(key=AssetKey(["a", "b", "c"]))
    def nested_asset() -> FakeDataFrame:
        return FakeDataFrame([1])

    assert materialize(
        [nested_asset],
        resources={"io_manager": MyIOManager(teradata=teradata_resource)},
    ).success
    assert handler.handled_outputs[0][0].table == "c"
    assert handler.handled_outputs[0][0].schema == "b"


def test_count_statement_scopes_to_the_partition_under_an_access_lock():
    table_slice = make_table_slice(
        partition_dimensions=[
            TablePartitionDimension(partition_expr="color", partitions=["red"])
        ]
    )
    assert TeradataDbClient.get_count_statement(table_slice) == (
        'LOCKING TABLE "my_db"."my_table" FOR ACCESS '
        'SELECT COUNT(*) FROM "my_db"."my_table" WHERE\n(color IN (\'red\'))'
    )
