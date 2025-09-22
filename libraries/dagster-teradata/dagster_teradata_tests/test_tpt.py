import pytest
from dagster import job, op, DagsterError
from dagster_teradata import TeradataResource
from unittest import mock
import tempfile
import os


class TestTpt:
    def test_ddl_operator_basic_execution(self):
        """Test basic DDL operator execution."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_ddl_operator(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.DdlOperator.ddl_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.ddl_operator(
                    ddl=["CREATE TABLE test (id INT);", "DROP TABLE test;"]
                )
                assert result == 0
                mock_exec.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_ddl_operator()

        example_job.execute_in_process()

    def test_ddl_operator_with_error_list(self):
        """Test DDL operator with custom error list."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_ddl_error_list(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.DdlOperator.ddl_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.ddl_operator(
                    ddl=["CREATE TABLE test (id INT);"],
                    error_list=[3807, 3808],  # Common Teradata error codes
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_ddl_error_list()

        example_job.execute_in_process()

    def test_ddl_operator_invalid_input(self):
        """Test DDL operator with invalid input."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_invalid_ddl(context):
            with pytest.raises(
                ValueError,
                match="ddl parameter must be a non-empty list of non-empty strings representing DDL statements.",
            ):
                context.resources.teradata.ddl_operator(ddl=[])

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_invalid_ddl()

        example_job.execute_in_process()

    def test_tdload_operator_file_to_table(self):
        """Test TPT file to table operation."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_file_to_table(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_file_name="/path/to/source.csv",
                    target_table="target_table",
                    source_format="Delimited",
                    source_text_delimiter=",",
                )
                assert result == 0
                mock_exec.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_file_to_table()

        example_job.execute_in_process()

    def test_tdload_operator_table_to_file(self):
        """Test TPT table to file operation."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_table_to_file(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/path/to/target.csv",
                    target_format="Delimited",
                    target_text_delimiter="|",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_table_to_file()

        example_job.execute_in_process()

    def test_tdload_operator_table_to_table(self):
        """Test TPT table to table operation."""
        mock_source_resource = TeradataResource(
            host="source_host",
            user="source_user",
            password="source_password",
        )
        TeradataResource(
            host="target_host",
            user="target_user",
            password="target_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_table_to_table(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_table="target_table",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_source_resource})
        def example_job():
            example_test_table_to_table()

        example_job.execute_in_process()

    def test_tdload_operator_with_select_stmt(self):
        """Test TPT operation with custom SELECT statement."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_select_stmt(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    select_stmt="SELECT id, name FROM source_table WHERE active = 1",
                    target_file_name="/path/to/output.csv",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_select_stmt()

        example_job.execute_in_process()

    def test_tdload_operator_with_insert_stmt(self):
        """Test TPT operation with custom INSERT statement."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_insert_stmt(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_file_name="/path/to/source.csv",
                    target_table="target_table",
                    insert_stmt="INSERT INTO target_table (col1, col2) VALUES (?, ?)",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_insert_stmt()

        example_job.execute_in_process()

    def test_tdload_operator_invalid_combination(self):
        """Test TPT operator with invalid parameter combination."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_invalid_combo(context):
            with pytest.raises(ValueError):
                context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    select_stmt="SELECT * FROM source_table",
                    target_file_name="/path/to/output.csv",
                )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_invalid_combo()

        example_job.execute_in_process()

    def test_tdload_operator_remote_execution(self):
        """Test TPT remote execution."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_remote(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/path/to/output.csv",
                    remote_host="remote_host",
                    remote_user="remote_user",
                    remote_password="remote_password",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_remote()

        example_job.execute_in_process()

    def test_tdload_operator_with_job_var_file(self):
        """Test TPT operation with pre-defined job variable file."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_job_var_file(context):
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
                tmp_file.write("SourceTable='test_table'\nTargetFile='output.csv'")
                tmp_path = tmp_file.name

            try:
                with mock.patch(
                    "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
                ) as mock_exec:
                    mock_exec.return_value = 0
                    result = context.resources.teradata.tdload_operator(
                        tdload_job_var_file=tmp_path
                    )
                    assert result == 0
            finally:
                os.unlink(tmp_path)

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_job_var_file()

        example_job.execute_in_process()

    def test_tdload_operator_custom_options(self):
        """Test TPT operation with custom options."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_custom_options(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/path/to/output.csv",
                    tdload_options="-j 4 -m 1000",  # 4 jobs, 1000 rows per message
                    tdload_job_name="custom_job_name",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_custom_options()

        example_job.execute_in_process()

    def test_ddl_operator_remote_execution(self):
        """Test DDL operator remote execution."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_ddl_remote(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.DdlOperator.ddl_operator"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.ddl_operator(
                    ddl=["CREATE TABLE remote_test (id INT);"],
                    remote_host="remote_host",
                    remote_user="remote_user",
                    remote_password="remote_password",
                )
                assert result == 0

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_ddl_remote()

        example_job.execute_in_process()

    def test_operator_timeout_handling(self):
        """Test operator timeout handling."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_timeout(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.TdLoadOperator.tdload_operator"
            ) as mock_exec:
                mock_exec.side_effect = DagsterError("TPT operation timed out")
                with pytest.raises(DagsterError, match="timed out"):
                    context.resources.teradata.tdload_operator(
                        source_table="large_table",
                        target_file_name="/path/to/large_output.csv",
                    )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_timeout()

        example_job.execute_in_process()

    def test_operator_error_recovery(self):
        """Test operator error recovery with acceptable error codes."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_test_error_recovery(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.DdlOperator.ddl_operator"
            ) as mock_exec:
                mock_exec.return_value = 3807  # Table already exists error
                result = context.resources.teradata.ddl_operator(
                    ddl=["CREATE TABLE test (id INT);"],
                    error_list=[3807],  # Accept table exists error
                )
                assert result == 3807  # Should return without raising exception

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_error_recovery()

        example_job.execute_in_process()
