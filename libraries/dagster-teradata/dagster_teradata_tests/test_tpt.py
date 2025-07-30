import pytest
from dagster import job, op, DagsterError
from dagster_teradata import TeradataResource
from unittest import mock
import tempfile
import os


class TestTpt:
    def test_file_to_table_mode(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
            # host="test-db-t3gwet5u19m2zwm9.env.clearscape.teradata.com",
            # user="demo_user",
            # password="mt255026",
        )

        @op(required_resource_keys={"teradata"})
        def example_file_to_table(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.execute_tdload"
            ) as mock_execute:
                mock_execute.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_file_name="/path/to/data.csv",
                    target_table="target_db.target_table",
                )
                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_file_to_table()

        example_job.execute_in_process()

    def test_table_to_file_mode(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_table_to_file(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.execute_tdload"
            ) as mock_execute:
                mock_execute.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_table="source_db.source_table",
                    target_file_name="/path/to/export.csv",
                )
                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_table_to_file()

        example_job.execute_in_process()

    def test_table_to_table_mode(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_table_to_table(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.execute_tdload"
            ) as mock_execute:
                mock_execute.return_value = 0
                result = context.resources.teradata.tdload_operator(
                    source_table="source_db.source_table",
                    target_table="target_db.target_table",
                )
                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_table_to_table()

        example_job.execute_in_process()

    def test_invalid_parameter_combinations(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_invalid_params(context):
            with pytest.raises(ValueError, match="Invalid parameter combination"):
                context.resources.teradata.tdload_operator()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_invalid_params()

        example_job.execute_in_process()

    def test_ddl_operator_execution(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_ddl_operator(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_ddl") as mock_execute:
                mock_execute.return_value = 0
                result = context.resources.teradata.ddl_operator(
                    ddl=["CREATE TABLE test_db.test_table (id INT)"]
                )
                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_ddl_operator()

        example_job.execute_in_process()

    def test_ddl_operator_error_handling(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_ddl_error(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_ddl") as mock_execute:
                mock_execute.side_effect = DagsterError("DDL execution failed")
                with pytest.raises(DagsterError, match="DDL execution failed"):
                    context.resources.teradata.ddl_operator(
                        ddl=["DROP TABLE non_existent_table"]
                    )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_ddl_error()

        example_job.execute_in_process()

    def test_tpt_timeout_handling(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_tpt_timeout(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.execute_tdload"
            ) as mock_execute:
                mock_execute.side_effect = DagsterError("Timeout")
                with pytest.raises(DagsterError, match="Timeout"):
                    context.resources.teradata.tdload_operator(
                        source_table="large_table",
                        target_file_name="/path/to/export.csv",
                    )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_tpt_timeout()

        example_job.execute_in_process()

    def test_custom_return_code_handling(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_custom_rc(context):
            with mock.patch(
                "dagster_teradata.ttu.tpt.execute_tdload"
            ) as mock_execute:
                mock_execute.return_value = 8
                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/path/to/export.csv",
                )
                assert result == 8

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_custom_rc()

        example_job.execute_in_process()

