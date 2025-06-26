import pytest
from dagster import job, op, DagsterError
from dagster_teradata import TeradataResource
from unittest import mock
import tempfile
import os


class TestBteq:
    def test_local_bteq_script_execution(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test basic local BTEQ script execution."""

        @op(required_resource_keys={"teradata"})
        def example_test_local_script(context):
            with mock.patch(
                "dagster_teradata.ttu.bteq.Bteq.execute_bteq_script_at_local"
            ) as mock_exec:
                mock_exec.return_value = 0
                result = context.resources.teradata.bteq_operator(
                    sql="SELECT * FROM dbc.dbcinfo;"
                )
                assert result == 0
                mock_exec.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_local_script()

        example_job.execute_in_process()

    def test_local_file_execution(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test BTEQ execution with local file."""

        @op(required_resource_keys={"teradata"})
        def example_test_local_file(context):
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
                tmp_file.write("SELECT * FROM dbc.dbcinfo;")
                tmp_path = tmp_file.name

            try:
                with mock.patch(
                    "dagster_teradata.ttu.bteq.Bteq._handle_local_bteq_file"
                ) as mock_exec:
                    mock_exec.return_value = 0
                    result = context.resources.teradata.bteq_operator(
                        file_path=tmp_path
                    )
                    assert result == 0
                    mock_exec.assert_called_once_with(file_path=tmp_path)
            finally:
                os.unlink(tmp_path)

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_local_file()

        example_job.execute_in_process()

    def test_invalid_file_path(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test error handling for invalid file path."""

        @op(required_resource_keys={"teradata"})
        def example_test_invalid_file(context):
            with pytest.raises(ValueError, match="invalid or does not exist"):
                context.resources.teradata.bteq_operator(file_path="/invalid/path.sql")

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_invalid_file()

        example_job.execute_in_process()

    def test_missing_credentials(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test error handling for missing remote credentials."""

        @op(required_resource_keys={"teradata"})
        def example_test_missing_creds(context):
            with pytest.raises(ValueError):
                context.resources.teradata.bteq_operator(
                    sql="SELECT * FROM dbc.dbcinfo;", remote_host="remote_host"
                )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_missing_creds()

        example_job.execute_in_process()

    def test_return_code_handling(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test custom return code handling."""

        @op(required_resource_keys={"teradata"})
        def example_test_return_codes(context):
            with mock.patch(
                "dagster_teradata.ttu.bteq.Bteq.execute_bteq_script_at_local"
            ) as mock_exec:
                mock_exec.return_value = 8
                result = context.resources.teradata.bteq_operator(
                    sql="DELETE FROM non_existent_table;", bteq_quit_rc=[0, 8]
                )
                assert result == 8

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_return_codes()

        example_job.execute_in_process()

    def test_timeout_handling(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test timeout handling."""

        @op(required_resource_keys={"teradata"})
        def example_test_timeout(context):
            with mock.patch(
                "dagster_teradata.ttu.bteq.Bteq.execute_bteq_script_at_local"
            ) as mock_exec:
                mock_exec.side_effect = DagsterError("Timeout")
                with pytest.raises(DagsterError, match="Timeout"):
                    context.resources.teradata.bteq_operator(
                        sql="SELECT * FROM large_table;", timeout=1
                    )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_timeout()

        example_job.execute_in_process()

    def test_no_sql_or_file(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test error when neither SQL nor file is provided."""

        @op(required_resource_keys={"teradata"})
        def example_test_no_input(context):
            with pytest.raises(
                ValueError, match="requires either the 'sql' or 'file_path' parameter"
            ):
                context.resources.teradata.bteq_operator()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_no_input()

        example_job.execute_in_process()

    def test_encoding_validation(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test file encoding validation."""

        @op(required_resource_keys={"teradata"})
        def example_test_encoding(context):
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
                tmp_file.write("SELECT * FROM dbc.dbcinfo;")
                tmp_path = tmp_file.name

            try:
                with mock.patch(
                    "dagster_teradata.ttu.bteq.is_valid_encoding"
                ) as mock_encoding:
                    mock_encoding.side_effect = UnicodeDecodeError(
                        "utf8", b"", 0, 1, "error"
                    )
                    with pytest.raises(ValueError, match="encoding is different"):
                        context.resources.teradata.bteq_operator(
                            file_path=tmp_path, bteq_script_encoding="UTF-8"
                        )
            finally:
                os.unlink(tmp_path)

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_encoding()

        example_job.execute_in_process()

    def test_timeout_with_custom_rc(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test timeout with custom return code handling."""

        @op(required_resource_keys={"teradata"})
        def example_test_timeout_rc(context):
            with mock.patch(
                "dagster_teradata.ttu.bteq.Bteq.execute_bteq_script_at_local"
            ) as mock_exec:
                mock_exec.return_value = 99
                result = context.resources.teradata.bteq_operator(
                    sql="SELECT * FROM large_table;",
                    timeout=30,
                    timeout_rc=99,
                    bteq_quit_rc=[99],
                )
                assert result == 99

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_timeout_rc()

        example_job.execute_in_process()

    def test_return_code_not_in_quit_rc(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test when return code is not in allowed quit codes."""

        @op(required_resource_keys={"teradata"})
        def example_test_unexpected_rc(context):
            with mock.patch(
                "dagster_teradata.ttu.bteq.Bteq.execute_bteq_script_at_local"
            ) as mock_exec:
                mock_exec.return_value = 42
                result = context.resources.teradata.bteq_operator(
                    sql="SELECT * FROM dbc.dbcinfo;", bteq_quit_rc=[0, 1]
                )
                assert (
                    result == 42
                )  # Still returns the code even though it's not in quit_rc

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_unexpected_rc()

        example_job.execute_in_process()

    def test_conflicting_ssh_auth(self):
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )
        """Test error when both password and key auth are provided."""

        @op(required_resource_keys={"teradata"})
        def example_test_conflicting_auth(context):
            with pytest.raises(ValueError):
                context.resources.teradata.bteq_operator(
                    sql="SELECT * FROM dbc.dbcinfo;",
                    remote_host="remote_host",
                    remote_user="remote_user",
                    remote_password="password",
                    ssh_key_path="/path/to/key",
                )

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_test_conflicting_auth()

        example_job.execute_in_process()
