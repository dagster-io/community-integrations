import pytest
from dagster import job, op, DagsterError
from dagster_teradata import TeradataResource
from unittest import mock
import paramiko
from dagster_teradata.ttu.utils.tpt_util import (
    prepare_tpt_ddl_script,
    prepare_tdload_job_var_file,
    execute_remote_command
)
from dagster_teradata.ttu.utils.encryption_utils import SecureCredentialManager
from dagster_teradata.ttu.utils.util import _setup_ssh_connection


class TestTpt:
    # Your existing tests here...

    def test_ssh_connection_success(self):
        """Test successful SSH connection establishment."""
        with mock.patch('paramiko.SSHClient') as mock_ssh:
            mock_client = mock.MagicMock()
            mock_ssh.return_value = mock_client

            result = _setup_ssh_connection(
                host="test-host",
                user="test-user",
                password="test-password",
                key_path=None,
                port=22,
                log=mock.MagicMock()
            )

            assert result == mock_client
            # Check that connect was called with the expected arguments (positional)
            mock_client.connect.assert_called_once_with(
                "test-host",  # hostname
                port=22,           # port
                username="test-user",
                password="test-password"
            )

    def test_ssh_connection_with_key(self):
        """Test SSH connection with key authentication."""
        with mock.patch('paramiko.SSHClient') as mock_ssh, \
                mock.patch('paramiko.RSAKey.from_private_key_file') as mock_key:
            mock_client = mock.MagicMock()
            mock_ssh.return_value = mock_client
            mock_key.return_value = "test-key"

            result = _setup_ssh_connection(
                host="test-host",
                user="test-user",
                password=None,
                key_path="/path/to/key",
                port=22,
                log=mock.MagicMock()
            )

            assert result == mock_client
            mock_client.connect.assert_called_with(
                hostname="test-host",
                port=22,
                username="test-user",
                pkey="test-key"
            )

    def test_ssh_connection_with_key(self):
        """Test SSH connection with key authentication."""
        with mock.patch('paramiko.SSHClient') as mock_ssh, \
                mock.patch('paramiko.RSAKey.from_private_key_file') as mock_key:
            mock_client = mock.MagicMock()
            mock_ssh.return_value = mock_client
            mock_key.return_value = "test-key"

            result = _setup_ssh_connection(
                host="test-host",
                user="test-user",
                password=None,
                key_path="/path/to/key",
                port=22,
                log=mock.MagicMock()
            )

            assert result == mock_client
            mock_client.connect.assert_called_with(
                "test-host",  # hostname
                port=22,  # port
                username="test-user",
                pkey="test-key"
            )

    def test_prepare_tpt_ddl_script(self):
        """Test TPT DDL script preparation."""
        mock_conn = mock.MagicMock()
        mock_conn.host = "test-host"
        mock_conn.user = "test-user"
        mock_conn.password = "test-password"

        sql = ["CREATE TABLE test_table (id INT)", "DROP TABLE test_table"]
        error_list = [3807]

        script = prepare_tpt_ddl_script(sql, error_list, mock_conn, "test_job")

        assert "test_job" in script
        assert "test-host" in script
        assert "test-user" in script
        assert "3807" in script
        assert "CREATE TABLE test_table (id INT)" in script
        assert "DROP TABLE test_table" in script

    def test_prepare_tdload_job_var_file(self):
        """Test TDLoad job variable file preparation."""
        mock_source_conn = mock.MagicMock()
        mock_source_conn.host = "source-host"
        mock_source_conn.user = "source-user"
        mock_source_conn.password = "source-password"

        mock_target_conn = mock.MagicMock()
        mock_target_conn.host = "target-host"
        mock_target_conn.user = "target-user"
        mock_target_conn.password = "target-password"

        # Test file_to_table mode
        job_vars = prepare_tdload_job_var_file(
            mode="file_to_table",
            source_table=None,
            select_stmt=None,
            insert_stmt="INSERT INTO test_table VALUES (?)",
            target_table="test_table",
            source_file_name="/tmp/source.csv",
            target_file_name=None,
            source_format="Delimited",
            target_format="Delimited",
            source_text_delimiter=",",
            target_text_delimiter=",",
            source_conn=mock_source_conn,
            target_conn=mock_target_conn
        )

        assert "TargetTdpId" in job_vars
        assert "TargetTable" in job_vars
        assert "SourceFileName" in job_vars
        assert "InsertStmt" in job_vars

    def test_secure_credential_manager(self):
        """Test secure credential encryption and decryption."""
        manager = SecureCredentialManager()

        test_data = "sensitive_password_123!"
        encrypted = manager.encrypt(test_data)
        decrypted = manager.decrypt(encrypted)

        assert decrypted == test_data
        assert encrypted != test_data

    def test_execute_remote_command(self):
        """Test remote command execution via SSH."""
        mock_ssh = mock.MagicMock()
        mock_stdin = mock.MagicMock()
        mock_stdout = mock.MagicMock()
        mock_stderr = mock.MagicMock()
        mock_channel = mock.MagicMock()

        mock_ssh.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value = b"command output"
        mock_stderr.read.return_value = b""

        exit_status, stdout, stderr = execute_remote_command(mock_ssh, "test command")

        assert exit_status == 0
        assert stdout == "command output"
        assert stderr == ""
        mock_ssh.exec_command.assert_called_once_with("test command")

    def test_remote_tpt_execution(self):
        """Test remote TPT execution via SSH."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_remote_tpt(context):
            with mock.patch("dagster_teradata.ttu.tpt._setup_ssh_connection") as mock_ssh, \
                    mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute, \
                    mock.patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os") as mock_get_os:
                mock_ssh_client = mock.MagicMock()
                mock_ssh.return_value = mock_ssh_client
                mock_execute.return_value = 0
                mock_get_os.return_value = "unix"

                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/remote/path/export.csv",
                    remote_host="remote-host",
                    remote_user="remote-user",
                    remote_password="remote-password",
                    remote_port=22
                )

                assert result == 0
                mock_ssh.assert_called_once()
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_remote_tpt()

        example_job.execute_in_process()

    def test_tpt_with_custom_options(self):
        """Test TPT with custom options."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_custom_options(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_execute.return_value = 0

                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/path/to/export.csv",
                    tdload_options="-m 1000000 -e 1000",
                    tdload_job_name="custom_job"
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_custom_options()

        example_job.execute_in_process()

    def test_tpt_with_job_var_file(self):
        """Test TPT with pre-defined job variable file."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_job_var_file(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute, \
                    mock.patch("dagster_teradata.ttu.utils.tpt_util.is_valid_file") as mock_is_valid:
                mock_execute.return_value = 0
                mock_is_valid.return_value = True

                result = context.resources.teradata.tdload_operator(
                    tdload_job_var_file="/path/to/job_vars.txt"
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_job_var_file()

        example_job.execute_in_process()

    def test_tpt_error_list_handling(self):
        """Test TPT error list handling."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_error_list(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_ddl") as mock_execute:
                mock_execute.return_value = 0

                result = context.resources.teradata.ddl_operator(
                    ddl=["CREATE TABLE test_table (id INT)"],
                    error_list=[3807, 3802]  # Table exists, database doesn't exist
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_error_list()

        example_job.execute_in_process()

    def test_tpt_file_format_handling(self):
        """Test TPT with different file formats."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_file_formats(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_execute.return_value = 0

                # Test with different formats and delimiters
                result = context.resources.teradata.tdload_operator(
                    source_file_name="/path/to/data.csv",
                    target_table="target_table",
                    source_format="Delimited",
                    target_format="Delimited",
                    source_text_delimiter="|",
                    target_text_delimiter="|"
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_file_formats()

        example_job.execute_in_process()

    def test_tpt_select_statement(self):
        """Test TPT with custom SELECT statement."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_select_stmt(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_execute.return_value = 0

                result = context.resources.teradata.tdload_operator(
                    select_stmt="SELECT id, name FROM source_table WHERE active = 1",
                    target_file_name="/path/to/export.csv"
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_select_stmt()

        example_job.execute_in_process()

    def test_tpt_insert_statement(self):
        """Test TPT with custom INSERT statement."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_insert_stmt(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_execute.return_value = 0

                result = context.resources.teradata.tdload_operator(
                    source_file_name="/path/to/data.csv",
                    target_table="target_table",
                    insert_stmt="INSERT INTO target_table VALUES (?, ?, ?)"
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_insert_stmt()

        example_job.execute_in_process()

    def test_tpt_different_source_target_connections(self):
        """Test TPT with different source and target connections."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_diff_connections(context):
            with mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_execute.return_value = 0

                # This would typically use a different target connection resource
                result = context.resources.teradata.tdload_operator(
                    source_table="source_db.source_table",
                    target_table="target_db.target_table"
                )

                assert result == 0
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_diff_connections()

        example_job.execute_in_process()

    def test_tpt_utility_verification(self):
        """Test TPT utility verification on remote host."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_utility_verify(context):
            with mock.patch("dagster_teradata.ttu.tpt._setup_ssh_connection") as mock_ssh, \
                    mock.patch("dagster_teradata.ttu.utils.tpt_util.verify_tpt_utility_on_remote_host") as mock_verify, \
                    mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_ssh_client = mock.MagicMock()
                mock_ssh.return_value = mock_ssh_client
                mock_verify.return_value = True
                mock_execute.return_value = 0

                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/remote/path/export.csv",
                    remote_host="remote-host",
                    remote_user="remote-user"
                )

                assert result == 0
                mock_verify.assert_called_once()
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_utility_verify()

        example_job.execute_in_process()

    def test_tpt_encrypted_file_transfer(self):
        """Test encrypted file transfer for TPT operations."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_encrypted_transfer(context):
            with mock.patch("dagster_teradata.ttu.tpt._setup_ssh_connection") as mock_ssh, \
                    mock.patch("dagster_teradata.ttu.utils.encryption_utils.generate_random_password") as mock_gen_pass, \
                    mock.patch("dagster_teradata.ttu.utils.encryption_utils.generate_encrypted_file_with_openssl") as mock_encrypt, \
                    mock.patch("dagster_teradata.ttu.utils.tpt_util.transfer_file_sftp") as mock_transfer, \
                    mock.patch("dagster_teradata.ttu.utils.encryption_utils.decrypt_remote_file") as mock_decrypt, \
                    mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute:
                mock_ssh_client = mock.MagicMock()
                mock_ssh.return_value = mock_ssh_client
                mock_gen_pass.return_value = "temp_password"
                mock_execute.return_value = 0

                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/remote/path/export.csv",
                    remote_host="remote-host",
                    remote_user="remote-user"
                )

                assert result == 0
                mock_gen_pass.assert_called_once()
                mock_encrypt.assert_called_once()
                mock_transfer.assert_called_once()
                mock_decrypt.assert_called_once()
                mock_execute.assert_called_once()

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_encrypted_transfer()

        example_job.execute_in_process()

    def test_tpt_secure_file_deletion(self):
        """Test secure file deletion after TPT operations."""
        mock_td_resource = TeradataResource(
            host="mock_host",
            user="mock_user",
            password="mock_password",
        )

        @op(required_resource_keys={"teradata"})
        def example_secure_delete(context):
            with mock.patch("dagster_teradata.ttu.tpt._setup_ssh_connection") as mock_ssh, \
                    mock.patch("dagster_teradata.ttu.tpt.execute_tdload") as mock_execute, \
                    mock.patch("dagster_teradata.ttu.utils.tpt_util.secure_delete") as mock_secure_delete, \
                    mock.patch("dagster_teradata.ttu.utils.tpt_util.remote_secure_delete") as mock_remote_delete:
                mock_ssh_client = mock.MagicMock()
                mock_ssh.return_value = mock_ssh_client
                mock_execute.return_value = 0

                result = context.resources.teradata.tdload_operator(
                    source_table="source_table",
                    target_file_name="/remote/path/export.csv",
                    remote_host="remote-host",
                    remote_user="remote-user"
                )

                assert result == 0
                # Verify secure deletion was attempted
                assert mock_secure_delete.called or mock_remote_delete.called

        @job(resource_defs={"teradata": mock_td_resource})
        def example_job():
            example_secure_delete()

        example_job.execute_in_process()