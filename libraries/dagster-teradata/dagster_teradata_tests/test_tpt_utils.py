from __future__ import annotations

import os
import stat
import unittest
from unittest.mock import MagicMock, patch

import pytest

from dagster import DagsterError

from dagster_teradata.resources import TeradataResource
from dagster_teradata.ttu.utils.tpt_util import (
    execute_remote_command,
    get_remote_os,
    get_remote_temp_directory,
    is_valid_remote_job_var_file,
    prepare_tpt_ddl_script,
    prepare_tdload_job_var_file,
    read_file,
    secure_delete,
    set_local_file_permissions,
    set_remote_file_permissions,
    transfer_file_sftp,
    verify_tpt_utility_on_remote_host,
    TPTConfig,
)


class TestTptUtils:
    def test_execute_remote_command_success(self):
        """Test successful remote command execution."""
        ssh_client = MagicMock()
        stdin = MagicMock()
        stdout = MagicMock()
        stderr = MagicMock()
        stdout.channel.recv_exit_status.return_value = 0
        stdout.read.return_value = b"command output"
        stderr.read.return_value = b""
        ssh_client.exec_command.return_value = (stdin, stdout, stderr)

        exit_status, stdout_data, stderr_data = execute_remote_command(
            ssh_client, "test command"
        )

        assert exit_status == 0
        assert stdout_data == "command output"
        assert stderr_data == ""
        ssh_client.exec_command.assert_called_once_with("test command")

    def test_execute_remote_command_failure(self):
        """Test remote command execution with failure."""
        ssh_client = MagicMock()
        stdin = MagicMock()
        stdout = MagicMock()
        stderr = MagicMock()
        stdout.channel.recv_exit_status.return_value = 1
        stdout.read.return_value = b""
        stderr.read.return_value = b"command failed"
        ssh_client.exec_command.return_value = (stdin, stdout, stderr)

        exit_status, stdout_data, stderr_data = execute_remote_command(
            ssh_client, "failing command"
        )

        assert exit_status == 1
        assert stdout_data == ""
        assert stderr_data == "command failed"

    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_get_remote_os_windows(self, mock_execute):
        """Test Windows OS detection."""
        mock_execute.return_value = (0, "Windows_NT", "")
        ssh_client = MagicMock()

        result = get_remote_os(ssh_client)

        assert result == "windows"
        mock_execute.assert_called_once_with(ssh_client, "echo %OS%")

    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_get_remote_os_unix(self, mock_execute):
        """Test Unix/Linux OS detection."""
        mock_execute.return_value = (0, "Linux", "")
        ssh_client = MagicMock()

        result = get_remote_os(ssh_client)

        assert result == "unix"

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_get_remote_temp_directory_windows(self, mock_execute, mock_get_os):
        """Test Windows temp directory detection."""
        mock_get_os.return_value = "windows"
        mock_execute.return_value = (0, "C:\\Windows\\Temp", "")
        ssh_client = MagicMock()

        result = get_remote_temp_directory(ssh_client)

        assert result == "C:\\Windows\\Temp"
        mock_execute.assert_called_once_with(ssh_client, "echo %TEMP%")

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    def test_get_remote_temp_directory_unix(self, mock_get_os):
        """Test Unix temp directory detection."""
        mock_get_os.return_value = "unix"
        ssh_client = MagicMock()

        result = get_remote_temp_directory(ssh_client)

        assert result == "/tmp"

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_get_remote_temp_directory_fallback(self, mock_execute, mock_get_os):
        """Test temp directory fallback when detection fails."""
        mock_get_os.return_value = "windows"
        mock_execute.return_value = (1, "%TEMP%", "error")
        ssh_client = MagicMock()

        result = get_remote_temp_directory(ssh_client)

        assert result == TPTConfig.TEMP_DIR_WINDOWS

    def test_prepare_tpt_ddl_script_basic(self):
        """Test basic TPT DDL script preparation."""
        teradata_connection_resource = TeradataResource(
            host="myhost", user="user", password="pass"
        )
        sql_statements = [
            "CREATE TABLE test1 (id INT)",
            "CREATE TABLE test2 (name VARCHAR(50))",
        ]

        script = prepare_tpt_ddl_script(
            sql=sql_statements,
            error_list=[3807],
            teradata_connection_resource=teradata_connection_resource,
            job_name="test_job",
        )

        assert "DEFINE JOB test_job" in script
        assert "CREATE TABLE test1 (id INT)" in script
        assert "CREATE TABLE test2 (name VARCHAR(50))" in script
        assert "TdpId = 'myhost'" in script
        assert "UserName = 'user'" in script
        assert "UserPassword = 'pass'" in script
        assert "ErrorList = ['3807']" in script

    def test_prepare_tpt_ddl_script_empty_sql(self):
        """Test TPT DDL script preparation with empty SQL."""
        teradata_connection_resource = TeradataResource(
            host="myhost", user="user", password="pass"
        )

        with pytest.raises(ValueError, match="non-empty list"):
            prepare_tpt_ddl_script(
                sql=[],
                error_list=None,
                teradata_connection_resource=teradata_connection_resource,
            )

    def test_prepare_tpt_ddl_script_no_error_list(self):
        """Test TPT DDL script preparation without error list."""
        teradata_connection_resource = TeradataResource(
            host="myhost", user="user", password="pass"
        )
        sql_statements = ["CREATE TABLE test (id INT)"]

        script = prepare_tpt_ddl_script(
            sql=sql_statements,
            error_list=None,
            teradata_connection_resource=teradata_connection_resource,
        )

        assert "ErrorList = ['']" in script

    def test_prepare_tdload_job_var_file_file_to_table(self):
        """Test job variable file preparation for file-to-table mode."""
        source_conn = TeradataResource(host="source_host", user="user", password="pass")

        content = prepare_tdload_job_var_file(
            mode="file_to_table",
            source_table=None,
            select_stmt=None,
            insert_stmt="INSERT INTO target VALUES (?)",
            target_table="target_table",
            source_file_name="/path/to/source.csv",
            target_file_name=None,
            source_format="Delimited",
            target_format="Delimited",
            source_text_delimiter=",",
            target_text_delimiter="|",
            source_conn=source_conn,
            target_conn=None,
        )

        assert "TargetTdpId='source_host'" in content
        assert "TargetUserName='user'" in content
        assert "TargetUserPassword='pass'" in content
        assert "TargetTable='target_table'" in content
        assert "SourceFileName='/path/to/source.csv'" in content
        assert "InsertStmt='INSERT INTO target VALUES (?)'" in content
        assert "SourceFormat='Delimited'" in content
        assert "TargetFormat='Delimited'" in content

    def test_prepare_tdload_job_var_file_table_to_file(self):
        """Test job variable file preparation for table-to-file mode."""
        source_conn = TeradataResource(host="source_host", user="user", password="pass")

        content = prepare_tdload_job_var_file(
            mode="table_to_file",
            source_table="source_table",
            select_stmt=None,
            insert_stmt=None,
            target_table=None,
            source_file_name=None,
            target_file_name="/path/to/target.csv",
            source_format="Delimited",
            target_format="Delimited",
            source_text_delimiter=",",
            target_text_delimiter="|",
            source_conn=source_conn,
            target_conn=None,
        )

        assert "SourceTdpId='source_host'" in content
        assert "SourceUserName='user'" in content
        assert "SourceUserPassword='pass'" in content
        assert "SourceTable='source_table'" in content
        assert "TargetFileName='/path/to/target.csv'" in content

    def test_prepare_tdload_job_var_file_table_to_table(self):
        """Test job variable file preparation for table-to-table mode."""
        source_conn = TeradataResource(host="source_host", user="user", password="pass")
        target_conn = TeradataResource(
            host="target_host", user="user2", password="pass2"
        )

        content = prepare_tdload_job_var_file(
            mode="table_to_table",
            source_table="source_table",
            select_stmt=None,
            insert_stmt="INSERT INTO target VALUES (?)",
            target_table="target_table",
            source_file_name=None,
            target_file_name=None,
            source_format="Delimited",
            target_format="Delimited",
            source_text_delimiter=",",
            target_text_delimiter="|",
            source_conn=source_conn,
            target_conn=target_conn,
        )

        assert "SourceTdpId='source_host'" in content
        assert "TargetTdpId='target_host'" in content
        assert "SourceTable='source_table'" in content
        assert "TargetTable='target_table'" in content
        assert "InsertStmt='INSERT INTO target VALUES (?)'" in content

    def test_prepare_tdload_job_var_file_invalid_mode(self):
        """Test job variable file preparation with invalid mode."""
        source_conn = TeradataResource(host="source_host", user="user", password="pass")

        with pytest.raises(ValueError, match="target_conn must be provided"):
            prepare_tdload_job_var_file(
                mode="table_to_table",
                source_table="source_table",
                select_stmt=None,
                insert_stmt=None,
                target_table="target_table",
                source_file_name=None,
                target_file_name=None,
                source_format="",
                target_format="",
                source_text_delimiter="",
                target_text_delimiter="",
                source_conn=source_conn,
                target_conn=None,  # Missing target connection
            )

    def test_read_file_success(self):
        """Test successful file reading."""
        content = "Sample file content"
        with open("temp_test_file.txt", "w") as f:
            f.write(content)

        read_content = read_file("temp_test_file.txt")
        assert read_content == content
        os.remove("temp_test_file.txt")

    def test_read_file_not_found(self):
        """Test file reading with non-existent file."""
        with pytest.raises(FileNotFoundError):
            read_file("non_existent_file.txt")

    @patch("subprocess.run")
    def test_secure_delete_with_shred(self, mock_run):
        """Test secure file deletion with shred available."""
        mock_run.return_value = MagicMock(returncode=0)

        # Create a temporary file for testing
        test_file = "temp_secure_file.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        secure_delete(test_file)

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "shred" in args[0]
        assert "--remove" in args

        # Clean up if file still exists
        if os.path.exists(test_file):
            os.remove(test_file)

    def test_set_local_file_permissions(self):
        """Test setting local file permissions."""
        test_file = "temp_permission_file.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        set_local_file_permissions(test_file)

        # Check that file has read-only permissions (0o400)
        file_stat = os.stat(test_file)
        assert file_stat.st_mode & 0o777 == 0o400

        # Clean up
        os.chmod(test_file, 0o644)  # Restore permissions for deletion
        os.remove(test_file)

    def test_set_local_file_permissions_not_found(self):
        """Test setting permissions on non-existent file."""
        with pytest.raises(DagsterError, match="File does not exist"):
            set_local_file_permissions("non_existent_file.txt")

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_set_remote_file_permissions_windows(self, mock_execute, mock_get_os):
        """Test setting remote file permissions on Windows."""
        mock_get_os.return_value = "windows"
        mock_execute.return_value = (0, "", "")
        ssh_client = MagicMock()

        set_remote_file_permissions(ssh_client, "C:\\temp\\file.txt")

        mock_execute.assert_called_once()
        call_args = mock_execute.call_args[0][1]
        assert "icacls" in call_args

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_set_remote_file_permissions_unix(self, mock_execute, mock_get_os):
        """Test setting remote file permissions on Unix."""
        mock_get_os.return_value = "unix"
        mock_execute.return_value = (0, "", "")
        ssh_client = MagicMock()

        set_remote_file_permissions(ssh_client, "/tmp/file.txt")

        mock_execute.assert_called_once_with(ssh_client, "chmod 400 /tmp/file.txt")

    @patch("paramiko.SSHClient.open_sftp")
    def test_is_valid_remote_job_var_file_exists(self, mock_open_sftp):
        """Test valid remote job variable file detection."""
        mock_sftp = MagicMock()
        mock_open_sftp.return_value = mock_sftp
        mock_stat = MagicMock()
        mock_stat.st_mode = stat.S_IFREG
        mock_sftp.stat.return_value = mock_stat

        ssh_client = MagicMock()
        ssh_client.open_sftp = mock_open_sftp

        result = is_valid_remote_job_var_file(ssh_client, "/remote/path/to/file")

        assert result is True
        mock_sftp.close.assert_called_once()

    @patch("paramiko.SSHClient.open_sftp")
    def test_is_valid_remote_job_var_file_not_exists(self, mock_open_sftp):
        """Test invalid remote job variable file detection."""
        mock_sftp = MagicMock()
        mock_open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError

        ssh_client = MagicMock()
        ssh_client.open_sftp = mock_open_sftp

        result = is_valid_remote_job_var_file(ssh_client, "/remote/path/to/file")

        assert result is False
        mock_sftp.close.assert_called_once()

    def test_is_valid_remote_job_var_file_none_path(self):
        """Test remote job variable file detection with None path."""
        ssh_client = MagicMock()
        result = is_valid_remote_job_var_file(ssh_client, None)
        assert result is False

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_verify_tpt_utility_on_remote_host_windows(self, mock_execute, mock_get_os):
        """Test TPT utility verification on Windows."""
        mock_get_os.return_value = "windows"
        mock_execute.return_value = (0, "C:\\Program Files\\tbuild.exe", "")
        ssh_client = MagicMock()

        verify_tpt_utility_on_remote_host(ssh_client, "tbuild")

        mock_execute.assert_called_once_with(ssh_client, "where tbuild")

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_verify_tpt_utility_on_remote_host_unix(self, mock_execute, mock_get_os):
        """Test TPT utility verification on Unix."""
        mock_get_os.return_value = "unix"
        mock_execute.return_value = (0, "/usr/bin/tbuild", "")
        ssh_client = MagicMock()

        verify_tpt_utility_on_remote_host(ssh_client, "tbuild")

        mock_execute.assert_called_once_with(ssh_client, "which tbuild")

    @patch("dagster_teradata.ttu.utils.tpt_util.get_remote_os")
    @patch("dagster_teradata.ttu.utils.tpt_util.execute_remote_command")
    def test_verify_tpt_utility_on_remote_host_not_found(
        self, mock_execute, mock_get_os
    ):
        """Test TPT utility verification when utility not found."""
        mock_get_os.return_value = "unix"
        mock_execute.return_value = (1, "", "command not found")
        ssh_client = MagicMock()

        with pytest.raises(DagsterError, match="not installed or not available"):
            verify_tpt_utility_on_remote_host(ssh_client, "tbuild")

    @patch("paramiko.SSHClient.open_sftp")
    def test_transfer_file_sftp_success(self, mock_open_sftp):
        """Test successful SFTP file transfer."""
        mock_sftp = MagicMock()
        mock_open_sftp.return_value = mock_sftp

        ssh_client = MagicMock()
        ssh_client.open_sftp = mock_open_sftp

        # Create a temporary local file
        test_file = "local_test_file.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        transfer_file_sftp(ssh_client, test_file, "remote_file.txt")

        mock_sftp.put.assert_called_once_with(test_file, "remote_file.txt")
        mock_sftp.close.assert_called_once()

        # Clean up
        os.remove(test_file)

    @patch("paramiko.SSHClient.open_sftp")
    def test_transfer_file_sftp_local_file_not_found(self, mock_open_sftp):
        """Test SFTP transfer with non-existent local file."""
        ssh_client = MagicMock()

        with pytest.raises(DagsterError, match="Local file does not exist"):
            transfer_file_sftp(ssh_client, "non_existent_file.txt", "remote_file.txt")

    def test_tpt_config_constants(self):
        """Test TPT configuration constants."""
        assert TPTConfig.DEFAULT_TIMEOUT == 5
        assert TPTConfig.FILE_PERMISSIONS_READ_ONLY == 0o400
        assert TPTConfig.TEMP_DIR_WINDOWS == "C:\\Windows\\Temp"
        assert TPTConfig.TEMP_DIR_UNIX == "/tmp"


if __name__ == "__main__":
    unittest.main()
