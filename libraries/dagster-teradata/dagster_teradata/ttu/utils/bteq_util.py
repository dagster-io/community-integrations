from __future__ import annotations

import os
import shutil
import stat
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from paramiko import SSHClient

from dagster import DagsterError


def identify_os(ssh_client: SSHClient) -> str:
    """
    Identify the operating system of a remote machine via SSH.

    Args:
        ssh_client (SSHClient): An active SSH connection to the remote machine.

    Returns:
        str: The name of the operating system in lowercase (e.g., 'linux', 'windows').

    Note:
        Uses 'uname' command for Unix-like systems and 'ver' for Windows systems.
    """
    stdin, stdout, stderr = ssh_client.exec_command("uname || ver")
    return stdout.read().decode().lower()


def verify_bteq_installed():
    """
    Verify if BTEQ is installed and available in the local system's PATH.

    Raises:
        DagsterError: If BTEQ executable is not found in the system PATH.
    """
    if shutil.which("bteq") is None:
        raise DagsterError(
            "BTEQ is not installed or not available in the system's PATH."
        )


def verify_bteq_installed_remote(ssh_client: SSHClient):
    """
    Verify if BTEQ is installed on a remote machine accessible via SSH.

    Args:
        ssh_client (SSHClient): An active SSH connection to the remote machine.

    Raises:
        DagsterError: If BTEQ executable is not found on the remote machine.

    Note:
        Detects the remote OS and uses appropriate commands to check for BTEQ:
        - Windows: 'where bteq'
        - macOS: Checks via zsh if available, falls back to 'which bteq'
        - Other Unix-like: 'which bteq'
    """
    # Detect OS
    os_info = identify_os(ssh_client)

    if "windows" in os_info:
        check_cmd = "where bteq"
    elif "darwin" in os_info:
        # Check if zsh exists first for remote shell compatibility
        stdin, stdout, stderr = ssh_client.exec_command("command -v zsh")
        zsh_path = stdout.read().strip()
        if zsh_path:
            # If zsh is available, use it to check for bteq in the user's environment
            check_cmd = 'zsh -l -c "which bteq"'
        else:
            # Fallback to default shell if zsh is not available
            check_cmd = "which bteq"
    else:
        # Default command for other Unix-like systems
        check_cmd = "which bteq"

    # Execute the command to check for bteq
    stdin, stdout, stderr = ssh_client.exec_command(check_cmd)
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().strip()
    error = stderr.read().strip()

    # Raise an error if bteq is not found in the PATH
    if exit_status != 0 or not output:
        raise DagsterError(
            "BTEQ is not installed or not available on the remote machine. (%s)", error
        )


def transfer_file_sftp(ssh_client: SSHClient, local_path: str, remote_path: str):
    """
    Transfer a file from local to remote machine using SFTP.

    Args:
        ssh_client (SSHClient): An active SSH connection to the remote machine.
        local_path (str): Path to the local file to be transferred.
        remote_path (str): Destination path on the remote machine.

    Note:
        Currently opens and immediately closes SFTP connection without transfer.
        This appears to be a placeholder implementation.
    """
    sftp = ssh_client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()


def prepare_bteq_script_for_remote_execution(
    teradata_connection_resource, sql: str
) -> str:
    """
    Build a BTEQ script with connection details for remote execution.

    Args:
        teradata_connection_resource: Resource containing Teradata connection details
                                      (host, user, password).
        sql (str): SQL commands to be executed.

    Returns:
        str: Complete BTEQ script including login commands and SQL.

    Note:
        The .LOGON command is included in the script for remote execution to avoid
        exposing credentials in command line arguments.
    """
    script_lines = []
    host = teradata_connection_resource.host
    login = teradata_connection_resource.user
    password = teradata_connection_resource.password
    script_lines.append(f".LOGON {host}/{login},{password}")
    return _prepare_bteq_script(script_lines, sql)


def prepare_bteq_script_for_local_execution(sql: str) -> str:
    """
    Build a BTEQ script for local execution.

    Args:
        sql (str): SQL commands to be executed.

    Returns:
        str: Complete BTEQ script including SQL commands.

    Note:
        For local execution, connection details are typically passed as command line
        arguments rather than in the script.
    """
    script_lines: list[str] = []
    return _prepare_bteq_script(script_lines, sql)


def _prepare_bteq_script(script_lines: list[str], sql: str) -> str:
    """
    Internal helper function to assemble BTEQ script components.

    Args:
        script_lines (list[str]): List of BTEQ commands to include before the SQL.
        sql (str): SQL commands to be executed.

    Returns:
        str: Complete BTEQ script with all commands and SQL.
    """
    script_lines.append(sql.strip())
    script_lines.append(".EXIT")
    return "\n".join(script_lines)


def _prepare_bteq_command(
    timeout: int,
    bteq_script_encoding: str,
    bteq_session_encoding: str,
    timeout_rc: int,
) -> list[str]:
    """
    Prepare the core BTEQ command with common parameters.

    Args:
        timeout (int): Maximum execution time in seconds before timeout.
        bteq_script_encoding (str): Character encoding for the script file.
        bteq_session_encoding (str): Character encoding for the session.
        timeout_rc (int): Return code to use when timeout occurs.

    Returns:
        list[str]: List of command components that can be joined into a full command.
    """
    bteq_core_cmd = ["bteq"]
    if bteq_session_encoding:
        bteq_core_cmd.append(f" -e {bteq_script_encoding}")
        bteq_core_cmd.append(f" -c {bteq_session_encoding}")
    bteq_core_cmd.append('"')
    bteq_core_cmd.append(f".SET EXITONDELAY ON MAXREQTIME {timeout}")
    if timeout_rc is not None and timeout_rc >= 0:
        bteq_core_cmd.append(f" RC {timeout_rc}")
    bteq_core_cmd.append(";")
    # Dagster doesn't display the script of BTEQ in UI but only in log so WIDTH is 500 enough
    bteq_core_cmd.append(" .SET WIDTH 500;")
    return bteq_core_cmd


def prepare_bteq_command_for_remote_execution(
    timeout: Optional[int],  # or: timeout: int | None
    bteq_script_encoding: str,
    bteq_session_encoding: str,
    timeout_rc: int,
) -> str:
    """
    Prepare the BTEQ command string for remote execution.

    Args:
        timeout (int): Maximum execution time in seconds before timeout.
        bteq_script_encoding (str): Character encoding for the script file.
        bteq_session_encoding (str): Character encoding for the session.
        timeout_rc (int): Return code to use when timeout occurs.

    Returns:
        str: Complete BTEQ command string for remote execution.
    """
    bteq_core_cmd = _prepare_bteq_command(
        timeout or 600, bteq_script_encoding, bteq_session_encoding, timeout_rc
    )
    bteq_core_cmd.append('"')
    return " ".join(bteq_core_cmd)


def prepare_bteq_command_for_local_execution(
    teradata_connection_resource,
    timeout: Optional[int],  # or: timeout: int | None
    bteq_script_encoding: str,
    bteq_session_encoding: str,
    timeout_rc: int,
) -> str:
    """
    Prepare the BTEQ command string for local execution.

    Args:
        teradata_connection_resource: Resource containing Teradata connection details.
        timeout (int): Maximum execution time in seconds before timeout.
        bteq_script_encoding (str): Character encoding for the script file.
        bteq_session_encoding (str): Character encoding for the session.
        timeout_rc (int): Return code to use when timeout occurs.

    Returns:
        str: Complete BTEQ command string for local execution including login details.
    """
    bteq_core_cmd = _prepare_bteq_command(
        timeout or 600, bteq_script_encoding, bteq_session_encoding, timeout_rc
    )
    host = teradata_connection_resource.host
    login = teradata_connection_resource.user
    password = teradata_connection_resource.password
    bteq_core_cmd.append(f" .LOGON {host}/{login},{password}")
    bteq_core_cmd.append('"')
    bteq_command_str = " ".join(bteq_core_cmd)
    return bteq_command_str


def is_valid_file(file_path: str) -> bool:
    """
    Check if a file exists at the given path.

    Args:
        file_path (str): Path to the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)


def is_valid_encoding(file_path: str, encoding: str = "UTF-8") -> bool:
    """
    Check if a file can be read with the specified encoding.

    Args:
        file_path (str): Path to the file to be checked.
        encoding (str, optional): Encoding to test. Defaults to "UTF-8".

    Returns:
        bool: True if the file can be read with the specified encoding, False otherwise.

    Raises:
        UnicodeDecodeError: If the file cannot be read with the specified encoding.
    """
    with open(file_path, encoding=encoding) as f:
        f.read()
        return True


def read_file(file_path: str, encoding: str = "UTF-8") -> str:
    """
    Read the content of a file with the specified encoding.

    Args:
        file_path (str): Path to the file to be read.
        encoding (str, optional): Encoding to use for reading. Defaults to "UTF-8".

    Returns:
        str: Content of the file as a string.

    Raises:
        FileNotFoundError: If the file does not exist.
        UnicodeDecodeError: If the file cannot be read with the specified encoding.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, encoding=encoding) as f:
        return f.read()


def is_valid_remote_bteq_script_file(
    ssh_client: SSHClient, remote_file_path: str | None, logger=None
) -> bool:
    """
    Check if a remote file is a valid BTEQ script file.

    Args:
        ssh_client (SSHClient): An active SSH connection to the remote machine.
        remote_file_path (str): Path to the remote file to check.
        logger (optional): Logger instance for error logging.

    Returns:
        bool: True if the file exists and is a regular file, False otherwise.
    """
    if remote_file_path:
        sftp_client = ssh_client.open_sftp()
        try:
            # Get file metadata
            file_stat = sftp_client.stat(remote_file_path)
            if file_stat.st_mode:
                is_regular_file = stat.S_ISREG(file_stat.st_mode)
                return is_regular_file
            return False
        except FileNotFoundError:
            if logger:
                logger.error("File does not exist on remote at : %s", remote_file_path)
            return False
        finally:
            sftp_client.close()
    else:
        return False
