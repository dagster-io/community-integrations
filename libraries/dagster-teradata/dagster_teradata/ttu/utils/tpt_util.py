"""
TPT Utility Functions for Teradata Parallel Transporter operations.

This module provides utility functions for TPT operations including file handling,
remote command execution, security operations, and TPT script preparation.
"""

from __future__ import annotations

import logging
import os
import shutil
import stat
import subprocess
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from paramiko import SSHClient

from dagster import DagsterError


class TPTConfig:
    """Configuration constants for TPT operations."""

    # Default timeout for subprocess operations in seconds
    DEFAULT_TIMEOUT = 5

    # File permissions for read-only access (owner read-only)
    FILE_PERMISSIONS_READ_ONLY = 0o400

    # Default temporary directory paths for different operating systems
    TEMP_DIR_WINDOWS = "C:\\Windows\\Temp"
    TEMP_DIR_UNIX = "/tmp"


def execute_remote_command(ssh_client: SSHClient, command: str) -> tuple[int, str, str]:
    """
    Execute a command on remote host and properly manage SSH channels.

    Args:
        ssh_client: SSH client connection
        command: Command to execute on remote host

    Returns:
        tuple[int, str, str]: Tuple containing (exit_status, stdout, stderr)

    Note:
        Properly closes all SSH channels to prevent resource leaks
    """
    # Execute command and get standard I/O channels
    stdin, stdout, stderr = ssh_client.exec_command(command)
    try:
        # Get exit status from the channel
        exit_status = stdout.channel.recv_exit_status()
        # Read and decode stdout and stderr
        stdout_data = stdout.read().decode().strip()
        stderr_data = stderr.read().decode().strip()
        return exit_status, stdout_data, stderr_data
    finally:
        # Ensure all channels are properly closed
        stdin.close()
        stdout.close()
        stderr.close()


def write_file(path: str, content: str) -> None:
    """
    Write content to a file with UTF-8 encoding.

    Args:
        path: File path to write to
        content: Content to write to the file
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def secure_delete(file_path: str, logger: logging.Logger | None = None) -> None:
    """
    Securely delete a file using shred if available, otherwise use os.remove.

    Args:
        file_path: Path to the file to be deleted
        logger: Optional logger instance for logging operations

    Note:
        - Uses 'shred' command for secure deletion on Unix/Linux systems
        - Falls back to regular deletion if shred is not available
        - Logs warnings but does not raise exceptions for deletion failures
    """
    logger = logger or logging.getLogger(__name__)

    # Check if file exists before attempting deletion
    if not os.path.exists(file_path):
        return

    try:
        # Check if shred utility is available
        if shutil.which("shred") is not None:
            # Use shred for secure file deletion (overwrite then remove)
            subprocess.run(
                ["shred", "--remove", file_path],
                check=True,
                timeout=TPTConfig.DEFAULT_TIMEOUT,
            )
            logger.info("Securely removed file using shred: %s", file_path)
        else:
            # Fall back to regular file deletion
            os.remove(file_path)
            logger.info("Removed file: %s", file_path)

    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        # Log warning but don't raise exception for deletion failures
        logger.warning("Failed to remove file %s: %s", file_path, str(e))


def remote_secure_delete(
    ssh_client: SSHClient, remote_files: list[str], logger: logging.Logger | None = None
) -> None:
    """
    Securely delete remote files via SSH. Attempts shred first, falls back to rm if shred is unavailable.

    Args:
        ssh_client: SSH client connection
        remote_files: List of remote file paths to delete
        logger: Optional logger instance for logging operations

    Note:
        - Handles both Windows and Unix/Linux remote systems
        - Uses different secure deletion methods based on remote OS
        - Continues processing other files if one file deletion fails
    """
    logger = logger or logging.getLogger(__name__)

    # Validate input parameters
    if not ssh_client or not remote_files:
        return

    try:
        # Detect remote operating system
        remote_os = get_remote_os(ssh_client, logger)
        windows_remote = remote_os == "windows"

        # Check if shred is available on remote Unix/Linux system
        shred_available = False
        if not windows_remote:
            exit_status, output, _ = execute_remote_command(
                ssh_client, "command -v shred"
            )
            shred_available = exit_status == 0 and output.strip() != ""

        # Process each remote file
        for file_path in remote_files:
            try:
                if windows_remote:
                    # Windows remote host - use del command with force and quiet options
                    replace_slash = file_path.replace("/", "\\")
                    execute_remote_command(
                        ssh_client,
                        f'if exist "{replace_slash}" del /f /q "{replace_slash}"',
                    )
                elif shred_available:
                    # Unix/Linux with shred available - use secure deletion
                    execute_remote_command(ssh_client, f"shred --remove {file_path}")
                else:
                    # Unix/Linux without shred - overwrite with zeros then delete
                    execute_remote_command(
                        ssh_client,
                        f"if [ -f {file_path} ]; then "
                        f"dd if=/dev/zero of={file_path} bs=4096 count=$(($(stat -c '%s' {file_path})/4096+1)) 2>/dev/null; "
                        f"rm -f {file_path}; fi",
                    )
            except Exception as e:
                # Log warning but continue with other files
                logger.warning(
                    "Failed to process remote file %s: %s", file_path, str(e)
                )

        logger.info("Processed remote files: %s", ", ".join(remote_files))
    except Exception as e:
        logger.warning("Failed to remove remote files: %s", str(e))


def terminate_subprocess(
    sp: subprocess.Popen | None, logger: logging.Logger | None = None
) -> None:
    """
    Terminate a subprocess gracefully with proper error handling.

    Args:
        sp: Subprocess to terminate
        logger: Optional logger instance for logging operations

    Note:
        - First attempts graceful termination with terminate()
        - Falls back to forceful kill() if graceful termination times out
        - Handles various termination scenarios safely
    """
    logger = logger or logging.getLogger(__name__)

    # Check if process exists and is still running
    if not sp or sp.poll() is not None:
        # Process is None or already terminated
        return

    logger.info("Terminating subprocess (PID: %s)", sp.pid)

    try:
        # Attempt graceful termination
        sp.terminate()
        sp.wait(timeout=TPTConfig.DEFAULT_TIMEOUT)
        logger.info("Subprocess terminated gracefully")
    except subprocess.TimeoutExpired:
        # Graceful termination failed, use forceful kill
        logger.warning(
            "Subprocess did not terminate gracefully within %d seconds, killing it",
            TPTConfig.DEFAULT_TIMEOUT,
        )
        try:
            sp.kill()
            sp.wait(timeout=2)  # Brief wait after kill
            logger.info("Subprocess killed successfully")
        except Exception as e:
            logger.error("Error killing subprocess: %s", str(e))
    except Exception as e:
        logger.error("Error terminating subprocess: %s", str(e))


def get_remote_os(ssh_client: SSHClient, logger: logging.Logger | None = None) -> str:
    """
    Detect the operating system of the remote host via SSH.

    Args:
        ssh_client: SSH client connection
        logger: Optional logger instance for logging operations

    Returns:
        str: Operating system type as string ('windows' or 'unix')

    Note:
        - Uses simple command execution to detect Windows vs Unix
        - Defaults to 'unix' if detection fails
    """
    logger = logger or logging.getLogger(__name__)

    if not ssh_client:
        logger.warning("No SSH client provided for OS detection")
        return "unix"

    try:
        # Check for Windows by echoing %OS% environment variable
        exit_status, stdout_data, stderr_data = execute_remote_command(
            ssh_client, "echo %OS%"
        )

        if "Windows" in stdout_data:
            return "windows"

        # Default to Unix-like system if Windows not detected
        return "unix"

    except Exception as e:
        logger.error("Error detecting remote OS: %s", str(e))
        return "unix"  # Default to Unix on error


def set_local_file_permissions(
    local_file_path: str, logger: logging.Logger | None = None
) -> None:
    """
    Set permissions for a local file to be read-only for the owner.

    Args:
        local_file_path: Path to the local file
        logger: Optional logger instance for logging operations

    Raises:
        DagsterError: If file doesn't exist or permission setting fails
    """
    logger = logger or logging.getLogger(__name__)

    if not local_file_path:
        logger.warning("No file path provided for permission setting")
        return

    # Verify file exists before attempting permission change
    if not os.path.exists(local_file_path):
        raise DagsterError(f"File does not exist: {local_file_path}")

    try:
        # Set file permission to read-only for the owner (400)
        os.chmod(local_file_path, TPTConfig.FILE_PERMISSIONS_READ_ONLY)
        logger.info("Set read-only permissions for file %s", local_file_path)
    except (OSError, PermissionError) as e:
        raise DagsterError(
            f"Error setting permissions for local file {local_file_path}: {str(e)}"
        )


def _set_windows_file_permissions(
    ssh_client: SSHClient, remote_file_path: str, logger: logging.Logger
) -> None:
    """
    Set restrictive permissions on Windows remote file using icacls.

    Args:
        ssh_client: SSH client connection
        remote_file_path: Path to remote file on Windows system
        logger: Logger instance for logging operations

    Raises:
        DagsterError: If permission setting fails
    """
    # Use icacls to set restrictive permissions (remove inheritance, grant read-only to owner)
    command = f'icacls "{remote_file_path}" /inheritance:r /grant:r "%USERNAME%":R'

    exit_status, stdout_data, stderr_data = execute_remote_command(ssh_client, command)

    if exit_status != 0:
        raise DagsterError(
            f"Failed to set restrictive permissions on Windows remote file {remote_file_path}. "
            f"Exit status: {exit_status}, Error: {stderr_data if stderr_data else 'N/A'}"
        )
    logger.info(
        "Set restrictive permissions (owner read-only) for Windows remote file %s",
        remote_file_path,
    )


def _set_unix_file_permissions(
    ssh_client: SSHClient, remote_file_path: str, logger: logging.Logger
) -> None:
    """
    Set read-only permissions on Unix/Linux remote file using chmod.

    Args:
        ssh_client: SSH client connection
        remote_file_path: Path to remote file on Unix system
        logger: Logger instance for logging operations

    Raises:
        DagsterError: If permission setting fails
    """
    # Use chmod to set read-only permissions (400)
    command = f"chmod 400 {remote_file_path}"

    exit_status, stdout_data, stderr_data = execute_remote_command(ssh_client, command)

    if exit_status != 0:
        raise DagsterError(
            f"Failed to set permissions (400) on remote file {remote_file_path}. "
            f"Exit status: {exit_status}, Error: {stderr_data if stderr_data else 'N/A'}"
        )
    logger.info("Set read-only permissions for remote file %s", remote_file_path)


def set_remote_file_permissions(
    ssh_client: SSHClient, remote_file_path: str, logger: logging.Logger | None = None
) -> None:
    """
    Set permissions for a remote file to be read-only for the owner.

    Args:
        ssh_client: SSH client connection
        remote_file_path: Path to the remote file
        logger: Optional logger instance for logging operations

    Raises:
        DagsterError: If permission setting fails

    Note:
        - Automatically detects remote OS and uses appropriate method
        - Uses icacls for Windows, chmod for Unix/Linux
    """
    logger = logger or logging.getLogger(__name__)

    # Validate input parameters
    if not ssh_client or not remote_file_path:
        logger.warning(
            "Invalid parameters: ssh_client=%s, remote_file_path=%s",
            bool(ssh_client),
            remote_file_path,
        )
        return

    try:
        # Detect remote operating system
        remote_os = get_remote_os(ssh_client, logger)

        # Use OS-specific permission setting method
        if remote_os == "windows":
            _set_windows_file_permissions(ssh_client, remote_file_path, logger)
        else:
            _set_unix_file_permissions(ssh_client, remote_file_path, logger)

    except DagsterError:
        # Re-raise DagsterErrors as-is
        raise
    except Exception as e:
        raise DagsterError(
            f"Error setting permissions for remote file {remote_file_path}: {str(e)}"
        )


def get_remote_temp_directory(
    ssh_client: SSHClient, logger: logging.Logger | None = None
) -> str:
    """
    Get the remote temporary directory path based on the operating system.

    Args:
        ssh_client: SSH client connection
        logger: Optional logger instance for logging operations

    Returns:
        str: Path to the remote temporary directory

    Note:
        - Returns Windows TEMP directory for Windows systems
        - Returns /tmp for Unix/Linux systems
        - Falls back to default if detection fails
    """
    logger = logger or logging.getLogger(__name__)

    try:
        # Detect remote operating system
        remote_os = get_remote_os(ssh_client, logger)

        if remote_os == "windows":
            # Get Windows TEMP directory
            exit_status, temp_dir, stderr_data = execute_remote_command(
                ssh_client, "echo %TEMP%"
            )

            if exit_status == 0 and temp_dir and temp_dir != "%TEMP%":
                return temp_dir
            logger.warning(
                "Could not get TEMP directory, using default: %s",
                TPTConfig.TEMP_DIR_WINDOWS,
            )
            return TPTConfig.TEMP_DIR_WINDOWS

        # Unix/Linux systems use /tmp
        return TPTConfig.TEMP_DIR_UNIX

    except Exception as e:
        logger.warning("Error getting remote temp directory: %s", str(e))
        # Default to Unix temp directory on error
        return TPTConfig.TEMP_DIR_UNIX


def is_valid_file(file_path: str) -> bool:
    """
    Check if the given path is a valid file.

    Args:
        file_path: Path to check

    Returns:
        bool: True if path exists and is a regular file, False otherwise
    """
    return os.path.isfile(file_path)


def verify_tpt_utility_installed(utility: str) -> None:
    """
    Verify if a TPT utility (e.g., tbuild or tdload) is installed and available in the system's PATH.

    Args:
        utility: Name of the TPT utility to verify

    Raises:
        DagsterError: If utility is not found in PATH
    """
    if shutil.which(utility) is None:
        raise DagsterError(
            f"TPT utility '{utility}' is not installed or not available in the system's PATH"
        )


def verify_tpt_utility_on_remote_host(
    ssh_client: SSHClient, utility: str, logger: logging.Logger | None = None
) -> None:
    """
    Verify if a TPT utility (tbuild/tdload) is installed on the remote host via SSH.

    Args:
        ssh_client: SSH client connection
        utility: Name of the utility to verify
        logger: Optional logger instance for logging operations

    Raises:
        DagsterError: If utility is not found or verification fails

    Note:
        - Uses 'where' command for Windows, 'which' command for Unix/Linux
        - Provides detailed error information if utility is not found
    """
    logger = logger or logging.getLogger(__name__)

    try:
        # Detect remote operating system
        remote_os = get_remote_os(ssh_client, logger)

        # Use OS-specific command to find utility
        if remote_os == "windows":
            command = f"where {utility}"
        else:
            command = f"which {utility}"

        exit_status, output, error = execute_remote_command(ssh_client, command)

        # Check if utility was found
        if exit_status != 0 or not output:
            raise DagsterError(
                f"TPT utility '{utility}' is not installed or not available in PATH on the remote host. "
                f"Command: {command}, Exit status: {exit_status}, "
                f"stderr: {error if error else 'N/A'}"
            )

        logger.info("TPT utility '%s' found at: %s", utility, output.split("\n")[0])

    except DagsterError:
        # Re-raise DagsterErrors as-is
        raise
    except Exception as e:
        raise DagsterError(
            f"Failed to verify TPT utility '{utility}' on remote host: {str(e)}"
        )


def prepare_tpt_ddl_script(
    sql: list[str],
    error_list: list[int] | None,
    teradata_connection_resource,
    job_name: str | None = None,
) -> str:
    """
    Prepare a TPT script for executing DDL statements.

    This method generates a TPT script that defines a DDL operator and applies the provided SQL statements.
    It also supports specifying a list of error codes to handle during the operation.

    Args:
        sql: A list of DDL statements to execute
        error_list: A list of error codes to handle during the operation
        teradata_connection_resource: Teradata connection resource with host, user, password
        job_name: The name of the TPT job. Defaults to unique name if None

    Returns:
        str: A formatted TPT script as a string

    Raises:
        ValueError: If the SQL statement list is empty or contains no valid statements
    """
    # Validate input parameters
    if not sql or not isinstance(sql, list):
        raise ValueError("SQL statement list must be a non-empty list")

    # Clean and escape each SQL statement
    sql_statements = [
        stmt.strip().rstrip(";").replace("'", "''")
        for stmt in sql
        if stmt and isinstance(stmt, str) and stmt.strip()
    ]

    # Check if we have any valid SQL statements after cleaning
    if not sql_statements:
        raise ValueError("No valid SQL statements found in the provided input")

    # Format SQL statements for TPT APPLY block with proper indentation
    apply_sql = ",\n".join(
        [
            f"('{stmt};')" if i == 0 else f"            ('{stmt};')"
            for i, stmt in enumerate(sql_statements)
        ]
    )

    # Generate job name if not provided
    if job_name is None:
        job_name = f"dagster_tptddl_{uuid.uuid4().hex}"

    # Format error list for inclusion in the TPT script
    if not error_list:
        error_list_stmt = "ErrorList = ['']"
    else:
        error_list_str = ", ".join([f"'{error}'" for error in error_list])
        error_list_stmt = f"ErrorList = [{error_list_str}]"

    # Extract connection parameters
    host = teradata_connection_resource.host
    login = teradata_connection_resource.user
    password = teradata_connection_resource.password

    # Build the complete TPT script
    tpt_script = f"""
        DEFINE JOB {job_name}
        DESCRIPTION 'TPT DDL Operation'
        (
        APPLY
            {apply_sql}
        TO OPERATOR ( $DDL ()
            ATTR
            (
                TdpId = '{host}',
                UserName = '{login}',
                UserPassword = '{password}',
                {error_list_stmt}
            )
        );
        );
        """
    return tpt_script


def prepare_tdload_job_var_file(
    mode: str,
    source_table: str | None,
    select_stmt: str | None,
    insert_stmt: str | None,
    target_table: str | None,
    source_file_name: str | None,
    target_file_name: str | None,
    source_format: str,
    target_format: str,
    source_text_delimiter: str,
    target_text_delimiter: str,
    source_conn,
    target_conn,
) -> str:
    """
    Prepare a tdload job variable file based on the specified mode.

    Args:
        mode: The operation mode ('file_to_table', 'table_to_file', or 'table_to_table')
        source_table: Name of the source table
        select_stmt: SQL SELECT statement for data extraction
        insert_stmt: SQL INSERT statement for data loading
        target_table: Name of the target table
        source_file_name: Path to the source file
        target_file_name: Path to the target file
        source_format: Format of source data
        target_format: Format of target data
        source_text_delimiter: Source text delimiter
        target_text_delimiter: Target text delimiter
        source_conn: Source connection parameters
        target_conn: Target connection parameters

    Returns:
        str: The content of the job variable file

    Raises:
        ValueError: If invalid parameters are provided for the specified mode
    """
    # Create a dictionary to store job variables
    job_vars = {}

    # Add appropriate parameters based on the operation mode
    if mode == "file_to_table":
        # File to table transfer mode
        job_vars.update(
            {
                "TargetTdpId": source_conn.host,
                "TargetUserName": source_conn.user,
                "TargetUserPassword": source_conn.password,
                "TargetTable": target_table,
                "SourceFileName": source_file_name,
            }
        )
        if insert_stmt:
            job_vars["InsertStmt"] = insert_stmt

    elif mode == "table_to_file":
        # Table to file export mode
        job_vars.update(
            {
                "SourceTdpId": source_conn.host,
                "SourceUserName": source_conn.user,
                "SourceUserPassword": source_conn.password,
                "TargetFileName": target_file_name,
            }
        )

        # Use either source table or select statement
        if source_table:
            job_vars["SourceTable"] = source_table
        elif select_stmt:
            job_vars["SourceSelectStmt"] = select_stmt

    elif mode == "table_to_table":
        # Table to table transfer mode
        if target_conn is None:
            raise ValueError("target_conn must be provided for 'table_to_table' mode")
        job_vars.update(
            {
                "SourceTdpId": source_conn.host,
                "SourceUserName": source_conn.user,
                "SourceUserPassword": source_conn.password,
                "TargetTdpId": target_conn.host,
                "TargetUserName": target_conn.user,
                "TargetUserPassword": target_conn.password,
                "TargetTable": target_table,
            }
        )

        # Use either source table or select statement
        if source_table:
            job_vars["SourceTable"] = source_table
        elif select_stmt:
            job_vars["SourceSelectStmt"] = select_stmt
        if insert_stmt:
            job_vars["InsertStmt"] = insert_stmt

    # Add common format parameters if specified
    if source_format:
        job_vars["SourceFormat"] = source_format
    if target_format:
        job_vars["TargetFormat"] = target_format
    if source_text_delimiter:
        job_vars["SourceTextDelimiter"] = source_text_delimiter
    if target_text_delimiter:
        job_vars["TargetTextDelimiter"] = target_text_delimiter

    # Format job variables as key='value' pairs
    job_var_content = "".join(
        [f"{key}='{value}',\n" for key, value in job_vars.items()]
    )
    # Remove trailing comma and newline
    job_var_content = job_var_content.rstrip(",\n")

    return job_var_content


def is_valid_remote_job_var_file(
    ssh_client: SSHClient,
    remote_job_var_file_path: str,
    logger: logging.Logger | None = None,
) -> bool:
    """
    Check if the given remote job variable file path is a valid file.

    Args:
        ssh_client: SSH client connection
        remote_job_var_file_path: Path to remote job variable file
        logger: Optional logger instance for logging operations

    Returns:
        bool: True if file exists and is a regular file, False otherwise
    """
    if remote_job_var_file_path:
        # Open SFTP connection to check remote file
        sftp_client = ssh_client.open_sftp()
        try:
            # Get file metadata to check if it's a regular file
            file_stat = sftp_client.stat(remote_job_var_file_path)
            if file_stat.st_mode:
                is_regular_file = stat.S_ISREG(file_stat.st_mode)
                return is_regular_file
            return False
        except FileNotFoundError:
            # File doesn't exist on remote host
            if logger:
                logger.error(
                    "File does not exist on remote at : %s", remote_job_var_file_path
                )
            return False
        finally:
            # Always close SFTP connection
            sftp_client.close()
    else:
        return False


def prepare_tpt_script(
    operator_type: str,
    source_table: str | None = None,
    select_stmt: str | None = None,
    target_table: str | None = None,
    source_file: str | None = None,
    target_file: str | None = None,
    format_options: dict | None = None,
    connection_params: dict | None = None,
) -> str:
    """
    Prepare TPT script based on operator type and parameters.

    Args:
        operator_type: Type of TPT operator
        source_table: Source table name
        select_stmt: SQL SELECT statement
        target_table: Target table name
        source_file: Source file path
        target_file: Target file path
        format_options: Formatting options
        connection_params: Connection parameters

    Returns:
        str: Prepared TPT script
    """
    pass


def prepare_tpt_variables(
    operator_type: str,
    source_table: str | None = None,
    target_table: str | None = None,
    source_file: str | None = None,
    target_file: str | None = None,
    format_options: dict | None = None,
    connection_params: dict | None = None,
) -> dict[str, str]:
    """
    Prepare TPT variables based on operator type and parameters.

    Args:
        operator_type: Type of TPT operator
        source_table: Source table name
        target_table: Target table name
        source_file: Source file path
        target_file: Target file path
        format_options: Formatting options
        connection_params: Connection parameters

    Returns:
        Dict[str, str]: Prepared TPT variables
    """
    pass


def validate_tpt_options(options: dict) -> bool:
    """
    Validate TPT options.

    Args:
        options: TPT options to validate

    Returns:
        bool: True if options are valid, False otherwise
    """
    # Implementation
    pass


def get_tpt_command(
    operator_type: str, script_file: str, var_file: str | None = None
) -> list[str]:
    """
    Get the TPT command based on operator type.

    Args:
        operator_type: Type of TPT operator
        script_file: Path to TPT script file
        var_file: Path to variable file (optional)

    Returns:
        List[str]: TPT command as list of arguments
    """
    # Implementation
    pass


def get_operator_type(
    source_table: str | None = None,
    select_stmt: str | None = None,
    target_table: str | None = None,
    source_file: str | None = None,
    target_file: str | None = None,
) -> str:
    """
    Determine the TPT operator type based on parameters.

    Args:
        source_table: Source table name
        select_stmt: SQL SELECT statement
        target_table: Target table name
        source_file: Source file path
        target_file: Target file path

    Returns:
        str: Determined operator type

    Raises:
        ValueError: If operator type cannot be determined from parameters
    """
    if source_file and target_table:
        return "tdload"  # File to table transfer
    elif (source_table or select_stmt) and target_file:
        return "tbuild"  # Table to file export
    elif (source_table or select_stmt) and target_table:
        return "tpump"  # Table to table transfer
    else:
        raise ValueError("Cannot determine TPT operator type from parameters")


def read_file(file_path: str, encoding: str = "UTF-8") -> str:
    """
    Read the content of a file with the specified encoding.

    Args:
        file_path: Path to the file to be read
        encoding: Encoding to use for reading the file (default: UTF-8)

    Returns:
        str: Content of the file as a string

    Raises:
        FileNotFoundError: If the file does not exist
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, encoding=encoding) as f:
        return f.read()


def transfer_file_sftp(
    ssh_client: SSHClient,
    local_path: str,
    remote_path: str,
    logger: logging.Logger | None = None,
) -> None:
    """
    Transfer a file from local to remote host using SFTP.

    Args:
        ssh_client: SSH client connection
        local_path: Local file path
        remote_path: Remote file path
        logger: Optional logger instance for logging operations

    Raises:
        DagsterError: If file transfer fails or local file doesn't exist
    """
    logger = logger or logging.getLogger(__name__)

    # Verify local file exists
    if not os.path.exists(local_path):
        raise DagsterError(f"Local file does not exist: {local_path}")

    sftp = None
    try:
        # Open SFTP connection and transfer file
        sftp = ssh_client.open_sftp()
        sftp.put(local_path, remote_path)
        logger.info(
            "Successfully transferred file from %s to %s", local_path, remote_path
        )
    except Exception as e:
        raise DagsterError(
            f"Failed to transfer file from {local_path} to {remote_path}: {str(e)}"
        )
    finally:
        # Ensure SFTP connection is closed
        if sftp:
            sftp.close()
