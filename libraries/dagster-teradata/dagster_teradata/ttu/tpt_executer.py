"""
TPT Executor for Teradata Parallel Transporter operations.

This module provides execution functions for TPT operations including DDL execution
and TPT load operations. It supports both local execution and remote execution via SSH
with secure file handling and cleanup.
"""

import os
import shutil
import subprocess
import tempfile
import uuid
import socket
from collections.abc import Generator
from contextlib import contextmanager
from dagster import DagsterError
from dagster_teradata.ttu.utils.encryption_utils import (
    generate_random_password,
    generate_encrypted_file_with_openssl,
    decrypt_remote_file,
)
from dagster_teradata.ttu.utils.tpt_util import (
    write_file,
    verify_tpt_utility_on_remote_host,
    transfer_file_sftp,
    execute_remote_command,
    set_remote_file_permissions,
    remote_secure_delete,
    secure_delete,
    set_local_file_permissions,
    terminate_subprocess,
)
from paramiko.client import SSHClient
from paramiko.ssh_exception import SSHException


def execute_ddl(
    self,
    tpt_script: str | list[str],
    remote_working_dir: str,
) -> int:
    """
    Execute DDL statements using TPT.

    Args:
        tpt_script: TPT script content as string or list of strings
        remote_working_dir: Remote working directory for temporary files

    Returns:
        int: Exit code of the TPT operation

    Raises:
        ValueError: If script is empty or invalid
        DagsterError: If execution fails
    """
    # Validate input script
    if not tpt_script:
        raise ValueError("TPT script cannot be empty")

    # Convert list of strings to single string if needed
    tpt_script_content = (
        "\n".join(tpt_script) if isinstance(tpt_script, list) else tpt_script
    )

    # Validate script content
    if not tpt_script_content.strip():
        raise ValueError("TPT script content cannot be empty")

    # Choose execution path based on SSH availability
    if self.ssh_client:
        self.log.info("Executing DDL via SSH")
        return _execute_tbuild_via_ssh(self, tpt_script_content, remote_working_dir)

    self.log.info("Executing DDL locally")
    return _execute_tbuild_locally(self, tpt_script_content)


def _execute_tbuild_via_ssh(
    self,
    tpt_script_content: str,
    remote_working_dir: str,
) -> int:
    """
    Execute tbuild command remotely via SSH.

    Args:
        tpt_script_content: TPT script content as string
        remote_working_dir: Remote working directory for temporary files

    Returns:
        int: Exit code of the tbuild operation

    Raises:
        DagsterError: If SSH connection fails or tbuild execution fails
    """
    # Create temporary directory for local file operations
    with preferred_temp_directory() as tmp_dir:
        # Generate unique file names
        local_script_file = os.path.join(
            tmp_dir, f"tbuild_script_{uuid.uuid4().hex}.sql"
        )

        # Write script content to local file
        write_file(local_script_file, tpt_script_content)

        encrypted_file_path = f"{local_script_file}.enc"
        remote_encrypted_script_file = os.path.join(
            remote_working_dir, os.path.basename(encrypted_file_path)
        )
        remote_script_file = os.path.join(
            remote_working_dir, os.path.basename(local_script_file)
        )
        job_name = f"tbuild_job_{uuid.uuid4().hex}"

        try:
            # Verify SSH connection is established
            if self.ssh_client:
                # Check if tbuild utility is available on remote host
                verify_tpt_utility_on_remote_host(
                    ssh_client=self.ssh_client, utility="tbuild", logger=self.log
                )

                # Generate password and encrypt the script file
                password = generate_random_password()
                generate_encrypted_file_with_openssl(
                    local_script_file, password, encrypted_file_path
                )

                # Transfer encrypted file to remote host
                transfer_file_sftp(
                    self.ssh_client,
                    encrypted_file_path,
                    remote_encrypted_script_file,
                    self.log,
                )

                # Decrypt the file on remote host
                decrypt_remote_file(
                    self.ssh_client,
                    remote_encrypted_script_file,
                    remote_script_file,
                    password,
                    self.log,
                )

                # Set appropriate permissions on remote file
                set_remote_file_permissions(
                    self.ssh_client, remote_script_file, self.log
                )

                # Build and execute tbuild command
                tbuild_cmd = ["tbuild", "-f", remote_script_file, job_name]
                self.log.info("Executing remote tbuild command")
                exit_status, output, error = execute_remote_command(
                    self.ssh_client, " ".join(tbuild_cmd)
                )
                self.log.info("tbuild output:\n%s", output)

                # Clean up remote temporary files
                remote_secure_delete(
                    self.ssh_client,
                    [remote_encrypted_script_file, remote_script_file],
                    self.log,
                )

                # Check execution status
                if exit_status != 0:
                    raise DagsterError(
                        f"tbuild failed with code {exit_status}: {error}"
                    )

                return exit_status

        # Handle specific SSH-related exceptions
        except (OSError, socket.gaierror) as e:
            self.log.error("SSH timeout: %s", str(e))
            raise DagsterError("SSH connection timeout")
        except SSHException as e:
            raise DagsterError(f"SSH error: {str(e)}")
        except Exception as e:
            raise DagsterError(f"Remote tbuild execution failed: {str(e)}")
        finally:
            # Clean up local temporary files
            secure_delete(encrypted_file_path, self.log)
            secure_delete(local_script_file, self.log)


def _execute_tbuild_locally(
    self,
    tpt_script_content: str,
) -> int:
    """
    Execute tbuild command locally.

    Args:
        tpt_script_content: TPT script content as string

    Returns:
        int: Exit code of the tbuild operation

    Raises:
        DagsterError: If tbuild binary not found or execution fails
    """
    # Create temporary directory for file operations
    with preferred_temp_directory() as tmp_dir:
        # Generate unique file name
        local_script_file = os.path.join(
            tmp_dir, f"tbuild_script_{uuid.uuid4().hex}.sql"
        )

        # Write script content to file
        write_file(local_script_file, tpt_script_content)
        set_local_file_permissions(local_script_file, self.log)

        job_name = f"tbuild_job_{uuid.uuid4().hex}"
        tbuild_cmd = ["tbuild", "-f", local_script_file, job_name]

        # Verify tbuild binary is available
        if not shutil.which("tbuild"):
            raise DagsterError("tbuild binary not found")

        sp = None
        try:
            self.log.info("Executing local tbuild command")
            # Execute tbuild command as subprocess
            sp = subprocess.Popen(
                tbuild_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,  # Create new process group
            )

            # Capture and log output, collecting error lines
            error_lines = []
            if sp.stdout:
                for line in iter(sp.stdout.readline, b""):
                    decoded_line = line.decode("UTF-8").strip()
                    self.log.info(decoded_line)
                    if "error" in decoded_line.lower():
                        error_lines.append(decoded_line)

            # Wait for process completion
            sp.wait()

            # Check return code
            if sp.returncode != 0:
                error_msg = "\n".join(error_lines) if error_lines else "Unknown error"
                raise DagsterError(
                    f"tbuild failed with code {sp.returncode}: {error_msg}"
                )

            return sp.returncode
        except Exception as e:
            self.log.error("tbuild execution error: %s", str(e))
            raise DagsterError(f"tbuild execution failed: {str(e)}")
        finally:
            # Clean up temporary file and terminate subprocess if needed
            secure_delete(local_script_file, self.log)
            terminate_subprocess(sp, self.log)


def execute_tdload(
    self,
    log,
    remote_working_dir: str,
    ssh_client=None,
    job_var_content: str | None = None,
    tdload_options: str | None = None,
    tdload_job_name: str | None = None,
) -> int:
    """
    Execute tdload operation with given parameters.

    Args:
        log: Logger instance for logging
        remote_working_dir: Remote working directory for temporary files
        ssh_client: SSH client for remote execution (optional)
        job_var_content: Job variable file content (optional)
        tdload_options: Additional tdload options (optional)
        tdload_job_name: Job name for tdload operation (optional)

    Returns:
        int: Exit code of the tdload operation
    """
    # Generate job name if not provided
    tdload_job_name = tdload_job_name or f"tdload_job_{uuid.uuid4().hex}"

    # Choose execution path based on SSH availability
    if ssh_client:
        log.info("Executing tdload via SSH")
        return _execute_tdload_via_ssh(
            log=log,
            ssh_client=ssh_client,
            remote_working_dir=remote_working_dir,
            job_var_content=job_var_content,
            tdload_options=tdload_options,
            tdload_job_name=tdload_job_name,
        )
    log.info("Executing tdload locally")
    return _execute_tdload_locally(
        log=log,
        job_var_content=job_var_content,
        tdload_options=tdload_options,
        tdload_job_name=tdload_job_name,
    )


def _execute_tdload_via_ssh(
    log,
    ssh_client: SSHClient,
    remote_working_dir: str,
    job_var_content: str | None,
    tdload_options: str | None,
    tdload_job_name: str | None,
) -> int:
    """
    Execute tdload command remotely via SSH.

    Args:
        log: Logger instance for logging
        ssh_client: Connected SSH client
        remote_working_dir: Remote working directory for temporary files
        job_var_content: Job variable file content
        tdload_options: Additional tdload options
        tdload_job_name: Job name for tdload operation

    Returns:
        int: Exit code of the tdload operation
    """
    # Create temporary directory for local file operations
    with preferred_temp_directory() as tmp_dir:
        # Generate unique file name for job variables
        local_job_var_file = os.path.join(
            tmp_dir, f"tdload_job_var_{uuid.uuid4().hex}.txt"
        )

        # Write job variable content to file
        write_file(local_job_var_file, job_var_content or "")

        # Transfer and execute on remote host
        return _transfer_to_and_execute_tdload_on_remote(
            log,
            ssh_client,
            local_job_var_file,
            remote_working_dir,
            tdload_options,
            tdload_job_name,
        )


def _transfer_to_and_execute_tdload_on_remote(
    log,
    ssh_client: SSHClient,
    local_job_var_file: str,
    remote_working_dir: str,
    tdload_options: str | None,
    tdload_job_name: str | None,
) -> int:
    """
    Transfer and execute tdload job on remote host.

    Args:
        log: Logger instance for logging
        ssh_client: Connected SSH client
        local_job_var_file: Path to local job variable file
        remote_working_dir: Remote working directory
        tdload_options: Additional tdload options
        tdload_job_name: Job name for tdload operation

    Returns:
        int: Exit code of the tdload operation

    Raises:
        DagsterError: If SSH connection fails or tdload execution fails
    """
    # Generate file paths for encrypted and decrypted files
    encrypted_file_path = f"{local_job_var_file}.enc"
    remote_encrypted_job_file = os.path.join(
        remote_working_dir, os.path.basename(encrypted_file_path)
    )
    remote_job_file = os.path.join(
        remote_working_dir, os.path.basename(local_job_var_file)
    )

    try:
        # Verify SSH connection
        if not ssh_client:
            raise DagsterError("SSH connection not established")

        # Check if tdload utility is available on remote host
        verify_tpt_utility_on_remote_host(ssh_client, "tdload", log)

        # Generate password and encrypt the job variable file
        password = generate_random_password()
        generate_encrypted_file_with_openssl(
            local_job_var_file, password, encrypted_file_path
        )

        # Transfer encrypted file to remote host
        transfer_file_sftp(
            ssh_client, encrypted_file_path, remote_encrypted_job_file, log
        )

        # Decrypt the file on remote host
        decrypt_remote_file(
            ssh_client,
            remote_encrypted_job_file,
            remote_job_file,
            password,
            log,
        )

        # Set appropriate permissions on remote file
        set_remote_file_permissions(ssh_client, remote_job_file, log)

        # Build tdload command
        tdload_cmd = _build_tdload_command(
            log, remote_job_file, tdload_options, tdload_job_name
        )

        # Execute tdload command on remote host
        log.info("Executing remote tdload command")
        exit_status, output, error = execute_remote_command(
            ssh_client, " ".join(tdload_cmd)
        )
        log.info("tdload output:\n%s", output)

        # Clean up remote temporary files
        remote_secure_delete(
            ssh_client, [remote_encrypted_job_file, remote_job_file], log
        )

        # Check execution status
        if exit_status != 0:
            raise DagsterError(f"tdload failed with code {exit_status}: {error}")

        return exit_status

    # Handle specific SSH-related exceptions
    except (OSError, socket.gaierror) as e:
        log.error("SSH timeout: %s", str(e))
        raise DagsterError("SSH connection timeout")
    except SSHException as e:
        raise DagsterError(f"SSH error: {str(e)}")
    except Exception as e:
        raise DagsterError(f"Remote tdload execution failed: {str(e)}")
    finally:
        # Clean up local temporary files
        secure_delete(encrypted_file_path, log)
        secure_delete(local_job_var_file, log)


def _execute_tdload_locally(
    log,
    job_var_content: str | None,
    tdload_options: str | None,
    tdload_job_name: str | None,
) -> int:
    """
    Execute tdload command locally.

    Args:
        log: Logger instance for logging
        job_var_content: Job variable file content
        tdload_options: Additional tdload options
        tdload_job_name: Job name for tdload operation

    Returns:
        int: Exit code of the tdload operation

    Raises:
        DagsterError: If tdload binary not found or execution fails
    """
    # Create temporary directory for file operations
    with preferred_temp_directory() as tmp_dir:
        # Generate unique file name for job variables
        local_job_var_file = os.path.join(
            tmp_dir, f"tdload_job_var_{uuid.uuid4().hex}.txt"
        )

        # Write job variable content to file
        write_file(local_job_var_file, job_var_content or "")
        set_local_file_permissions(local_job_var_file, log)

        # Build tdload command
        tdload_cmd = _build_tdload_command(
            log, local_job_var_file, tdload_options, tdload_job_name
        )

        # Verify tdload binary is available
        if not shutil.which("tdload"):
            raise DagsterError("tdload binary not found")

        sp = None
        try:
            log.info("Executing local tdload command")
            # Execute tdload command as subprocess
            sp = subprocess.Popen(
                tdload_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )

            # Capture and log output, collecting error lines
            error_lines = []
            if sp.stdout:
                for line in iter(sp.stdout.readline, b""):
                    decoded_line = line.decode("UTF-8").strip()
                    log.info(decoded_line)
                    if "error" in decoded_line.lower():
                        error_lines.append(decoded_line)

            # Wait for process completion
            sp.wait()

            # Check return code
            if sp.returncode != 0:
                error_msg = "\n".join(error_lines) if error_lines else "Unknown error"
                raise DagsterError(
                    f"tdload failed with code {sp.returncode}: {error_msg}"
                )

            return sp.returncode
        except Exception as e:
            log.error("tdload execution error: %s", str(e))
            raise DagsterError(f"tdload execution failed: {str(e)}")
        finally:
            # Clean up temporary file and terminate subprocess if needed
            secure_delete(local_job_var_file, log)
            terminate_subprocess(sp, log)


def _build_tdload_command(
    log, job_var_file: str, tdload_options: str | None, tdload_job_name: str | None
) -> list[str]:
    """
    Build tdload command with proper options.

    Args:
        log: Logger instance for logging
        job_var_file: Path to job variable file
        tdload_options: Additional tdload options as string
        tdload_job_name: Job name for tdload operation

    Returns:
        list[str]: List of command arguments for subprocess execution
    """
    # Base tdload command with job variable file
    tdload_cmd = ["tdload", "-j", job_var_file]

    # Add additional options if provided
    if tdload_options:
        import shlex

        try:
            # Use shlex for proper shell-style parsing of options
            parsed_options = shlex.split(tdload_options)
            tdload_cmd.extend(parsed_options)
        except ValueError:
            # Fallback to simple split if shlex parsing fails
            log.warning("Using simple split for tdload_options")
            tdload_cmd.extend(tdload_options.split())

    # Add job name if provided
    if tdload_job_name:
        tdload_cmd.append(tdload_job_name)

    return tdload_cmd


def on_kill(self) -> None:
    """Clean up resources when task is terminated."""
    self.log.info("TPT Hook cleanup initiated")


@contextmanager
def preferred_temp_directory(prefix: str = "tpt_") -> Generator[str, None, None]:
    """
    Context manager for creating temporary directories.

    Args:
        prefix: Prefix for temporary directory name

    Yields:
        str: Path to created temporary directory

    Raises:
        OSError: If temporary directory cannot be created or accessed
    """
    try:
        # Try to use system temp directory
        temp_dir = tempfile.gettempdir()
        if not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
            raise OSError("Temp dir not usable")
    except Exception:
        # Fallback to Dagster home directory
        temp_dir = get_dagster_home_dir()

    # Create and yield temporary directory
    with tempfile.TemporaryDirectory(dir=temp_dir, prefix=prefix) as tmp:
        yield tmp


def get_dagster_home_dir(self) -> str:
    """
    Get Dagster home directory.

    Returns:
        str: Path to Dagster home directory
    """
    return os.environ.get("DAGSTER_HOME", os.path.expanduser("~/.dagster"))
