import os
import socket
import subprocess
import tempfile
from contextlib import contextmanager
from typing import Optional, List, Union, Literal, cast

import paramiko
from dagster import DagsterError
from paramiko.client import SSHClient
from paramiko.ssh_exception import SSHException

from dagster_teradata.ttu.utils.bteq_util import (
    prepare_bteq_command_for_local_execution,
    prepare_bteq_command_for_remote_execution,
    prepare_bteq_script_for_local_execution,
    prepare_bteq_script_for_remote_execution,
    verify_bteq_installed,
    verify_bteq_installed_remote,
    is_valid_file,
    is_valid_remote_bteq_script_file,
    transfer_file_sftp,
    read_file,
    is_valid_encoding,
)
from dagster_teradata.ttu.utils.encryption_utils import (
    SecureCredentialManager,
    generate_random_password,
    generate_encrypted_file_with_openssl,
    decrypt_remote_file_to_string,
    get_stored_credentials,
)


class Bteq:
    """
    Main BTEQ operator class for executing Teradata BTEQ commands either locally or remotely.

    Features:
    - Local and remote execution via SSH
    - Secure credential handling and encryption
    - File-based and direct SQL execution
    - Timeout and error handling
    - Encoding support (ASCII, UTF-8, UTF-16)
    - Temporary file management

    Attributes:
        connection: Legacy connection object (maintained for compatibility)
        teradata_connection_resource: Contains Teradata connection parameters
        log: Logger instance for operation logging
        cred_manager: SecureCredentialManager instance for encryption/decryption
        ssh_client: Active SSH connection for remote execution (None for local)
    """

    def __init__(self, connection, teradata_connection_resource, log):
        """
        Initialize BTEQ operator with connection resources and logger.

        Args:
            connection: Legacy connection object (maintained for compatibility)
            teradata_connection_resource: Contains Teradata connection parameters
                including host, username, password
            log: Logger instance for operation logging
        """
        self.remote_port = None
        self.ssh_key_path = None
        self.remote_remote_password = None
        self.remote_user = None
        self.remote_host = None
        self.file_path = None
        self.temp_file_read_encoding = None
        self.bteq_quit_rc = None
        self.bteq_session_encoding = None
        self.timeout_rc = None
        self.bteq_script_encoding = None
        self.sql = None
        self.remote_working_dir = None
        self.timeout = None
        self.connection = connection
        self.log = log
        self.teradata_connection_resource = teradata_connection_resource
        self.cred_manager = SecureCredentialManager()
        self.ssh_client = None  # Will hold active SSH connection if remote execution

    def bteq_operator(
        self,
        sql: Optional[str] = None,
        file_path: Optional[str] = None,
        remote_host: Optional[str] = None,
        remote_user: Optional[str] = None,
        remote_password: Optional[str] = None,
        ssh_key_path: Optional[str] = None,
        remote_port: int = 22,
        remote_working_dir: str = "/tmp",
        bteq_script_encoding: Optional[str] = "utf-8",
        bteq_session_encoding: Optional[str] = "ASCII",
        bteq_quit_rc: Union[int, List[int]] = 0,
        timeout: int | Literal[600] = 600,
        timeout_rc: int | None = None,
        temp_file_read_encoding: Optional[str] = "UTF-8",
    ) -> int | None:
        """
        Execute BTEQ commands either locally or remotely.

        Args:
            sql: SQL commands to execute directly
            file_path: Path to file containing SQL commands
            remote_host: Hostname for remote execution (None for local)
            remote_user: Username for remote authentication
            remote_password: Password for remote authentication
            ssh_key_path: Path to SSH private key for authentication
            remote_port: SSH port (default: 22)
            remote_working_dir: Remote working directory (default: '/tmp')
            bteq_script_encoding: Encoding for BTEQ script file
            bteq_session_encoding: Encoding for BTEQ session
            bteq_quit_rc: Acceptable return codes (default: 0)
            timeout: Maximum execution time in seconds (default: 600)
            timeout_rc: Specific return code for timeout cases
            temp_file_read_encoding: Encoding for reading temporary files

        Returns:
            int: Exit status code from BTEQ execution, or None if no execution occurred

        Raises:
            ValueError: For invalid input parameters
            DagsterError: For execution failures or timeouts
        """
        self.sql = sql
        self.file_path = file_path
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.remote_remote_password = remote_password
        self.ssh_key_path = ssh_key_path
        self.remote_port = remote_port
        self.remote_working_dir = remote_working_dir
        self.bteq_script_encoding = bteq_script_encoding
        self.bteq_session_encoding = bteq_session_encoding
        self.bteq_quit_rc = bteq_quit_rc
        self.timeout = timeout
        self.timeout_rc = timeout_rc
        self.temp_file_read_encoding = temp_file_read_encoding

        # Local execution
        if not self.remote_host:
            if self.sql:
                bteq_script = prepare_bteq_script_for_local_execution(sql=self.sql)
                self.log.debug(
                    "Executing BTEQ script with SQL content: %s", bteq_script
                )
                return self.execute_bteq_script(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                )
            elif self.file_path:
                if not is_valid_file(self.file_path):
                    raise ValueError(
                        f"The provided file path '{self.file_path}' is invalid or does not exist."
                    )
                try:
                    is_valid_encoding(
                        self.file_path, self.temp_file_read_encoding or "UTF-8"
                    )
                except UnicodeDecodeError as e:
                    errmsg = f"The provided file '{self.file_path}' encoding is different from BTEQ I/O encoding i.e.'UTF-8'."
                    if self.bteq_script_encoding:
                        errmsg = f"The provided file '{self.file_path}' encoding is different from the specified BTEQ I/O encoding '{self.bteq_script_encoding}'."
                    raise ValueError(errmsg) from e
                return self._handle_local_bteq_file(file_path=self.file_path)

        # Remote execution
        elif self.remote_host:
            if self.sql:
                bteq_script = prepare_bteq_script_for_remote_execution(
                    teradata_connection_resource=self.teradata_connection_resource,
                    sql=self.sql,
                )
                self.log.debug(
                    "Executing BTEQ script with SQL content: %s", bteq_script
                )
                return self.execute_bteq_script(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                    self.remote_host,
                    self.remote_user,
                    self.remote_remote_password,
                    self.ssh_key_path,
                    self.remote_port,
                )
            if self.file_path:
                if not self._setup_ssh_connection(
                    host=self.remote_host,
                    user=cast(str, self.remote_user),
                    password=self.remote_remote_password,
                    key_path=self.ssh_key_path,
                    port=self.remote_port,
                ):
                    raise DagsterError(
                        "Failed to establish SSH connection. Please check the provided credentials."
                    )
                if (
                    self.file_path
                    and self.ssh_client
                    and is_valid_remote_bteq_script_file(
                        self.ssh_client, self.file_path
                    )
                ):
                    return self._handle_remote_bteq_file(
                        ssh_client=self.ssh_client,
                        file_path=self.file_path,
                    )
                raise ValueError(
                    f"The provided remote file path '{self.file_path}' is invalid or file does not exist on remote machine at given path."
                )
            else:
                raise ValueError(
                    "BteqOperator requires either the 'sql' or 'file_path' parameter. Both are missing."
                )
        return None

    def execute_bteq_script(
        self,
        bteq_script: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: Optional[int],  # or: timeout: int | None
        timeout_rc: int | None,
        bteq_session_encoding: str | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        temp_file_read_encoding: str | None,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_password: str | None = None,
        ssh_key_path: str | None = None,
        remote_port: int = 22,
    ) -> int | None:
        """
        Execute BTEQ script either locally or remotely.

        Args:
            bteq_script: The BTEQ script content to execute
            remote_working_dir: Working directory for remote execution
            bteq_script_encoding: Encoding for BTEQ script file
            timeout: Maximum execution time in seconds
            timeout_rc: Return code for timeout cases
            bteq_session_encoding: Encoding for BTEQ session
            bteq_quit_rc: Acceptable return codes
            temp_file_read_encoding: Encoding for reading temporary files
            remote_host: Remote hostname (None for local)
            remote_user: Remote username
            remote_password: Remote password
            ssh_key_path: Path to SSH private key
            remote_port: SSH port

        Returns:
            int: Exit status code from BTEQ execution

        Note:
            Delegates to execute_bteq_script_at_local or execute_bteq_script_at_remote
            based on whether remote_host is specified
        """
        if remote_host:
            return self.execute_bteq_script_at_remote(
                bteq_script=bteq_script,
                remote_working_dir=remote_working_dir,
                bteq_script_encoding=bteq_script_encoding,
                timeout=timeout,
                timeout_rc=timeout_rc,
                bteq_session_encoding=bteq_session_encoding,
                bteq_quit_rc=bteq_quit_rc,
                temp_file_read_encoding=temp_file_read_encoding,
                remote_host=remote_host,
                remote_user=remote_user,
                remote_password=remote_password,
                ssh_key_path=ssh_key_path,
                remote_port=remote_port,
            )
        return self.execute_bteq_script_at_local(
            bteq_script=bteq_script,
            bteq_script_encoding=bteq_script_encoding,
            timeout=timeout,
            timeout_rc=timeout_rc,
            bteq_quit_rc=bteq_quit_rc,
            bteq_session_encoding=bteq_session_encoding,
            temp_file_read_encoding=temp_file_read_encoding,
        )

    def execute_bteq_script_at_remote(
        self,
        bteq_script: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: Optional[int],  # or: timeout: int | None
        timeout_rc: int | None,
        bteq_session_encoding: str | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        temp_file_read_encoding: str | None,
        remote_host: str | None,
        remote_user: str | None,
        remote_password: str | None,
        ssh_key_path: str | None,
        remote_port: int = 22,
    ) -> int | None:
        """
        Execute BTEQ script on a remote machine via SSH.

        Args:
            bteq_script: The BTEQ script content to execute
            remote_working_dir: Working directory on remote machine
            bteq_script_encoding: Encoding for BTEQ script file
            timeout: Maximum execution time in seconds
            timeout_rc: Return code for timeout cases
            bteq_session_encoding: Encoding for BTEQ session
            bteq_quit_rc: Acceptable return codes
            temp_file_read_encoding: Encoding for reading temporary files
            remote_host: Remote hostname
            remote_user: Remote username
            remote_password: Remote password
            ssh_key_path: Path to SSH private key
            remote_port: SSH port

        Returns:
            int: Exit status code from BTEQ execution

        Raises:
            DagsterError: For SSH failures, execution errors, or timeouts
        """
        with self.preferred_temp_directory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "bteq_script.txt")
            with open(
                file_path, "w", encoding=str(temp_file_read_encoding or "UTF-8")
            ) as f:
                f.write(bteq_script)
            return self._transfer_to_and_execute_bteq_on_remote(
                file_path,
                remote_working_dir,
                bteq_script_encoding,
                timeout,
                timeout_rc,
                bteq_quit_rc,
                bteq_session_encoding,
                tmp_dir,
                cast(str, remote_host),
                cast(str, remote_user),
                remote_password,
                ssh_key_path,
                remote_port,
            )

    def _transfer_to_and_execute_bteq_on_remote(
        self,
        file_path: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: Optional[int],  # or: timeout: int | None
        timeout_rc: int | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        bteq_session_encoding: str | None,
        tmp_dir: str,
        remote_host: str,
        remote_user: str,
        remote_password: str | None = None,
        ssh_key_path: str | None = None,
        remote_port: int = 22,
    ) -> int | None:
        """
        Transfer and execute BTEQ script on remote machine with encryption.

        Args:
            file_path: Local path to BTEQ script file
            remote_working_dir: Remote working directory
            bteq_script_encoding: Encoding for BTEQ script file
            timeout: Maximum execution time in seconds
            timeout_rc: Return code for timeout cases
            bteq_quit_rc: Acceptable return codes
            bteq_session_encoding: Encoding for BTEQ session
            tmp_dir: Local temporary directory
            remote_host: Remote hostname
            remote_user: Remote username
            remote_password: Remote password
            ssh_key_path: Path to SSH private key
            remote_port: SSH port

        Returns:
            int: Exit status code from BTEQ execution

        Note:
            - Uses OpenSSL AES-256-CBC encryption for secure file transfer
            - Automatically cleans up temporary files
        """
        encrypted_file_path = None
        remote_encrypted_path = None
        try:
            if not self._setup_ssh_connection(
                host=remote_host,
                user=remote_user,
                password=remote_password,
                key_path=ssh_key_path,
                port=remote_port,
            ):
                raise DagsterError(
                    "Failed to establish SSH connection. Please check the provided credentials."
                )
            if self.ssh_client is None:
                raise DagsterError(
                    "Failed to establish SSH connection. `ssh_client` is None."
                )
            verify_bteq_installed_remote(self.ssh_client)
            password = generate_random_password()  # Encryption/Decryption password
            encrypted_file_path = os.path.join(tmp_dir, "bteq_script.enc")
            generate_encrypted_file_with_openssl(
                file_path, password, encrypted_file_path
            )
            remote_encrypted_path = os.path.join(
                remote_working_dir or "", "bteq_script.enc"
            )

            transfer_file_sftp(
                self.ssh_client, encrypted_file_path, remote_encrypted_path
            )

            bteq_command_str = prepare_bteq_command_for_remote_execution(
                timeout=timeout,
                bteq_script_encoding=bteq_script_encoding or "",
                bteq_session_encoding=bteq_session_encoding or "",
                timeout_rc=timeout_rc or -1,
            )
            self.log.debug("Executing BTEQ command: %s", bteq_command_str)

            exit_status, stdout, stderr = decrypt_remote_file_to_string(
                self.ssh_client,
                remote_encrypted_path,
                password,
                bteq_command_str,
            )

            failure_message = None
            self.log.debug("stdout : %s", stdout)
            self.log.debug("stderr : %s", stderr)
            self.log.debug("exit_status : %s", exit_status)

            if "Failure" in stderr or "Error" in stderr:
                failure_message = stderr
            # Raising an exception if there is any failure in bteq and also user wants to fail the
            # task otherwise just log the error message as warning to not fail the task.
            if (
                failure_message
                and exit_status != 0
                and exit_status
                not in (
                    bteq_quit_rc
                    if isinstance(bteq_quit_rc, (list, tuple))
                    else [bteq_quit_rc if bteq_quit_rc is not None else 0]
                )
            ):
                raise DagsterError(f"BTEQ task failed with error: {failure_message}")
            if failure_message:
                self.log.warning(failure_message)
                return exit_status
            # If we get here, everything succeeded
            return exit_status  # Explicit return instead of else block
        except (OSError, socket.gaierror):
            raise DagsterError(
                "SSH connection timed out. Please check the network or server availability."
            )
        except SSHException as e:
            raise DagsterError(
                f"An unexpected error occurred during SSH connection: {str(e)}"
            )
        except DagsterError as e:
            raise e
        except Exception as e:
            raise DagsterError(
                f"An unexpected error occurred while executing BTEQ script on remote machine: {str(e)}"
            )
        finally:
            # Remove the local script file
            if encrypted_file_path and os.path.exists(encrypted_file_path):
                os.remove(encrypted_file_path)
            # Cleanup: Delete the remote temporary file
            if encrypted_file_path:
                cleanup_en_command = f"rm -f {remote_encrypted_path}"
                if self.ssh_client and self.connection:
                    if self.ssh_client is None:
                        raise DagsterError(
                            "Failed to establish SSH connection. `ssh_client` is None."
                        )
                    self.ssh_client.exec_command(cleanup_en_command)

    def execute_bteq_script_at_local(
        self,
        bteq_script: str,
        bteq_script_encoding: str | None,
        timeout: Optional[int],  # or: timeout: int | None
        timeout_rc: int | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        bteq_session_encoding: str | None,
        temp_file_read_encoding: str | None,
    ) -> int | None:
        """
        Execute BTEQ script on local machine.

        Args:
            bteq_script: The BTEQ script content to execute
            bteq_script_encoding: Encoding for BTEQ script file
            timeout: Maximum execution time in seconds
            timeout_rc: Return code for timeout cases
            bteq_quit_rc: Acceptable return codes
            bteq_session_encoding: Encoding for BTEQ session
            temp_file_read_encoding: Encoding for reading temporary files

        Returns:
            int: Exit status code from BTEQ execution

        Raises:
            DagsterError: For execution failures or timeouts
        """
        verify_bteq_installed()
        bteq_command_str = prepare_bteq_command_for_local_execution(
            teradata_connection_resource=self.teradata_connection_resource,
            timeout=timeout,
            bteq_script_encoding=bteq_script_encoding or "",
            bteq_session_encoding=bteq_session_encoding or "",
            timeout_rc=timeout_rc or -1,
        )
        self.log.debug("Executing BTEQ command: %s", bteq_command_str)

        process = subprocess.Popen(
            bteq_command_str,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            preexec_fn=os.setsid,
        )
        encode_bteq_script = bteq_script.encode(str(temp_file_read_encoding or "UTF-8"))
        self.log.debug("encode_bteq_script : %s", encode_bteq_script)
        stdout_data, _ = process.communicate(input=encode_bteq_script)
        self.log.debug("stdout_data : %s", stdout_data)
        try:
            # https://docs.python.org/3.10/library/subprocess.html#subprocess.Popen.wait  timeout is in seconds
            process.wait(
                timeout=(timeout or 0) + 60
            )  # Adding 1 minute extra for BTEQ script timeout
        except subprocess.TimeoutExpired:
            self.on_kill()
            raise DagsterError(f"BTEQ command timed out after {timeout} seconds.")

        failure_message = None
        if stdout_data is None:
            raise DagsterError("Process stdout is None. Unable to read BTEQ output.")
        decoded_line = ""
        for line in stdout_data.splitlines():
            try:
                decoded_line = line.decode("UTF-8").strip()
                self.log.debug("decoded_line : %s", decoded_line)
            except UnicodeDecodeError:
                self.log.warning("Failed to decode line: %s", line)
            if "Failure" in decoded_line or "Error" in decoded_line:
                failure_message = decoded_line
        # Raising an exception if there is any failure in bteq and also user wants to fail the
        # task otherwise just log the error message as warning to not fail the task.
        if (
            failure_message
            and process.returncode != 0
            and process.returncode
            not in (
                bteq_quit_rc
                if isinstance(bteq_quit_rc, (list, tuple))
                else [bteq_quit_rc if bteq_quit_rc is not None else 0]
            )
        ):
            raise DagsterError(f"BTEQ task failed with error: {failure_message}")
        if failure_message:
            self.log.warning(failure_message)

        return process.returncode

    def on_kill(self):
        """Terminate the subprocess if running."""
        conn = self.connection
        process = conn.get("sp")
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.log.warning(
                    "Subprocess did not terminate in time. Forcing kill..."
                )
                process.kill()
            except Exception as e:
                self.log.error("Failed to terminate subprocess: %s", str(e))

    def get_dagster_home_dir(self) -> str:
        """Get the DAGSTER_HOME directory from environment variables."""
        return os.environ.get("DAGSTER_HOME", "~/.dagster_home")

    @contextmanager
    def preferred_temp_directory(self, prefix="bteq_"):
        """
        Context manager for creating a temporary directory.

        Args:
            prefix: Prefix for the temporary directory name

        Yields:
            str: Path to the created temporary directory

        Note:
            Falls back to DAGSTER_HOME if system temp directory is not usable
        """
        try:
            temp_dir = tempfile.gettempdir()
            if not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
                raise OSError("OS temp dir not usable")
        except Exception:
            temp_dir = self.get_dagster_home_dir()

        with tempfile.TemporaryDirectory(dir=temp_dir, prefix=prefix) as tmp:
            yield tmp

    def _handle_remote_bteq_file(
        self, ssh_client: SSHClient, file_path: str | None
    ) -> int | None:
        """
        Handle execution of a remote BTEQ script file.

        Args:
            ssh_client: Active SSH connection
            file_path: Path to remote BTEQ script file

        Returns:
            int: Exit status code from BTEQ execution

        Raises:
            ValueError: For invalid file path
        """
        if file_path:
            with ssh_client:
                sftp = ssh_client.open_sftp()
                try:
                    with sftp.open(file_path, "r") as remote_file:
                        file_content = remote_file.read().decode(
                            self.temp_file_read_encoding or "UTF-8"
                        )
                finally:
                    sftp.close()
                bteq_script = prepare_bteq_script_for_remote_execution(
                    teradata_connection_resource=self.teradata_connection_resource,
                    sql=file_content,
                )
                self.log.debug(
                    "Executing BTEQ script with SQL content: %s", bteq_script
                )
                return self.execute_bteq_script_at_remote(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                    remote_host=self.remote_host,
                    remote_user=self.remote_user,
                    remote_password=self.remote_remote_password,
                    ssh_key_path=self.ssh_key_path,
                    remote_port=self.remote_port or 22,
                )
        else:
            raise ValueError(
                "Please provide a valid file path for the BTEQ script to be executed on the remote machine."
            )

    def _handle_local_bteq_file(self, file_path: str) -> int | None:
        """
        Handle execution of a local BTEQ script file.

        Args:
            file_path: Path to local BTEQ script file

        Returns:
            int: Exit status code from BTEQ execution, or None if file is invalid
        """
        if file_path and is_valid_file(file_path):
            file_content = read_file(
                file_path, encoding=str(self.temp_file_read_encoding or "UTF-8")
            )
            bteq_script = prepare_bteq_script_for_local_execution(
                sql=file_content,
            )
            self.log.debug("Executing BTEQ script with SQL content: %s", bteq_script)
            result = self.execute_bteq_script(
                bteq_script,
                self.remote_working_dir,
                self.bteq_script_encoding,
                self.timeout,
                self.timeout_rc,
                self.bteq_session_encoding,
                self.bteq_quit_rc,
                self.temp_file_read_encoding,
            )
            return result
        return None

    def _setup_ssh_connection(
        self,
        host: str,
        user: Optional[str],
        password: Optional[str],
        key_path: Optional[str],
        port: int,
    ) -> bool:
        """
        Establish SSH connection using either password or key authentication.

        Args:
            host: Remote hostname
            user: Remote username
            password: Remote password (optional if key_path provided)
            key_path: Path to SSH private key (optional if password provided)
            port: SSH port

        Returns:
            bool: True if connection succeeded, False otherwise

        Raises:
            DagsterError: If connection fails

        Note:
            - Tries stored credentials if no password provided
            - Prompts for password if no credentials available
            - Stores new credentials if successfully authenticated
        """
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if key_path:
                key = paramiko.RSAKey.from_private_key_file(key_path)
                self.ssh_client.connect(host, port=port, username=user, pkey=key)
            else:
                if not password:
                    if user is None:
                        raise ValueError(
                            "Username is required to fetch stored credentials"
                        )
                    # Attempt to retrieve stored credentials
                    creds = get_stored_credentials(self, host, user)
                    password = (
                        self.cred_manager.decrypt(creds["password"]) if creds else None
                    )

                self.ssh_client.connect(
                    host, port=port, username=user, password=password
                )

            self.log.info(f"SSH connected to {user}@{host}")
            return True
        except Exception as e:
            raise DagsterError(f"SSH connection failed: {e}")
