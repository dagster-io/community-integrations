import getpass
import os
import socket
import subprocess
import tempfile
from contextlib import contextmanager
from typing import Optional, List, Union, Literal

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
)


from dagster_teradata.ttu.utils.encryption_utils import *

class Bteq:
    """
    Main BTEQ operator class with enhanced features:
    - Directory processing
    - Force login from script
    - Secure credential handling
    - Remote execution via SSH
    """

    def __init__(self, connection, teradata_connection_resource, log):
        """
        Initialize BTEQ operator with connection resources and logger.

        Args:
            connection: Legacy connection object (maintained for compatibility)
            teradata_connection_resource: Contains Teradata connection parameters
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

    # Core Methods ============================================================

    def bteq_operator(
        self,
        sql: str = None,
        file_path: str = None,
        remote_host: Optional[str] = None,
        remote_user: Optional[str] = None,
        remote_password: Optional[str] = None,
        ssh_key_path: Optional[str] = None,
        remote_port: int = 22,
        remote_working_dir: str = "/tmp",
        bteq_script_encoding: Optional[str] = 'utf-8',
        bteq_session_encoding: Optional[str] = 'ASCII',
        bteq_quit_rc: Union[int, List[int]] = 0,
        timeout: int | Literal[600] = 600,  # Default to 10 minutes
        timeout_rc: int | None = None,
        temp_file_read_encoding: Optional[str] = 'UTF-8',
    ) -> int | None:

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

        """Execute the BTEQ script either in local machine or on remote host based on ssh_conn_id."""
        # Remote execution
        if not self.remote_working_dir:
            self.remote_working_dir = "/tmp"
        # Handling execution on local:
        if not self.remote_host:
            if self.sql:
                bteq_script = prepare_bteq_script_for_local_execution(
                    sql=self.sql,
                )
                self.log.info("Executing BTEQ script with SQL content: %s", bteq_script)
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
                if not is_valid_file(self.file_path):
                    raise ValueError(
                        f"The provided file path '{self.file_path}' is invalid or does not exist."
                    )
                try:
                    is_valid_encoding(self.file_path, self.temp_file_read_encoding or "UTF-8")
                except UnicodeDecodeError as e:
                    errmsg = f"The provided file '{self.file_path}' encoding is different from BTEQ I/O encoding i.e.'UTF-8'."
                    if self.bteq_script_encoding:
                        errmsg = f"The provided file '{self.file_path}' encoding is different from the specified BTEQ I/O encoding '{self.bteq_script_encoding}'."
                    raise ValueError(errmsg) from e
                return self._handle_local_bteq_file(
                    file_path=self.file_path)
        # Execution on Remote machine
        elif self.remote_host:
            # When sql statement is provided as input through sql parameter, Preparing the bteq script
            if self.sql:
                bteq_script = prepare_bteq_script_for_remote_execution(
                    conn=self.connection,
                    sql=self.sql,
                )
                self.log.info("Executing BTEQ script with SQL content: %s", bteq_script)
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
            if self.file_path:

                if self.file_path and is_valid_remote_bteq_script_file(self.ssh_client, self.file_path):
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
        timeout: int,
        timeout_rc: int | None,
        bteq_session_encoding: str | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        temp_file_read_encoding: str | None,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_remote_password: str | None = None,
        ssh_key_path: str | None = None,
        remote_port: int = 22,
    ) -> int | None:
        """Execute the BTEQ script either in local machine or on remote host based on ssh_conn_id."""
        # Remote execution
        if self.remote_host:
            # Write script to local temp file
            # Encrypt the file locally
            return self.execute_bteq_script_at_remote(
                bteq_script=bteq_script,
                remote_working_dir=remote_working_dir,
                bteq_script_encoding=bteq_script_encoding,
                timeout=timeout,
                timeout_rc=timeout_rc,
                bteq_session_encoding= bteq_session_encoding,
                bteq_quit_rc=bteq_quit_rc,
                temp_file_read_encoding=temp_file_read_encoding,
                remote_host=remote_host,
                remote_user=remote_user,
                remote_remote_password=remote_remote_password,
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
        timeout: int,
        timeout_rc: int | None,
        bteq_session_encoding: str | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        temp_file_read_encoding: str | None,
        remote_host: str | None = None,
        remote_user: str | None = None,
        remote_remote_password: str | None = None,
        ssh_key_path: str | None = None,
        remote_port: int = 22,
    ) -> int | None:
        with (
            self.preferred_temp_directory() as tmp_dir,
        ):

            file_path = os.path.join(tmp_dir, "bteq_script.txt")
            with open(file_path, "w", encoding=str(temp_file_read_encoding or "UTF-8")) as f:
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
                remote_host,
                remote_user,
                remote_remote_password,
                ssh_key_path,
                remote_port,
            )

    def _transfer_to_and_execute_bteq_on_remote(
        self,
        file_path: str,
        remote_working_dir: str | None,
        bteq_script_encoding: str | None,
        timeout: int,
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
        encrypted_file_path = None
        remote_encrypted_path = None
        try:
            if not self._setup_ssh_connection(
                    remote_host, remote_user, remote_password,
                    ssh_key_path, remote_port
            ):
                raise DagsterError("SSH connection failed")
            if self.ssh_client is None:
                raise DagsterError("Failed to establish SSH connection. `ssh_client` is None.")
            verify_bteq_installed_remote(self.ssh_client)
            password = generate_random_password()  # Encryption/Decryption password
            encrypted_file_path = os.path.join(tmp_dir, "bteq_script.enc")
            generate_encrypted_file_with_openssl(file_path, password, encrypted_file_path)
            remote_encrypted_path = os.path.join(remote_working_dir or "", "bteq_script.enc")

            transfer_file_sftp(self.ssh_client, encrypted_file_path, remote_encrypted_path)

            bteq_command_str = prepare_bteq_command_for_remote_execution(
                timeout=timeout,
                bteq_script_encoding=bteq_script_encoding or "",
                bteq_session_encoding=bteq_session_encoding or "",
                timeout_rc=timeout_rc or -1,
            )
            self.log.info("Executing BTEQ command: %s", bteq_command_str)

            exit_status, stdout, stderr = decrypt_remote_file_to_string(
                self.ssh_client,
                remote_encrypted_path,
                password,
                bteq_command_str,
            )

            failure_message = None
            self.log.info("stdout : %s", stdout)
            self.log.info("stderr : %s", stderr)
            self.log.info("exit_status : %s", exit_status)

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
            else:
                raise DagsterError("SSH connection is not established. `ssh_hook` is None or invalid.")
        except (OSError, socket.gaierror):
            raise DagsterError(
                "SSH connection timed out. Please check the network or server availability."
            )
        except SSHException as e:
            raise DagsterError(f"An unexpected error occurred during SSH connection: {str(e)}")
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
        timeout: int,
        timeout_rc: int | None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None,
        bteq_session_encoding: str | None,
        temp_file_read_encoding: str | None,
    ) -> int | None:
        verify_bteq_installed()
        bteq_command_str = prepare_bteq_command_for_local_execution(
            teradata_connection_resource = self.teradata_connection_resource,
            timeout=timeout,
            bteq_script_encoding=bteq_script_encoding or "",
            bteq_session_encoding=bteq_session_encoding or "",
            timeout_rc=timeout_rc or -1,
        )
        self.log.info("Executing BTEQ command: %s", bteq_command_str)

        process = subprocess.Popen(
            bteq_command_str,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            preexec_fn=os.setsid,
        )
        encode_bteq_script = bteq_script.encode(str(temp_file_read_encoding or "UTF-8"))
        self.log.info("encode_bteq_script : %s", encode_bteq_script)
        stdout_data, _ = process.communicate(input=encode_bteq_script)
        self.log.info("stdout_data : %s", stdout_data)
        try:
            # https://docs.python.org/3.10/library/subprocess.html#subprocess.Popen.wait  timeout is in seconds
            process.wait(timeout=timeout + 60)  # Adding 1 minute extra for BTEQ script timeout
        except subprocess.TimeoutExpired:
            self.on_kill()
            raise DagsterError(f"BTEQ command timed out after {timeout} seconds.")
        conn = self.connection
        conn["sp"] = process  # For `on_kill` support
        failure_message = None
        if stdout_data is None:
            raise DagsterError("Process stdout is None. Unable to read BTEQ output.")
        decoded_line = ""
        for line in stdout_data.splitlines():
            try:
                decoded_line = line.decode("UTF-8").strip()
                self.log.info("decoded_line : %s", decoded_line)
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

    def contains_template(parameter_value):
        # Check if the parameter contains Jinja templating syntax
        return "{{" in parameter_value and "}}" in parameter_value

    def on_kill(self):
        """Terminate the subprocess if running."""
        conn = self.connection
        process = conn.get("sp")
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                process.kill()
            except Exception as e:
                self.log.error("Failed to terminate subprocess: %s", str(e))

    def get_airflow_home_dir(self) -> str:
        """Get the AIRFLOW_HOME directory."""
        return os.environ.get("AIRFLOW_HOME", "~/airflow")

    @contextmanager
    def preferred_temp_directory(self, prefix="bteq_"):
        try:
            temp_dir = tempfile.gettempdir()
            if not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
                raise OSError("OS temp dir not usable")
        except Exception:
            temp_dir = self.get_airflow_home_dir()

        with tempfile.TemporaryDirectory(dir=temp_dir, prefix=prefix) as tmp:
            yield tmp

    def _handle_remote_bteq_file(
            self,
            ssh_client: SSHClient,
            file_path: str | None
    ) -> int | None:
        if file_path:
            with ssh_client:
                sftp = ssh_client.open_sftp()
                try:
                    with sftp.open(file_path, "r") as remote_file:
                        file_content = remote_file.read().decode(self.temp_file_read_encoding or "UTF-8")
                finally:
                    sftp.close()
                # rendered_content = original_content
                # if self.contains_template(original_content):
                #     rendered_content = self.render_template(original_content, context)
                bteq_script = prepare_bteq_script_for_remote_execution(
                    conn=self.connection,
                    sql=file_content,
                )
                self.log.info("Executing BTEQ script with SQL content: %s", bteq_script)
                return self.execute_bteq_script_at_remote(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                )
        else:
            raise ValueError(
                "Please provide a valid file path for the BTEQ script to be executed on the remote machine."
            )

    def _handle_local_bteq_file(
            self,
            file_path: str
    ) -> int | None:
        if file_path and is_valid_file(file_path):
            file_content = read_file(file_path, encoding=str(self.temp_file_read_encoding or "UTF-8"))
            # Manually render using operator's context
            # rendered_content = file_content
            # if self.contains_template(file_content):
            #     rendered_content = self.render_template(file_content, context)
            bteq_script = prepare_bteq_script_for_local_execution(
                sql=file_content,
            )
            self.log.info("Executing BTEQ script with SQL content: %s", bteq_script)
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
            user: str,
            password: Optional[str],
            key_path: Optional[str],
            port: int
    ) -> bool:
        """Establish SSH connection using either password or key auth."""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if key_path:
                key = paramiko.RSAKey.from_private_key_file(key_path)
                self.ssh_client.connect(host, port=port, username=user, pkey=key)
            else:
                if not password:
                    creds = get_stored_credentials(host, user)
                    password = self.cred_manager.decrypt(creds['password']) if creds else None

                if not password:
                    password = getpass.getpass(f"SSH password for {user}@{host}: ")
                    store_credentials(host, user, password)

                self.ssh_client.connect(host, port=port, username=user, password=password)

            self.log.info(f"SSH connected to {user}@{host}")
            return True
        except Exception as e:
            raise DagsterError(f"SSH connection failed: {e}")