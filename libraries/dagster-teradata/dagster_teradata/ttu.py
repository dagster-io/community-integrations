import getpass
import json
import os
import platform
import signal
import subprocess
from datetime import datetime
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Optional

import paramiko
from cryptography.fernet import Fernet
from dagster import DagsterError


class SecureCredentialManager:
    """
    Handles secure storage and retrieval of credentials using symmetric encryption.
    Provides methods for encrypting/decrypting sensitive data with Fernet encryption.
    """

    def __init__(self, key_file: Optional[str] = None):
        """
        Initialize the credential manager.

        Args:
            key_file: Optional path to encryption key file. Defaults to ~/.ssh/bteq_cred_key
        """
        self.key = None
        self.key_file = key_file or os.path.expanduser("~/.ssh/bteq_cred_key")
        self._load_or_generate_key()

    def _load_or_generate_key(self):
        """
        Load encryption key from file or generate a new one.
        Creates the key file with secure permissions (600) if generating new key.
        """
        try:
            if os.path.exists(self.key_file):
                with open(self.key_file, 'rb') as f:
                    self.key = f.read()
            else:
                # Generate new encryption key
                self.key = Fernet.generate_key()
                with open(self.key_file, 'wb') as f:
                    f.write(self.key)
                os.chmod(self.key_file, 0o600)  # Restrict permissions
        except Exception as e:
            raise DagsterError(f"Failed to handle encryption key: {e}")

    def encrypt(self, data: str) -> str:
        """
        Encrypt sensitive data using Fernet symmetric encryption.

        Args:
            data: Plaintext string to encrypt

        Returns:
            Encrypted string (base64 encoded)
        """
        f = Fernet(self.key)
        return f.encrypt(data.encode()).decode()

    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypt sensitive data using Fernet symmetric encryption.

        Args:
            encrypted_data: Encrypted string (base64 encoded)

        Returns:
            Decrypted plaintext string
        """
        f = Fernet(self.key)
        return f.decrypt(encrypted_data.encode()).decode()


class Bteq:
    """
    Main BTEQ operator class that handles both local and remote BTEQ script execution.
    Provides secure credential management and SSH connectivity for remote operations.
    """

    def __init__(self, connection, teradata_connection_resource, log):
        """
        Initialize the BTEQ operator.

        Args:
            connection: Connection object (unused in current implementation)
            teradata_connection_resource: Contains Teradata connection parameters
            log: Logger instance for operation logging
        """
        self.timeout = None
        self.connection = connection
        self.log = log
        self.teradata_connection_resource = teradata_connection_resource
        self.cred_manager = SecureCredentialManager()  # For secure credential storage
        self.ssh_client = None  # Will hold SSH connection if established

    def _setup_ssh_connection(self, ssh_host: str, ssh_user: str, ssh_password: Optional[str] = None,
                              ssh_key_path: Optional[str] = None, ssh_port: int = 22) -> bool:
        """
        Establish SSH connection to remote host using either password or key authentication.

        Args:
            ssh_host: Remote hostname or IP address
            ssh_user: SSH username
            ssh_password: Optional SSH password (prompts if not provided)
            ssh_key_path: Optional path to SSH private key
            ssh_port: SSH port (default: 22)

        Returns:
            bool: True if connection succeeded

        Raises:
            DagsterError: If connection fails
        """
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if ssh_key_path:
                # Key-based authentication
                key = paramiko.RSAKey.from_private_key_file(ssh_key_path)
                self.ssh_client.connect(ssh_host, port=ssh_port, username=ssh_user, pkey=key)
            else:
                # Password authentication
                if not ssh_password:
                    # Try to get password from secure storage
                    creds = self._get_stored_credentials(ssh_host, ssh_user)
                    if creds:
                        ssh_password = self.cred_manager.decrypt(creds['password'])

                if not ssh_password:
                    # Prompt for password if not found
                    ssh_password = getpass.getpass(f"Enter SSH password for {ssh_user}@{ssh_host}: ")
                    # Store the password securely
                    self._store_credentials(ssh_host, ssh_user, ssh_password)

                self.ssh_client.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password)

            self.log.info(f"SSH connection established to {ssh_user}@{ssh_host}")
            return True
        except Exception as e:
            raise DagsterError(f"SSH connection failed: {e}")

    def _store_credentials(self, host: str, username: str, password: str) -> bool:
        """
        Store encrypted credentials in a JSON file with secure permissions.

        Args:
            host: Target hostname or IP address
            username: Authentication username
            password: Plaintext password to encrypt and store

        Returns:
            bool: True if storage succeeded, False otherwise

        Security Notes:
            - Credentials file has 600 permissions
            - Password is encrypted before storage
            - Uses atomic file write to prevent corruption
            - Maintains existing credentials when updating
        """
        cred_file = os.path.expanduser("~/.ssh/bteq_credentials.json")
        cred_key = f"{username}@{host}"

        # Ensure .ssh directory exists with secure permissions
        ssh_dir = os.path.dirname(cred_file)
        os.makedirs(ssh_dir, mode=0o700, exist_ok=True)

        # Load existing credentials if file exists
        creds = {}
        try:
            if os.path.exists(cred_file):
                with open(cred_file, 'r') as f:
                    creds = json.load(f)
        except json.JSONDecodeError as e:
            self.log.error(f"Credentials file corrupted: {e}")
            return False
        except Exception as e:
            self.log.error(f"Error reading credentials file: {e}")
            return False

        # Store new credentials with encrypted password
        try:
            creds[cred_key] = {
                'username': username,
                'password': self.cred_manager.encrypt(password),
                'timestamp': datetime.utcnow().isoformat()  # Add metadata for auditing
            }
        except Exception as e:
            self.log.error(f"Password encryption failed: {e}")
            return False

        # Write credentials with secure file permissions
        try:
            # Write to temporary file first (atomic operation)
            temp_file = f"{cred_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(creds, f, indent=2)  # Pretty print for debugging
            os.chmod(temp_file, 0o600)  # Restrict permissions

            # Atomic rename to replace existing file
            os.replace(temp_file, cred_file)
            return True
        except Exception as e:
            self.log.error(f"Failed to store credentials: {e}")
            try:
                os.unlink(temp_file)  # Clean up temp file if exists
            except:
                pass
            return False

    def _get_stored_credentials(self, host: str, username: str) -> Optional[dict]:
        """
        Retrieve stored credentials for a host/user combination.

        Args:
            host: Target hostname or IP address
            username: Authentication username

        Returns:
            dict: Stored credentials if found, None otherwise
        """
        cred_file = os.path.expanduser("~/.ssh/bteq_credentials.json")
        cred_key = f"{username}@{host}"

        try:
            if os.path.exists(cred_file):
                with open(cred_file, 'r') as f:
                    creds = json.load(f)
                    return creds.get(cred_key)
        except Exception as e:
            self.log.warning(f"Failed to read credentials file: {e}")

        return None

    def _execute_remote_bteq(self, bteq_script_path: str, remote_working_dir: str) -> tuple:
        """
        Execute BTEQ script on remote host via SSH.

        Args:
            bteq_script_path: Local path to BTEQ script
            remote_working_dir: Working directory on remote host

        Returns:
            tuple: (output, error, exit_status)

        Raises:
            DagsterError: If execution fails
        """
        if not self.ssh_client:
            raise DagsterError("SSH connection not established")

        try:
            # Transfer the script to remote host via SFTP
            sftp = self.ssh_client.open_sftp()
            remote_script_name = os.path.basename(bteq_script_path)
            remote_script_path = remote_working_dir + "/" + remote_script_name
            sftp.put(bteq_script_path, remote_script_path)
            sftp.close()

            # Execute the script remotely
            command = f"cd {remote_working_dir} && bteq < {remote_script_name}"
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=self.timeout)

            # Read output and errors
            output = stdout.read().decode()
            error = stderr.read().decode()
            exit_status = stdout.channel.recv_exit_status()

            return output, error, exit_status
        except Exception as e:
            raise DagsterError(f"Remote BTEQ execution failed: {e}")

    def bteq_operator(
            self,
            bteq_script: str = None,
            bteq_script_file: str = None,
            xcom_push_flag=False,
            timeout: int | None = None,
            remote_host: Optional[str] = None,
            remote_user: Optional[str] = None,
            remote_password: Optional[str] = None,
            ssh_key_path: Optional[str] = None,
            remote_port: int = 22,
            remote_working_dir: str = "/tmp",
            expected_return_code: int = 0,
    ) -> Optional[str]:
        """
        Main BTEQ operator method that executes BTEQ scripts (string or file) either locally or remotely.

        Args:
            bteq_script: BTEQ script content to execute
            bteq_script_file: Path to BTEQ script file to execute
            xcom_push_flag: If True, returns last line of output for XCom
            timeout: Execution timeout in seconds
            remote_host: If specified, executes remotely on this host
            remote_user: SSH username for remote execution
            remote_password: SSH password for remote execution
            ssh_key_path: Path to SSH private key for authentication
            remote_port: SSH port (default: 22)
            remote_working_dir: Remote working directory (default: /tmp)
            expected_return_code: Expected return code for successful execution (default: 0)
        Returns:
            str: Last line of output if xcom_push_flag=True, otherwise None

        Raises:
            DagsterError: If execution fails or times out
        """
        # Set timeout (default 5 minutes if not specified)
        self.timeout = timeout if timeout is not None else 300

        # Prepare connection parameters
        conn = {
            "host": self.teradata_connection_resource.host,
            "login": self.teradata_connection_resource.user,
            "password": self.teradata_connection_resource.password,
            "bteq_output_width": self.teradata_connection_resource.bteq_output_width,
            "bteq_session_encoding": self.teradata_connection_resource.bteq_session_encoding,
            "bteq_quit_zero": self.teradata_connection_resource.bteq_quit_zero,
            "console_output_encoding": self.teradata_connection_resource.console_output_encoding,
        }

        self.log.info("Executing BTEQ script...")

        # Use temporary directory for script file
        with TemporaryDirectory(prefix="dagster_ttu_bteq_") as tmpdir:
            tmpfile_path = None
            last_line = ""
            failure_message = "BTEQ operation failed - check logs for details"

            try:
                # Create temporary script file
                with NamedTemporaryFile(dir=tmpdir, mode="wb", delete=False) as tmpfile:
                    # Handle script file input
                    if bteq_script_file:
                        try:
                            with open(bteq_script_file, 'r') as script_file:
                                file_content = script_file.read()
                            bteq_file_content, masked_content = self._prepare_bteq_script(
                                file_content,
                                conn["host"],
                                conn["login"],
                                conn["password"],
                                conn["bteq_output_width"],
                                conn["bteq_session_encoding"],
                                conn["bteq_quit_zero"],
                            )
                        except IOError as e:
                            raise DagsterError(f"Failed to read BTEQ script file: {e}")
                    # Handle script string input
                    else:
                        bteq_file_content, masked_content = self._prepare_bteq_script(
                            bteq_script,
                            conn["host"],
                            conn["login"],
                            conn["password"],
                            conn["bteq_output_width"],
                            conn["bteq_session_encoding"],
                            conn["bteq_quit_zero"],
                        )

                    self.log.debug("Generated BTEQ script:\n%s", masked_content)

                    tmpfile.write(bytes(bteq_file_content, "UTF8"))
                    tmpfile.flush()
                    tmpfile.seek(0)
                    tmpfile_path = tmpfile.name

                if remote_host:
                    # Remote execution
                    self._setup_ssh_connection(
                        ssh_host=remote_host,
                        ssh_user=remote_user,
                        ssh_password=remote_password,
                        ssh_key_path=ssh_key_path,
                        ssh_port=remote_port
                    )

                    output, error, returncode = self._execute_remote_bteq(tmpfile_path, remote_working_dir)

                    # Process remote execution output
                    self.log.info("BTEQ Output:")
                    for line in output.splitlines():
                        decoded_line = line.strip()
                        self.log.info(decoded_line)
                        last_line = decoded_line
                        if "Failure" in decoded_line:
                            failure_message = decoded_line
                        if "RDBMS CRASHED" in decoded_line:
                            failure_message = decoded_line
                            self.log.error("Detected RDBMS crash.")
                            raise DagsterError(f"BTEQ command aborted due to: {failure_message}")

                    if error:
                        self.log.error("BTEQ Errors:")
                        for line in error.splitlines():
                            self.log.error(line.strip())

                    conn["sp"] = type('obj', (object,), {'returncode': returncode})
                else:
                    # Local execution
                    if platform.system() == "Windows":
                        conn["sp"] = subprocess.Popen(
                            ["bteq"],
                            stdin=open(tmpfile_path, 'rb'),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            cwd=tmpdir,
                            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                        )
                    else:
                        conn["sp"] = subprocess.Popen(
                            ["bteq"],
                            stdin=open(tmpfile_path, 'rb'),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            cwd=tmpdir,
                            preexec_fn=os.setsid,
                        )

                    # Process local execution output
                    self.log.info("BTEQ Output:")
                    for line in iter(conn["sp"].stdout.readline, b""):
                        decoded_line = line.decode(conn["console_output_encoding"]).strip()
                        self.log.info(decoded_line)
                        last_line = decoded_line
                        if "Failure" in decoded_line:
                            failure_message = decoded_line
                        if "RDBMS CRASHED" in decoded_line:
                            failure_message = decoded_line
                            self.log.error("Detected RDBMS crash. Terminating BTEQ subprocess.")
                            self.on_kill()
                            raise DagsterError(f"BTEQ command aborted due to: {failure_message}")

                    # Wait for process completion with timeout
                    try:
                        conn["sp"].wait(timeout=self.timeout)
                        self.log.info("BTEQ command exited with return code %s", conn["sp"].returncode)
                    except subprocess.TimeoutExpired:
                        self.on_kill()
                        raise DagsterError(f"BTEQ command timed out after {timeout} seconds")

                # Check return code
                if conn["sp"].returncode != expected_return_code:
                    raise DagsterError(
                        f"BTEQ command exited with return code {conn['sp'].returncode} due to: {failure_message}"
                    )

                # Return last line if xcom_push_flag is set
                return last_line if xcom_push_flag else None

            finally:
                # Cleanup resources
                try:
                    if tmpfile_path and os.path.exists(tmpfile_path):
                        os.remove(tmpfile_path)
                except Exception as e:
                    self.log.warning("Failed to remove temp file %s: %s", tmpfile_path, e)

                if self.ssh_client:
                    self.ssh_client.close()
                    self.ssh_client = None

    def on_kill(self):
        """
        Terminate the running BTEQ process gracefully.
        Handles both local and remote process termination.
        """
        self.log.debug("Attempting to kill child process...")
        conn = self.get_conn()
        if conn.get("sp"):
            sp = conn.get("sp")
            try:
                if hasattr(sp, 'kill'):  # Local process
                    if platform.system() == "Windows":
                        # Send CTRL_BREAK_EVENT to process group
                        sp.send_signal(signal.CTRL_BREAK_EVENT)
                    else:
                        # Send SIGTERM to the process group
                        os.killpg(os.getpgid(sp.pid), signal.SIGTERM)

                    sp.wait(timeout=self.timeout)
                    self.log.info("Subprocess terminated successfully.")
                else:  # Remote process
                    self.log.info("Remote process termination not implemented - connection closed")
            except subprocess.TimeoutExpired:
                self.log.warning("Subprocess did not terminate in time. Forcing kill...")
                if hasattr(sp, 'kill'):
                    conn["sp"].kill()
                    self.log.info("Subprocess killed forcefully.")
            except (ProcessLookupError, OSError) as e:
                self.log.error("Failed to terminate subprocess: %s", e)

    @staticmethod
    def _prepare_bteq_script(
            bteq_string: str,
            host: str,
            login: str,
            password: str,
            bteq_output_width: int,
            bteq_session_encoding: str,
            bteq_quit_zero: bool,
    ) -> str:
        """
        Prepare a complete BTEQ script with connection parameters and commands.

        Args:
            bteq_string: BTEQ commands to execute
            host: Teradata hostname
            login: Teradata username
            password: Teradata password
            bteq_output_width: Output width setting
            bteq_session_encoding: Character encoding
            bteq_quit_zero: Whether to force quit with code 0

        Returns:
            str: Complete BTEQ script ready for execution

        Raises:
            ValueError: If any required parameters are invalid
        """
        # Validate inputs
        if not bteq_string or not bteq_string.strip():
            raise ValueError("BTEQ script cannot be empty.")
        if not host:
            raise ValueError("Host parameter cannot be empty.")
        if not login:
            raise ValueError("Login parameter cannot be empty.")
        if not password:
            raise ValueError("Password parameter cannot be empty.")
        if not isinstance(bteq_output_width, int) or bteq_output_width <= 0:
            raise ValueError("BTEQ output width must be a positive integer.")
        if not bteq_session_encoding:
            raise ValueError("BTEQ session encoding cannot be empty.")

        # Build BTEQ script
        bteq_list = [
            f".LOGON {host}/{login},{password};",
            ".IF ERRORCODE <> 0 THEN .QUIT 8;",  # Exit if login fails
            f".SET WIDTH {bteq_output_width};",
            f".SET SESSION CHARSET '{bteq_session_encoding}';",
            bteq_string.strip(),  # User-provided BTEQ commands
        ]

        if bteq_quit_zero:
            bteq_list.append(".QUIT 0;")  # Force successful exit

        # Standard script footer
        bteq_list.extend([".LOGOFF;", ".EXIT;"])
        bteq_script = "\n".join(bteq_list)

        # Create a masked version for logging
        masked_script = bteq_script.replace(
            f",{password};",
            ",*******;"  # Mask password
        )

        return bteq_script, masked_script  # Return both versions