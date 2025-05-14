from tempfile import gettempdir, NamedTemporaryFile, TemporaryDirectory
from dagster import DagsterError
import subprocess
import os
class Bteq:
    def __init__(self, connection, teradata_connection_resource, log):
        self.connection = connection
        self.log = log
        self.teradata_connection_resource = teradata_connection_resource

    def execute_bteq(self, bteq_script: str, xcom_push_flag=False, timeout: int | None = None) -> str:
        """
        Executes BTEQ sentences using BTEQ binary.
        :param bteq: string of BTEQ sentences
        :param xcom_push_flag: Flag for pushing last line of BTEQ Log to XCom
        :param timeout: Timeout in seconds for the BTEQ execution.
        :return: The last line of the BTEQ log if xcom_push_flag is True, otherwise None.
        """
        conn = {'host': self.teradata_connection_resource.host, 'login': self.teradata_connection_resource.user,
                'password': self.teradata_connection_resource.password,
                'bteq_output_width': self.teradata_connection_resource.bteq_output_width,
                'bteq_session_encoding': self.teradata_connection_resource.bteq_session_encoding,
                'bteq_quit_zero': self.teradata_connection_resource.bteq_quit_zero,
                'console_output_encoding': self.teradata_connection_resource.console_output_encoding,}
        self.log.info("Executing BTEQ script...")

        with TemporaryDirectory(prefix='dagster_ttu_bteq_') as tmpdir:
            with NamedTemporaryFile(dir=tmpdir, mode='wb') as tmpfile:
                bteq_file_content  = self._prepare_bteq_script(bteq_script,
                                                       conn['host'],
                                                       conn['login'],
                                                       conn['password'],
                                                       conn['bteq_output_width'],
                                                       conn['bteq_session_encoding'],
                                                       conn['bteq_quit_zero']
                                                       )
                self.log.debug("Generated BTEQ script:\n%s", bteq_file_content)

                tmpfile.write(bytes(bteq_file_content,'UTF8'))
                tmpfile.flush()
                tmpfile.seek(0)

                conn['sp'] = subprocess.Popen(['bteq'],
                    stdin = tmpfile,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.STDOUT,
                    cwd = tmpdir,
                    preexec_fn = os.setsid)

                line = ''
                failure_message = "An error occurred during the BTEQ operation. Please review the full BTEQ output for details."
                self.log.info("BTEQ Output:")
                for line in iter(conn["sp"].stdout.readline, b""):
                    decoded_line = line.decode(conn["console_output_encoding"]).strip()
                    self.log.info(decoded_line)
                    last_line = decoded_line
                    if "Failure" in decoded_line:
                        # Save the last failure message
                        failure_message = decoded_line

                # Wait for the BTEQ process to complete with optional timeout
                try:
                    conn["sp"].wait(timeout=timeout)
                    self.log.info("BTEQ command exited with return code %s", conn["sp"].returncode)
                except subprocess.TimeoutExpired:
                    self.on_kill()
                    raise DagsterError(f"BTEQ command timed out after {timeout} seconds")

                # Raise an exception if the BTEQ command failed
                if conn["sp"].returncode:
                    raise DagsterError(
                        f"BTEQ command exited with return code {conn['sp'].returncode} due to: {failure_message}"
                    )

                # Return the last line of the BTEQ log if xcom_push_flag is True
                if xcom_push_flag:
                    return last_line
                return None

    def on_kill(self):
        """
        Terminates the subprocess if it is running.
        Ensures that the process is terminated gracefully and logs the status.
        """
        self.log.debug("Attempting to kill child process...")
        conn = self.get_conn()
        if conn.get("sp"):
            try:
                self.log.info("Terminating subprocess...")
                conn["sp"].terminate()
                conn["sp"].wait(timeout=5)
                self.log.info("Subprocess terminated successfully.")
            except subprocess.TimeoutExpired:
                self.log.warning("Subprocess did not terminate in time. Forcing kill...")
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
        Prepare a BTEQ file with connection parameters for executing SQL sentences with BTEQ syntax.
        :param bteq_string: BTEQ sentences to execute.
        :param host: Teradata Host.
        :param login: Username for login.
        :param password: Password for login.
        :param bteq_output_width: Width of BTEQ output in the console.
        :param bteq_session_encoding: Session encoding. See official Teradata docs for possible values.
        :param bteq_quit_zero: If True, force a .QUIT 0 sentence at the end of the script (forcing return code = 0).
        :return: A formatted BTEQ script as a string.
        :raises ValueError: If any required parameters are invalid.
        """
        # Validate input parameters
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

        # Construct the BTEQ script
        bteq_list = [
            f".LOGON {host}/{login},{password};",
            ".IF ERRORCODE <> 0 THEN .QUIT 8;",
            f".SET WIDTH {bteq_output_width};",
            f".SET SESSION CHARSET '{bteq_session_encoding}';",
            bteq_string.strip(),
        ]

        # Add optional .QUIT 0 command if specified
        if bteq_quit_zero:
            bteq_list.append(".QUIT 0;")

        # Ensure proper termination of the script
        bteq_list.extend([".LOGOFF;", ".EXIT;"])

        # Join the script lines with newlines
        bteq_script = "\n".join(bteq_list)

        return bteq_script