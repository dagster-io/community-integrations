import os
import shutil
import subprocess
import tempfile
import uuid
import socket
from collections.abc import Generator
from contextlib import contextmanager
from typing import Dict, List, Optional
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
    prepare_tpt_script,
    prepare_tpt_variables,
    get_tpt_command,
)
from paramiko.client import SSHClient
from paramiko.ssh_exception import SSHException


def execute_ddl(
    self,
    tpt_script: str | list[str],
    remote_working_dir: str,
) -> int:
    """Execute DDL statements using TPT."""
    if not tpt_script:
        raise ValueError("TPT script cannot be empty")

    tpt_script_content = (
        "\n".join(tpt_script) if isinstance(tpt_script, list) else tpt_script
    )

    if not tpt_script_content.strip():
        raise ValueError("TPT script content cannot be empty")

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
    """Execute tbuild command remotely via SSH."""
    with preferred_temp_directory() as tmp_dir:
        local_script_file = os.path.join(
            tmp_dir, f"tbuild_script_{uuid.uuid4().hex}.sql"
        )
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
            if self.ssh_client:
                verify_tpt_utility_on_remote_host(ssh_client = self.ssh_client, utility = "tbuild", logger = self.log)
                password = generate_random_password()
                generate_encrypted_file_with_openssl(
                    local_script_file, password, encrypted_file_path
                )

                transfer_file_sftp(
                    self.ssh_client,
                    encrypted_file_path,
                    remote_encrypted_script_file,
                    self.log,
                )
                decrypt_remote_file(
                    self.ssh_client,
                    remote_encrypted_script_file,
                    remote_script_file,
                    password,
                    self.log,
                )

                set_remote_file_permissions(
                    self.ssh_client, remote_script_file, self.log
                )

                tbuild_cmd = ["tbuild", "-f", remote_script_file, job_name]
                self.log.info("Executing remote tbuild command")
                exit_status, output, error = execute_remote_command(
                    self.ssh_client, " ".join(tbuild_cmd)
                )
                self.log.info("tbuild output:\n%s", output)

                remote_secure_delete(
                    self.ssh_client,
                    [remote_encrypted_script_file, remote_script_file],
                    self.log,
                )

                if exit_status != 0:
                    raise DagsterError(
                        f"tbuild failed with code {exit_status}: {error}"
                    )

                return exit_status
        except (OSError, socket.gaierror) as e:
            self.log.error("SSH timeout: %s", str(e))
            raise DagsterError("SSH connection timeout")
        except SSHException as e:
            raise DagsterError(f"SSH error: {str(e)}")
        except Exception as e:
            raise DagsterError(f"Remote tbuild execution failed: {str(e)}")
        finally:
            secure_delete(encrypted_file_path, self.log)
            secure_delete(local_script_file, self.log)


def _execute_tbuild_locally(
    self,
    tpt_script_content: str,
) -> int:
    """Execute tbuild command locally."""
    with preferred_temp_directory() as tmp_dir:
        local_script_file = os.path.join(
            tmp_dir, f"tbuild_script_{uuid.uuid4().hex}.sql"
        )
        write_file(local_script_file, tpt_script_content)
        set_local_file_permissions(local_script_file, self.log)

        job_name = f"tbuild_job_{uuid.uuid4().hex}"
        tbuild_cmd = ["tbuild", "-f", local_script_file, job_name]

        if not shutil.which("tbuild"):
            raise DagsterError("tbuild binary not found")

        sp = None
        try:
            self.log.info("Executing local tbuild command")
            sp = subprocess.Popen(
                tbuild_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
            )
            error_lines = []
            if sp.stdout:
                for line in iter(sp.stdout.readline, b""):
                    decoded_line = line.decode("UTF-8").strip()
                    self.log.info(decoded_line)
                    if "error" in decoded_line.lower():
                        error_lines.append(decoded_line)

            sp.wait()
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
    """Execute tdload operation with given parameters."""
    tdload_job_name = tdload_job_name or f"tdload_job_{uuid.uuid4().hex}"
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
    """Execute tdload command remotely via SSH."""
    with preferred_temp_directory() as tmp_dir:
        local_job_var_file = os.path.join(
            tmp_dir, f"tdload_job_var_{uuid.uuid4().hex}.txt"
        )
        write_file(local_job_var_file, job_var_content or "")
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
    """Transfer and execute tdload job on remote host."""
    encrypted_file_path = f"{local_job_var_file}.enc"
    remote_encrypted_job_file = os.path.join(
        remote_working_dir, os.path.basename(encrypted_file_path)
    )
    remote_job_file = os.path.join(
        remote_working_dir, os.path.basename(local_job_var_file)
    )

    try:
        if not ssh_client:
            raise DagsterError("SSH connection not established")

        verify_tpt_utility_on_remote_host(ssh_client, "tdload", log)
        password = generate_random_password()
        generate_encrypted_file_with_openssl(
            local_job_var_file, password, encrypted_file_path
        )

        transfer_file_sftp(
            ssh_client, encrypted_file_path, remote_encrypted_job_file, log
        )
        decrypt_remote_file(
            ssh_client,
            remote_encrypted_job_file,
            remote_job_file,
            password,
            log,
        )

        set_remote_file_permissions(ssh_client, remote_job_file, log)
        tdload_cmd = _build_tdload_command(
            log, remote_job_file, tdload_options, tdload_job_name
        )

        log.info("Executing remote tdload command")
        exit_status, output, error = execute_remote_command(
            ssh_client, " ".join(tdload_cmd)
        )
        log.info("tdload output:\n%s", output)

        remote_secure_delete(
            ssh_client, [remote_encrypted_job_file, remote_job_file], log
        )

        if exit_status != 0:
            raise DagsterError(f"tdload failed with code {exit_status}: {error}")

        return exit_status
    except (OSError, socket.gaierror) as e:
        log.error("SSH timeout: %s", str(e))
        raise DagsterError("SSH connection timeout")
    except SSHException as e:
        raise DagsterError(f"SSH error: {str(e)}")
    except Exception as e:
        raise DagsterError(f"Remote tdload execution failed: {str(e)}")
    finally:
        secure_delete(encrypted_file_path, log)
        secure_delete(local_job_var_file, log)


def _execute_tdload_locally(
    log,
    job_var_content: str | None,
    tdload_options: str | None,
    tdload_job_name: str | None,
) -> int:
    """Execute tdload command locally."""
    with preferred_temp_directory() as tmp_dir:
        local_job_var_file = os.path.join(
            tmp_dir, f"tdload_job_var_{uuid.uuid4().hex}.txt"
        )
        write_file(local_job_var_file, job_var_content or "")
        set_local_file_permissions(local_job_var_file, log)

        tdload_cmd = _build_tdload_command(
            log, local_job_var_file, tdload_options, tdload_job_name
        )

        if not shutil.which("tdload"):
            raise DagsterError("tdload binary not found")

        sp = None
        try:
            log.info("Executing local tdload command")
            sp = subprocess.Popen(
                tdload_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )

            error_lines = []
            if sp.stdout:
                for line in iter(sp.stdout.readline, b""):
                    decoded_line = line.decode("UTF-8").strip()
                    log.info(decoded_line)
                    if "error" in decoded_line.lower():
                        error_lines.append(decoded_line)

            sp.wait()
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
            secure_delete(local_job_var_file, log)
            terminate_subprocess(sp, log)


def _build_tdload_command(
    log, job_var_file: str, tdload_options: str | None, tdload_job_name: str | None
) -> list[str]:
    """Build tdload command with proper options."""
    tdload_cmd = ["tdload", "-j", job_var_file]

    if tdload_options:
        import shlex

        try:
            parsed_options = shlex.split(tdload_options)
            tdload_cmd.extend(parsed_options)
        except ValueError:
            log.warning("Using simple split for tdload_options")
            tdload_cmd.extend(tdload_options.split())

    if tdload_job_name:
        tdload_cmd.append(tdload_job_name)

    return tdload_cmd


def on_kill(self) -> None:
    """Clean up resources when task is terminated."""
    self.log.info("TPT Hook cleanup initiated")


@contextmanager
def preferred_temp_directory(prefix: str = "tpt_") -> Generator[str, None, None]:
    """Context manager for creating temporary directories."""
    try:
        temp_dir = tempfile.gettempdir()
        if not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
            raise OSError("Temp dir not usable")
    except Exception:
        temp_dir = get_dagster_home_dir()
    with tempfile.TemporaryDirectory(dir=temp_dir, prefix=prefix) as tmp:
        yield tmp


def get_dagster_home_dir(self) -> str:
    """Get Dagster home directory."""
    return os.environ.get("DAGSTER_HOME", os.path.expanduser("~/.dagster"))
