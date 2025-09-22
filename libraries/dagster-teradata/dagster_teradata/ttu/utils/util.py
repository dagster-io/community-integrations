from typing import Optional

import paramiko
from dagster import DagsterError

from dagster_teradata.ttu.utils.encryption_utils import (
    get_stored_credentials,
    SecureCredentialManager,
)
from paramiko import SSHClient


def _setup_ssh_connection(
    host: str,
    user: Optional[str],
    password: Optional[str],
    key_path: Optional[str],
    port: int,
    log=None,
) -> SSHClient:
    """
    Establish SSH connection using either password or key authentication.

    Args:
        host: Remote hostname
        user: Remote username
        password: Remote password (optional if key_path provided)
        key_path: Path to SSH private key (optional if password provided)
        port: SSH port
        log: Optional logger instance

    Returns:
        SSHClient: Established SSH connection

    Raises:
        DagsterError: If connection fails
    """
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if key_path:
            key = paramiko.RSAKey.from_private_key_file(key_path)
            ssh_client.connect(host, port=port, username=user, pkey=key)
        else:
            if not password:
                # Attempt to retrieve stored credentials
                creds = get_stored_credentials(host, user)
                if creds:
                    cred_manager = SecureCredentialManager()
                    password = cred_manager.decrypt(creds["password"])
                else:
                    raise ValueError("No password or key provided for SSH connection")

            ssh_client.connect(host, port=port, username=user, password=password)

        if log:
            log.info(f"SSH connected to {user}@{host}")
        return ssh_client
    except Exception as e:
        if log:
            log.error(f"SSH connection failed: {e}")
        raise DagsterError(f"SSH connection failed: {e}")
