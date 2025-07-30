from typing import Optional

import paramiko
from dagster import DagsterError

from dagster_teradata.ttu.utils.encryption_utils import (
    get_stored_credentials,
)
from paramiko import SSHClient


def _setup_ssh_connection(
    self,
    host: str,
    user: Optional[str],
    password: Optional[str],
    key_path: Optional[str],
    port: int,
) -> SSHClient:
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
                    raise ValueError("Username is required to fetch stored credentials")
                # Attempt to retrieve stored credentials
                creds = get_stored_credentials(self, host, user)
                password = (
                    self.cred_manager.decrypt(creds["password"]) if creds else None
                )

            self.ssh_client.connect(host, port=port, username=user, password=password)

        self.log.info(f"SSH connected to {user}@{host}")
        return self.ssh_client
    except Exception as e:
        raise DagsterError(f"SSH connection failed: {e}")
