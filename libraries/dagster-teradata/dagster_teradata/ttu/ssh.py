"""SSH client construction shared by the BTEQ and TPT operators."""

import os

import paramiko
from paramiko.ssh_exception import SSHException

SYSTEM_KNOWN_HOSTS = "/etc/ssh/ssh_known_hosts"


class RejectUnknownHostKeyPolicy(paramiko.MissingHostKeyPolicy):
    """Reject hosts missing from known_hosts, explaining how to trust them."""

    def missing_host_key(self, client, hostname, key):
        raise SSHException(
            f"Host key for {hostname} ({key.get_name()} {key.fingerprint}) is not in "
            f"known_hosts ({SYSTEM_KNOWN_HOSTS} or ~/.ssh/known_hosts). After "
            "confirming the fingerprint with the host's administrator, add the key "
            "to ~/.ssh/known_hosts (for example with ssh-keyscan), or pass "
            "ssh_known_hosts_path='/path/to/known_hosts'."
        )


def new_ssh_client(known_hosts_path: str | None = None) -> paramiko.SSHClient:
    """Create an SSH client that verifies the server's host key.

    Keys are loaded from the system and user known_hosts files and from
    `known_hosts_path` when given. A host is rejected when its key is not loaded
    or does not match the loaded key.
    """
    client = paramiko.SSHClient()
    if os.path.isfile(SYSTEM_KNOWN_HOSTS):
        client.load_system_host_keys(SYSTEM_KNOWN_HOSTS)
    client.load_system_host_keys()
    if known_hosts_path:
        client.load_system_host_keys(os.path.expanduser(known_hosts_path))
    client.set_missing_host_key_policy(RejectUnknownHostKeyPolicy())
    return client
