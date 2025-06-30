from __future__ import annotations

import json
import os
import secrets
import string
import subprocess
from datetime import datetime
from typing import Optional

from cryptography.fernet import Fernet
from dagster import DagsterError


class SecureCredentialManager:
    """
    A secure credential manager that handles encryption and decryption of sensitive data
    using Fernet symmetric encryption.

    Attributes:
        key (bytes): The encryption key used for cryptographic operations.
        key_file (str): Path to the file where the encryption key is stored.
    """

    def __init__(self, key_file: Optional[str] = None):
        """
        Initialize the credential manager with an optional custom key file path.

        Args:
            key_file (Optional[str]): Path to the encryption key file. If not provided,
                                     defaults to '~/.ssh/bteq_cred_key'.

        Note:
            If the key file doesn't exist, a new encryption key will be generated and stored.
        """
        self.key = None
        self.key_file = key_file or os.path.expanduser("~/.ssh/bteq_cred_key")
        self._load_or_generate_key()

    def _load_or_generate_key(self):
        """
        Load an existing encryption key or generate a new one with secure permissions.

        Raises:
            DagsterError: If there's any issue in loading or generating the key.
        """
        try:
            if os.path.exists(self.key_file):
                with open(self.key_file, "rb") as f:
                    self.key = f.read()
            else:
                self.key = Fernet.generate_key()
                key_dir = os.path.dirname(self.key_file)
                os.makedirs(key_dir, exist_ok=True)

                with open(self.key_file, "wb") as f:
                    f.write(self.key)
                os.chmod(self.key_file, 0o600)  # Restrict to owner-only permissions
        except Exception as e:
            raise DagsterError(f"Encryption key handling failed: {e}")

    def encrypt(self, data: str) -> str:
        """
        Encrypt sensitive string data using Fernet symmetric encryption.

        Args:
            data (str): The plaintext string to encrypt.

        Returns:
            str: The encrypted string in URL-safe base64 format.

        Example:
            >>> manager = SecureCredentialManager()
            >>> encrypted = manager.encrypt("my_password")
        """
        if self.key is None:
            raise ValueError("Encryption key is not initialized")
        f = Fernet(self.key)
        return f.encrypt(data.encode()).decode()

    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypt an encrypted string back to plaintext.

        Args:
            encrypted_data (str): The encrypted string in URL-safe base64 format.

        Returns:
            str: The decrypted plaintext string.

        Raises:
            cryptography.fernet.InvalidToken: If the encrypted data is invalid or corrupted.

        Example:
            >>> manager = SecureCredentialManager()
            >>> decrypted = manager.decrypt(encrypted_string)
        """
        if self.key is None:
            raise ValueError("Encryption key is not initialized")
        f = Fernet(self.key)
        return f.decrypt(encrypted_data.encode()).decode()


def generate_random_password(length: int = 12) -> str:
    """
    Generate a cryptographically secure random password.

    Args:
        length (int, optional): Length of the password to generate. Defaults to 12.

    Returns:
        str: A randomly generated password containing letters, digits, and special characters.

    Note:
        Uses Python's secrets module for cryptographically secure random generation.
        The password includes:
        - Uppercase and lowercase letters
        - Digits
        - Special punctuation characters

    Example:
        >>> password = generate_random_password(16)
    """
    # Define the character set: letters, digits, and special characters
    characters = string.ascii_letters + string.digits + string.punctuation
    # Generate a random password using cryptographically secure random selection
    password = "".join(secrets.choice(characters) for _ in range(length))
    return password


def generate_encrypted_file_with_openssl(
    file_path: str, password: str, out_file: str
) -> None:
    """
    Encrypt a file using OpenSSL with AES-256-CBC encryption.

    Args:
        file_path (str): Path to the input file to encrypt.
        password (str): Password to use for encryption.
        out_file (str): Path where the encrypted file will be saved.

    Raises:
        subprocess.CalledProcessError: If the OpenSSL command fails.
        FileNotFoundError: If the input file doesn't exist.

    Note:
        Uses the following OpenSSL parameters:
        - AES-256-CBC encryption algorithm
        - Salt for additional security
        - PBKDF2 for key derivation
        - Password-based encryption

    Example:
        >>> generate_encrypted_file_with_openssl("plain.txt", "secret", "encrypted.enc")
    """
    # Run openssl enc with AES-256-CBC, pbkdf2, salt
    cmd = [
        "openssl",
        "enc",
        "-aes-256-cbc",
        "-salt",
        "-pbkdf2",
        "-pass",
        f"pass:{password}",
        "-in",
        file_path,
        "-out",
        out_file,
    ]
    subprocess.run(cmd, check=True)


def decrypt_remote_file_to_string(
    ssh_client, remote_enc_file: str, password: str, bteq_command_str: str
) -> tuple[int, str, str]:
    """
    Decrypt a remote file and pipe the output to a BTEQ command.

    Args:
        ssh_client: An active SSH connection to the remote machine.
        remote_enc_file (str): Path to the encrypted file on the remote machine.
        password (str): Password used to decrypt the file.
        bteq_command_str (str): BTEQ command to execute with the decrypted content.

    Returns:
        tuple[int, str, str]: A tuple containing:
            - exit_status (int): The exit code of the remote command
            - output (str): The stdout output from the command
            - err (str): The stderr output from the command

    Note:
        The decryption uses OpenSSL with the same parameters as encryption:
        - AES-256-CBC
        - PBKDF2 key derivation
        - Salt

    Example:
        >>> exit_code, output, error = decrypt_remote_file_to_string(
        ...     ssh_client, "remote.enc", "password", "bteq < commands.sql"
        ... )
    """
    # Run openssl decrypt command on remote machine
    quoted_password = shell_quote_single(password)

    decrypt_cmd = (
        f"openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:{quoted_password} -in {remote_enc_file} | "
        + bteq_command_str
    )
    stdin, stdout, stderr = ssh_client.exec_command(decrypt_cmd)
    # Wait for command to finish
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().decode()
    err = stderr.read().decode()
    return exit_status, output, err


def shell_quote_single(s: str) -> str:
    """
    Properly escape and single-quote a string for safe use in shell commands.

    Args:
        s (str): The string to be quoted.

    Returns:
        str: The safely quoted string.

    Example:
        >>> shell_quote_single("don't break")
        "'don'\\''t break'"
    """
    # Escape single quotes in s, then wrap in single quotes
    # In shell, to include a single quote inside single quotes, close, add '\'' and reopen
    return "'" + s.replace("'", "'\\''") + "'"


def store_credentials(self, host: str, user: str, password: str) -> bool:
    """
    Securely store SSH credentials in an encrypted JSON file.

    Args:
        host (str): The hostname or IP address of the server.
        user (str): The username for authentication.
        password (str): The password to store (will be encrypted).

    Returns:
        bool: True if credentials were successfully stored, False otherwise.

    Note:
        - Credentials are stored in ~/.ssh/bteq_credentials.json
        - The directory is created with 700 permissions if it doesn't exist
        - The credentials file has 600 permissions (owner read/write only)
        - Password is encrypted before storage
        - Uses atomic write (writes to temp file then renames) to prevent corruption

    Example:
        >>> store_credentials("example.com", "user1", "securepassword")
    """
    cred_file = os.path.expanduser("~/.ssh/bteq_credentials.json")
    os.makedirs(os.path.dirname(cred_file), mode=0o700, exist_ok=True)

    creds = {}
    if os.path.exists(cred_file):
        try:
            with open(cred_file, "r") as f:
                creds = json.load(f)
        except Exception as e:
            self.log.error(f"Failed to read credentials: {e}")

    cred_key = f"{user}@{host}"
    creds[cred_key] = {
        "username": user,
        "password": self.cred_manager.encrypt(password),
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        temp_file = f"{cred_file}.tmp"
        with open(temp_file, "w") as f:
            json.dump(creds, f, indent=2)
        os.chmod(temp_file, 0o600)
        os.replace(temp_file, cred_file)
        return True
    except Exception as e:
        self.log.error(f"Failed to store credentials: {e}")
        return False


def get_stored_credentials(self, host: str, user: str) -> Optional[dict]:
    """
    Retrieve stored SSH credentials if they exist.

    Args:
        host (str): The hostname or IP address of the server.
        user (str): The username for which to retrieve credentials.

    Returns:
        Optional[dict]: A dictionary containing the stored credentials if found,
                        or None if no credentials exist or an error occurred.
                        The dictionary contains:
                        - username: The stored username
                        - password: The encrypted password
                        - timestamp: When the credentials were stored

    Example:
        >>> creds = get_stored_credentials("example.com", "user1")
        >>> if creds:
        ...     password = cred_manager.decrypt(creds["password"])
    """
    cred_file = os.path.expanduser("~/.ssh/bteq_credentials.json")
    cred_key = f"{user}@{host}"

    try:
        if os.path.exists(cred_file):
            with open(cred_file, "r") as f:
                return json.load(f).get(cred_key)
    except Exception as e:
        self.log.warning(f"Failed to read credentials: {e}")
    return None
