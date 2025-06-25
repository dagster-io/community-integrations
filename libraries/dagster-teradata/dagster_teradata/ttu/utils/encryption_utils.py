#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
    Handles secure storage and retrieval of credentials using Fernet encryption.
    Manages encryption keys and provides methods for secure credential storage.
    """

    def __init__(self, key_file: Optional[str] = None):
        """
        Initialize credential manager with optional custom key file path.
        Defaults to ~/.ssh/bteq_cred_key.
        """
        self.key = None
        self.key_file = key_file or os.path.expanduser("~/.ssh/bteq_cred_key")
        self._load_or_generate_key()

    def _load_or_generate_key(self):
        """Load existing encryption key or generate new key with secure permissions."""
        try:
            if os.path.exists(self.key_file):
                with open(self.key_file, "rb") as f:
                    self.key = f.read()
            else:
                self.key = Fernet.generate_key()
                with open(self.key_file, "wb") as f:
                    f.write(self.key)
                os.chmod(self.key_file, 0o600)  # Restrict to owner-only permissions
        except Exception as e:
            raise DagsterError(f"Encryption key handling failed: {e}")

    def encrypt(self, data: str) -> str:
        """Encrypt sensitive string data using Fernet symmetric encryption."""
        f = Fernet(self.key)
        return f.encrypt(data.encode()).decode()

    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt encrypted string back to plaintext."""
        f = Fernet(self.key)
        return f.decrypt(encrypted_data.encode()).decode()


def generate_random_password(length=12):
    # Define the character set: letters, digits, and special characters
    characters = string.ascii_letters + string.digits + string.punctuation
    # Generate a random password
    password = "".join(secrets.choice(characters) for _ in range(length))
    return password


def generate_encrypted_file_with_openssl(file_path: str, password: str, out_file: str):
    # Write plaintext temporarily to file

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
    ssh_client, remote_enc_file, password, bteq_command_str
):
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


def shell_quote_single(s):
    # Escape single quotes in s, then wrap in single quotes
    # In shell, to include a single quote inside single quotes, close, add '\'' and reopen
    return "'" + s.replace("'", "'\\''") + "'"


def store_credentials(self, host: str, user: str, password: str) -> bool:
    """Securely store SSH credentials in encrypted format."""
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
    """Retrieve stored SSH credentials if they exist."""
    cred_file = os.path.expanduser("~/.ssh/bteq_credentials.json")
    cred_key = f"{user}@{host}"

    try:
        if os.path.exists(cred_file):
            with open(cred_file, "r") as f:
                return json.load(f).get(cred_key)
    except Exception as e:
        self.log.warning(f"Failed to read credentials: {e}")
    return None
