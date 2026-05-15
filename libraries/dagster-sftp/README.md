# dagster-sftp

A Dagster integration for interacting with SFTP servers, providing a high-performance resource for file transfer operations with support for parallel transfers, batch operations, and advanced filtering capabilities.

## Features

- **High-Performance Transfers**: Automatic parallelization of large file transfers using asyncSSH
- **Batch Operations**: Efficient concurrent processing of multiple files
- **Advanced Filtering**: Pattern matching with glob, date-based filtering, and file type filtering
- **Progress Monitoring**: Built-in progress callbacks for tracking transfer status
- **Flexible Authentication**: Support for both password and key-based authentication
- **Comprehensive File Operations**: Upload, download, delete, rename, and directory management
- **Type-Safe**: Full type hints and Pydantic configuration

## Installation

```bash
pip install dagster-sftp
```

## Quick Start

### Basic Configuration

```python
from dagster import Definitions, asset
from dagster_sftp import SFTPResource

# Password authentication
sftp_resource = SFTPResource(
    host="sftp.example.com",
    username="myuser",
    password="mypassword",
    port=22,  # optional, defaults to 22
)

# Key-based authentication
sftp_resource = SFTPResource(
    host="sftp.example.com",
    username="myuser",
    private_key="/path/to/private/key",
    passphrase="key_passphrase",  # optional
    known_hosts="/path/to/known_hosts"  # optional
)

defs = Definitions(
    resources={
        "sftp": sftp_resource
    }
)
```

### Basic File Operations

```python
from dagster import asset
from dagster_sftp import SFTPResource

@asset
def download_files(sftp: SFTPResource):
    """Download a file from SFTP server."""
    # Download a single file
    sftp.get_file(
        remote_path="/data/report.csv",
        local_path="/tmp/report.csv"
    )

    # Upload a file
    sftp.put_file(
        local_path="/tmp/processed_report.csv",
        remote_path="/data/processed/report.csv"
    )

    # Delete a file
    sftp.delete_file("/data/old_report.csv")

    return {"status": "Files processed successfully"}
```

### Complete Example: Sensor for new files + Asset for processing

```python
from datetime import datetime, timedelta
import dagster as dg
import tempfile
from dagster_sftp import SFTPResource, SFTPFileInfo, SFTPFileInfoConfig


@dg.asset
def my_sftp_asset(context: dg.AssetExecutionContext, config: SFTPFileInfoConfig ,sftp: SFTPResource):
    """Demonstrate SFTP asset usage."""
    context.log.info(f"Processing file {config.path}")
    with tempfile.NamedTemporaryFile() as tmp_file:
        sftp.get_file(config.path, tmp_file.name)
        # Process the file...

@dg.sensor(target=my_sftp_asset)
def sftp_file_sensor(context: dg.SensorEvaluationContext, sftp: dg.SFTPResource):
    """Detect new files on SFTP server and trigger processing."""
    last_check = datetime.fromisoformat(context.cursor) if context.cursor else None
    current_check = datetime.now()
    
    try:
        new_files = sftp.list_files(
        base_path="/incoming",
        pattern="*.csv",
        files_only=True,
        modified_after=last_check
        )
    
        if not new_files:
            return dg.SkipReason(f"No new files found since {last_check.isoformat()}")
        
        return dg.SensorResult(
            run_requests=[
                dg.RunRequest(
                    asset_selection=[my_sftp_asset.key],
                    run_key=file.id,
                    run_config=dg.RunConfig(
                        ops={my_sftp_asset.key.to_python_identifier(): {"config": file.to_config_dict()}}
                    ),
                ) for file in new_files
            ],
            cursor=current_check.isoformat(),
        )
    
    except Exception as e:
        context.log.error(f"Error checking SFTP: {str(e)}")
        return dg.Failure(f"Error checking SFTP: {str(e)}")
```

### File Listing with Filtering

```python
import dagster as dg
from datetime import datetime, timedelta
from dagster_sftp import SFTPResource

@dg.asset
def process_recent_files(sftp: SFTPResource):
    """Process files modified in the last week."""
    # List CSV files modified in the last 7 days
    recent_files = sftp.list_files(
        base_path="/data/exports",
        pattern="*.csv",
        files_only=True,
        modified_after=datetime.now() - timedelta(days=7)
    )

    for file_info in recent_files:
        print(f"Processing {file_info.filename}")
        print(f"  Size: {file_info.size} bytes")
        print(f"  Modified: {file_info.modified_time}")

        # Download and process each file
        local_path = f"/tmp/{file_info.filename}"
        sftp.get_file(file_info.path, local_path)
        # Process the file...

    return {"files_processed": len(recent_files)}
```

### Directory Operations

```python
import dagster as dg
from dagster_sftp import SFTPResource

@dg.op
def manage_directories(context: dg.OpExecutionContext, sftp: SFTPResource):
    """Create and manage directory structures."""
    # Create directory structure (including parents)
    sftp.mkdir("/remote/data/2024/reports", mode=0o755)

    # Check if path is directory
    if sftp.is_dir("/remote/data"):
        # Recursively list all Python files
        python_files = sftp.list_files(
            base_path="/remote/data",
            pattern="**/*.py",  # Recursive search
            files_only=True
        )

    # Move/rename directories
    sftp.rename(
        old_path="/remote/old_reports",
        new_path="/remote/archived_reports"
    )

    # Remove directory tree (use with caution!)
    if sftp.file_exists("/remote/temp"):
        context.log.warning("Removing /remote/temp...")
        sftp.rmtree("/remote/temp")

    return {"directories": "managed"}
```

### Performance Tips

The SFTP resource uses asyncSSH internally for optimal performance:

1. **Parallel Transfers**: Large files are automatically split into chunks and transferred in parallel
2. **Concurrent Operations**: Batch operations (like deleting multiple files) are performed concurrently
3. **Connection Pooling**: Efficiently manages SFTP connections
4. **Configurable Parallelization**: Tune `max_requests` and `block_size` for your network conditions

```python
# For large files on fast networks
sftp.get_file(
    remote_path="/data/large_file.zip",
    local_path="/tmp/large_file.zip",
    max_requests=256,  # Increase parallel chunks
    block_size=131072  # 128KB blocks
)

# For many small files
files = [f"/data/file_{i}.txt" for i in range(1000)]
sftp.delete_files(files)  # Concurrent deletion, much faster than sequential
```

## Configuration Reference

### SFTPResource Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | Required | SFTP server hostname or IP address |
| `port` | int | 22 | SFTP server port |
| `username` | str | Required | Username for authentication |
| `password` | str | None | Password for authentication |
| `private_key` | str | None | Path to private key file |
| `passphrase` | str | None | Passphrase for encrypted private key |
| `known_hosts` | str | None | Path to known_hosts file (None disables host key checking) |
| `keepalive_interval` | int | 15 | Seconds between keepalive packets |
| `connect_timeout` | int | 60 | Connection timeout in seconds |
| `default_max_requests` | int | -1 | Max concurrent SFTP requests (-1 for server default) |
| `default_block_size` | int | -1 | Block size for parallel transfers (-1 for server default) |

## API Reference

### Main Classes

#### SFTPResource

The main resource class for SFTP operations. All methods are synchronous but use asyncSSH internally for performance.

**Key Methods:**

- `list_files()` - List and filter files with glob patterns
- `get_file()` - Download file(s) from server
- `put_file()` - Upload file(s) to server
- `delete_file()` - Delete a single file
- `delete_files()` - Delete multiple files concurrently
- `file_exists()` - Check if a file/directory exists
- `mkdir()` - Create directory (with parents)
- `rmtree()` - Remove directory tree
- `is_dir()` - Check if path is a directory
- `is_file()` - Check if path is a regular file
- `rename()` - Rename or move files/directories
- `get_file_info()` - Get detailed file metadata

#### FileInfo

Data class containing file/directory metadata returned by `list_files()` and `get_file_info()`.

**Attributes:**

- `filename` - Name of the file
- `path` - Full path to the file
- `size` - File size in bytes
- `is_dir` - True if directory
- `is_file` - True if regular file
- `is_link` - True if symbolic link
- `mode` - Unix permission mode
- `modified_time` - Last modification time
- `accessed_time` - Last access time (optional)
- `owner` - Owner UID (optional)
- `group` - Group GID (optional)

## Development

### Testing

```bash
make test
```

### Building

```bash
make build
```

### Code Quality

```bash
# Format and lint
make ruff

# Type checking
make check
```

## License

This integration is part of the Dagster Community Integrations project and follows the same licensing terms as the main repository
