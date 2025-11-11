# dagster-sharepoint

A Dagster integration for interacting with SharePoint document libraries using the Microsoft Graph API. This integration provides a Dagster resource that enables file operations, folder management, and data extraction from SharePoint.

## Features

- **Authentication**: Secure authentication using Azure AD client credentials
- **File Operations**: Upload, download, delete, move, and rename files
- **Folder Management**: Create folders, list contents, and navigate folder structures
- **Search**: Search for files across SharePoint document libraries
- **Batch Operations**: List newly created files, filter by extension, recursive operations

## Installation

```bash
pip install dagster-sharepoint
```

## Prerequisites

Before using this integration, you need to set up Azure AD authentication:

1. Register an application in Azure AD
2. Grant the application appropriate SharePoint permissions (e.g., `Sites.ReadWrite.All`)
3. Create a client secret for the application
4. Note down:
   - Tenant ID
   - Client ID (Application ID)
   - Client Secret
   - SharePoint Site ID

## Usage

### Basic Setup

```python
import dagster as dg
from dagster_sharepoint import SharePointResource
import os

# Configure the resource
defs = dg.Definitions(
    resources={
        "sharepoint": SharePointResource(
            site_id=os.getenv("SHAREPOINT_SITE_ID"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )
    }
)
```

### File Operations

```python
import dagster as dg
from dagster_sharepoint import SharePointResource


@dg.asset
def sharepoint_file_operations(sharepoint: SharePointResource):
    # Upload a file
    with open("local_report.xlsx", "rb") as f:
        result = sharepoint.upload_file(
            file_name="report_2024.xlsx",
            content=f,
            folder_path="Documents/Reports/2024"
        )

    if result.success:
        print(f"Uploaded: {result.file_info.name}")

    # Download a file
    content = sharepoint.download_file_by_path("Documents/Reports/report.xlsx")

    # Move a file
    moved_file = sharepoint.move_file_by_path(
        source_file_path="Documents/Temp/draft.docx",
        destination_folder_path="Documents/Final",
        new_name="final_report.docx"
    )

    # Delete a file
    sharepoint.delete_file_by_path("Documents/Temp/old_file.xlsx")
```
### Folder Operations

```python
import dagster as dg
from dagster_sharepoint import SharePointResource


@dg.asset
def manage_folders(sharepoint: SharePointResource):
    # Create a new folder
    new_folder = sharepoint.create_folder(
        folder_name="2024_Q4",
        parent_path="Documents/Reports"
    )

    # List all folders recursively
    folders = sharepoint.list_folders(
        folder_path="Documents",
        recursive=True
    )

    for folder in folders:
        print(f"Folder: {folder.name} (contains {folder.child_count} items)")
```

### Sensor and Asset Pattern

```python
import dagster as dg
from datetime import datetime, timedelta
from dagster_sharepoint import SharePointResource, FileInfoConfig


@dg.asset
def my_asset(context: dg.AssetExecutionContext, sharepoint: SharePointResource, config: FileInfoConfig):
   """
   Example dg.asset that processes SharePoint files.

   This would be triggered by the sharepoint_new_files.
   """
   context.log.info(f"Processing file from SharePoint {config}")
   contents = sharepoint.download_file(config.id)
   context.log.info(f"Downloaded file {config.parent_path}/{config.name}")
   
   # Process file contents...
   
   return contents


@dg.sensor(
   name="sharepoint_new_files",
   minimum_interval_seconds=600,
   target=[my_asset],
)
def sharepoint_new_files(
        context: dg.SensorEvaluationContext,
        sharepoint: SharePointResource,
) -> dg.SensorResult:
   """
   Sensor that checks for new or created files in SharePoint.
 
   This dg.sensor:
   1. Checks a configured SharePoint folder for files created since the last run
   2. Triggers runs for each new file found
   3. Stores the last check timestamp in cursor storage
   """


   last_check = datetime.fromisoformat(context.cursor) if context.cursor else datetime.now() - timedelta(weeks=999)
   current_check = datetime.now()

   try:
      newly_created_files = sharepoint.list_newly_created_files(
         since_timestamp=last_check,
         file_name_glob_pattern="*/my/file/pattern*.csv",
         recursive=True,
      )
      if not newly_created_files:
         return dg.SkipReason(f"No new files found since {last_check.isoformat()}")

      return dg.SensorResult(
         run_requests=[
            dg.RunRequest(
               asset_selection=[my_asset.key],
               run_key=file.id,
               run_config=dg.RunConfig(
                  ops={my_asset.key.to_python_identifier(): {"config": file.to_config_dict()}}
               ),
            )
            for file in newly_created_files
         ],
         cursor=current_check.isoformat(),
      )

   except Exception as e:
      context.log.error(f"Error checking SharePoint: {str(e)}")
      return dg.Failure(f"Error checking SharePoint: {str(e)}")
```

## Testing

```bash
# Run tests
make test

# Run linting and formatting
make ruff

# Run type checking
make check
```

## Development

```bash
# Install development dependencies
make install

# Build the package
make build
```

## License

See LICENSE file in the repository.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.