import fnmatch
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote

import dagster as dg
import requests
from dagster._record import record
from pydantic import Field, PrivateAttr

logger = dg.get_dagster_logger(__name__)


class FileInfoConfig(dg.Config):
    """
    A trimmed down version of FileInfo that can be used to pass into assets
    """

    id: str
    name: str
    extension: str
    created_datetime: str
    last_modified_datetime: str
    size: int
    parent_path: str | None = None
    created_by: str | None = None
    last_modified_by: str | None = None
    mime_type: str | None = None


@record
class FileInfo:
    """SharePoint file information."""

    id: str
    name: str
    web_url: str
    download_url: str
    created_datetime: datetime
    last_modified_datetime: datetime
    size: int
    parent_path: str | None = None
    created_by: str | None = None
    last_modified_by: str | None = None
    mime_type: str | None = None

    def to_config_dict(self) -> dict:
        """Convert to SharepointFile config for use in assets."""
        return FileInfoConfig(
            id=self.id,
            name=self.name,
            extension=self.extension,
            created_datetime=self.created_datetime.isoformat(),
            last_modified_datetime=self.last_modified_datetime.isoformat(),
            size=self.size,
            parent_path=self.parent_path,
            created_by=self.created_by,
            last_modified_by=self.last_modified_by,
            mime_type=self.mime_type,
        ).model_dump()

    @property
    def size_mb(self) -> float:
        """File size in MB.

        :return: File size in megabytes
        :rtype: float
        """
        return self.size / (1024 * 1024)

    @property
    def extension(self) -> str:
        """File extension.

        :return: File extension in lowercase (e.g., '.xlsx')
        :rtype: str
        """
        return Path(self.name).suffix.lower()

    @property
    def name_without_extension(self) -> str:
        """File name without extension.

        :return: File name without extension
        :rtype: str
        """
        return Path(self.name).stem


@record
class FolderInfo:
    """SharePoint folder information."""

    name: str
    id: str
    web_url: str
    created_datetime: datetime
    last_modified_datetime: datetime
    child_count: int
    parent_path: str | None = None

    @property
    def is_root(self) -> bool:
        """Check if this is a root folder.

        :return: True if this is a root folder
        :rtype: bool
        """
        return self.parent_path is None or self.parent_path == ""


@record
class UploadResult:
    """Result of file upload operation."""

    success: bool
    file_info: FileInfo | None = None
    error_message: str | None = None
    upload_duration_seconds: float | None = None


@record
class DriveInfo:
    """Information about a SharePoint drive (document library)."""

    id: str
    name: str
    web_url: str
    drive_type: str


class SharePointResource(dg.ConfigurableResource):
    """Resource for interacting with SharePoint using the Microsoft Graph API.

    This resource provides methods for managing files and folders in SharePoint document libraries,
    including uploading, downloading, listing, searching, and moving files.

    Examples:
        .. code-block:: python

            from dagster import Definitions, EnvVar, asset
            from dagster_sharepoint import SharePointResource

            @asset
            def download_sharepoint_files(sharepoint: SharePointResource):
                # List all Excel files in a folder
                files = sharepoint.list_files(
                    folder_path="Reports/2024",
                    file_extension=".xlsx",
                    recursive=True
                )

                # Download a specific file
                for file in files:
                    content = sharepoint.download_file(file.id)
                    # Process the file content...

            @asset
            def upload_to_sharepoint(sharepoint: SharePointResource):
                # Upload a file to SharePoint
                with open("report.pdf", "rb") as f:
                    result = sharepoint.upload_file(
                        file_name="monthly_report.pdf",
                        content=f,
                        folder_path="Reports/2024/November"
                    )
                    if result.success:
                        print(f"File uploaded: {result.file_info.web_url}")

            Definitions(
                assets=[download_sharepoint_files, upload_to_sharepoint],
                resources={
                    "sharepoint": SharePointResource(
                        site_id=EnvVar("SHAREPOINT_SITE_ID"),
                        client_id=EnvVar("AZURE_CLIENT_ID"),
                        client_secret=EnvVar("AZURE_CLIENT_SECRET"),
                        tenant_id=EnvVar("AZURE_TENANT_ID"),
                    ),
                },
            )
    """

    site_id: str = Field(description="SharePoint site ID")

    client_id: str = Field(description="Azure AD Application (client) ID")
    client_secret: str = Field(description="Azure AD Application client secret")

    tenant_id: str = Field(description="Azure AD Tenant ID")

    _token_cache: tuple[str, int] | None = PrivateAttr(default=None)

    @property
    def access_token(self) -> str:
        """Get access token with automatic refresh.

        :return: Access token for API requests
        :rtype: str
        """
        # Check if we have a cached token and if it's still valid
        if self._token_cache:
            token, expiry_time = self._token_cache
            if time.time() < expiry_time:
                return token

        # Fetch new token
        token, expires_in = self._fetch_new_token()

        # Cache with expiration (subtract 5 minutes for safety)
        expiry_time = int(time.time()) + expires_in - 300
        self._token_cache = (token, expiry_time)

        return token

    def _fetch_new_token(self) -> tuple[str, int]:
        """Fetch a new token from the API.

        :return: Tuple of (access_token, expires_in_seconds)
        :rtype: Tuple[str, int]
        """
        token_url = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )
        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }

        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        token = response.json()["access_token"]
        expires_in = response.json().get("expires_in", 3600)
        return token, expires_in

    def _get_headers(self) -> dict[str, str]:
        """Get authorization headers for API requests.

        :return: Headers dict with authorization token
        :rtype: Dict[str, str]
        """
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _parse_datetime(self, datetime_str: str) -> datetime:
        """Parse Graph API datetime string.

        :param datetime_str: ISO format datetime string from Graph API
        :type datetime_str: str
        :return: Parsed datetime object
        :rtype: datetime
        """
        # Graph API returns ISO format: 2024-01-15T10:30:00Z
        return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an authenticated request to Graph API with retry logic.

        :param method: HTTP method (GET, POST, PUT, DELETE)
        :type method: str
        :param url: Full URL for the API endpoint
        :type url: str
        :param kwargs: Additional request arguments
        :type kwargs: Any
        :return: Response object
        :rtype: requests.Response
        """
        headers = kwargs.pop("headers", {})
        headers.update(self._get_headers())

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=headers, **kwargs)

                if response.status_code == 429:  # Too Many Requests
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                if attempt == max_retries - 1:
                    raise
                if e.response.status_code >= 500:
                    time.sleep(2**attempt)  # Exponential backoff
                    continue
                raise
        raise RuntimeError("Max retries exceeded")

    def get_drive_id(self) -> str:
        """Get the default document library drive ID for the site.

        :return: Drive ID
        :rtype: str
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive"
        response = self._make_request("GET", url)
        return response.json()["id"]

    def list_drives(self) -> list[DriveInfo]:
        """List all document libraries (drives) in the site.

        :return: List of DriveInfo objects
        :rtype: List[DriveInfo]
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        response = self._make_request("GET", url)

        drives = []
        for drive in response.json()["value"]:
            drives.append(
                DriveInfo(
                    id=drive["id"],
                    name=drive["name"],
                    web_url=drive["webUrl"],
                    drive_type=drive["driveType"],
                )
            )
        return drives

    def get_item_by_path(self, path: str, drive_id: str | None = None) -> dict:
        """Get item metadata by path.

        :param path: Path to the item
        :type path: str
        :param drive_id: Optional drive ID (uses default if not provided)
        :type drive_id: Optional[str]
        :return: Item metadata from Graph API
        :rtype: Dict
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        # Clean and encode the path
        clean_path = path.strip("/")
        if clean_path:
            encoded_path = quote(clean_path, safe="")
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/root:/{encoded_path}"
        else:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/root"

        response = self._make_request("GET", url)
        return response.json()

    def list_folders(
        self,
        folder_path: str = "",
        recursive: bool = False,
        drive_id: str | None = None,
    ) -> list[FolderInfo]:
        """List all folders in a path.

        :param folder_path: Path within the drive (e.g., "Documents/2024")
        :type folder_path: str
        :param recursive: Whether to search subfolders recursively
        :type recursive: bool
        :param drive_id: Specific drive ID, uses default if not provided
        :type drive_id: Optional[str]
        :return: List of FolderInfo objects
        :rtype: List[FolderInfo]
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        folders = []

        # Get folders in current path
        folders.extend(self._list_folders_in_path(folder_path, drive_id))

        # Handle recursive search
        if recursive:
            # Get all subfolders and recursively search them
            for folder in list(
                folders
            ):  # Use list() to avoid modifying while iterating
                subfolder_path = (
                    f"{folder_path}/{folder.name}" if folder_path else folder.name
                )
                folders.extend(self.list_folders(subfolder_path, True, drive_id))

        return folders

    def _list_folders_in_path(
        self, folder_path: str, drive_id: str
    ) -> list[FolderInfo]:
        """List folders in a specific path (non-recursive).

        :param folder_path: Path to search in
        :type folder_path: str
        :param drive_id: Drive ID
        :type drive_id: str
        :return: List of FolderInfo objects
        :rtype: List[FolderInfo]
        """
        # Get the folder item
        if folder_path:
            parent_item = self.get_item_by_path(folder_path, drive_id)
            parent_id = parent_item["id"]
        else:
            parent_id = "root"

        # List children
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{parent_id}/children"

        folders = []
        while url:
            response = self._make_request("GET", url)
            data = response.json()

            for item in data["value"]:
                if "folder" in item:  # This is a folder
                    folders.append(
                        FolderInfo(
                            name=item["name"],
                            id=item["id"],
                            web_url=item["webUrl"],
                            created_datetime=self._parse_datetime(
                                item["createdDateTime"]
                            ),
                            last_modified_datetime=self._parse_datetime(
                                item["lastModifiedDateTime"]
                            ),
                            child_count=item.get("folder", {}).get("childCount", 0),
                            parent_path=folder_path,
                        )
                    )

            # Handle pagination
            url = data.get("@odata.nextLink")

        return folders

    def list_files(
        self,
        folder_path: str = "",
        file_extension: str | None = None,
        recursive: bool = False,
        drive_id: str | None = None,
    ) -> list[FileInfo]:
        """List files in a folder.

        :param folder_path: Path within the drive
        :type folder_path: str
        :param file_extension: Filter by extension (e.g., ".xlsx")
        :type file_extension: Optional[str]
        :param recursive: Whether to search subfolders
        :type recursive: bool
        :param drive_id: Specific drive ID
        :type drive_id: Optional[str]
        :return: List of FileInfo objects
        :rtype: List[FileInfo]
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        files = []

        # Get files in current folder
        files.extend(self._list_files_in_folder(folder_path, file_extension, drive_id))

        # Handle recursive search
        if recursive:
            # Get all subfolders recursively
            all_folders = self.list_folders(
                folder_path, recursive=True, drive_id=drive_id
            )

            # Get files from each subfolder
            for folder in all_folders:
                subfolder_path = (
                    f"{folder.parent_path}/{folder.name}"
                    if folder.parent_path
                    else folder.name
                )
                files.extend(
                    self._list_files_in_folder(subfolder_path, file_extension, drive_id)
                )

        return files

    def _list_files_in_folder(
        self, folder_path: str, file_extension: str | None, drive_id: str
    ) -> list[FileInfo]:
        """List files in a specific folder (non-recursive).

        :param folder_path: Path to the folder
        :type folder_path: str
        :param file_extension: Optional extension filter
        :type file_extension: Optional[str]
        :param drive_id: Drive ID
        :type drive_id: str
        :return: List of FileInfo objects
        :rtype: List[FileInfo]
        """
        # Get the folder item
        if folder_path:
            parent_item = self.get_item_by_path(folder_path, drive_id)
            parent_id = parent_item["id"]
        else:
            parent_id = "root"

        # List children
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{parent_id}/children"

        files = []
        while url:
            response = self._make_request("GET", url)
            data = response.json()

            for item in data["value"]:
                if "file" in item:  # This is a file
                    file_name = item["name"]
                    if file_extension is None or file_name.lower().endswith(
                        file_extension.lower()
                    ):
                        files.append(
                            FileInfo(
                                name=file_name,
                                id=item["id"],
                                web_url=item["webUrl"],
                                download_url=item.get(
                                    "@microsoft.graph.downloadUrl", ""
                                ),
                                created_datetime=self._parse_datetime(
                                    item["createdDateTime"]
                                ),
                                last_modified_datetime=self._parse_datetime(
                                    item["lastModifiedDateTime"]
                                ),
                                size=item["size"],
                                created_by=item.get("createdBy", {})
                                .get("user", {})
                                .get("displayName"),
                                last_modified_by=item.get("lastModifiedBy", {})
                                .get("user", {})
                                .get("displayName"),
                                parent_path=folder_path,
                                mime_type=item.get("file", {}).get("mimeType"),
                            )
                        )

            # Handle pagination
            url = data.get("@odata.nextLink")

        return files

    def download_file(self, file_id: str, drive_id: str | None = None) -> bytes:
        """Download a file by its ID.

        :param file_id: The file's ID
        :type file_id: str
        :param drive_id: Drive ID (uses default if not provided)
        :type drive_id: Optional[str]
        :return: File content as bytes
        :rtype: bytes
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{file_id}/content"
        response = self._make_request("GET", url)
        return response.content

    def download_file_by_path(
        self, file_path: str, drive_id: str | None = None
    ) -> bytes:
        """Download a file by its path.

        :param file_path: Path to the file (e.g., "Documents/report.xlsx")
        :type file_path: str
        :param drive_id: Drive ID
        :type drive_id: Optional[str]
        :return: File content as bytes
        :rtype: bytes
        """
        file_item = self.get_item_by_path(file_path, drive_id)
        return self.download_file(file_item["id"], drive_id)

    def upload_file(
        self,
        file_name: str,
        content: bytes | BinaryIO,
        folder_path: str = "",
        drive_id: str | None = None,
        overwrite: bool = True,
    ) -> UploadResult:
        """Upload a file to SharePoint.

        :param file_name: Name for the file
        :type file_name: str
        :param content: File content (bytes or file-like object)
        :type content: Union[bytes, BinaryIO]
        :param folder_path: Target folder path
        :type folder_path: str
        :param drive_id: Drive ID
        :type drive_id: Optional[str]
        :param overwrite: Whether to overwrite existing files
        :type overwrite: bool
        :return: UploadResult object
        :rtype: UploadResult
        """
        start_time = time.time()

        try:
            if not drive_id:
                drive_id = self.get_drive_id()

            # Convert file-like object to bytes
            content_bytes: bytes
            if hasattr(content, "read"):
                content_bytes = content.read()  # type: ignore
            else:
                content_bytes = content  # type: ignore

            # Build the upload path
            if folder_path:
                upload_path = f"{folder_path.strip('/')}/{file_name}"
            else:
                upload_path = file_name

            # Use simple upload for files < 4MB, otherwise use upload session
            if len(content_bytes) < 4 * 1024 * 1024:
                result = self._simple_upload(upload_path, content_bytes, drive_id)
            else:
                result = self._upload_large_file(upload_path, content_bytes, drive_id)

            # Create FileInfo from result
            file_info = FileInfo(
                name=result["name"],
                id=result["id"],
                web_url=result["webUrl"],
                download_url=result.get("@microsoft.graph.downloadUrl", ""),
                created_datetime=self._parse_datetime(result["createdDateTime"]),
                last_modified_datetime=self._parse_datetime(
                    result["lastModifiedDateTime"]
                ),
                size=result["size"],
                parent_path=folder_path,
            )

            return UploadResult(
                success=True,
                file_info=file_info,
                upload_duration_seconds=time.time() - start_time,
            )

        except Exception as e:
            logger.error(f"Upload failed: {traceback.format_exc()}")
            raise e

    def _simple_upload(self, path: str, content: bytes, drive_id: str) -> dict:
        """Upload a small file (< 4MB) using simple upload.

        :param path: Full path for the file
        :type path: str
        :param content: File content
        :type content: bytes
        :param drive_id: Drive ID
        :type drive_id: str
        :return: Upload response from Graph API
        :rtype: Dict
        """
        encoded_path = quote(path, safe="")
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/root:/{encoded_path}:/content"

        headers = self._get_headers()
        headers["Content-Type"] = "application/octet-stream"

        response = self._make_request("PUT", url, data=content, headers=headers)
        return response.json()

    def _upload_large_file(self, path: str, content: bytes, drive_id: str) -> dict:
        """Upload a large file using upload session.

        :param path: Full path for the file
        :type path: str
        :param content: File content
        :type content: bytes
        :param drive_id: Drive ID
        :type drive_id: str
        :return: Upload response from Graph API
        :rtype: Dict
        """
        # Create upload session
        encoded_path = quote(path, safe="")
        session_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/root:/{encoded_path}:/createUploadSession"

        session_response = self._make_request(
            "POST",
            session_url,
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
        )

        upload_url = session_response.json()["uploadUrl"]

        # Upload in chunks (10MB chunks)
        chunk_size = 10 * 1024 * 1024
        file_size = len(content)

        response = None
        for i in range(0, file_size, chunk_size):
            chunk_end = min(i + chunk_size, file_size)
            chunk = content[i:chunk_end]

            headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {i}-{chunk_end - 1}/{file_size}",
            }

            response = requests.put(upload_url, data=chunk, headers=headers)
            response.raise_for_status()

        # Return the final response
        if response:
            return response.json()
        else:
            # Empty file case
            return {"id": "", "name": path.split("/")[-1], "size": 0}

    def delete_file(self, file_id: str, drive_id: str | None = None) -> None:
        """Delete a file by ID.

        :param file_id: File ID
        :type file_id: str
        :param drive_id: Drive ID
        :type drive_id: Optional[str]
        :return: None
        :rtype: None
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{file_id}"
        self._make_request("DELETE", url)

    def delete_file_by_path(self, file_path: str, drive_id: str | None = None) -> None:
        """Delete a file by path.

        :param file_path: Path to the file
        :type file_path: str
        :param drive_id: Drive ID
        :type drive_id: Optional[str]
        :return: None
        :rtype: None
        """
        file_item = self.get_item_by_path(file_path, drive_id)
        self.delete_file(file_item["id"], drive_id)

    def create_folder(
        self, folder_name: str, parent_path: str = "", drive_id: str | None = None
    ) -> FolderInfo:
        """Create a new folder.

        :param folder_name: Name for the new folder
        :type folder_name: str
        :param parent_path: Parent folder path
        :type parent_path: str
        :param drive_id: Drive ID
        :type drive_id: Optional[str]
        :return: FolderInfo for the created folder
        :rtype: FolderInfo
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        # Get parent folder
        if parent_path:
            parent_item = self.get_item_by_path(parent_path, drive_id)
            parent_id = parent_item["id"]
        else:
            parent_id = "root"

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{parent_id}/children"

        body = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename",
        }

        response = self._make_request("POST", url, json=body)
        result = response.json()

        return FolderInfo(
            name=result["name"],
            id=result["id"],
            web_url=result["webUrl"],
            created_datetime=self._parse_datetime(result["createdDateTime"]),
            last_modified_datetime=self._parse_datetime(result["lastModifiedDateTime"]),
            child_count=0,
            parent_path=parent_path,
        )

    def move_file(
        self,
        file_id: str,
        destination_folder_path: str,
        new_name: str | None = None,
        drive_id: str | None = None,
    ) -> FileInfo:
        """Move a file to a different folder and optionally rename it.

        :param file_id: ID of the file to move
        :type file_id: str
        :param destination_folder_path: Path to the destination folder
        :type destination_folder_path: str
        :param new_name: Optional new name for the file
        :type new_name: Optional[str]
        :param drive_id: Drive ID (uses default if not provided)
        :type drive_id: Optional[str]
        :return: FileInfo for the moved file
        :rtype: FileInfo
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        # Get the destination folder
        dest_folder = self.get_item_by_path(destination_folder_path, drive_id)

        # Build the patch body
        body: dict[str, Any] = {"parentReference": {"id": dest_folder["id"]}}

        # Add new name if provided
        if new_name:
            body["name"] = new_name

        # Make the move request
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{file_id}"
        response = self._make_request("PATCH", url, json=body)
        result = response.json()

        # Return FileInfo for the moved file
        return FileInfo(
            name=result["name"],
            id=result["id"],
            web_url=result["webUrl"],
            download_url=result.get("@microsoft.graph.downloadUrl", ""),
            created_datetime=self._parse_datetime(result["createdDateTime"]),
            last_modified_datetime=self._parse_datetime(result["lastModifiedDateTime"]),
            size=result["size"],
            created_by=result.get("createdBy", {}).get("user", {}).get("displayName"),
            last_modified_by=result.get("lastModifiedBy", {})
            .get("user", {})
            .get("displayName"),
            parent_path=destination_folder_path,
            mime_type=result.get("file", {}).get("mimeType"),
        )

    def move_file_by_path(
        self,
        source_file_path: str,
        destination_folder_path: str,
        new_name: str | None = None,
        drive_id: str | None = None,
    ) -> FileInfo:
        """Move a file by path to a different folder and optionally rename it.

        :param source_file_path: Current path to the file
        :type source_file_path: str
        :param destination_folder_path: Path to the destination folder
        :type destination_folder_path: str
        :param new_name: Optional new name for the file
        :type new_name: Optional[str]
        :param drive_id: Drive ID (uses default if not provided)
        :type drive_id: Optional[str]
        :return: FileInfo for the moved file
        :rtype: FileInfo
        """
        # Get the file ID from the path
        file_item = self.get_item_by_path(source_file_path, drive_id)

        # Use the file name from source if new name not provided
        if not new_name:
            new_name = file_item["name"]

        return self.move_file(
            file_item["id"], destination_folder_path, new_name, drive_id
        )

    def rename_file(
        self, file_id: str, new_name: str, drive_id: str | None = None
    ) -> FileInfo:
        """Rename a file without moving it.

        :param file_id: ID of the file to rename
        :type file_id: str
        :param new_name: New name for the file
        :type new_name: str
        :param drive_id: Drive ID (uses default if not provided)
        :type drive_id: Optional[str]
        :return: FileInfo for the renamed file
        :rtype: FileInfo
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        # Build the patch body with just the new name
        body = {"name": new_name}

        # Make the rename request
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/items/{file_id}"
        response = self._make_request("PATCH", url, json=body)
        result = response.json()

        # Return FileInfo for the renamed file
        return FileInfo(
            name=result["name"],
            id=result["id"],
            web_url=result["webUrl"],
            download_url=result.get("@microsoft.graph.downloadUrl", ""),
            created_datetime=self._parse_datetime(result["createdDateTime"]),
            last_modified_datetime=self._parse_datetime(result["lastModifiedDateTime"]),
            size=result["size"],
            created_by=result.get("createdBy", {}).get("user", {}).get("displayName"),
            last_modified_by=result.get("lastModifiedBy", {})
            .get("user", {})
            .get("displayName"),
            mime_type=result.get("file", {}).get("mimeType"),
        )

    def list_newly_created_files(
        self,
        since_timestamp: datetime,
        folder_path: str = "",
        file_name_glob_pattern: str | None = None,
        file_extension: str | None = None,
        recursive: bool = False,
        drive_id: str | None = None,
    ) -> list[FileInfo]:
        """List files that have been created or modified since a given timestamp.

        :param since_timestamp: Only return files created after this time
        :type since_timestamp: datetime
        :param folder_path: Path within the drive to search
        :type folder_path: str
        :param file_name_glob_pattern: Optional pattern to match file names
        :type file_name_glob_pattern: Optional[str]
        :param file_extension: Filter by extension (e.g., ".xlsx")
        :type file_extension: Optional[str]
        :param recursive: Whether to search subfolders
        :type recursive: bool
        :param drive_id: Specific drive ID
        :type drive_id: Optional[str]
        :return: List of FileInfo objects for new/modified files
        :rtype: List[FileInfo]
        """
        if since_timestamp.tzinfo is None:
            since_timestamp = since_timestamp.replace(tzinfo=timezone.utc)

        # Get all files in the specified path
        all_files = self.list_files(
            folder_path=folder_path,
            file_extension=file_extension,
            recursive=recursive,
            drive_id=drive_id,
        )

        if file_name_glob_pattern:
            # Filter files by name pattern if provided
            all_files = [
                file
                for file in all_files
                if fnmatch.fnmatch(file.name.lower(), file_name_glob_pattern.lower())
            ]

        # Filter to only files modified since the timestamp
        modified_files = [
            file for file in all_files if file.created_datetime >= since_timestamp
        ]

        # Sort by modification time (newest first)
        modified_files.sort(key=lambda f: f.created_datetime, reverse=True)

        return modified_files

    def search_files(
        self,
        search_query: str,
        file_extension: str | None = None,
        drive_id: str | None = None,
        limit: int = 100,
    ) -> list[FileInfo]:
        """Search for files across the drive.

        :param search_query: Search query string
        :type search_query: str
        :param file_extension: Filter by extension
        :type file_extension: Optional[str]
        :param drive_id: Drive ID
        :type drive_id: Optional[str]
        :param limit: Maximum results to return
        :type limit: int
        :return: List of FileInfo objects
        :rtype: List[FileInfo]
        """
        if not drive_id:
            drive_id = self.get_drive_id()

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{drive_id}/root/search(q='{quote(search_query)}')"

        files = []
        count = 0

        while url and count < limit:
            response = self._make_request("GET", url)
            data = response.json()

            for item in data["value"]:
                if "file" in item:
                    file_name = item["name"]
                    if file_extension is None or file_name.lower().endswith(
                        file_extension.lower()
                    ):
                        files.append(
                            FileInfo(
                                name=file_name,
                                id=item["id"],
                                web_url=item["webUrl"],
                                download_url=item.get(
                                    "@microsoft.graph.downloadUrl", ""
                                ),
                                created_datetime=self._parse_datetime(
                                    item["createdDateTime"]
                                ),
                                last_modified_datetime=self._parse_datetime(
                                    item["lastModifiedDateTime"]
                                ),
                                size=item["size"],
                                created_by=item.get("createdBy", {})
                                .get("user", {})
                                .get("displayName"),
                                last_modified_by=item.get("lastModifiedBy", {})
                                .get("user", {})
                                .get("displayName"),
                            )
                        )
                        count += 1
                        if count >= limit:
                            break

            url = data.get("@odata.nextLink")

        return files[:limit]
