"""
Test suite for SharePointResource using responses library.

Uses the responses library for mocking HTTP requests with modern best practices.
All test methods include proper type hints and use the newest syntax.
"""

from datetime import datetime
from typing import Generator
from unittest.mock import patch

import pytest
import responses
from requests.exceptions import HTTPError
from responses import matchers

from dagster_sharepoint.resource import (
    DriveInfo,
    FileInfo,
    FolderInfo,
    SharePointResource,
    UploadResult,
)


class TestSharePointResource:
    """Base test class for SharePointResource functionality."""

    @pytest.fixture
    def sharepoint_resource(self) -> SharePointResource:
        """Create a test SharePointResource instance."""
        return SharePointResource(
            site_id="test_site_id",
            client_id="test_client_id",
            client_secret="test_client_secret",
            tenant_id="test_tenant_id",
        )

    @pytest.fixture(autouse=True)
    def mock_responses(self) -> Generator[responses.RequestsMock, None, None]:
        """Auto-use fixture to activate responses for all tests."""
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            yield rsps


class TestSharePointResourceAuthentication(TestSharePointResource):
    """Test authentication functionality."""

    @responses.activate
    def test_fetch_new_token(self, sharepoint_resource: SharePointResource) -> None:
        """Test new token retrieval using responses."""
        token_response = {
            "access_token": "test_access_token",
            "expires_in": 3600,
        }

        # Mock the token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json=token_response,
            status=200,
            match=[
                matchers.urlencoded_params_matcher(
                    {
                        "grant_type": "client_credentials",
                        "client_id": "test_client_id",
                        "client_secret": "test_client_secret",
                        "scope": "https://graph.microsoft.com/.default",
                    }
                )
            ],
        )

        # Call the method
        token, expires_in = sharepoint_resource._fetch_new_token()

        # Verify the token
        assert token == "test_access_token"
        assert expires_in == 3600
        assert len(responses.calls) == 1

    @responses.activate
    def test_access_token_property_caching(
        self, sharepoint_resource: SharePointResource
    ) -> None:
        """Test access token property with caching."""
        # Mock token endpoint for multiple calls
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        with patch("time.time", return_value=1000) as mock_time:
            # First access - should fetch new token
            token = sharepoint_resource.access_token
            assert token == "test_token"
            assert len(responses.calls) == 1

            # Second access within expiry - should use cached token
            mock_time.return_value = 1500  # 500 seconds later
            token = sharepoint_resource.access_token
            assert token == "test_token"
            assert len(responses.calls) == 1  # Still only one call

            # Third access after expiry - should fetch new token
            mock_time.return_value = 5000  # Well past expiry

            # Add another response for the second token fetch
            responses.add(
                responses.POST,
                "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
                json={"access_token": "new_test_token", "expires_in": 3600},
                status=200,
            )

            token = sharepoint_resource.access_token
            assert token == "new_test_token"
            assert len(responses.calls) == 2


class TestSharePointResourceAPICalls(TestSharePointResource):
    """Test API call functionality."""

    @responses.activate
    def test_make_request_success(
        self, sharepoint_resource: SharePointResource
    ) -> None:
        """Test successful API request."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock API endpoint
        test_url = "https://graph.microsoft.com/v1.0/test"
        responses.add(
            responses.GET,
            test_url,
            json={"data": "test"},
            status=200,
            match=[
                matchers.header_matcher(
                    {
                        "Authorization": "Bearer test_token",
                        "Content-Type": "application/json",
                    }
                )
            ],
        )

        # Call the method
        response = sharepoint_resource._make_request("GET", test_url)

        # Verify the response
        assert response.json() == {"data": "test"}
        assert response.status_code == 200
        assert len(responses.calls) == 2  # One for token, one for API

    @responses.activate
    def test_make_request_rate_limit_retry(
        self, sharepoint_resource: SharePointResource
    ) -> None:
        """Test rate limit handling with retry."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        test_url = "https://test.com"

        # First request returns 429 (rate limit)
        responses.add(
            responses.GET,
            test_url,
            status=429,
            headers={"Retry-After": "1"},  # Use 1 second for faster tests
        )

        # Second request succeeds
        responses.add(
            responses.GET,
            test_url,
            json={"success": True},
            status=200,
        )

        with patch("time.sleep") as mock_sleep:
            # Call the method
            response = sharepoint_resource._make_request("GET", test_url)

            # Verify retry logic
            assert response.status_code == 200
            assert response.json() == {"success": True}
            assert len(responses.calls) == 3  # Token + 429 + success
            mock_sleep.assert_called_once_with(1)

    @responses.activate
    def test_make_request_server_error_retry(
        self, sharepoint_resource: SharePointResource
    ) -> None:
        """Test server error retry with exponential backoff."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        test_url = "https://test.com"

        # First request returns 500 (server error)
        responses.add(
            responses.GET,
            test_url,
            status=500,
            json={"error": "Internal Server Error"},
        )

        # Second request succeeds
        responses.add(
            responses.GET,
            test_url,
            json={"success": True},
            status=200,
        )

        with patch("time.sleep") as mock_sleep:
            # Call the method
            response = sharepoint_resource._make_request("GET", test_url)

            # Verify retry logic
            assert response.status_code == 200
            assert response.json() == {"success": True}
            assert len(responses.calls) == 3  # Token + 500 + success
            mock_sleep.assert_called_once_with(1)  # 2^0 = 1


class TestSharePointResourceDriveOperations(TestSharePointResource):
    """Test drive-related operations."""

    @responses.activate
    def test_get_drive_id(self, sharepoint_resource: SharePointResource) -> None:
        """Test getting default drive ID."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock drive endpoint
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Call the method
        drive_id = sharepoint_resource.get_drive_id()

        # Verify
        assert drive_id == "test_drive_id"
        assert len(responses.calls) == 2

    @responses.activate
    def test_list_drives(self, sharepoint_resource: SharePointResource) -> None:
        """Test listing all drives."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock drives endpoint
        drives_response = {
            "value": [
                {
                    "id": "drive1",
                    "name": "Documents",
                    "webUrl": "https://sharepoint.com/docs",
                    "driveType": "documentLibrary",
                },
                {
                    "id": "drive2",
                    "name": "Site Assets",
                    "webUrl": "https://sharepoint.com/assets",
                    "driveType": "documentLibrary",
                },
            ]
        }

        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives",
            json=drives_response,
            status=200,
        )

        # Call the method
        drives = sharepoint_resource.list_drives()

        # Verify
        assert len(drives) == 2
        assert isinstance(drives[0], DriveInfo)
        assert drives[0].id == "drive1"
        assert drives[0].name == "Documents"
        assert drives[1].id == "drive2"
        assert drives[1].name == "Site Assets"


class TestSharePointResourceFileOperations(TestSharePointResource):
    """Test file-related operations."""

    @responses.activate
    def test_get_item_by_path(self, sharepoint_resource: SharePointResource) -> None:
        """Test getting item by path."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock item by path endpoint
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root:/Documents%2Ftest.xlsx",
            json={"id": "item_id", "name": "test.xlsx"},
            status=200,
        )

        # Call the method
        item = sharepoint_resource.get_item_by_path("Documents/test.xlsx")

        # Verify
        assert item["id"] == "item_id"
        assert item["name"] == "test.xlsx"

    @responses.activate
    def test_list_files(self, sharepoint_resource: SharePointResource) -> None:
        """Test listing files."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock list children endpoint
        items_response = {
            "value": [
                {
                    "name": "test.xlsx",
                    "id": "file1",
                    "webUrl": "https://sharepoint.com/test.xlsx",
                    "@microsoft.graph.downloadUrl": "https://download.url",
                    "createdDateTime": "2024-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2024-01-02T00:00:00Z",
                    "size": 1024,
                    "file": {"mimeType": "application/vnd.ms-excel"},
                },
                {"name": "folder1", "id": "folder1", "folder": {"childCount": 5}},
            ]
        }

        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/root/children",
            json=items_response,
            status=200,
        )

        # Call the method
        files = sharepoint_resource.list_files()

        # Verify
        assert len(files) == 1  # Only files, not folders
        assert isinstance(files[0], FileInfo)
        assert files[0].name == "test.xlsx"
        assert files[0].size == 1024
        assert files[0].mime_type == "application/vnd.ms-excel"

    @responses.activate
    def test_list_files_with_extension_filter(
        self, sharepoint_resource: SharePointResource
    ) -> None:
        """Test listing files with extension filter."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock list children endpoint
        items_response = {
            "value": [
                {
                    "name": "test.xlsx",
                    "id": "file1",
                    "webUrl": "https://sharepoint.com/test.xlsx",
                    "@microsoft.graph.downloadUrl": "https://download.url",
                    "createdDateTime": "2024-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2024-01-02T00:00:00Z",
                    "size": 1024,
                    "file": {},
                },
                {
                    "name": "test.docx",
                    "id": "file2",
                    "webUrl": "https://sharepoint.com/test.docx",
                    "@microsoft.graph.downloadUrl": "https://download.url",
                    "createdDateTime": "2024-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2024-01-02T00:00:00Z",
                    "size": 2048,
                    "file": {},
                },
            ]
        }

        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/root/children",
            json=items_response,
            status=200,
        )

        # Call the method with extension filter
        files = sharepoint_resource.list_files(file_extension=".xlsx")

        # Verify
        assert len(files) == 1
        assert files[0].name == "test.xlsx"

    @responses.activate
    def test_list_folders(self, sharepoint_resource: SharePointResource) -> None:
        """Test listing folders."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock list children endpoint
        items_response = {
            "value": [
                {
                    "name": "Folder1",
                    "id": "folder1",
                    "webUrl": "https://sharepoint.com/folder1",
                    "createdDateTime": "2024-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2024-01-02T00:00:00Z",
                    "folder": {"childCount": 5},
                },
                {"name": "file.txt", "id": "file1", "file": {}},
            ]
        }

        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/root/children",
            json=items_response,
            status=200,
        )

        # Call the method
        folders = sharepoint_resource.list_folders()

        # Verify
        assert len(folders) == 1  # Only folders, not files
        assert isinstance(folders[0], FolderInfo)
        assert folders[0].name == "Folder1"
        assert folders[0].child_count == 5


class TestSharePointResourceUploadDownload(TestSharePointResource):
    """Test upload and download functionality."""

    @responses.activate
    def test_download_file(self, sharepoint_resource: SharePointResource) -> None:
        """Test downloading a file."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock download endpoint
        file_content = b"file content"
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/file_id_123/content",
            body=file_content,
            status=200,
            content_type="application/octet-stream",
        )

        # Call the method
        content = sharepoint_resource.download_file("file_id_123")

        # Verify
        assert content == file_content

    @responses.activate
    def test_upload_small_file(self, sharepoint_resource: SharePointResource) -> None:
        """Test uploading a small file."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock upload endpoint
        upload_response = {
            "name": "test.txt",
            "id": "file_id",
            "webUrl": "https://sharepoint.com/test.txt",
            "@microsoft.graph.downloadUrl": "https://download.url",
            "createdDateTime": "2024-01-01T00:00:00Z",
            "lastModifiedDateTime": "2024-01-02T00:00:00Z",
            "size": 100,
        }

        responses.add(
            responses.PUT,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root:/Documents%2Ftest.txt:/content",
            json=upload_response,
            status=201,
        )

        # Call the method
        content = b"small file content"
        result = sharepoint_resource.upload_file("test.txt", content, "Documents")

        # Verify
        assert isinstance(result, UploadResult)
        assert result.success is True
        assert result.file_info is not None
        assert result.file_info.name == "test.txt"
        assert result.file_info.size == 100

    @responses.activate
    def test_upload_large_file(self, sharepoint_resource: SharePointResource) -> None:
        """Test uploading a large file (multipart upload)."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock create upload session
        responses.add(
            responses.POST,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root:/large.bin:/createUploadSession",
            json={"uploadUrl": "https://sn3302.up.1drv.com/up/upload-session"},
            status=200,
        )

        # Mock upload chunks (for 50MB file with 10MB chunks = 5 chunks)
        content_size = 50 * 1024 * 1024  # 50MB
        chunk_size = 10 * 1024 * 1024  # 10MB
        num_chunks = (content_size + chunk_size - 1) // chunk_size

        for index, chunk in enumerate(range(num_chunks), 1):
            # Mock each chunk upload
            if index == num_chunks:
                json_data = {
                    "name": "large.bin",
                    "id": "file_id",
                    "webUrl": "https://sharepoint.com/large.bin",
                    "@microsoft.graph.downloadUrl": "https://download.url",
                    "createdDateTime": "2024-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2024-01-02T00:00:00Z",
                    "size": content_size,
                }
            else:
                json_data = {}

            responses.add(
                responses.PUT,
                "https://sn3302.up.1drv.com/up/upload-session",
                json=json_data,  # Intermediate responses
                status=202 if chunk < num_chunks - 1 else 201,
            )

        # Call the method with large content
        content = b"x" * content_size
        result = sharepoint_resource.upload_file("large.bin", content)

        # Verify
        assert isinstance(result, UploadResult)
        assert result.success is True
        assert result.file_info is not None
        assert result.file_info.name == "large.bin"
        assert result.file_info.size == content_size

    @responses.activate
    def test_delete_file(self, sharepoint_resource: SharePointResource) -> None:
        """Test deleting a file."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock delete endpoint
        responses.add(
            responses.DELETE,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/file_id_123",
            status=204,
        )

        # Call the method
        sharepoint_resource.delete_file("file_id_123")

        # Verify the DELETE request was made
        delete_calls = [
            call for call in responses.calls if call.request.method == "DELETE"
        ]
        assert len(delete_calls) == 1

    @responses.activate
    def test_create_folder(self, sharepoint_resource: SharePointResource) -> None:
        """Test creating a folder."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock get parent folder
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root:/Documents",
            json={"id": "parent_folder_id"},
            status=200,
        )

        # Mock create folder endpoint
        folder_response = {
            "name": "NewFolder",
            "id": "folder_id",
            "webUrl": "https://sharepoint.com/newfolder",
            "createdDateTime": "2024-01-01T00:00:00Z",
            "lastModifiedDateTime": "2024-01-02T00:00:00Z",
        }

        responses.add(
            responses.POST,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/parent_folder_id/children",
            json=folder_response,
            status=201,
            match=[
                matchers.json_params_matcher(
                    {
                        "name": "NewFolder",
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "rename",
                    }
                )
            ],
        )

        # Call the method
        folder = sharepoint_resource.create_folder("NewFolder", "Documents")

        # Verify
        assert isinstance(folder, FolderInfo)
        assert folder.name == "NewFolder"
        assert folder.id == "folder_id"


class TestSharePointResourceSearch(TestSharePointResource):
    """Test search functionality."""

    @responses.activate
    def test_search_files(self, sharepoint_resource: SharePointResource) -> None:
        """Test searching for files."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock search endpoint
        search_response = {
            "value": [
                {
                    "name": "report.xlsx",
                    "id": "file1",
                    "webUrl": "https://sharepoint.com/report.xlsx",
                    "@microsoft.graph.downloadUrl": "https://download.url",
                    "createdDateTime": "2024-01-01T00:00:00Z",
                    "lastModifiedDateTime": "2024-01-02T00:00:00Z",
                    "size": 1024,
                    "file": {},
                }
            ]
        }

        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root/search(q='report')",
            json=search_response,
            status=200,
        )

        # Call the method
        files = sharepoint_resource.search_files("report", limit=10)

        # Verify
        assert len(files) == 1
        assert files[0].name == "report.xlsx"


class TestSharePointResourceErrorHandling(TestSharePointResource):
    """Test error handling."""

    @responses.activate
    def test_upload_file_error_handling(
        self, sharepoint_resource: SharePointResource
    ) -> None:
        """Test upload error handling."""
        # Mock token endpoint
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # Mock upload endpoint to return an error
        responses.add(
            responses.PUT,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root:/test.txt:/content",
            json={"error": {"message": "Upload failed"}},
            status=500,
        )

        # Call the method
        with pytest.raises(HTTPError):
            sharepoint_resource.upload_file("test.txt", b"content")

    def test_parse_datetime(self, sharepoint_resource: SharePointResource) -> None:
        """Test datetime parsing."""
        # Test ISO format with Z
        dt = sharepoint_resource._parse_datetime("2024-01-15T10:30:00Z")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30

    def test_file_info_properties(self) -> None:
        """Test FileInfo property methods."""
        file_info = FileInfo(
            name="test_file.xlsx",
            id="123",
            web_url="https://example.com",
            download_url="https://download.com",
            created_datetime=datetime.now(),
            last_modified_datetime=datetime.now(),
            size=1048576,  # 1MB
        )

        assert file_info.size_mb == 1.0
        assert file_info.extension == ".xlsx"
        assert file_info.name_without_extension == "test_file"

    def test_folder_info_properties(self) -> None:
        """Test FolderInfo property methods."""
        # Test root folder
        root_folder = FolderInfo(
            name="root",
            id="123",
            web_url="https://example.com",
            created_datetime=datetime.now(),
            last_modified_datetime=datetime.now(),
            child_count=5,
            parent_path=None,
        )
        assert root_folder.is_root is True

        # Test non-root folder
        sub_folder = FolderInfo(
            name="subfolder",
            id="456",
            web_url="https://example.com/sub",
            created_datetime=datetime.now(),
            last_modified_datetime=datetime.now(),
            child_count=3,
            parent_path="Documents",
        )
        assert sub_folder.is_root is False


class TestSharePointResourceIntegration(TestSharePointResource):
    """Test integration scenarios with multiple API calls."""

    @responses.activate
    def test_full_file_lifecycle(self, sharepoint_resource: SharePointResource) -> None:
        """Test complete file lifecycle: upload, list, download, delete."""
        # Mock token endpoint (will be reused)
        responses.add(
            responses.POST,
            "https://login.microsoftonline.com/test_tenant_id/oauth2/v2.0/token",
            json={"access_token": "test_token", "expires_in": 3600},
            status=200,
        )

        # Mock get drive ID (will be reused)
        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drive",
            json={"id": "test_drive_id"},
            status=200,
        )

        # 1. Upload file
        upload_response = {
            "name": "lifecycle_test.txt",
            "id": "file_id_lifecycle",
            "webUrl": "https://sharepoint.com/lifecycle_test.txt",
            "@microsoft.graph.downloadUrl": "https://download.url",
            "createdDateTime": "2024-01-01T00:00:00Z",
            "lastModifiedDateTime": "2024-01-02T00:00:00Z",
            "size": 14,
        }

        responses.add(
            responses.PUT,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/root:/lifecycle_test.txt:/content",
            json=upload_response,
            status=201,
        )

        content = b"test lifecycle"
        upload_result = sharepoint_resource.upload_file("lifecycle_test.txt", content)
        assert upload_result.success is True

        # 2. List files to verify upload
        list_response = {
            "value": [
                {
                    **upload_response,
                    "file": {},
                }
            ]
        }

        responses.add(
            responses.GET,
            "https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/root/children",
            json=list_response,
            status=200,
        )

        files = sharepoint_resource.list_files()
        assert len(files) == 1
        assert files[0].name == "lifecycle_test.txt"

        # 3. Download file
        responses.add(
            responses.GET,
            f"https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/{upload_response['id']}/content",
            body=content,
            status=200,
        )

        downloaded_content = sharepoint_resource.download_file(upload_response["id"])
        assert downloaded_content == content

        # 4. Delete file
        responses.add(
            responses.DELETE,
            f"https://graph.microsoft.com/v1.0/sites/test_site_id/drives/test_drive_id/items/{upload_response['id']}",
            status=204,
        )

        sharepoint_resource.delete_file(upload_response["id"])

        # Verify all operations were called
        method_counts = {}
        for call in responses.calls:
            method = call.request.method
            method_counts[method] = method_counts.get(method, 0) + 1

        assert method_counts["POST"] >= 1  # Token
        assert method_counts["GET"] >= 3  # Drive ID, list, download
        assert method_counts["PUT"] == 1  # Upload
        assert method_counts["DELETE"] == 1  # Delete
