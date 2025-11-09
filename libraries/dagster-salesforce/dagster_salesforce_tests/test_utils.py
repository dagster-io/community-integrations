# pyright: reportAttributeAccessIssue=false
"""
Test suite for utility functions in dagster_salesforce.utils.

Tests datetime conversion and job waiting logic with various scenarios.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from simple_salesforce.bulk2 import _Bulk2Client, JobState
from simple_salesforce.exceptions import SalesforceOperationError

from dagster_salesforce.utils import (
    datetime_to_soql_str,
    _wait_for_job,
)


class TestDateTimeToSOQLStr:
    """Test the datetime_to_soql_str function."""

    def test_converts_datetime_to_soql_format(self) -> None:
        """Test conversion of datetime object to SOQL format string."""
        # Create a specific datetime
        dt = datetime(2024, 1, 15, 10, 30, 45)

        # Convert to SOQL
        result = datetime_to_soql_str(dt)

        # Verify format
        assert result == "2024-01-15T10:30:45Z"

    def test_handles_different_datetime_values(self) -> None:
        """Test various datetime values."""
        # Test midnight
        dt_midnight = datetime(2024, 1, 1, 0, 0, 0)
        assert datetime_to_soql_str(dt_midnight) == "2024-01-01T00:00:00Z"

        # Test end of day
        dt_end = datetime(2024, 12, 31, 23, 59, 59)
        assert datetime_to_soql_str(dt_end) == "2024-12-31T23:59:59Z"

        # Test with microseconds (should be truncated)
        dt_micro = datetime(2024, 6, 15, 14, 30, 45, 123456)
        assert datetime_to_soql_str(dt_micro) == "2024-06-15T14:30:45Z"

    def test_handles_none_value(self) -> None:
        """Test that None input returns None."""
        result = datetime_to_soql_str(None)
        assert result is None

    def test_timezone_aware_datetime(self) -> None:
        """Test handling of timezone-aware datetime objects."""
        # Create timezone-aware datetime
        dt_utc = datetime(2024, 3, 15, 15, 45, 30, tzinfo=timezone.utc)
        result = datetime_to_soql_str(dt_utc)
        assert result == "2024-03-15T15:45:30Z"

        # With offset timezone
        dt_offset = datetime(
            2024, 3, 15, 15, 45, 30, tzinfo=timezone(timedelta(hours=5))
        )
        result = datetime_to_soql_str(dt_offset)
        # The format should still work, even though it doesn't adjust for timezone
        assert result == "2024-03-15T15:45:30Z"


class TestWaitForJob:
    """Test the _wait_for_job function."""

    @pytest.fixture
    def mock_client(self) -> MagicMock:
        """Create a mock Bulk2Client."""
        client = MagicMock(spec=_Bulk2Client)
        client.DEFAULT_WAIT_TIMEOUT_SECONDS = 10  # Short timeout for tests
        client.MAX_CHECK_INTERVAL_SECONDS = 2.0
        return client

    def test_successful_job_completion(self, mock_client: MagicMock) -> None:
        """Test successful job completion."""
        # Mock job info responses - first in_progress, then complete
        mock_client.get_job.side_effect = [
            {"state": JobState.in_progress},
            {"state": JobState.job_complete},
        ]

        # Call wait_for_job
        with patch("dagster_salesforce.utils.time.sleep") as mock_sleep:
            result = _wait_for_job(mock_client, "job123", is_query=True, wait=1.5)

        # Verify result
        assert result == JobState.job_complete

        # Verify get_job was called correctly
        assert mock_client.get_job.call_count == 2
        mock_client.get_job.assert_called_with("job123", True)

        # Verify sleep was called once (between checks)
        assert mock_sleep.call_count == 1

    def test_job_failure_aborted(self, mock_client: MagicMock) -> None:
        """Test job failure with aborted status."""
        # Mock job info with aborted status
        mock_client.get_job.return_value = {
            "state": JobState.aborted,
            "errorMessage": "Job was aborted by user",
        }

        # Verify exception is raised
        with pytest.raises(SalesforceOperationError) as exc_info:
            _wait_for_job(mock_client, "job456", is_query=False, wait=1.5)

        assert "Job failure" in str(exc_info.value)
        assert "Job was aborted by user" in str(exc_info.value)

    def test_job_failure_failed(self, mock_client: MagicMock) -> None:
        """Test job failure with failed status."""
        # Mock job info with failed status
        mock_client.get_job.return_value = {
            "state": JobState.failed,
            "errorMessage": "Query execution failed",
        }

        # Verify exception is raised
        with pytest.raises(SalesforceOperationError) as exc_info:
            _wait_for_job(mock_client, "job789", is_query=True, wait=1.5)

        assert "Job failure" in str(exc_info.value)
        assert "Query execution failed" in str(exc_info.value)

    def test_job_failure_no_error_message(self, mock_client: MagicMock) -> None:
        """Test job failure when no error message is provided."""
        # Mock job info with failed status but no error message
        job_info = {"state": JobState.failed}
        mock_client.get_job.return_value = job_info

        # Verify exception is raised with job info as message
        with pytest.raises(SalesforceOperationError) as exc_info:
            _wait_for_job(mock_client, "job999", is_query=True, wait=1.5)

        assert "Job failure" in str(exc_info.value)
        assert str(job_info) in str(exc_info.value)

    def test_job_timeout(self, mock_client: MagicMock) -> None:
        """Test job timeout scenario."""
        # Mock job that stays in progress
        mock_client.get_job.return_value = {"state": JobState.in_progress}
        mock_client.DEFAULT_WAIT_TIMEOUT_SECONDS = 10  # 10 second timeout

        # Use freezegun to control time
        initial_time = datetime(2024, 1, 1, 12, 0, 0)
        with freeze_time(initial_time) as frozen_time:
            # Mock sleep to advance time instead of sleeping
            def mock_sleep(duration):
                frozen_time.tick(delta=timedelta(seconds=duration))

            with patch("dagster_salesforce.utils.time.sleep", side_effect=mock_sleep):
                with pytest.raises(SalesforceOperationError) as exc_info:
                    _wait_for_job(mock_client, "job_timeout", is_query=True, wait=1.5)

        assert "Job timeout. Job status: " in str(exc_info.value)
        assert ("InProgress" in str(exc_info.value)) or (
            "in_progress" in str(exc_info.value)
        )  # different versions of simple_salesforce return different job state text

    def test_exponential_backoff(self, mock_client: MagicMock) -> None:
        """Test exponential backoff in delay calculation."""
        # Mock job that stays in progress for several iterations
        mock_client.get_job.side_effect = [
            {"state": JobState.in_progress},
            {"state": JobState.in_progress},
            {"state": JobState.in_progress},
            {"state": JobState.job_complete},
        ]
        mock_client.MAX_CHECK_INTERVAL_SECONDS = 10.0

        # Track sleep calls
        sleep_calls = []

        def track_sleep(duration):
            sleep_calls.append(duration)

        with patch("dagster_salesforce.utils.time.sleep", side_effect=track_sleep):
            result = _wait_for_job(mock_client, "job_backoff", is_query=True, wait=2.0)

        # Verify completion
        assert result == JobState.job_complete

        # Verify exponential backoff pattern (2^0, 2^1, 2^2)
        assert len(sleep_calls) == 3
        assert sleep_calls[0] == 1.0  # 2.0^0 = 1.0
        assert sleep_calls[1] == 2.0  # 2.0^1 = 2.0
        assert sleep_calls[2] == 4.0  # 2.0^2 = 4.0

    def test_max_check_interval_limit(self, mock_client: MagicMock) -> None:
        """Test that delay is capped at MAX_CHECK_INTERVAL_SECONDS."""
        # Set max interval
        mock_client.MAX_CHECK_INTERVAL_SECONDS = 5.0

        # Mock job that stays in progress for many iterations
        mock_client.get_job.side_effect = [
            {"state": JobState.in_progress},
            {"state": JobState.in_progress},
            {"state": JobState.in_progress},
            {"state": JobState.in_progress},
            {"state": JobState.job_complete},
        ]

        # Track sleep calls
        sleep_calls = []

        def track_sleep(duration):
            sleep_calls.append(duration)

        with patch("dagster_salesforce.utils.time.sleep", side_effect=track_sleep):
            result = _wait_for_job(mock_client, "job_max", is_query=False, wait=3.0)

        # Verify completion
        assert result == JobState.job_complete

        # Verify delays: 3^0=1, 3^1=3, 3^2=9 (capped to 5), 3^3=27 (capped to 5)
        assert len(sleep_calls) == 4
        assert sleep_calls[0] == 1.0
        assert sleep_calls[1] == 3.0
        assert sleep_calls[2] == 5.0  # Capped at MAX_CHECK_INTERVAL_SECONDS
        assert sleep_calls[3] == 5.0  # Capped at MAX_CHECK_INTERVAL_SECONDS

    def test_different_wait_values(self, mock_client: MagicMock) -> None:
        """Test different wait parameter values."""
        # Test with wait < 1 (should use min of 1)
        mock_client.get_job.side_effect = [
            {"state": JobState.in_progress},
            {"state": JobState.job_complete},
        ]

        sleep_calls = []
        with patch(
            "dagster_salesforce.utils.time.sleep",
            side_effect=lambda x: sleep_calls.append(x),
        ):
            result = _wait_for_job(
                mock_client, "job_small_wait", is_query=True, wait=0.5
            )

        assert result == JobState.job_complete
        # 0.5^0 = 1.0 (first delay)
        assert sleep_calls[0] == 1.0

    def test_query_vs_ingest_job_types(self, mock_client: MagicMock) -> None:
        """Test that is_query parameter is passed correctly."""
        mock_client.get_job.return_value = {"state": JobState.job_complete}

        # Test query job
        result = _wait_for_job(mock_client, "query_job", is_query=True, wait=1.5)
        assert result == JobState.job_complete
        mock_client.get_job.assert_called_with("query_job", True)

        # Reset mock
        mock_client.reset_mock()

        # Test ingest job
        result = _wait_for_job(mock_client, "ingest_job", is_query=False, wait=1.5)
        assert result == JobState.job_complete
        mock_client.get_job.assert_called_with("ingest_job", False)
