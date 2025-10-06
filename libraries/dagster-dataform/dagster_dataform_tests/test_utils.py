from unittest.mock import patch
from dagster_dataform.utils import get_epoch_time_ago


@patch("dagster_dataform.utils.datetime")
def test_get_epoch_time_ago_basic(mock_datetime):
    """Test basic functionality with 5 minutes."""
    from datetime import datetime, timezone, timedelta

    # Mock current time
    mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    result = get_epoch_time_ago(5)

    # Should return timestamp 5 minutes ago
    expected = int((mock_now - timedelta(minutes=5)).timestamp())
    assert result == expected


@patch("dagster_dataform.utils.datetime")
def test_get_epoch_time_ago_zero(mock_datetime):
    """Test with 0 minutes."""
    from datetime import datetime, timezone

    mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    result = get_epoch_time_ago(0)

    # Should return current timestamp
    expected = int(mock_now.timestamp())
    assert result == expected


@patch("dagster_dataform.utils.datetime")
def test_get_epoch_time_ago_returns_integer(mock_datetime):
    """Test that function returns integer."""
    from datetime import datetime, timezone

    mock_datetime.now.return_value = datetime(
        2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc
    )

    result = get_epoch_time_ago(10)

    assert isinstance(result, int)
