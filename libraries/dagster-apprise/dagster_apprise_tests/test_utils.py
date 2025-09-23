"""Tests for minimal utility functions."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from dagster_apprise._utils import load_apprise_config, validate_apprise_url


class TestValidateAppriseUrl:
    """Test URL validation functionality."""

    def test_valid_urls(self):
        """Test validation of valid Apprise URLs."""
        valid_urls = [
            # Pushover
            "pover://user@token",
            "pover://user@token?priority=high",
            "pover://user@token?sound=spacealarm",
            # Email
            "mailto://user:pass@gmail.com",
            "mailto://user:pass@smtp.gmail.com:587",
            "mailto://user:pass@mail.example.com:465?secure=yes",
            # Discord
            "discord://webhook_id/webhook_token",
            "discord://webhook_id/webhook_token?avatar=yes",
            "discord://webhook_id/webhook_token?tts=yes",
            # Slack
            "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
            # Matrix
            "matrix://user:pass@hostname",
            "matrix://user:pass@hostname?format=text",
            "matrix://user:pass@hostname?image=yes",
            # Gotify
            "gotify://hostname/token",
            "gotify://hostname/token?priority=high",
            "gotify://hostname/token?format=text",
            # IFTTT
            "ifttt://webhook_id/event_name",
            "ifttt://webhook_id/event_name?format=text",
            "ifttt://webhook_id/event_name?image=yes",
            # Ntfy
            "ntfy://hostname/topic",
            "ntfy://hostname/topic?format=text",
            "ntfy://hostname/topic?image=yes",
        ]

        for url in valid_urls:
            assert validate_apprise_url(url) is True

    def test_slack_url_validation(self):
        """Test Slack URL validation specifically."""
        # Slack URLs need to be in a specific format
        slack_url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
        assert validate_apprise_url(slack_url) is True

    def test_email_url_variations(self):
        """Test various email URL formats."""
        email_urls = [
            "mailto://user:pass@gmail.com",
            "mailto://user:pass@smtp.gmail.com:587",
            "mailto://user:pass@mail.example.com:465?secure=yes",
            "mailto://user:pass@mail.example.com:25?secure=no",
            "mailto://user:pass@mail.example.com?secure=yes&verify=yes",
        ]

        for url in email_urls:
            assert validate_apprise_url(url) is True

    def test_chat_platform_urls(self):
        """Test chat platform URL formats."""
        chat_urls = [
            "discord://webhook_id/webhook_token",
            # other chat platform URLs require valid credentials or specific formats
        ]

        for url in chat_urls:
            assert validate_apprise_url(url) is True

    def test_messaging_app_urls(self):
        """Test messaging app URL formats."""
        messaging_urls = [
            "matrix://user:pass@hostname",
            # other messaging app URLs require valid credentials or specific formats
        ]

        for url in messaging_urls:
            assert validate_apprise_url(url) is True

    def test_push_notification_urls(self):
        """Test push notification service URL formats."""
        push_urls = [
            "pover://user@token",
            "pover://user@token?priority=high",
            "gotify://hostname/token",
            # other push notification URLs require valid credentials or specific formats
        ]

        for url in push_urls:
            assert validate_apprise_url(url) is True

    def test_urls_with_parameters(self):
        """Test URLs with various parameter combinations."""
        param_urls = [
            "pover://user@token?priority=high&sound=spacealarm",
            "mailto://user:pass@gmail.com?secure=yes&verify=yes",
            "discord://webhook_id/webhook_token?avatar=yes&tts=yes",
            "matrix://user:pass@hostname?format=text&image=yes",
            # telegram URLs require specific format
        ]

        for url in param_urls:
            assert validate_apprise_url(url) is True

    def test_invalid_urls(self):
        """Test validation of invalid URLs."""
        invalid_urls = [
            "not-a-url",
            "http://invalid",
            "pover://",  # missing user/token
            "",  # empty string
            None,  # None value
        ]

        for url in invalid_urls:
            if url is not None:
                assert validate_apprise_url(url) is False

    def test_url_validation_exception(self):
        """Test URL validation when Apprise raises an exception."""
        with patch("dagster_apprise._utils.apprise.Apprise") as mock_apprise:
            mock_apprise.return_value.add.side_effect = Exception("Apprise error")

            result = validate_apprise_url("test://url")
            assert result is False


class TestLoadAppriseConfig:
    """Test config file loading functionality."""

    def test_load_valid_config_file(self):
        """Test loading a valid Apprise config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("""
urls:
  - pover://user@token
  - https://hooks.slack.com/services/TEST/WEBHOOK
""")
            f.flush()

            with patch("dagster_apprise._utils.apprise.Apprise") as mock_apprise:
                mock_service = Mock()
                mock_service.url = "pover://user@token"
                mock_apprise.return_value.__iter__.return_value = [mock_service]
                mock_apprise.return_value.add.return_value = True

                urls = load_apprise_config(f.name)
                assert len(urls) == 1
                assert urls[0] == "pover://user@token"

            Path(f.name).unlink()

    def test_load_invalid_config_file(self):
        """Test loading an invalid config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            f.flush()

            with patch("dagster_apprise._utils.apprise.Apprise") as mock_apprise:
                mock_apprise.return_value.add.return_value = False

                urls = load_apprise_config(f.name)
                assert urls == []

            Path(f.name).unlink()

    def test_load_nonexistent_config_file(self):
        """Test loading a non-existent config file."""
        with patch("dagster_apprise._utils.apprise.Apprise") as mock_apprise:
            mock_apprise.return_value.add.return_value = False

            urls = load_apprise_config("/nonexistent/file.yaml")
            assert urls == []

    def test_load_config_exception(self):
        """Test config loading when an exception occurs."""
        with patch("dagster_apprise._utils.apprise.Apprise") as mock_apprise:
            mock_apprise.side_effect = Exception("Apprise error")

            urls = load_apprise_config("test.yaml")
            assert urls == []
