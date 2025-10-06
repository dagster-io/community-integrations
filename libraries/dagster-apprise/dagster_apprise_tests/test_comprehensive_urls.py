"""Comprehensive tests for Apprise URL validation with many valid URL types."""

from dagster_apprise._utils import validate_apprise_url


class TestComprehensiveAppriseUrls:
    """Test comprehensive Apprise URL validation."""

    def test_valid_urls_comprehensive(self):
        """Test validation of many valid Apprise URLs."""
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
            # Slack (scheme-based)
            "slack://xoxb-123456789012-123456789012-abcdefghijklmnop",
            "slack://xoxb-123456789012-123456789012-abcdefghijklmnop/#general",
            # Matrix
            "matrix://user:pass@hostname",
            "matrix://user:pass@hostname?format=text",
            "matrix://user:pass@hostname?image=yes",
            # Telegram
            "tgram://123456789:ABCDEF123456789abcdefABCDEF123456789/987654321",
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
            # Mattermost (alias)
            "mmost://hostname/token",
            # Pushbullet
            "pbul://o.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            # Prowl
            "prowl://0123456789012345678901234567890123456789",
            # Rocket.Chat
            "rocket://user:pass@hostname/room",
        ]

        for url in valid_urls:
            assert validate_apprise_url(url) is True, f"URL {url} should be valid"

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
            assert validate_apprise_url(url) is True, f"Email URL {url} should be valid"

    def test_pushover_url_variations(self):
        """Test various Pushover URL formats."""
        pushover_urls = [
            "pover://user@token",
            "pover://user@token?priority=high",
            "pover://user@token?sound=spacealarm",
            "pover://user@token?priority=high&sound=spacealarm",
            "pover://user@token?priority=low&sound=pushover",
        ]

        for url in pushover_urls:
            assert (
                validate_apprise_url(url) is True
            ), f"Pushover URL {url} should be valid"

    def test_discord_url_variations(self):
        """Test various Discord URL formats."""
        discord_urls = [
            "discord://webhook_id/webhook_token",
            "discord://webhook_id/webhook_token?avatar=yes",
            "discord://webhook_id/webhook_token?tts=yes",
            "discord://webhook_id/webhook_token?avatar=yes&tts=yes",
        ]

        for url in discord_urls:
            assert (
                validate_apprise_url(url) is True
            ), f"Discord URL {url} should be valid"

    def test_matrix_url_variations(self):
        """Test various Matrix URL formats."""
        matrix_urls = [
            "matrix://user:pass@hostname",
            "matrix://user:pass@hostname?format=text",
            "matrix://user:pass@hostname?image=yes",
            "matrix://user:pass@hostname?format=text&image=yes",
        ]

        for url in matrix_urls:
            assert (
                validate_apprise_url(url) is True
            ), f"Matrix URL {url} should be valid"

    def test_gotify_url_variations(self):
        """Test various Gotify URL formats."""
        gotify_urls = [
            "gotify://hostname/token",
            "gotify://hostname/token?priority=high",
            "gotify://hostname/token?format=text",
            "gotify://hostname/token?priority=high&format=text",
        ]

        for url in gotify_urls:
            assert (
                validate_apprise_url(url) is True
            ), f"Gotify URL {url} should be valid"

    def test_ifttt_url_variations(self):
        """Test various IFTTT URL formats."""
        ifttt_urls = [
            "ifttt://webhook_id/event_name",
            "ifttt://webhook_id/event_name?format=text",
            "ifttt://webhook_id/event_name?image=yes",
            "ifttt://webhook_id/event_name?format=text&image=yes",
        ]

        for url in ifttt_urls:
            assert validate_apprise_url(url) is True, f"IFTTT URL {url} should be valid"

    def test_ntfy_url_variations(self):
        """Test various Ntfy URL formats."""
        ntfy_urls = [
            "ntfy://hostname/topic",
            "ntfy://hostname/topic?format=text",
            "ntfy://hostname/topic?image=yes",
            "ntfy://hostname/topic?format=text&image=yes",
        ]

        for url in ntfy_urls:
            assert validate_apprise_url(url) is True, f"Ntfy URL {url} should be valid"

    def test_slack_webhook_urls(self):
        """Test Slack webhook URL formats."""
        slack_urls = [
            "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
            "https://hooks.slack.com/services/T11111111/B11111111/YYYYYYYYYYYYYYYYYYYYYYYY",
        ]

        for url in slack_urls:
            assert validate_apprise_url(url) is True, f"Slack URL {url} should be valid"

    def test_urls_with_parameters(self):
        """Test URLs with various parameter combinations."""
        param_urls = [
            "pover://user@token?priority=high&sound=spacealarm",
            "mailto://user:pass@gmail.com?secure=yes&verify=yes",
            "discord://webhook_id/webhook_token?avatar=yes&tts=yes",
            "matrix://user:pass@hostname?format=text&image=yes",
            "gotify://hostname/token?priority=high&format=text",
            "ifttt://webhook_id/event_name?format=text&image=yes",
            "ntfy://hostname/topic?format=text&image=yes",
        ]

        for url in param_urls:
            assert (
                validate_apprise_url(url) is True
            ), f"Parameterized URL {url} should be valid"

    def test_invalid_urls(self):
        """Test validation of invalid URLs."""
        invalid_urls = [
            "not-a-url",
            "http://invalid",
            "pover://",  # Missing user/token
            "",  # Empty string
            None,  # None value
            "slack://token@channel",  # Invalid Slack format
            "tgram://bot_token/chat_id",  # Invalid Telegram format
            "telegram://bot_token/chat_id",  # Invalid Telegram format
            "msteams://webhook_url",  # Invalid Teams format
            "rocket://user:pass@hostname",  # Invalid Rocket.Chat format
            "mattermost://hostname/token",  # Invalid Mattermost format
            "zulip://botname:token@hostname",  # Invalid Zulip format
            "twilio://account_sid:auth_token@from_number",  # Invalid Twilio format
            "bulk_sms://user:pass@bulksms.com",  # Invalid Bulk SMS format
            "webhook://hostname/path",  # Invalid Webhook format
            "sns://access_key:secret_key@region/topic",  # Invalid SNS format
            "sqs://access_key:secret_key@region/queue",  # Invalid SQS format
            "gchat://webhook_url",  # Invalid Google Chat format
            "join://api_key/device_id",  # Invalid Join format
            "kavenegar://api_key/sender",  # Invalid Kavenegar format
            "mailgun://user:pass@hostname",  # Invalid Mailgun format
            "pagerduty://integration_key",  # Invalid PagerDuty format
            "prowl://api_key",  # Invalid Prowl format
            "pushbullet://access_token",  # Invalid Pushbullet format
            "sendgrid://api_key",  # Invalid SendGrid format
            "serverchan://send_key",  # Invalid ServerChan format
            "signal://user:pass@hostname",  # Invalid Signal format
            "simplepush://key",  # Invalid Simplepush format
            "spontit://user:pass@hostname",  # Invalid Spontit format
            "syslog://hostname",  # Invalid Syslog format
            "twitter://consumer_key:consumer_secret@access_token:access_token_secret",  # Invalid Twitter format
            "vonage://api_key:api_secret@from_number",  # Invalid Vonage format
            "webex://webhook_url",  # Invalid Webex format
            "xmpp://user:pass@hostname",  # Invalid XMPP format
        ]

        for url in invalid_urls:
            if url is not None:
                assert (
                    validate_apprise_url(url) is False
                ), f"URL {url} should be invalid"
