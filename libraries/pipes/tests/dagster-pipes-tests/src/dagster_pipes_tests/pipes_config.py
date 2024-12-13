from serde import field, serde


@serde
class General:
    error_reporting: bool = (
        False  # Whether the Pipes implementation supports automatic error reporting
    )


@serde
class Messages:
    """
    The names of these fields directly map to possible Pipes message types.
    """

    log: bool = True
    report_custom_message: bool = True
    report_asset_materialization: bool = True
    report_asset_check: bool = True
    log_external_stream: bool = False


@serde
class MessageChannel:
    s3: bool = False
    databricks: bool = False


@serde
class PipesConfig:
    """
    This configuration class describes the capabilities of a specific Pipes implementation.
    """

    general: General = field(default_factory=General)
    message_channel: MessageChannel = field(default_factory=MessageChannel)
    messages: Messages = field(default_factory=Messages)
