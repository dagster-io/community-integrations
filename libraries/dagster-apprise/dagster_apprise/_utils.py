"""Internal utility functions for Apprise integration.

Only minimal helpers that are broadly useful at runtime should live here.
"""

import logging

import apprise

logger = logging.getLogger(__name__)


def validate_apprise_url(url: str) -> bool:
    """Validate that a URL is a valid Apprise URL.

    Returns True if the URL parses and can be added to an Apprise instance.
    """
    try:
        apobj = apprise.Apprise()
        return apobj.add(url)
    except Exception as exc:  # pragma: no cover - defensive logging only
        logger.debug(f"Invalid Apprise URL {url}: {exc}")
        return False


def load_apprise_config(config_file: str) -> list[str]:
    """Load Apprise service URLs from a configuration file.

    Returns a list of normalized service URLs when the file is loadable; otherwise [].
    """
    try:
        apobj = apprise.Apprise()
        if apobj.add(config_file):
            urls: list[str] = []
            for service in apobj:
                if hasattr(service, "url") and service.url is not None:
                    urls.append(str(service.url))
            return urls
        logger.warning(f"Failed to load Apprise config file: {config_file}")
        return []
    except Exception as exc:  # pragma: no cover - defensive logging only
        logger.error(f"Error loading Apprise config file {config_file}: {exc}")
        return []
