"""Hooks for Apprise notifications."""

import logging
from collections.abc import Iterable
from typing import Optional

import dagster as dg

from .resource import AppriseConfig, AppriseResource

logger = logging.getLogger(__name__)


def apprise_failure_hook(context: dg.HookContext) -> None:
    """Hook that sends failure notifications via Apprise."""
    try:
        apprise_resource = context.resources.get("apprise")
        if not apprise_resource:
            logger.warning("Apprise resource not found, skipping notification")
            return

        run = getattr(context, "dagster_run", None)
        if run is None:
            logger.warning("No dagster_run available in hook context")
            return
        asset_key = getattr(context, "asset_key", "unknown")
        success = apprise_resource.notify_run_status(
            run=run, status="FAILURE", message=f"Asset {asset_key} failed"
        )

        if success:
            logger.info(f"Sent failure notification for asset {asset_key}")
        else:
            logger.error(f"Failed to send failure notification for asset {asset_key}")

    except Exception as e:
        logger.error(f"Error in apprise failure hook: {e}")


def apprise_success_hook(context: dg.HookContext) -> None:
    """Hook that sends success notifications via Apprise."""
    try:
        apprise_resource = context.resources.get("apprise")
        if not apprise_resource:
            logger.warning("Apprise resource not found, skipping notification")
            return

        run = getattr(context, "dagster_run", None)
        if run is None:
            logger.warning("No dagster_run available in hook context")
            return
        asset_key = getattr(context, "asset_key", "unknown")
        success = apprise_resource.notify_run_status(
            run=run,
            status="SUCCESS",
            message=f"Asset {asset_key} completed successfully",
        )

        if success:
            logger.info(f"Sent success notification for asset {asset_key}")
        else:
            logger.error(f"Failed to send success notification for asset {asset_key}")

    except Exception as e:
        logger.error(f"Error in apprise success hook: {e}")


def apprise_on_failure(
    *,
    urls: Optional[Iterable[str]] = None,
    config_file: Optional[str] = None,
    base_url: Optional[str] = None,
    webserver_base_url: Optional[str] = None,
    title_prefix: str = "Dagster",
) -> dg.HookDefinition:
    """Decorator-style failure hook with inline Apprise configuration.

    If an `apprise` resource is not supplied at runtime, this hook supplies one
    with the provided configuration for the duration of the hook.
    """

    @dg.hook
    def _hook(context: dg.HookContext) -> None:
        resource = context.resources.get("apprise")
        if resource is None:
            cfg = AppriseConfig(
                urls=list(urls or []),
                config_file=config_file,
                base_url=base_url,
                webserver_base_url=webserver_base_url,
                title_prefix=title_prefix,
            )
            resource = AppriseResource(config=cfg)
        try:
            run = getattr(context, "dagster_run", None)
            if run is None:
                logger.warning("No dagster_run available in hook context")
                return
            asset_key = getattr(context, "asset_key", "unknown")
            ok = resource.notify_run_status(
                run=run, status="FAILURE", message=f"Asset {asset_key} failed"
            )
            if ok:
                logger.info(f"Sent failure notification for asset {asset_key}")
            else:
                logger.error(
                    f"Failed to send failure notification for asset {asset_key}"
                )
        except Exception as e:  # pragma: no cover
            logger.error(f"Error in apprise_on_failure hook: {e}")

    return _hook


def apprise_on_success(
    *,
    urls: Optional[Iterable[str]] = None,
    config_file: Optional[str] = None,
    base_url: Optional[str] = None,
    webserver_base_url: Optional[str] = None,
    title_prefix: str = "Dagster",
) -> dg.HookDefinition:
    """Decorator-style success hook with inline Apprise configuration.

    If an `apprise` resource is not supplied at runtime, this hook supplies one.
    """

    @dg.hook
    def _hook(context: dg.HookContext) -> None:
        resource = context.resources.get("apprise")
        if resource is None:
            cfg = AppriseConfig(
                urls=list(urls or []),
                config_file=config_file,
                base_url=base_url,
                webserver_base_url=webserver_base_url,
                title_prefix=title_prefix,
            )
            resource = AppriseResource(config=cfg)
        try:
            run = getattr(context, "dagster_run", None)
            if run is None:
                logger.warning("No dagster_run available in hook context")
                return
            asset_key = getattr(context, "asset_key", "unknown")
            ok = resource.notify_run_status(
                run=run,
                status="SUCCESS",
                message=f"Asset {asset_key} completed successfully",
            )
            if ok:
                logger.info(f"Sent success notification for asset {asset_key}")
            else:
                logger.error(
                    f"Failed to send success notification for asset {asset_key}"
                )
        except Exception as e:  # pragma: no cover
            logger.error(f"Error in apprise_on_success hook: {e}")

    return _hook


def create_apprise_hooks() -> dict[str, dg.HookDefinition]:
    """Create a dictionary of Apprise hooks."""
    return {
        "apprise_failure": dg.hook(apprise_failure_hook),
        "apprise_success": dg.hook(apprise_success_hook),
    }


def make_apprise_on_run_failure_sensor(
    *,
    urls: Optional[Iterable[str]] = None,
    config_file: Optional[str] = None,
    monitored_jobs: Optional[Iterable[str]] = None,
    exclude_jobs: Optional[Iterable[str]] = None,
    base_url: Optional[str] = None,
    webserver_base_url: Optional[str] = None,
    title_prefix: str = "Dagster",
    name: str = "apprise_on_run_failure",
    description: Optional[str] = None,
) -> dg.SensorDefinition:
    """Create a run-failure sensor that sends Apprise notifications.

    This factory is convenient for users who want a self-contained sensor without
    separately defining the `apprise` resource.
    """

    include_patterns = list(monitored_jobs or ["*"])
    exclude_patterns = list(exclude_jobs or [])

    def _matches(name: str, patterns: Iterable[str]) -> bool:
        import fnmatch

        return any(fnmatch.fnmatch(name, p) for p in patterns)

    cfg = AppriseConfig(
        urls=list(urls or []),
        config_file=config_file,
        base_url=base_url,
        webserver_base_url=webserver_base_url,
        title_prefix=title_prefix,
    )
    resource = AppriseResource(config=cfg)

    @dg.run_status_sensor(
        name=name,
        run_status=dg.DagsterRunStatus.FAILURE,
        description=description or "Send Apprise notifications when runs fail",
    )
    def _sensor(context: dg.RunStatusSensorContext) -> None:
        run = getattr(context, "dagster_run", None)
        if run is None:
            logger.warning("No dagster_run available in hook context")
            return
        job = run.job_name
        if include_patterns and not _matches(job, include_patterns):
            logger.debug(f"Skipping job {job}: not in monitored patterns")
            return
        if exclude_patterns and _matches(job, exclude_patterns):
            logger.debug(f"Skipping job {job}: matched exclude patterns")
            return

        try:
            ok = resource.notify_run_status(
                run=run, status="FAILURE", message=f"Run failed for job {job}"
            )
            if ok:
                logger.info(f"Sent failure notification for run {run.run_id}")
            else:
                logger.error(
                    f"Failed to send failure notification for run {run.run_id}"
                )
        except Exception as e:  # pragma: no cover
            logger.error(f"Error in make_apprise_on_run_failure_sensor: {e}")

    return _sensor
