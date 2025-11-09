import time
import dagster as dg
from datetime import datetime, timedelta
from typing import Literal

from simple_salesforce.bulk2 import _Bulk2Client, JobState
from simple_salesforce.exceptions import SalesforceOperationError


logger = dg.get_dagster_logger(__name__)

LOG_INTERVAL_SECONDS = 600.0
BACKOFF_DIVISOR = 1000.0


def datetime_to_soql_str(dt: datetime | None) -> str | None:
    """
    Convert a datetime object to a SOQL-compatible string.

    :param dt: The datetime object to convert.
    :type dt: Union[datetime, None]
    :return: The SOQL-compatible string representation of the datetime.
    :rtype: str
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def _wait_for_job(
    client: _Bulk2Client, job_id: str, is_query: bool, wait: float
) -> Literal[JobState.job_complete]:
    start_time = datetime.now()
    expiration_time = start_time + timedelta(
        seconds=client.DEFAULT_WAIT_TIMEOUT_SECONDS
    )
    last_log_time, delay_count, job_status = start_time, 0, None

    while datetime.now() < expiration_time:
        job_info = client.get_job(job_id, is_query)
        job_status = job_info["state"]

        # Check for completion
        if job_status == JobState.job_complete:
            return job_status

        # Check for failure
        if job_status in [JobState.aborted, JobState.failed]:
            error_message = job_info.get("errorMessage") or job_info
            raise SalesforceOperationError(
                f"Job failure. Response content: {error_message}"
            )

        # Log progress every 10 minutes
        current_time = datetime.now()
        if (current_time - last_log_time).total_seconds() >= LOG_INTERVAL_SECONDS:
            elapsed = int((current_time - start_time).total_seconds())
            logger.debug(f"Job {dict(job_info)} (elapsed: {elapsed}s)")
            last_log_time = current_time

        # Calculate next delay with exponential backoff
        delay = min(wait**delay_count, client.MAX_CHECK_INTERVAL_SECONDS)
        delay_count += 1
        time.sleep(delay)

    raise SalesforceOperationError(f"Job timeout. Job status: {job_status}")
