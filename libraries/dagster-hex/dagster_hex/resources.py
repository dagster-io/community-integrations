import datetime
import logging
import time
from typing import Any, Dict, List, Optional, cast
from urllib.parse import urljoin

import requests
from dagster import Failure, Field, StringSource, get_dagster_logger, resource

import dagster_hex
from dagster_hex.types import (
    HexOutput,
    NotificationDetails,
    ProjectResponse,
    RunResponse,
    StatusResponse,
)

from .consts import (
    COMPLETE,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_TIMEOUT,
    HEX_API_BASE,
    TERMINAL_STATUSES,
    VALID_STATUSES,
)


class HexResource:
    """
    This class exposes methods on top of the Hex REST API.

    Args:
        api_key (str): Hex API Token to use for authentication
        base_url (str): Base URL for the API
        request_max_retries (int): Number of times to retry a failed request
        request_retry_delay (int): Time, in seconds, to wait between retries
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = HEX_API_BASE,
        log: logging.Logger = get_dagster_logger(),
        request_max_retries: int = 3,
        request_retry_delay: float = 0.25,
    ):
        self._log = log
        self._api_key = api_key
        self._request_max_retries = request_max_retries
        self._request_retry_delay = request_retry_delay
        self.api_base_url = base_url

    def make_request(
        self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Creates and sends a request to the desired Hex API endpoint

        Args:
            method (str): The http method use for this request (e.g. "GET", "POST").
            endpoint (str): The Hex API endpoint to send this request to.
            data (Optional(dict): Query parameters to pass to the API endpoint

        Returns:
            Dict[str, Any]: Parsed json data from the response to this request
        """

        user_agent = f"HexDagsterOp/{dagster_hex.__version__}"
        headers = {"Authorization": f"Bearer {self._api_key}", "User-Agent": user_agent}
        url = urljoin(self.api_base_url, endpoint)

        num_retries = 0
        while True:
            try:
                if method == "GET":
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=data,
                    )
                elif method == "POST":
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=headers,
                        json=data,
                    )
                else:
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=headers,
                        data=data,
                    )
                if response.status_code == 401:
                    try:
                        msg = response.json()
                    except requests.exceptions.JSONDecodeError:
                        msg = response.text
                    raise Failure(f"Received 401 status from Hex: {msg}")
                if response.status_code == 403:
                    try:
                        msg = response.json()
                    except requests.exceptions.JSONDecodeError:
                        msg = response.text
                    raise Failure(f"Received 403 status from Hex: {msg}")
                if response.status_code == 422:
                    # 422 statuses from Hex may have additional error information as a
                    # JSON payload
                    try:
                        msg = response.json()
                    except requests.exceptions.JSONDecodeError:
                        msg = response.text
                    raise Failure(f"Received 422 status from Hex: {msg}")

                response.raise_for_status()
                if response.headers.get("Content-Type", "").startswith(
                    "application/json"
                ):
                    try:
                        response_json = response.json()
                    except requests.exceptions.JSONDecodeError:
                        self._log.error("Failed to decode response from API.")
                        self._log.error("API returned: %s", response.text)
                        raise Failure(
                            "Unexpected response from Hex API.Failed to decode to JSON."
                        )
                    return response_json
            except requests.RequestException as e:
                self._log.error("Request to Hex API failed: %s", e)
                if num_retries == self._request_max_retries:
                    break
                num_retries += 1
                time.sleep(self._request_retry_delay)

        raise Failure("Exceeded max number of retries.")

    def get_all_projects(
        self,
    ) -> List[Dict[str, Any]]:
        method = "GET"
        endpoint = "api/v1/projects"
        response = cast(
            List[Dict[str, Any]],
            self.make_request(method=method, endpoint=endpoint, data=None),
        )
        return response

    def get_project(
        self,
        project_id: str,
    ) -> ProjectResponse:
        """
        Fetches the details of a Hex Project

        Args:
            project_id (str): The Hex Project ID

        Returns:
            RunResponse
        """
        method = "GET"
        endpoint = f"api/v1/projects/{project_id}"
        response = cast(
            ProjectResponse,
            self.make_request(method=method, endpoint=endpoint, data=None),
        )
        return response

    def get_project_details(
        self,
        project_id: str,
    ) -> Dict[str, Any]:
        try:
            project_details = self.get_project(project_id)
            return {
                "project_id": project_id,
                "title": project_details["title"],
                "description": project_details["description"],
                "creator": project_details["creator"]["email"],
                "last_edited_at": project_details["lastEditedAt"],
                "last_published_at": project_details["lastPublishedAt"],
                "categories": project_details["categories"],
            }
        except Exception as e:
            self._log.error(f"Error fetching project details for {project_id}: {e}")
            raise e

    def run_project(
        self,
        project_id: str,
        inputs: Optional[Dict[str, Any]] = None,
        update_cache: bool = False,  # Deprecated
        use_cached_sql: bool = True,
        notifications: List[NotificationDetails] = [],
        dry_run: bool = False,
        update_published_results: bool = False,
        view_id: Optional[str] = None,
    ) -> RunResponse:
        """Trigger a run of the latest published version of a project.

        Args:
            project_id (str): The Hex Project ID (UUID format)
            inputs (Optional[Dict[str, Any]]): Input parameters for this project run.
                Should be a dictionary of variable_name: value pairs. Only parameters
                that are added to the published app can be set. Cannot be used with view_id.
            update_cache (bool): [DEPRECATED] Use update_published_results instead.
            use_cached_sql (bool): When false, the project will run without using
                any cached SQL results, and will update those cached SQL results.
                Defaults to True.
            notifications (List[NotificationDetails]): List of notification details
                that will be delivered once a project run completes.
            dry_run (bool): When true, performs a dry run without running the project.
                Useful for validating API call structure. Defaults to False.
            update_published_results (bool): When true, updates the cached state of
                the published app with latest run results. Requires "Can Edit" permissions.
                Cannot be true if custom inputs are provided. Defaults to False.
            view_id (Optional[str]): SavedView viewId to use for the project run.
                If specified, uses the saved view's inputs. Cannot be used with inputs.

        Returns:
            RunResponse

        Raises:
            ValueError: If both inputs and view_id are provided
        """
        if inputs is not None and view_id is not None:
            raise ValueError("Cannot specify both inputs and view_id")

        method = "POST"
        endpoint = f"/api/v1/project/{project_id}/run"

        if update_cache:
            self._log.warn(
                "`update_cache` is deprecated. Please use `update_published_results` to update "
                "the published app cache and `use_cached_sql=False` to refresh SQL query caches."
            )

        data: Dict[str, Any] = {
            "useCachedSqlResults": use_cached_sql,
            "dryRun": dry_run,
            "updatePublishedResults": update_published_results,
        }

        if inputs:
            data["inputParams"] = inputs

        if view_id:
            data["viewId"] = view_id

        if notifications:
            data["notifications"] = notifications

        response = cast(
            RunResponse,
            self.make_request(
                method=method,
                endpoint=endpoint,
                data=data,
            ),
        )
        return response

    def run_status(self, project_id, run_id) -> StatusResponse:
        """
        Fetches the status of a run for a given project

        Args:
            project_id (str): The Project ID to fetch results for
            run_id (str): The Run ID of the project to fetch results for

        Returns:
            StatusResponse
        """

        endpoint = f"api/v1/project/{project_id}/run/{run_id}"
        method = "GET"

        response = cast(
            StatusResponse,
            self.make_request(method=method, endpoint=endpoint, data=None),
        )
        return response

    def cancel_run(self, project_id, run_id) -> str:
        """
        Cancels a run in progress.
        Args:
            project_id: The Project ID for the run
            run_id: The Run ID to cancel

        Returns:
            The Run ID that was cancelled

        """
        endpoint = f"api/v1/project/{project_id}/run/{run_id}"
        method = "DELETE"

        self.make_request(method=method, endpoint=endpoint)
        return run_id

    def run_and_poll(
        self,
        project_id: str,
        inputs: Optional[dict],
        notifications: List[NotificationDetails] = [],
        update_cache: bool = False,
        use_cached_sql: bool = True,
        kill_on_timeout: bool = True,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
    ) -> HexOutput:
        """Trigger a project and poll until complete

        Args:
            project_id (str): The Hex project id
            inputs (dict) additional input parameters, a json-serializable dictionary
                of variable_name: value pairs.
            update_cache (bool): [DEPRECATED] When true, this run will update the cached state of the
                published app with the latest run results.
                Additionally, any SQL cells that have caching enabled will be
                re-executed as part of this run. Note that this cannot be set to true
                if custom input parameters are provided.
            use_cached_sql (bool): When false, the project will run without using
                any cached SQL results, and will update those cached SQL results.
            kill_on_timeout (bool): attempt to stop the project if the timeout is
                reached. If false, the project will continue running indefinitely in the
                background until completion.
            poll_interval (int): time to wait in between polls to the API
            poll_timeout (int): maximum time to wait for the project to complete

        Returns:
            Dict[str, Any]: Parsed json output from the API
        """
        if update_cache:
            self._log.warn(
                "`update_cache` is deprecated. Please use `use_cached_sql` instead."
            )
        run_response = self.run_project(
            project_id,
            inputs=inputs,
            notifications=notifications,
            use_cached_sql=use_cached_sql,
        )
        run_id = run_response["runId"]
        poll_start = datetime.datetime.now()
        while True:
            run_status = self.run_status(project_id, run_id)
            project_status = run_status["status"]

            self._log.debug(run_status)
            self._log.info(
                f"Polling Hex Project {project_id}. Current status: {project_status}."
            )

            if project_status not in VALID_STATUSES:
                raise Failure(
                    f"Received an unexpected status from the API: {project_status}"
                )

            if project_status == COMPLETE:
                break

            if project_status in TERMINAL_STATUSES:
                raise Failure(
                    f"Project Run failed with status {project_status}. "
                    f"See Run URL for more info {run_response['runUrl']}"
                )

            if (
                poll_timeout
                and datetime.datetime.now()
                > poll_start + datetime.timedelta(seconds=poll_timeout)
            ):
                if kill_on_timeout:
                    self.cancel_run(project_id, run_id)
                raise Failure(
                    f"Project {project_id} for run: {run_id}' timed out after "
                    f"{datetime.datetime.now() - poll_start}. "
                    f"Last status was {project_status}. "
                    f"TraceId: {run_status['traceId']}"
                )

            time.sleep(poll_interval)
        return HexOutput(run_response=run_response, status_response=run_status)


@resource(
    config_schema={
        "api_key": Field(
            StringSource,
            is_required=True,
            description="Hex API Key. You can find this on the Hex settings page",
        ),
        "base_url": Field(
            StringSource,
            default_value="https://app.hex.tech",
            description="Hex Base URL for API requests.",
        ),
        "request_max_retries": Field(
            int,
            default_value=3,
            description="The maximum times requests to the Hex API should be retried "
            "before failing.",
        ),
        "request_retry_delay": Field(
            float,
            default_value=0.25,
            description="Time (in seconds) to wait between each request retry.",
        ),
    },
    description="This resource helps manage Hex",
)
def hex_resource(context) -> HexResource:
    """
    This resource allows users to programmatically interface with the Hex REST API
    """
    return HexResource(
        api_key=context.resource_config["api_key"],
        base_url=context.resource_config["base_url"],
        log=context.log,
        request_max_retries=context.resource_config["request_max_retries"],
        request_retry_delay=context.resource_config["request_retry_delay"],
    )
