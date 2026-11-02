import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

import httpx
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from nebius.api.nebius.ai.v1 import (
    EndpointServiceClient,
    EndpointStatus,
    GetEndpointRequest,
)
from nebius.sdk import SDK

from dagster_nebius._utils import Deadline, failure, positive


class NebiusEndpointResource(TreatAsResourceParam):
    """Discover and call an existing authenticated HTTP Endpoint.

    This resource never creates, starts, stops or deletes an Endpoint. IAM
    credentials authenticate the SDK; endpoint_token authenticates the served
    application. Inference requests are not retried and redirects are refused.
    """

    def __init__(
        self,
        *,
        sdk_factory: Callable[[], SDK],
        endpoint_id: str,
        project_id: str,
        endpoint_token: str,
        health_path: str,
        endpoint_url: str | None = None,
        request_timeout: float = 30,
        poll_interval: float = 5,
        http_client_factory: Callable[[], httpx.Client] = httpx.Client,
    ):
        if not endpoint_id or not project_id or not endpoint_token:
            raise ValueError("endpoint_id, project_id and endpoint_token are required")
        self._validate_path(health_path)
        self.sdk_factory = sdk_factory
        self.endpoint_id = endpoint_id
        self.project_id = project_id
        self._token = endpoint_token
        self.health_path = health_path
        self.endpoint_url = endpoint_url
        self.request_timeout = positive(request_timeout, "request_timeout")
        self.poll_interval = positive(poll_interval, "poll_interval")
        self.http_client_factory = http_client_factory

    def get_endpoint(self) -> dict[str, str]:
        """Return only nonsecret identity/state/URL fields from the endpoint."""
        return self._get(Deadline(self.request_timeout))

    def _get(self, deadline: Deadline) -> dict[str, str]:
        sdk = self.sdk_factory()
        try:
            timeout = min(self.request_timeout, deadline.remaining())
            endpoint = (
                EndpointServiceClient(sdk)
                .get(
                    GetEndpointRequest(id=self.endpoint_id),
                    timeout=timeout,
                    auth_timeout=timeout,
                    retries=2,
                )
                .wait()
            )
            if (
                endpoint.metadata.id != self.endpoint_id
                or endpoint.metadata.parent_id != self.project_id
            ):
                raise failure("Endpoint identity/project does not match configuration")
            urls = list(endpoint.status.public_endpoints)
            url = self.endpoint_url
            if url is not None:
                if url not in urls:
                    raise failure("Configured URL is not advertised by this Endpoint")
            else:
                https_urls = [u for u in urls if u.startswith("https://")]
                if len(https_urls) > 1:
                    raise failure(
                        "Multiple Endpoint URLs; select endpoint_url explicitly"
                    )
                url = https_urls[0] if https_urls else ""
            if url:
                parsed = urlsplit(url)
                if (
                    parsed.scheme != "https"
                    or not parsed.hostname
                    or parsed.username
                    or parsed.password
                    or parsed.query
                    or parsed.fragment
                ):
                    raise failure(
                        "Endpoint must advertise an HTTPS URL without credentials/query/fragment"
                    )
            return {
                "id": self.endpoint_id,
                "url": url or "",
                "state": str(endpoint.status.state),
            }
        except Exception as exc:
            from dagster import Failure

            if isinstance(exc, Failure):
                raise
            raise failure(
                "Cannot read Nebius Endpoint",
                endpoint_id=self.endpoint_id,
                error_type=type(exc).__name__,
            ) from None
        finally:
            try:
                sdk.sync_close(timeout=self.request_timeout)
            except Exception:
                pass

    def wait_until_ready(self, *, timeout: float = 300) -> dict[str, str]:
        """Require RUNNING, a managed URL and a successful application probe."""
        deadline = Deadline(timeout)
        while True:
            endpoint = self._get(deadline)
            state = endpoint["state"]
            if state == str(EndpointStatus.State.RUNNING) and endpoint["url"]:
                try:
                    response = self._request(
                        endpoint["url"], "GET", self.health_path, None, deadline
                    )
                except httpx.TransportError:
                    response = None
                if response is not None:
                    if response.is_success:
                        return endpoint
                    if response.status_code in {401, 403} or response.is_redirect:
                        raise failure(
                            "Endpoint health probe rejected authentication or redirected",
                            endpoint_id=self.endpoint_id,
                        )
            elif state not in {
                str(EndpointStatus.State.PROVISIONING),
                str(EndpointStatus.State.STARTING),
                str(EndpointStatus.State.IMAGE_PULLING),
                str(EndpointStatus.State.RUNNING),
            }:
                raise failure(
                    "Endpoint is stopped, failed, deleting or in an unknown state",
                    endpoint_id=self.endpoint_id,
                    state=state,
                )
            time.sleep(min(self.poll_interval, deadline.remaining()))

    def request(self, method: str, path: str, *, json: Any = None) -> httpx.Response:
        """Make one bounded application request. Call wait_until_ready first if needed.

        Returns a buffered response; non-2xx statuses raise a redacted Dagster
        failure. Cancelling this call cannot guarantee cancellation of server work.
        """
        self._validate_path(path)
        deadline = Deadline(self.request_timeout)
        endpoint = self._get(deadline)
        if (
            endpoint["state"] != str(EndpointStatus.State.RUNNING)
            or not endpoint["url"]
        ):
            raise failure("Endpoint is not running with an accessible URL")
        try:
            response = self._request(endpoint["url"], method, path, json, deadline)
        except httpx.TransportError:
            raise failure(
                "Endpoint request failed; processing status is unknown. Request was not retried.",
                endpoint_id=self.endpoint_id,
            ) from None
        if not response.is_success:
            raise failure(
                "Endpoint request returned an unsuccessful HTTP status; request was not retried",
                endpoint_id=self.endpoint_id,
                status=str(response.status_code),
            )
        return response

    def _request(
        self, url: str, method: str, path: str, payload: Any, deadline: Deadline
    ) -> httpx.Response:
        self._validate_path(path)
        with self.http_client_factory() as client:
            return client.request(
                method,
                url.rstrip("/") + path,
                json=payload,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=min(self.request_timeout, deadline.remaining()),
                follow_redirects=False,
            )

    @staticmethod
    def _validate_path(path: str) -> None:
        if (
            not path.startswith("/")
            or path.startswith("//")
            or "\\" in path
            or urlsplit(path).fragment
        ):
            raise ValueError("Use an absolute application path under the Endpoint URL")
