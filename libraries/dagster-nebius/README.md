# dagster-nebius

Run containerized GPU assets on **Nebius Serverless Jobs** from Dagster, with
Pipes logs, materialization metadata and cancellation. A separate resource
calls **existing Serverless Endpoints**. This package does not use Token Factory.

Local tests exercise Dagster, the released Nebius SDK message types and a real
Pipes worker against a local S3-compatible test server.
Community ownership/publishing is pending.

## Install

From this checkout (a published PyPI release is not assumed):

```sh
cd libraries/dagster-nebius
uv sync
```

The initial supported range is Python 3.10+, Dagster 1.13.21–1.13.x and
`nebius` 0.6.10–0.6.x. Dependencies are deliberately constrained around the
released interfaces used here. The GPU image needs `dagster-pipes` and Boto3,
plus workload dependencies, rather than the full Dagster package.

## Before running

Provide an existing Nebius project/subnet, an available GPU platform/preset,
a container image, and a private Nebius Object Storage bucket for protocol
messages. Nebius Object Storage is accessed through its S3-compatible API.
The caller also provides the workload's input/output storage.

There are three distinct authentication boundaries:

- The Dagster worker uses a Nebius SDK credential provider for Jobs/Endpoints
  control-plane operations. Service-account credentials support token renewal;
  `SDK(config_reader=Config())` is convenient with a configured local CLI.
- Nebius Object Storage uses separate access-key credentials, an explicit
  regional HTTPS endpoint such as `https://storage.<region>.nebius.cloud`, and
  its region. IAM access tokens are not Object Storage access keys. The
  orchestrator reads protocol objects; the GPU container writes protocol objects
  and its outputs. Use supported secret references for the worker's credentials.
- Endpoint application requests use that Endpoint's configured bearer token,
  not the control-plane IAM token.

Both Dagster and the workload need outbound storage access; the workload also
needs image/input access. No inbound connection to Dagster is required. Keep
all secrets out of Pipes extras, asset metadata, source control and log output.
See [Nebius Job management](https://docs.nebius.com/serverless/jobs/manage),
[SDK authentication](https://github.com/nebius/pysdk#readme) and
[Nebius Object Storage setup](https://docs.nebius.com/object-storage/interfaces/aws-cli).

## Run an asset

```python
from datetime import timedelta
import boto3
import dagster as dg
from botocore.config import Config as BotoConfig
from nebius.aio.cli_config import Config
from nebius.api.nebius.ai.v1 import JobSpec
from nebius.api.nebius.compute.v1 import DiskSpec
from nebius.sdk import SDK
from dagster_nebius import PipesNebiusClient

# Factories create new clients during execution, after Dagster forks workers.
def sdk_factory():
    return SDK(config_reader=Config())

def s3_factory():
    return boto3.client(
        "s3", endpoint_url="https://storage.<region>.nebius.cloud",
        region_name="<region>",
        config=BotoConfig(connect_timeout=5, read_timeout=10,
                          retries={"max_attempts": 2}),
    )

@dg.asset
def gpu_result(context: dg.AssetExecutionContext, nebius: PipesNebiusClient):
    return nebius.run(
        context=context,
        project_id="<project-id>",
        job_spec=JobSpec(
            image="<instrumented-image-by-digest>",
            platform="<platform>", preset="<preset>", subnet_id="<subnet-id>",
            disk=JobSpec.DiskSpec(
                type=DiskSpec.DiskType.NETWORK_SSD, size_bytes=100 * 1024**3,
            ),
            timeout=timedelta(minutes=10), restart_attempts=0,
            # Supply environment/secret references for the worker's
            # Nebius Object Storage client.
        ),
        wait_timeout=900,
        extras={"input_uri": "s3://input-bucket/data"},
    ).get_materialize_result(implicit_materialization=False)

defs = dg.Definitions(
    assets=[gpu_result],
    resources={"nebius": PipesNebiusClient(
        sdk_factory=sdk_factory, s3_client_factory=s3_factory,
        bucket="<protocol-bucket>",
    )},
)
```

In the container, use the standard `PipesS3MessageWriter` to send messages to
Nebius Object Storage through its S3-compatible API:

```python
import boto3
from dagster_pipes import PipesS3MessageWriter, open_dagster_pipes

# Configure endpoint, region, credentials and bounded Boto3 timeouts here.
s3 = boto3.client("s3", endpoint_url="https://storage.<region>.nebius.cloud")
with open_dagster_pipes(message_writer=PipesS3MessageWriter(s3)) as pipes:
    input_uri = pipes.get_extra("input_uri")
    # Run computation and persist output successfully before reporting it.
    pipes.report_asset_materialization(
        metadata={"output_uri": "s3://output-bucket/unique-attempt/result"},
    )
```

The runnable [GPU example](examples/gpu_asset/README.md) includes the image,
secret references and attempt-specific output paths. Output locations come
from the workload; the package does not invent or query a Jobs artifact API.

## Existing Endpoint

```python
from dagster_nebius import NebiusEndpointResource

endpoint = NebiusEndpointResource(
    sdk_factory=sdk_factory,
    endpoint_id="<endpoint-id>", project_id="<project-id>",
    endpoint_token="<resolve-secret-at-runtime>",
    health_path="/health",  # The served application defines this path.
)
endpoint.wait_until_ready(timeout=300)
response = endpoint.request("POST", "/infer", json={"input": "example"})
```

Configure this resource within worker execution when resolving secrets. When
multiple managed HTTPS URLs are advertised, explicitly select `endpoint_url`.
Readiness requires both `RUNNING` and a successful application probe. Only an
OpenAI-compatible serving image supports OpenAI API paths. Authentication must
be enabled on the Endpoint; the package requires its token. Responses are
buffered; streaming inference is not part of this release.

Requests do not follow redirects and inference requests are never retried.
Cancelling an HTTP request does not guarantee cancellation of remote work.
The resource never creates, starts, stops, deletes or otherwise mutates the
Endpoint. In particular, a Dagster failure cannot tear down a shared service.

## Lifecycle and failure contract

- `Create` returns a lifecycle operation, not a completed workload. The client
  polls the operation and Job; only `COMPLETED` plus a valid Pipes session and
  all expected explicit materializations is success. Errors, cancellation,
  missing/invalid messages, unknown states and externally deleted resources fail.
- Protocol chunks are polled synchronously alongside Job status. This avoids
  background-thread exceptions being lost. Reader state/prefixes are per
  invocation; standard `PipesS3MessageWriter` supplies the numbered chunk format.
  A missing chunk is retried without skipping later indexes. Other Object Storage
  errors propagate. Logs/stdout/stderr emitted inside the Pipes scope arrive in batches.
- Provider startup/image-pull logs are separate. Use
  `nebius ai job logs <job-id> --follow` or the Nebius Console for diagnostics.
  The package does not assume a Job-specific log API contract.
- `DAGSTER_PIPES_*` environment names are reserved. Keep context/extras small;
  this implementation uses environment context injection, not source packaging.
- A UUID name/idempotency key and receipt are logged before Create. **Create and
  Cancel mutation retries are disabled.** Read RPCs have bounded SDK retries.
  An uncertain submission fails without launching another Job. Its Dagster
  failure metadata retains the full submission receipt: `job_name`, `project_id`,
  `idempotency_key`, `pipes_uri` and `spec_sha256` (the submitted spec fingerprint,
  including Pipes bootstrap variables). Raw specs and SDK errors are excluded
  because they can contain secrets. Use the receipt's project/name to list
  **all matching Jobs**, then inspect their specs and
  operation IDs before cancellation or resubmission. Names are not unique: a
  live request with the same name but a new key created a second Job.
  Automatic Dagster retries are disabled; reexecution with a nonzero retry number
  is rejected. A new Dagster run is a new submission, so reconcile first.
- Positive provider `JobSpec.timeout` is required; provider restart attempts
  must be zero. `wait_timeout` also covers provisioning; `drain_timeout` defaults
  to 60 seconds and `cancel_timeout` to 120 seconds. `rpc_timeout` defaults to 30
  seconds; `poll_interval` to 5. A live provisioning cancellation exceeded the
  default 120-second confirmation window before eventually reaching `CANCELLED`;
  reconcile every `UNCONFIRMED` Job independently. Supply Boto3 clients with finite
  connect/read/retry budgets: an in-flight storage call can extend the local deadline by that budget.
- On interruption, timeout or supervision failure while active, cancellation is
  forwarded by default and terminal state is verified. Failed cancellation is
  logged as **UNCONFIRMED** with the Job ID. The original failure/interruption is
  retained. `forward_termination=False` leaves the Job running after local failure.
- SIGKILL/machine loss cannot run cleanup. No automatic reconnect or orphan
  reconciler is provided. A live 60-second provider timeout produced
  `TimeoutExceeded` before the worker started, and a separate 300-second timeout
  terminated a Job after its orchestrator was killed with SIGKILL. These are
  observed cases, not a universal provisioning deadline: the provider documents
  a separate [30-minute capacity wait](https://docs.nebius.com/serverless/lifecycle#provisioning-timeout).
  Keep the local wait/cancellation budgets and reconcile unconfirmed Jobs.
  Enforcing cleanup after orchestrator loss requires an independent watchdog
  with durable submission receipts and permission to inspect/cancel Jobs; this
  package does not provide one. Increasing `wait_timeout` cannot address process loss.
- Job records, protocol objects and outputs are retained; shared buckets and
  volumes are never deleted. Configure a protocol-prefix lifecycle policy.
  Workload success does not prove resource release or a billing amount.

Client factories must return new clients, owned/closed by the invocation. Use
synchronous Dagster execution; invoking the SDK's synchronous methods from an
active async stack is unsupported. There is no multi-node orchestration,
provider restart aggregation, endpoint autoscaling or scale-to-zero guarantee.

## Development and release gates

```sh
uv sync
make test
uv run ruff check
uv run ruff format --check
make check       # shared CI currently uses ty
make pyright     # CONTRIBUTING.md also requires Pyright
make build
```

Ordinary tests make no cloud calls. They include fault injection, SDK message
copying and a real worker subprocess against a local Moto S3 HTTP server.
The supplemental workflow checks Python 3.10 and 3.12, Pyright, and clean wheel
installation; the repository's generic workflows handle Ruff/ty/pytest/releases.

The [live tests](dagster_nebius_tests/test_live.py) are opt-in and billable
(`NEBIUS_RUN_LIVE_TESTS=1`). Do not give fork PRs cloud secrets.
Revalidate relevant cases when changing SDK versions, lifecycle logic or provider
configuration. Long-term idempotency retention and arbitrary model-specific
Endpoint behavior are not guaranteed by these tests. The package has
no assigned production support SLA or publishing owner yet. Release through the
community repository's existing `dagster_nebius-X.Y.Z` tag workflow once those
owners and gates are agreed; there is no separate release workflow here.
