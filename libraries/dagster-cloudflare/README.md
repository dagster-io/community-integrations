# dagster-cloudflare

Run each Dagster run in its own [Cloudflare Container](https://developers.cloudflare.com/containers/).

`CloudflareContainerRunLauncher` works like the Docker and ECS run launchers: the
Dagster daemon or webserver launches a run, and the run executes
`dagster api execute_run` in a new container. The container is started by a small
dispatcher Worker that you deploy to your own Cloudflare account. The Worker keeps
one Durable Object per run, so a repeated launch request never starts a second
container, and it records each run's state and exit code for Dagster run monitoring.

```
Dagster daemon ──HTTPS + token──▶ dispatcher Worker ──▶ Durable Object (per run) ──▶ Container
      ▲                                                                                 │
      └──────────────────── run events (Dagster instance storage) ◀─────────────────────┘
```

## Install

```sh
pip install dagster-cloudflare
```

## Requirements

- A Cloudflare account with Containers (Workers Paid plan) and `wrangler`.
- A Dagster code image that Cloudflare can run (`linux/amd64`).
- Dagster instance storage that the container can reach over the internet, usually
  Postgres. Two ways that work:
  - Postgres with a public address and TLS, credentials passed as environment variables.
  - Postgres behind [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/applications/non-http/)
    with a Cloudflare Tunnel. `worker/container/with-access-tunnel.sh` starts
    `cloudflared access tcp` in the container before the run starts.
- Compute logs and IO managers must also use remote storage (for example S3 or R2),
  because the container's disk is removed after the run.

## Set up

1. Deploy the Worker in [`worker/`](worker/):

   ```sh
   cd worker
   # Edit Dockerfile (FROM your code image) and wrangler.jsonc.
   wrangler secret put DISPATCH_TOKEN          # any long random string
   wrangler secret put RUN_ENV_JSON            # optional: {"DAGSTER_PG_PASSWORD": "..."}
   wrangler deploy
   ```

2. Configure the launcher in `dagster.yaml`:

   ```yaml
   run_launcher:
     module: dagster_cloudflare
     class: CloudflareContainerRunLauncher
     config:
       endpoint: https://dagster-runs.<your-subdomain>.workers.dev
       token:
         env: DAGSTER_CLOUDFLARE_TOKEN
       instance: standard-2          # or {vcpu: 1, memoryMib: 6144, diskMb: 8000}
       env_vars:
         - DAGSTER_PG_HOST           # copied from the launcher's environment
         - DAGSTER_PG_PASSWORD
         - LOG_LEVEL=INFO            # set here

   run_monitoring:
     enabled: true
   ```

3. Turn on run monitoring (above). The launcher reports the container state to
   Dagster, so a container that stops without finishing its run fails the run.

## Configuration

| Field | Required | Description |
|---|---|---|
| `endpoint` | yes | HTTPS URL of the Worker. Plain `http` is accepted only for `localhost`. |
| `token` | yes | The Worker's `DISPATCH_TOKEN`. |
| `instance` | no | Instance type (`lite`, `standard-1` .. `standard-4`) or a custom size `{vcpu, memoryMib, diskMb}`. Default: the Worker's `DEFAULT_INSTANCE`. |
| `image` | no | Name of an image in the Worker's `containers[].images`. Default: the Worker's `DEFAULT_IMAGE`. |
| `env_vars` | no | Environment for the run. `KEY` copies the value from the launcher process; `KEY=VALUE` sets it. Merged over the Worker's `RUN_ENV_JSON`. |
| `request_timeout` | no | Seconds per request to the Worker. Default 30. |

Per-run overrides with run tags:

| Tag | Example |
|---|---|
| `dagster-cloudflare/instance` | `standard-4` or `{"vcpu": 2, "memoryMib": 12288, "diskMb": 12000}` |
| `dagster-cloudflare/image` | `gpu` |

## Behavior

- **Launch.** The launcher sends the run's `execute_run` command. If the response is
  lost, it asks the Worker for the run's state before it reports a failure.
- **Health.** Run monitoring reads the state from the Worker. A container that
  exits with code 137 (killed, usually for memory) is reported with that cause.
- **Terminate.** The Worker sends `SIGTERM`, so Dagster records the run as canceled,
  and stops the container after `STOP_TIMEOUT_MS` if it is still running.
- **Retention.** The Worker deletes a finished run's state after `RETENTION_DAYS`.

## Limits

- Cloudflare Containers have instance size, disk, and concurrency limits; see
  [limits](https://developers.cloudflare.com/containers/platform-details/limits/).
  Set Dagster's run queue limit (`max_concurrent_runs`) to match. `max_instances`
  is not accepted for `scheduling_policy: durable_object`; `wrangler` rejects it.
- The image is fixed at `wrangler deploy` time. Deploy the Worker again when
  your code image changes.
- Each run pays for container start time; short runs are dominated by it.

## Try it locally

`wrangler dev` runs the Worker and its containers on your machine with Docker, so
you can test the whole path before you deploy:

```sh
cd worker
printf 'DISPATCH_TOKEN=%s\n' "$DAGSTER_CLOUDFLARE_TOKEN" > .dev.vars
npx wrangler dev --port 8787
```

Set `endpoint: http://localhost:8787` in `dagster.yaml`. Notes:

- `wrangler dev` builds the image for `linux/amd64`, also on Apple silicon.
- Instance sizes are not applied; the local container has no memory limit.
- The run container reaches services on your machine through
  `host.docker.internal` (Docker Desktop), not `localhost`. Set it for the run
  only, for example `env_vars: [DAGSTER_PG_HOST=host.docker.internal]`.
- On Linux, the local container runtime must reach `wrangler` on the host from
  the Docker bridge. A host firewall that blocks this (for example `ufw` with
  default rules) makes each start fail after about 20 seconds with
  `Container failed to start`.

## Develop

```sh
make test          # Python tests; also runs the Worker tests when node is installed
make test-worker   # Worker tests only
make ruff
make check
```

The Python tests launch real Dagster runs through a local stand-in for the Worker
(`dagster_cloudflare_tests/fake_worker.py`) that runs each command as a process.
