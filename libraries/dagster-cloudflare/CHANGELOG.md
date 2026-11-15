# Changelog

## [0.0.1]

### Added

- `CloudflareContainerRunLauncher`: runs each Dagster run in its own Cloudflare Container through a dispatcher Worker.
- Reference dispatcher Worker (one Durable Object per run) with an example `wrangler.jsonc`, `Dockerfile`, and an optional Cloudflare Access tunnel entrypoint for Postgres.
