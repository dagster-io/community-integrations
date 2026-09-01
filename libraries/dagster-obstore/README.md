# dagster-obstore

A Dagster integration for [`obstore`](https://github.com/developmentseed/obstore), a Rust-backed Python object store client with a single uniform interface across providers.

## Supported providers

- **AWS S3** and **Backblaze B2** via `S3ObjectStore`. Target Backblaze B2 by setting `endpoint` to the B2 S3-compatible endpoint (`https://s3.<region>.backblazeb2.com`) and `region` to the matching B2 region.
- **Azure Blob Storage** via `AzureBlobObjectStore`.
- **Google Cloud Storage** via `GCSObjectStore`.

## Requirements

Install docker-compose or podman-compose in your system, this is used to spin up a moto[s3] and azurite server during the tests.

## Test

```sh
make test
```

## Build

```sh
make build
```
