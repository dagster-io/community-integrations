<div align="center">
    <img alt="Cover Image" src=".github/cover.png">
</div>

---

Community built and maintained integrations in the Dagster ecosystem.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Releases

Building and releasing integrations is accomplished with GitHub actions where each library or integration has its own workflow.

For example, the `dagster-modal` integration has a workflow defined at `.github/workflows/release-dagster-modal.yml`.

Releases are triggered by creating a git tag of the pattern `<integration-name>-vX.X.X`.

```sh
$ git tag dagster-modal-v0.0.1
$ git push origin dagster-modal-v0.0.1
```

### Variables

The GitHub action uses the `uv deploy` command, and requires the `UV_PUBLISH_TOKEN` and `UV_PUBLISH_URL` variables to be set.

Maintainers are to ensure the variables are present in the _production_ environment of the [repository settings](https://github.com/dagster-io/community-integrations/settings).
