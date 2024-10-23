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

Releases are triggered by creating a git tag of the pattern `<integration-name>-vX.X.X`. For example:

```sh
$ git tag dagster_modal-v0.0.1
$ git push origin dagster_modal-v0.0.1
```

Where the tag must match the prefix of the files generated in the `dist/` folder:

```sh
python-3.11 main ~/src/community-integrations/libraries/dagster-modal
$ ls dist/
total 16
-rw-r--r--@ 2.0K Oct 23 14:06 dagster_modal-0.0.1-py3-none-any.whl
-rw-r--r--@ 1.6K Oct 23 14:06 dagster_modal-0.0.1.tar.gz
```

### Variables

The GitHub action uses the `uv deploy` command, and requires the `UV_PUBLISH_TOKEN` and `UV_PUBLISH_URL` variables to be set.

Maintainers are to ensure the variables are present in the _production_ environment of the [repository settings](https://github.com/dagster-io/community-integrations/settings).
