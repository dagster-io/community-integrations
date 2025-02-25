# Contributing

We're excited that you're interested in contributing to the Dagster community integrations repository!

This project aims to foster growth and collaboration within the Dagster ecosystem by providing a space for users to share their custom integrations without needing to manage the build or release process of their packages.

Need help or want to talk to someone first? Join us in our [Slack](https://dagster.io/slack) in the `#community-contributions` channel.

## Getting started

1. Fork the `dagster-io/community-integrations` repository on GitHub.
2. Clone your forked repository

   ```sh
   # clone the repository
   git clone https://github.com/your-username/community-integrations.git

   # navigate to the directory
   cd community-integrations
   ```

## Standards

- Standard code formatting is enforced using the [Ruff](https://github.com/astral-sh/ruff) package.
- Unit tests should be included for core functionality of the integration.
- Running a static type checker like [Pyright](https://github.com/microsoft/pyright) is heavily encouraged.

For each integration, unit tests, formatting, and Pyright are _required_ to pass before it can be merged into the _main_ branch.

Each integration typically includes a `Makefile` that will have directives for the above checks:

- `make test`
- `make ruff`
- `make check`

## Creating a new integration

Integrations live in the `libraries/` folder of this repository. We recommend using `uv` for project management.

Packages are to be named `dagster-<name>` where `<name>` is the name of the tool or service that you are integrating with.

1. Navigate to the `libraries/` directory:

   ```sh
   cd libraries
   ```

2. Create a copy the `_template` integration:

   ```sh
   cp -r _template dagster-<my-package>
   ```

3. Replace the references of `example-integration` with the name of your module.
4. Update the README to include:

   - A clear description of what your integration does
   - Installation instructions
   - Usage examples
   - Any dependencies or prerequisites

5. Add your integration code, ensuring it follows Dagster's best practices and coding standards.
6. Include tests for your integration to ensure reliability.
7. Create GitHub Actions workflows using the templates located at `.github/workflows/template-*`.

## Running GitHub Actions workflows locally

You can replicate most of the GitHub Actions workflows locally using [`act`](https://nektosact.com/). This provides a smoother development experience than pushing to GitHub and waiting for GitHub Actions to run on each commit.

1. Follow the [installation instructions on the `act` website](https://nektosact.com/installation/index.html).
2. Make sure you're in the root of the `community-integrations` repository (i.e. the folder containing the hidden `.github` folder).
3. Ensure Docker Engine is running.
4. Run a workflow using the `act` command. For example:

   ```sh
   act -W .github/workflows/quality-check-dagster-anthropic.yml
   ```

> [!NOTE]  
> To run a workflow that uses `testcontainers`, use the following [workaround](https://github.com/nektos/act/issues/501#issuecomment-2344539500):
>
> ```sh
> act --env TESTCONTAINERS_HOST_OVERRIDE=`ipconfig getifaddr en0` -W .github/workflows/quality-check-dagster-iceberg.yml
> ```

## Code review process

1. A Dagster maintainer will review your pull request.
2. Address any feedback or requested changes.
3. Once approved, your integration will be merged into the main repository.

## Community Guidelines

- Be respectful and inclusive in all interactions.
- Provide constructive feedback on other contributions.
- Follow the [Dagster Code of Conduct](https://github.com/dagster-io/dagster/blob/master/.github/CODE_OF_CONDUCT.md).

## Need Help?

We love collaboration and want to enable you. If you are unsure of where to start, please reach out.

You can join us either on [Slack](dagster.io/slack) or on GitHub, in [discussions](https://github.com/dagster-io/dagster/discussions) or by [opening an issue](https://github.com/dagster-io/dagster/issues/new/choose).

Thank you for contributing to the Dagster community! Your efforts help make data orchestration more accessible and powerful for everyone.
