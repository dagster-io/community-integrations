# Contributing

We're excited that you're interested in contributing to the Dagster community integrations repository!

This project aims to foster growth and collaboration within the Dagster ecosystem by providing a space for users to share their custom integrations, without needing to manage the build or release process of their packages.

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

- Standard code formatting is enforced using the [ruff](https://github.com/astral-sh/ruff) package
- Unit tests should be included for core functionality of the integration
- Running a static type checker like [pyright](https://github.com/microsoft/pyright) is heavily encouraged

For each integration unit tests, formatting, and pyright are *required* to pass before it can be merged into the _main_ branch.

Each integration typically includes a `Makefile` that will have directives for the above checks, for example:

- `make test`
- `make ruff`
- `make check`

## Creating a new integration

Integrations live in the `libraries/` folder of this repository. We recommend using `uv` for project management.

Packages are to be named `dagster-contrib-<name>` where `<name>` is the name of the tool or service that you are integrating with.

1. Navigate to the `libraries/` directory
    ```sh
    cd libraries
    ```
2. Create a copy the `_template` integration
    ```sh
    cp -r _template dagster-contrib-<my-package>
    ```
3. Replace the references of `example-integration` with the name of your module
3. Update the README to include:
    * A clear description of what your integration does
    * Installation instructions
    * Usage examples
    * Any dependencies or prerequisites
4. Add your integration code, ensuring it follows Dagster's best practices and coding standards.
5. Include tests for your integration to ensure reliability.
6. Create GitHub action workflows using the templates located at `.github/workflows/template-*`

## Code review process

1. A Dagster maintainer will review your pull request.
2. Address any feedback or requested changes.
3. Once approved, your integration will be merged into the main repository.


## Community Guidelines

* Be respectful and inclusive in all interactions.
* Provide constructive feedback on other contributions.
* Follow the Dagster code of conduct

## Need Help?

We love collaboration, and want to enable you. If you are unsure of where to start, please reach out.

You can join us either on [Slack](dagster.io/slack), or on GitHub in discussions or by opening an issue.

Thank you for contributing to the Dagster community! Your efforts help make data orchestration more accessible and powerful for everyone.
