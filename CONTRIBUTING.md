# Contributing

We're excited that you're interested in contributing to the Dagster community integrations repository!

This project aims to foster growth and collaboration within the Dagster ecosystem by providing a space for users to share their custom integrations, without needing to manage the build or release process of their packages.

## Getting started

1. Fork the `dagster-io/community-integrations` repository on GitHub.

2. Clone your forked repository

    ```sh
    # clone the repository
    git clone https://github.com/your-username/community-integrations.git

    # navigate to the directory
    cd community-integrations
    ```

3. Create a new branch for your integration

    ```sh
    git checkout -b my-new-integration
    ```

4. See below for adding an integration...

## Adding an integration

Integrations live in the `libraries/` folder of this repository. We recommend using `uv` for project scaffolding, and management.

1. Navigate to the `libraries/` directory
    ```sh
    cd libraries
    ```
2. Scaffold a new project using `uv`
    ```sh
    uv init <name of integration>
    ```
3. Include a README file in your integration directory with:
    * A clear description of what your integration does
    * Installation instructions
    * Usage examples
    * Any dependencies or prerequisites
4. Add your integration code, ensuring it follows Dagster's best practices and coding standards.
5. Include tests for your integration to ensure reliability.
6. Create a GitHub action workflow using the template at `.github/workflows/template-release.yml`


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
