# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Common Changelog](https://common-changelog.org/), and
this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2023-12-11

_Add support for private GitHub repositories._

### Changed

-   Change the GitHub indexer source to allow cloning private repositories.
    Users should create a private access token (PAT) and store it in a secret. Use
    the secret together with the `access_token_env_var` to authenticate with GitHub
    on clone.

[0.2.0]: https://github.com/apibara/dna/releases/tag/operator/v0.2.0
