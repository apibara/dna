# Contributing to Apibara

First off, thank you for contributing to Apibara ❤️! Apibara is built by
developers passionate about open-source like you.

This guide will go through the recommended workflow for new contributors.

## Before you start

The first step is to decide what to work on. The best place to look for work is
the [project's issues](https://github.com/apibara/dna/issues). Issues labelled
"Good first issue" are small tasks that don't require specialized knowledge of
the Apibara codebase.

If you're an Apibara user, you can also propose changes to Apibara that solve
your pain points. In this case, please open an issue before starting to work.

After you decide what to work on, hop on the
[Discord server](https://discord.gg/VDh2CRQ4) to chat with our team. The
`#open-source` channel is perfect for syncing and receiving early feedback on
your contribution.

## Development

Apibara DNA is built in Rust. We use [Nix](https://nixos.org/) to create
reproducible builds. While Nix is terrific, we understand it's also a complex
tool, and this section will help you start with it. For an easier setup, you can
**use the provided devcontainer** that installs and configures Nix for you.

**Installing Nix**

If you don't have Nix installed, install it on your system. The
["Zero to Nix"](https://zero-to-nix.com/start/install) guide has a great
tutorial on it.

**Clone the project**

Clone the project using git:

```
git clone git@github.com:apibara/dna.git
# or
git clone https://github.com/apibara/dna.git
# or
gh repo clone apibara/dna
```

**Setting up the development environment**

We use Nix to create the development environment will all the required
dependencies. From the project root, run:

```
nix develop
```

You will then be in a bash shell with the correct Cargo, rust version, and all
the required external libraries and tools (like the `protoc` compiler).

Nix may ask you to accept an "untrusted substituter": this is the nix way of
asking if you trust our public build cache that will save you from rebuilding
the project from scratch. Unless you have good reason to not trust us, you should
accept it!

**Building & Testing**

You can use all the standard cargo commands for development.

Before submitting your PR, ensure all the projects build with Nix. For example:

```
nix build .#all-crates
```

You can use nix to run tests in the same environment that will be used in GitHub Actions.

```
nix build .#unit-tests
```

Integration tests require connecting to the Docker daemon and for this reason they
cannot be run inside a nix build environment. The CI runs them in two steps: first
it builds a nextest archive and then runs tests from it.

```
nix build .#integration-tests-archive
nix develop .#integration -c run-integration-tests
```

**Committing changes**

This project prefixes changes with the subproject it changed and then a short
commit message. Usually, we prefer a single commit for each unit of work, so use
`git commit --amend` freely while developing.\
If the commit is large, please briefly describe why you made this change.

```
sink: change X to Y

This change is needed because ...
```

Don't use any prefix if your change concerns CI or the build system.

## Code Review

Open a Pull Request on GitHub when you think your contribution is ready for
review, and we will review it as soon as possible.

The review process helps us keep the project's code quality high, so expect to
receive comments and suggestions on improving the code.

**Pull Request description**

Using the starting template, update the pull request message to summarize your
changes and how to test everything is working correctly.

This will help us when:

-   reviewing the PR to more easily understand why a change is needed.
-   testing the changes locally before merging.
-   making a new release, your message will become part of the CHANGELOG.

**Updating your Pull Request**

After updating the code with your changes, amend the previous commit:

```
git commit --amend
```

This step ensures that git history always looks clean and tidy. Push changes to
your fork:

```
git push --force-with-lease
```

After your PR is in good shape, we will merge it into the main branch and ship
it. Congratulations, the code you wrote is now used by hundreds of developers
and thousands of end users 🎊

## Merging PRs

> [!NOTE]  
> This section is only relevant if you're a maintainer.

This repository uses merge commits when merging PRs. When merging PRs you MUST
do the following:

-   Change the merge commit title to a brief description of the PR. In most cases
    you can use the PR title.
-   Change the merge commit description to an actual description of the PR
    content. If you can, describe how to test the changes are working. If the
    contributor filled out `PULL_REQUEST_TEMPLATE` correctly you can reuse their
    content (removing the comments between `<!-- -->`).

We switched to merge commits for PRs for the following reasons:

-   It's hard to keep contributors commit clean.
-   There's a lot of context added in a PR and rebase workflows lose that
    information.
-   Crafting CHANGELOGs becomes easier, simply read through `git log --merges`.
-   The code being tested in the CI is the same code that will land in `main`.
    This means we don't need to re-run all tests and checks.

## Making a release

> [!NOTE]  
> We are in the process of updating how releases are made.
> This is just a draft of the release process.

-   Releases are cut from the `release` branch for normal releases. See below to
    learn more about backporting fixes.
-   Start by opening a PR from `main` into `release`. This PR should contain no
    changes other than changes to the CHANGELOGs and version numbers.
-   The `cd-check.yml` pipeline is executed. This pipeline simply builds the
    binaries (we follow the ["not rocket
    science"](https://graydon2.dreamwidth.org/1597.html) rule).
-   Once the PR is merged, nothing happens.
-   To actually make a release, create a new release on GitHub targeting the
    `release` branch. Releases must have a tag name starting with the project name
    and followed by the version number, e.g. `sink-console/v1.0.0`. Mark the release
    as pre-release.
-   Pushing a new tag triggers the `release.yml` pipeline. This pipeline builds
    the binaries for Linux and MacOS and uploads them to the release. Docker images
    are also built and uploaded. The release pre-release status will be removed
    automatically.

The release workflow MUST allow us to release fixes for older version of the
software:

-   We want to support DNA v1 for a few months after the release of V2.
-   We want to introduce more breaking changes if they make the software better,
    but we want users to upgrade software at their own pace.
-   We need to hotfix older versions of the software that are still used by
    paying customers.

Once it becomes necessary to backport changes, we create a new `release/<name>`
branch for that stream of changes.

-   `release/v1`: contains DNA v1-related changes.
-   `release/starknet-rpc-0_6`: contains changes to the Starknet RPC 0.6
    ingestion code.

Visualized, the end-to-end git workflow is the following:

```txt
feat/xyz                o---o   \
                       /     \   \
feat/abc      o-----o /       \   \
             /       x         \   \
main     ---o-------o-A---------B---C----
                                     \
release  -----------------------------D---
```

With the following:

-   `feat/abc` and `feat/xyz` are feature branches.
-   `A` is the merge commit for `feat/abc` into `main`.
-   `B` is the merge commit for `feat/xyz` into `main`.
-   `C` is the commit with the CHANGELOG and version update. In practice, this
    commit also comes from a branch and PR but we didn't include it in the diagram
    to keep it clean.
-   `D` is the merge commit into `release`. This commit will be tagged and the
    release built off of it.
