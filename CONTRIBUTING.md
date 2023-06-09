# Contributing to Apibara

First off, thank you for contributing to Apibara ❤️! Apibara is built by
developers passionate about open-source like you.

This guide will go through the recommended workflow for new contributors.

## Before you start

The first step is to decide what to work on. The best place to look for work is
the [project's issues](https://github.com/apibara/dna/issues).
Issues labelled "Good first issue" are small tasks that don't require
specialized knowledge of the Apibara codebase.

If you're an Apibara user, you can also propose changes to Apibara that solve
your pain points. In this case, please open an issue before starting to work.

After you decide what to work on, hop on the [Discord
server](https://discord.gg/VDh2CRQ4) to chat with our team. The `#open-source`
channel is perfect for syncing and receiving early feedback on your
contribution.

## Development

Apibara DNA is built in Rust. We use [Nix](https://nixos.org/) to create
reproducible builds. While Nix is terrific, we understand it's also a complex
tool, and this section will help you start with it.
For an easier setup, you can **use the provided devcontainer** that installs and
configures Nix for you.

**Installing Nix**

If you don't have Nix installed, install it on your system. The ["Zero to
Nix"](https://zero-to-nix.com/start/install) guide has a great tutorial on it.

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

**Building & Testing**

You can use all the standard cargo commands for development. Before submitting
your PR, ensure all the projects build with Nix. For example:

```
nix build .#apibara-starknet
```

**Committing changes**

This project prefixes changes with the subproject it changed and then a short
commit message. Usually, we prefer a single commit for each unit of work, so
use `git commit --amend` freely while developing.  
If the commit is large, please briefly describe why you made this change.

```
starknet: change X to Y

This change is needed because ...
```

Don't use any prefix if your change concerns CI or the build system.

We recommend splitting significant contributions up into many independent
stacked changes. Our team recommends using [Graphite for stacked
changes](https://graphite.dev/).


## Code Review

Open a Pull Request on GitHub when you think your contribution is ready for
review, and we will review it as soon as possible.

The review process helps us keep the project's code quality high, so expect to
receive comments and suggestions on improving the code.

**Updating your Pull Request**

After updating the code with your changes, amend the previous commit:

```
git commit --amend
# or with graphite
gt commit amend # gt ca also works
```

This step ensures that git history always looks clean and tidy. Push changes to your fork:

```
git push --force-with-lease
```

After your PR is in good shape, we will merge it into the main branch and ship
it. Congratulations, the code you wrote is now used by hundreds of developers
and thousands of end users 🎊
