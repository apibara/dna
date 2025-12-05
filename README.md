<p align="center">
    <img width="400" src="https://user-images.githubusercontent.com/282580/176315678-e7ab5a9b-5561-41e4-b314-62f99fd90d2f.png" />
</p>

---

# Apibara Direct Node Access and SDK

This repository contains the canonical implementation of the Apibara Direct Node
Access (DNA) protocol and the integrations built on top of it.\
The protocol enables developers to easily and efficiently stream any onchain
data directly into their application.

## Contributing

We are open to contributions.

-   Read the
    [CONTRIBUTING.md](https://github.com/apibara/dna/blob/main/CONTRIBUTING.md)
    guide to learn more about the process.
-   Some contributions are [rewarded on OnlyDust](https://app.onlydust.com/p/apibara).
    If you're interested in paid contributions, get in touch before submitting a PR.

## Development

Apibara DNA is developed against stable Rust. We provide a
[nix](https://nixos.org/) environment to simplify installing all dependencies
required by the project.

-   if you have nix installed, simply run `nix develop`.
-   if you don't have nix installed, you should install Rust using your favorite
    tool.

## Platform Support

**Tier 1**

These platforms are tested against every pull request.

-   linux-x86_64
-   macos-aarch64

**Tier 2**

These platform are tested on new releases.

-   linux-aarch64 - used for multi-arch docker images.

**Unsupported**

These platforms are not supported.

-   windows - if you're a developer using Windows, we recommend the [Windows
    Subsystem for Linux (WSL)](https://learn.microsoft.com/en-us/windows/wsl/).
-   macos-x86_64 - given the slowness of CI runners for this platform, we cannot
    provide builds for it.

## License

Copyright 2025 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
