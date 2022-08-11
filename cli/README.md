# Apibara CLI

The Apibara CLI is used to manage Apibara nodes.


## Extending the CLI with plugins

The CLI can be extended with plugins, using any programming language.
Developers create an extension `foo` by creating a new executable `apibara-foo`
and making sure it's available in `$PATH`.

For example, the command `apibara my-chain start` is delegated to the
`apibara-my-chain` executable, passing the `start` argument to it.

Developers can use this extension mechanism to add new functionality to Apibara
while keeping the familiar developer experience.
