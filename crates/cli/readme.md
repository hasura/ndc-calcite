# ndc-calcite-cli

ndc-calcite-cli is used to configure a deployment of ndc-calcite.
It is intended to be automatically downloaded and invoked via the Hasura CLI, as a plugin.

## Create a configuration

Create a configuration in a new directory using the following commands:

1. Initialize a configuration:

   ```sh
   CONNECTION_URI='<calcite-connection-string>' cargo run --bin ndc-calcite-cli -- --context='<directory>' initialize
   ```

2. Update the configuration by introspecting the database:

   ```sh
   CONNECTION_URI='<calcite-connection-string>' cargo run --bin ndc-calcite-cli -- --context='<directory>' update
   ```

## Upgrade a configuration

An older configuration version (e.g. "v3") can be upgraded to a newer one using the `upgrade` command.

```sh
cargo run --bin ndc-calcite-cli -- upgrade --dir-from <DIR_FROM> --dir-to <DIR_TO>
```

## Native Operations

Native Operations can be listed, added, and deleted using ndc-calcite-cli:

```sh
$ CONNECTION_URI='<calcite-connection-string>' cargo run --bin ndc-calcite-cli -- --context='<directory>' native-operation --help
Commands on Native Operations

Usage: hasura-ndc-calcite native-operation <COMMAND>

Commands:
  list    List the existing Native Operations
  create  Create a new Native Operation from a SQL file
  delete  Delete an existing Native Operation from the configuration
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```
