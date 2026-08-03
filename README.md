![Baton Logo](./docs/images/baton-logo.png)

# `baton-databricks` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-databricks.svg)](https://pkg.go.dev/github.com/conductorone/baton-databricks) ![ci](https://github.com/conductorone/baton-databricks/actions/workflows/ci.yaml/badge.svg) ![verify](https://github.com/conductorone/baton-databricks/actions/workflows/verify.yaml/badge.svg)

`baton-databricks` is a connector for Databricks built using the
[Baton SDK](https://github.com/conductorone/baton-sdk). It communicates with the
Databricks API, to sync data about Databricks identities (users, groups and
service principals), roles and workspaces.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

To work with the connector, you can choose from multiple ways to run it, but the
main requirement is to have a Databricks account and its ID. You can find the ID
of an account, after you log into account platform and click on your username in
right top corner that will open a dropdown menu with the account ID along other
options.

Another requirement is to have valid credentials to run the connector with. The
connector authenticates with an OAuth service principal using the client
credentials flow, which works across the account and every workspace the service
principal has access to.

To set this up, create a service principal and add an OAuth secret (client ID
and secret) to it. Go to the user management tab, click the Service Principals
tab, click Add Service principal, and name it. Then add an OAuth secret to it by
clicking Generate secret. Use this client ID and secret to authenticate. The
service principal needs admin access to the Databricks account and each
workspace you want to sync.

# Using Azure Databricks

To work with Azure Databricks, you need to provide the hostname flag.

```bash
baton-databricks --hostname "azuredatabricks.net"
```

# Getting Started

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-databricks

BATON_ACCOUNT_ID=account_id BATON_DATABRICKS_CLIENT_ID=client_id BATON_DATABRICKS_CLIENT_SECRET=client_secret baton-databricks
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_ACCOUNT_ID=account_id -e BATON_DATABRICKS_CLIENT_ID=client_id -e BATON_DATABRICKS_CLIENT_SECRET=client_secret ghcr.io/conductorone/baton-databricks:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-databricks/cmd/baton-databricks@main

BATON_ACCOUNT_ID=account_id BATON_DATABRICKS_CLIENT_ID=client_id BATON_DATABRICKS_CLIENT_SECRET=client_secret baton-databricks
baton resources
```

# Data Model

`baton-databricks` will fetch information about the following Databricks resources:

- Account
- Workspaces
- Groups
- Service Principals
- Users
- Roles

By default, the connector fetches all resources from the account and every
workspace the service principal can access. To exclude specific workspaces from
the sync, pass them to the `--databricks-exclude-workspaces` flag (or the
`BATON_DATABRICKS_EXCLUDE_WORKSPACES` environment variable) as a comma-separated
list. Each entry can be a workspace name, deployment name, or numeric workspace
ID. Excluded workspaces and their roles are skipped entirely.

## Group povisioning limitations
provisioning of account groups from a workspace token is not supported, if you need to provision groups you can only do it using the client-id and client-secret flow,
this is due to the fact that the Databricks API does not allow provisioning of groups from a workspace token.
[here](https://docs.databricks.com/aws/en/admin/users-groups/groups#:~:text=Types%20of%20groups%20in%20Databricks,permissions%20to%20identity%20federated%20workspaces.) are the different types of groups in Databricks

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually
building spreadsheets. We welcome contributions, and ideas, no matter how
small&mdash;our goal is to make identity and permissions sprawl less painful for
everyone. If you have questions, problems, or ideas: Please open a GitHub Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-databricks` Command Line Usage

```
baton-databricks

Usage:
  baton-databricks [flags]
  baton-databricks [command]

Available Commands:
  capabilities       Get connector capabilities
  completion         Generate the autocompletion script for the specified shell
  config             Get the connector config schema
  health-check       Check the health of a running connector
  help               Help about any command

Flags:
      --account-hostname string                          The hostname used to connect to the Databricks account API. If not set, it will be calculated from the hostname field. ($BATON_ACCOUNT_HOSTNAME)
      --account-id string                                required: The Databricks account ID used to connect to the Databricks Account and Workspace API ($BATON_ACCOUNT_ID)
      --auth-method string                               ($BATON_AUTH_METHOD)
      --client-id string                                 The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string                             The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --databricks-client-id string                      required: The Databricks service principal's client ID used to connect to the Databricks Account and Workspace API ($BATON_DATABRICKS_CLIENT_ID)
      --databricks-client-secret string                  required: The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API ($BATON_DATABRICKS_CLIENT_SECRET)
      --databricks-exclude-workspaces strings            Workspaces to exclude from sync, identified by workspace name, deployment name, or numeric workspace ID ($BATON_DATABRICKS_EXCLUDE_WORKSPACES)
      --external-resource-c1z string                     The path to the c1z file to sync external baton resources with ($BATON_EXTERNAL_RESOURCE_C1Z)
      --external-resource-entitlement-id-filter string   The entitlement that external users, groups must have access to sync external baton resources ($BATON_EXTERNAL_RESOURCE_ENTITLEMENT_ID_FILTER)
  -f, --file string                                      The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
      --health-check                                     Enable the HTTP health check endpoint ($BATON_HEALTH_CHECK)
      --health-check-port int                            Port for the HTTP health check endpoint ($BATON_HEALTH_CHECK_PORT) (default 8081)
  -h, --help                                             help for baton-databricks
      --hostname string                                  The Databricks hostname used to connect to the Databricks API ($BATON_HOSTNAME) (default "cloud.databricks.com")
      --http-timeout-seconds int                         HTTP client timeout in seconds (max 1800) ($BATON_HTTP_TIMEOUT_SECONDS) (default 300)
      --keep-previous-sync-c1z                           Keep the previously synced c1z on disk to enable ETag replay across service-mode syncs (requires a connector that supports ETag replay; costs one c1z of local disk) ($BATON_KEEP_PREVIOUS_SYNC_C1Z)
      --log-format string                                The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string                                 The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --log-level-debug-expires-at string                The timestamp indicating when debug-level logging should expire ($BATON_LOG_LEVEL_DEBUG_EXPIRES_AT)
      --log-path strings                                 The file path to write logs to ($BATON_LOG_PATH)
      --otel-collector-endpoint string                   The endpoint of the OpenTelemetry collector to send observability data to (used for both tracing and logging if specific endpoints are not provided) ($BATON_OTEL_COLLECTOR_ENDPOINT)
      --parallel-sync                                    Deprecated: use --workers instead. ($BATON_PARALLEL_SYNC)
  -p, --provisioning                                     This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
      --skip-entitlements-and-grants                     This must be set to skip syncing of entitlements and grants ($BATON_SKIP_ENTITLEMENTS_AND_GRANTS)
      --skip-full-sync                                   This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
      --storage-engine string                            The storage engine to use when opening the sync c1z file: sqlite or pebble. Leave unset to use the baton-sdk default. ($BATON_STORAGE_ENGINE)
      --sync-resource-types strings                      The resource type IDs to sync ($BATON_SYNC_RESOURCE_TYPES)
      --sync-resources strings                           The resource IDs to sync ($BATON_SYNC_RESOURCES)
      --task-concurrency int                             The number of Baton tasks to run concurrently in service mode. Tasks may include sync, grant, revoke, and more. Minimum value is 1, maximum value is 100. ($BATON_TASK_CONCURRENCY) (default 3)
      --ticketing                                        This must be set to enable ticketing support ($BATON_TICKETING)
  -v, --version                                          version for baton-databricks
      --workers int                                      The number of sync workers to use. -1 for auto-detect, 0 for sequential, >0 for parallel ($BATON_WORKERS)

Use "baton-databricks [command] --help" for more information about a command.
```
