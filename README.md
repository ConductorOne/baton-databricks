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

Another requirement is to have valid credentials to run the connector with. This
will decide how connector will be executed. You can use either the OAuth client
credentials flow or the Bearer auth flow. OAuth can be used across account and
all workspaces you have access to. Bearer auth can be used only for a specific
workspace.

To use the OAuth, you need to create a service principal and add OAuth secret
(client id and secret) to it. You can do that by going to the user management
tab and clicking on the Service Principals tab. Then click on the Add Service
principal button and name it. You then need to add OAuth secret to it by
clicking on the Generate secret button. You can use this secret to authenticate
across all workspaces that service principal has access to. This requires admin
access to the Databricks account and each workspace you want to sync.

To use bearer auth, you need to provide a Databricks workspace access token. You
can create a new token by logging into the workspace and going into user
settings. Then go to Developer tab and create a new access token. This will try
to work with only specified workspaces and their respective tokens. You can
provide multiple tokens by separating them with a comma. This method requires
admin access to each workspace you want to sync.

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
docker run --rm -v $(pwd):/out -e BATON_ACCOUNT_ID=account_id BATON_DATABRICKS_CLIENT_ID=client_id BATON_DATABRICKS_CLIENT_SECRET=client_secret ghcr.io/conductorone/baton-databricks:latest -f "/out/sync.c1z"
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

By default, connector will fetch all resources from the account and all
workspaces. You can limit the scope of the sync by providing a list of
workspaces to sync with. You can do that by providing a comma-separated list of
workspace hostnames to the `--workspaces` flag. You can also provide a list of
workspace access tokens to the `--workspace-tokens` flag. This will limit the
sync to only workspaces that are associated with those tokens. You can also use
both flags at the same time. If you do that, connector will sync with all
workspaces that are associated with provided tokens and all workspaces that are
in the list of workspaces.

When authenticating with `--workspace-tokens` instead of the OAuth client ID and
secret, also pass `--auth-method workspace-token` (or set
`BATON_AUTH_METHOD=workspace-token`), otherwise the connector validates against
the OAuth fields by default and rejects the config.

To instead exclude specific workspaces from the sync, pass them to the
`--databricks-exclude-workspaces` flag (or the
`BATON_DATABRICKS_EXCLUDE_WORKSPACES` environment variable) as a comma-separated
list. Each entry can be a workspace name, deployment name, or numeric workspace
ID. Excluded workspaces and their roles are skipped entirely.

## Incremental sync

By default, `baton-databricks` does a full resync of every resource on every run.
You can opt into an additional, cheap pathway that polls a Databricks audit log
between full syncs to pick up access changes early, by setting
`--enable-incremental-sync` (or `BATON_ENABLE_INCREMENTAL_SYNC`). Full syncs
still run as the correctness backstop; incremental sync does not detect
deletions, which are only caught by the next full sync.

Incremental sync requires:

- `--sql-warehouse-id` (or `BATON_SQL_WAREHOUSE_ID`), the ID of a Databricks SQL
  warehouse the connector can use to query the `system.access.audit` table. A
  small serverless warehouse is recommended to minimize cold-start latency.
- `--sql-warehouse-workspace` (or `BATON_SQL_WAREHOUSE_WORKSPACE`), the
  deployment name of the workspace that hosts that SQL warehouse. SQL
  warehouses only exist in one workspace, so this is required whenever more
  than one workspace is available; with only one workspace it's inferred
  automatically.
- A one-time setup performed by a Databricks admin, which the connector cannot
  do on its own:
  - An account admin must [enable the `access` system
    schema](https://docs.databricks.com/en/admin/system-tables/index.html) for
    the account's Unity Catalog metastore.
  - A metastore admin must grant `SELECT` on `system.access` to the service
    principal or user the connector authenticates as.

Once enabled, ongoing polling only needs that `SELECT` grant plus warehouse
access; no further elevated privilege is required.

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
      --databricks-exclude-workspaces strings            Workspaces to exclude from sync, identified by workspace name, deployment name, or numeric workspace ID. Mutually exclusive with workspaces. ($BATON_DATABRICKS_EXCLUDE_WORKSPACES)
      --enable-incremental-sync                          Poll a Databricks audit-log event feed between full syncs to pick up access changes early. Deletions are still only caught by the next full sync. ($BATON_ENABLE_INCREMENTAL_SYNC)
      --external-resource-c1z string                     The path to the c1z file to sync external baton resources with ($BATON_EXTERNAL_RESOURCE_C1Z)
      --external-resource-entitlement-id-filter string   The entitlement that external users, groups must have access to sync external baton resources ($BATON_EXTERNAL_RESOURCE_ENTITLEMENT_ID_FILTER)
      --external-resource-traits strings                 Resource type traits (e.g. "user", "group", "app") to sync and match from the external resource c1z. When unset the matcher falls back to user and group; passing this flag replaces the full set rather than adding to it. ($BATON_EXTERNAL_RESOURCE_TRAITS)
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
      --sql-warehouse-id string                          ID of the Databricks SQL warehouse used to query system.access.audit. Required when incremental sync is enabled. ($BATON_SQL_WAREHOUSE_ID)
      --sql-warehouse-workspace string                   Deployment name of the workspace that hosts the SQL warehouse (sql-warehouse-id), since SQL warehouses only exist in one workspace. Required when incremental sync is enabled and more than one workspace is available; if omitted with only one workspace available, that workspace is used automatically. ($BATON_SQL_WAREHOUSE_WORKSPACE)
      --storage-engine string                            The storage engine to use when opening the sync c1z file: sqlite or pebble. Leave unset to use the baton-sdk default. ($BATON_STORAGE_ENGINE)
      --sync-resource-types strings                      The resource type IDs to sync ($BATON_SYNC_RESOURCE_TYPES)
      --sync-resources strings                           The resource IDs to sync ($BATON_SYNC_RESOURCES)
      --task-concurrency int                             The number of Baton tasks to run concurrently in service mode. Tasks may include sync, grant, revoke, and more. Minimum value is 1, maximum value is 100. ($BATON_TASK_CONCURRENCY) (default 3)
      --ticketing                                        This must be set to enable ticketing support ($BATON_TICKETING)
  -v, --version                                          version for baton-databricks
      --workers int                                      The number of sync workers to use. -1 for auto-detect, 0 for sequential, >0 for parallel ($BATON_WORKERS)
      --workspace-tokens strings                         required: The Databricks personal access tokens scoped to specific workspaces used to connect to the Databricks Workspace API ($BATON_WORKSPACE_TOKENS)
      --workspaces strings                               Limit syncing to the specified workspaces, by deployment name, not workspace ID. Required when using workspace tokens, in the same order as workspace-tokens. Mutually exclusive with databricks-exclude-workspaces. ($BATON_WORKSPACES)

Use "baton-databricks [command] --help" for more information about a command.
```
