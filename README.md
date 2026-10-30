![Baton Logo](./docs/images/baton-logo.png)

# `baton-databricks` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-databricks.svg)](https://pkg.go.dev/github.com/conductorone/baton-databricks) ![ci](https://github.com/conductorone/baton-databricks/actions/workflows/ci.yaml/badge.svg) ![verify](https://github.com/conductorone/baton-databricks/actions/workflows/verify.yaml/badge.svg)

`baton-databricks` is a connector for Databricks built using the
[Baton SDK](https://github.com/conductorone/baton-sdk). It communicates with the
Databricks API, to sync data about Databricks identities (users, groups and
service principals), roles, workspaces and Unity Catalog metastores.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

To work with the connector, you can choose from multiple ways to run it, but the
main requirement is to have a Databricks account and its ID. You can find the ID
of an account, after you log into account platform and click on your username in
right top corner that will open a dropdown menu with the account ID along other
options.

Another requirement is to have valid credentials to run the connector with. The
connector authenticates with the OAuth client credentials flow, using an
account-level service principal that works across the account and every
workspace it has access to.

To set this up, create a service principal and add an OAuth secret (client ID
and secret) to it. You can do that by going to the user management tab and
clicking on the Service Principals tab. Then click on the Add Service principal
button and name it. You then need to add an OAuth secret to it by clicking on
the Generate secret button. You can use this secret to authenticate across all
workspaces that service principal has access to. This requires admin access to
the Databricks account and each workspace you want to sync.

Syncing Unity Catalog needs more than account admin on its own: the service
principal also has to be able to read the privileges recorded on each metastore.
See [Unity Catalog permissions](#unity-catalog-permissions) for the exact
requirement.

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
docker run --rm -v $(pwd):/out -e BATON_ACCOUNT_ID=account_id BATON_DATABRICKS_CLIENT_ID=client_id BATON_DATABRICKS_CLIENT_SECRET=client_secret public.ecr.aws/conductorone/baton-databricks:latest -f "/out/sync.c1z"
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
- Metastores

The connector fetches all resources from the account and every workspace the
service principal can access. To limit the scope, pass a comma-separated list of
workspaces to the `--workspaces` flag. Each entry can be a workspace name,
deployment name, or numeric workspace ID, matched case-insensitively.

`--workspaces` also scopes which metastores sync: only a metastore attached to a
listed workspace is synced, so narrowing the list removes the other metastores
and their grants from ConductorOne. A list that names no workspace on the
account is rejected during credential validation, before any sync runs.

Metastores are Unity Catalog securables and are opt-in: the type stays off until
it is enabled for the connector in ConductorOne.

## Authentication

OAuth is the only authentication method: an account-level service principal's
client ID and secret, supplied through `--databricks-client-id` and
`--databricks-client-secret`. There is no personal access token (PAT) or
workspace-token option, and no username/password option.

OAuth requires a reachable account API. If the account API check fails at
startup, the connector fails validation rather than falling back to a
workspace-only sync, even when `--workspaces` is set.

To instead exclude specific workspaces from the sync, pass them to the
`--databricks-exclude-workspaces` flag (or the
`BATON_DATABRICKS_EXCLUDE_WORKSPACES` environment variable) as a comma-separated
list. Each entry can be a workspace name, deployment name, or numeric workspace
ID. Excluded workspaces and their roles are skipped entirely. A metastore
attached only to excluded workspaces is not synced, and its grants are removed
from ConductorOne.

## Incremental sync

By default, `baton-databricks` does a full resync of every resource on every run.
You can opt into an additional, cheap pathway that polls a Databricks audit log
between full syncs to pick up access changes early. Full syncs still run as the
correctness backstop. Incremental sync picks up added and removed group
memberships, but deleted users, groups and service principals are only caught
by the next full sync.

Role assignments that Databricks stores in access-control rule sets (group roles
such as `roles/group.manager`, service principal roles, and the account
`marketplace.admin` role) are also only caught by the next full sync: they are
logged under the `accountsAccessControl` audit service, which the connector does
not query.

**Incremental sync is enabled by setting `--sql-warehouse-id` (or
`BATON_SQL_WAREHOUSE_ID`) and disabled by leaving it unset or empty.** There is
no separate on/off flag. The incremental-sync setup (warehouse found,
`system.access.audit` queryable, at least one workspace in sync scope) is not
checked at startup, so a misconfiguration never blocks full syncs or
provisioning. Instead, each event-feed poll fails with an error naming the
problem until it is fixed.

Incremental sync requires a reachable Account API to resolve audit events back
to synced resources, same as the rest of the connector. Audit events are only
resolved for workspaces in sync scope, so `--workspaces` and
`--databricks-exclude-workspaces` apply to incremental sync the same way they
apply to full syncs.

### Requirements

Incremental sync works only when all of the following are true:

- **Unity Catalog.** The SQL warehouse is in a workspace enabled for Unity
  Catalog, and the metastore is on Unity Catalog Privilege Model Version 1.0
  ([system tables
  requirements](https://docs.databricks.com/aws/en/admin/system-tables/)).
- **System tables.** The connector queries the `system.access.audit` system
  table, which Databricks currently offers as a Public Preview.
- **A SQL warehouse, preferably serverless.** Pass its ID with
  `--sql-warehouse-id`. Any warehouse works, including the **Starter
  Warehouse** Databricks creates in each workspace, so you don't need to create
  one.
- **Grants for the service principal** the connector authenticates as, on top of
  the Account admin role it already needs:
  - `USE CATALOG` on the `system` catalog
  - `USE SCHEMA` and `SELECT` on the `system.access` schema
  - `CAN USE` on the SQL warehouse

  For example, run as a metastore admin, replacing `<application-id>` with the
  service principal's application (client) ID:

  ```sql
  GRANT USE CATALOG ON CATALOG system TO `<application-id>`;
  GRANT USE SCHEMA, SELECT ON SCHEMA system.access TO `<application-id>`;
  ```

**We recommend a serverless SQL warehouse.** It starts in seconds, while a pro
or classic warehouse can take several minutes. Each poll waits at most about
5.5 minutes for its statement, including warehouse startup: the statement
request waits up to 30 seconds, then the connector polls for up to 5 more
minutes. A poll that runs out of time fails with a `DeadlineExceeded` error and
is retried on the next poll. Serverless SQL warehouses require the Premium plan
or above.

**Incremental sync covers one cloud region.** `system.access.audit` holds
workspace-level events only for workspaces in the same cloud region as the
workspace that queries it; account-level events are global ([system tables
reference](https://docs.databricks.com/aws/en/admin/system-tables/)). The
connector finds the warehouse's workspace automatically, searching every
workspace except those excluded with `--databricks-exclude-workspaces`, so the
warehouse cannot live in an excluded workspace. It receives
incremental updates only for workspaces in that region. Workspaces in other
regions are still covered by full syncs.

Audit log entries are delivered with a delay, so the connector reads them about
four hours after they happen.

## Group provisioning

Account groups are provisioned through the OAuth client ID and secret flow.
[Here](https://docs.databricks.com/aws/en/admin/users-groups/groups#:~:text=Types%20of%20groups%20in%20Databricks,permissions%20to%20identity%20federated%20workspaces.)
are the different types of groups in Databricks.

## Role provisioning

The account admin role and workspace entitlements (`workspace-access`,
`databricks-sql-access`, `allow-cluster-create`, `allow-instance-pool-create`)
can be granted and revoked.

A workspace entitlement can only be granted to a principal that is already
assigned to that workspace. Otherwise the grant fails with a
`FailedPrecondition` error; grant the workspace membership first.

Revoking workspace membership deletes the principal's whole workspace
assignment, so a principal that is also a workspace admin loses admin access
to that workspace too. This also applies to groups: revoking workspace
membership from a group removes the group's entire workspace assignment,
including admin access if the group is a workspace admin, so its members lose
it.

## Unity Catalog

A metastore is the top of the Databricks Unity Catalog hierarchy. The resource
tree the connector syncs is:

```
account
├── workspace
│   └── role
├── user
├── group
├── service principal
└── metastore
```

A metastore exposes its privileges as entitlements (`CREATE_CATALOG`,
`READ_METADATA`, `USE_SHARE` and so on) plus a non-grantable `owner`. A
privilege held by a group is expandable, so ConductorOne resolves the group's
members as holders.

`MANAGE` and `ALL_PRIVILEGES` are not offered: Databricks rejects both at the
metastore level.

A metastore lives on the account plane, but its privileges are read through a
workspace attached to it. The connector picks that workspace itself: the first
running one in scope where it can read every privilege, which it proves by
owning the metastore or by holding Admin directly on that workspace. Grant and
Revoke resolve the same route from the metastore id.

A metastore syncs through a running workspace in scope. The sync fails, naming
the metastore, while an attached workspace in scope is still provisioning, or
while the metastore's workspace list cannot be read.

## Unity Catalog provisioning

A metastore privilege can be granted to and revoked from a user, group or
service principal. The `owner` entitlement is read-only: granting or revoking it
fails with an `InvalidArgument` error.

## Unity Catalog permissions

The service principal must own the metastore, or be assigned **Admin** directly
on at least one running workspace attached to it. Admin inherited only through a
group is not enough. Otherwise the sync fails naming the metastore.

The connector requests the `all-apis` OAuth scope. Each resource type declares
the scopes, roles and privileges it needs, so `./baton-databricks capabilities`
reports them per type.

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
      --sql-warehouse-id string                          Setting this enables incremental sync; leaving it empty disables it. ID of the Databricks SQL warehouse used to query the system.access.audit log between full syncs, so access changes show up before the next full sync (deletions are still only caught by full syncs). The warehouse can be in any workspace in the same cloud region as the workspaces to update incrementally; Databricks only exposes workspace-level audit events for that region. The connector discovers the warehouse's workspace automatically. ($BATON_SQL_WAREHOUSE_ID)
      --storage-engine string                            The storage engine to use when opening the sync c1z file: sqlite or pebble. Defaults to pebble when unset. ($BATON_STORAGE_ENGINE)
      --sync-resource-types strings                      The resource type IDs to sync ($BATON_SYNC_RESOURCE_TYPES)
      --sync-resources strings                           The resource IDs to sync ($BATON_SYNC_RESOURCES)
      --task-concurrency int                             The number of Baton tasks to run concurrently in service mode. Tasks may include sync, grant, revoke, and more. Minimum value is 1, maximum value is 100. ($BATON_TASK_CONCURRENCY) (default 3)
      --ticketing                                        This must be set to enable ticketing support ($BATON_TICKETING)
  -v, --version                                          version for baton-databricks
      --workers int                                      The number of sync workers to use. -1 for auto-detect, 0 for sequential, >0 for parallel ($BATON_WORKERS)
      --workspaces strings                               Limit syncing to the specified workspaces, identified by workspace name, deployment name, or numeric workspace ID. Mutually exclusive with databricks-exclude-workspaces. ($BATON_WORKSPACES)

Use "baton-databricks [command] --help" for more information about a command.
```
