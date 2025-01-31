![Baton Logo](./docs/images/baton-logo.png)

# `baton-databricks` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-databricks.svg)](https://pkg.go.dev/github.com/conductorone/baton-databricks) ![main ci](https://github.com/conductorone/baton-databricks/actions/workflows/main.yaml/badge.svg)

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
will decide how connector will be executed. You can use either OAuth client 
credentials flow or Basic auth flow (username and password) or Bearer auth flow. 
Both OAuth and Basic can be used across account and all workspaces you have 
access to. Bearer auth can be used only for a specific workspace.

To use the OAuth, you need to create a service principal and add OAuth secret 
(client id and secret) to it. You can do that by going to the user management 
tab and clicking on the Service Principals tab. Then click on the Add Service 
principal button and name it. You then need to add OAuth secret to it by 
clicking on the Generate secret button. You can use this secret to authenticate 
across all workspaces that service principal has access to. To use basic auth, 
you just need to provide a username and password of a user that has access to 
the Databricks API. Both methods require admin access to the Databricks account 
and each workspace you want to sync.

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

BATON_ACCOUNT_ID=account_id BATON_USERNAME=username BATON_PASSWORD=password baton-databricks
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_ACCOUNT_ID=account_id BATON_USERNAME=username BATON_PASSWORD=password ghcr.io/conductorone/baton-databricks:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-databricks/cmd/baton-databricks@main

BATON_ACCOUNT_ID=account_id BATON_USERNAME=username BATON_PASSWORD=password baton-databricks
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
  help               Help about any command

Flags:
      --account-hostname string           The hostname used to connect to the Databricks account API ($BATON_ACCOUNT_HOSTNAME) (default "accounts.cloud.databricks.com")
      --account-id string                 required: The Databricks account ID used to connect to the Databricks Account and Workspace API ($BATON_ACCOUNT_ID)
      --client-id string                  The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string              The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --databricks-client-id string       The Databricks service principal's client ID used to connect to the Databricks Account and Workspace API ($BATON_DATABRICKS_CLIENT_ID)
      --databricks-client-secret string   The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API ($BATON_DATABRICKS_CLIENT_SECRET)
  -f, --file string                       The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                              help for baton-databricks
      --hostname string                   The Databricks hostname used to connect to the Databricks API ($BATON_HOSTNAME) (default "cloud.databricks.com")
      --log-format string                 The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string                  The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --password string                   The Databricks password used to connect to the Databricks API ($BATON_PASSWORD)
  -p, --provisioning                      This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
      --skip-full-sync                    This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
      --ticketing                         This must be set to enable ticketing support ($BATON_TICKETING)
      --username string                   The Databricks username used to connect to the Databricks API ($BATON_USERNAME)
  -v, --version                           version for baton-databricks
      --workspace-tokens strings          The Databricks access tokens scoped to specific workspaces used to connect to the Databricks Workspace API ($BATON_WORKSPACE_TOKENS)
      --workspaces strings                Limit syncing to the specified workspaces ($BATON_WORKSPACES)

Use "baton-databricks [command] --help" for more information about a command.
```
