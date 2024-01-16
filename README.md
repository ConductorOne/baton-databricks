![Baton Logo](./docs/images/baton-logo.png)

# `baton-databricks` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-databricks.svg)](https://pkg.go.dev/github.com/conductorone/baton-databricks) ![main ci](https://github.com/conductorone/baton-databricks/actions/workflows/main.yaml/badge.svg)

`baton-databricks` is a connector for Databricks built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It communicates with the Databricks API, to sync data about Databricks identities (users, groups and service principals), roles and workspaces. 

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

To work with the connector, you can choose from multiple ways to run it, but the main requirement is to have a Databricks account and its ID. You can find the ID of an account, after you log into account platform and click on your username in right top corner that will open a dropdown menu with the account ID along other options.

Another requirement is to have valid credentials to run the connector with. You can use either basic auth (username and password).

To get more data about workspaces, you will need to provide a Databricks workspace access token for each workspace you want to sync. You can create a new token by logging into the workspace and going into user settings. Then go to Developer tab and create a new token. You can also use a basic auth across all workspaces you have access to, but this is not recommended as the token is more secure and must be scoped to specific workspaces.

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

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

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
      --account-id string          The Databricks account ID used to connect to the Databricks Account and Workspace API. ($BATON_ACCOUNT_ID)
      --client-id string           The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string       The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
  -f, --file string                The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                       help for baton-databricks
      --log-format string          The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string           The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --password string            The Databricks password used to connect to the Databricks API. ($BATON_PASSWORD)
  -p, --provisioning               This must be set in order for provisioning actions to be enabled. ($BATON_PROVISIONING)
      --username string            The Databricks username used to connect to the Databricks API. ($BATON_USERNAME)
  -v, --version                    version for baton-databricks
      --workspace-tokens strings   The Databricks access tokens scoped to specific workspaces used to connect to the Databricks Workspace API. ($BATON_WORKSPACE_TOKENS)
      --workspaces strings         Limit syncing to the specified workspaces. ($BATON_WORKSPACES)

Use "baton-databricks [command] --help" for more information about a command.
```
