package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-databricks/pkg/connector"
	"github.com/conductorone/baton-databricks/pkg/databricks"
)

var version = "dev"

func main() {
	ctx := context.Background()

	cfg := &config{}
	cmd, err := cli.NewCmd(ctx, "baton-databricks", cfg, validateConfig, getConnector)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version
	cmdFlags(cmd)

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// config defines the external configuration required for the connector to run.
// you can run this connector in multiple modes:
// 1. Account API mode: you provide a SCIM token and an account ID, and the connector will use the Account API to fetch resources.
// 2. Workspace API mode: you provide an access token and a list of workspaces, and the connector will use the Workspace API to fetch resources.
// TODO 3. Both?
func prepareClientConfigs(cfg *config) []databricks.Config {
	if cfg.IsSCIMAuth() {
		// SCIM token is only available to use with the Account API
		fmt.Printf("using account API mode\n\taccount ID: %s\n\tSCIM token: %s\n", cfg.AccountId, cfg.SCIMToken)

		cAuth := databricks.NewTokenAuth(cfg.SCIMToken)
		fmt.Printf("auth: %+v\n", cAuth)

		c := databricks.NewAccountConfig(cfg.AccountId, cAuth)
		fmt.Printf("config: %+v\n", c)

		return []databricks.Config{c}
	}

	if cfg.IsTokenAuth() {
		fmt.Printf("using workspace API mode\n\taccess token: %s\n\tworkspaces: %v\n", cfg.Token, cfg.Workspaces)

		var cv []databricks.Config

		// HERE we're expecting user to have one token over multiple workspaces
		// TODO: validate if this is possible
		cAuth := databricks.NewTokenAuth(cfg.Token)

		for _, workspace := range cfg.Workspaces {
			c := databricks.NewWorkspaceConfig(workspace, cAuth)

			cv = append(cv, c)
		}

		return cv
	}

	if cfg.IsBasicAuth() {
		var cv []databricks.Config
		cAuth := databricks.NewBasicAuth(cfg.Username, cfg.Password)

		if cfg.IsAccountAPI() {
			fmt.Printf("using account API mode\n\taccount ID: %s\n", cfg.AccountId)
			c := databricks.NewAccountConfig(cfg.AccountId, cAuth)
			cv = append(cv, c)
		}

		if cfg.IsWorkspaceAPI() {
			fmt.Printf("using workspace API mode\n\tworkspaces: %v\n", cfg.Workspaces)

			for _, workspace := range cfg.Workspaces {
				c := databricks.NewWorkspaceConfig(workspace, cAuth)
				cv = append(cv, c)
			}
		}

		return cv
	}

	return nil
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)
	dc := prepareClientConfigs(cfg)

	cb, err := connector.New(ctx, dc)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	c, err := connectorbuilder.NewConnector(ctx, cb)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return c, nil
}
