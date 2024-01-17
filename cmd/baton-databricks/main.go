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

// Config defines the external configuration required for the connector to run.
// You can run this connector in multiple modes, Account API mode or Workspace API mode.
// If you specify workspaces, connector will try to sync resources in those workspaces as well.
func prepareClientConfigs(ctx context.Context, cfg *config) []databricks.Config {
	var cv []databricks.Config
	var cAuth databricks.Auth
	l := ctxzap.Extract(ctx)

	// Account API configs
	if cfg.IsBasicAuth() {
		l.Info("using basic auth", zap.String("account-id", cfg.AccountId), zap.String("username", cfg.Username))
		cAuth = databricks.NewBasicAuth(cfg.Username, cfg.Password)
		c := databricks.NewAccountConfig(cfg.AccountId, cAuth)
		cv = append(cv, c)
	} else if cfg.IsOauth() {
		l.Info("using oauth", zap.String("account-id", cfg.AccountId), zap.String("client-id", cfg.DatabricksClientId))
		cAuth = databricks.NewOAuth2(cfg.AccountId, cfg.DatabricksClientId, cfg.DatabricksClientSecret)
		c := databricks.NewAccountConfig(cfg.AccountId, cAuth)
		cv = append(cv, c)
	}

	// Workspace API configs
	if cfg.AreWorkspacesSet() {
		for _, workspace := range cfg.Workspaces {
			c := databricks.NewWorkspaceConfig(workspace, cfg.AccountId, cAuth)
			cv = append(cv, c)
		}
	}

	return cv
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)
	dc := prepareClientConfigs(ctx, cfg)
	cb, err := connector.New(ctx, cfg.AccountId, dc)
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
