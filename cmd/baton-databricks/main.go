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

func prepareClientAuth(ctx context.Context, cfg *config) databricks.Auth {
	l := ctxzap.Extract(ctx)

	switch {
	case cfg.IsBasicAuth():
		l.Info("using basic auth", zap.String("account-id", cfg.AccountId), zap.String("username", cfg.Username))
		cAuth := databricks.NewBasicAuth(cfg.Username, cfg.Password)
		return cAuth
	case cfg.IsOauth():
		l.Info("using oauth", zap.String("account-id", cfg.AccountId), zap.String("client-id", cfg.DatabricksClientId))
		cAuth := databricks.NewOAuth2(cfg.AccountId, cfg.DatabricksClientId, cfg.DatabricksClientSecret)
		return cAuth
	case cfg.AreTokensSet():
		l.Info("using access token", zap.String("account-id", cfg.AccountId))
		cAuth := databricks.NewTokenAuth(cfg.Workspaces, cfg.Tokens)
		return cAuth
	default:
		return &databricks.NoAuth{}
	}
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)
	auth := prepareClientAuth(ctx, cfg)
	cb, err := connector.New(ctx, cfg.AccountId, auth, cfg.Workspaces)
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
