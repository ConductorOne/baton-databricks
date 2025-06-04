package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-databricks/cmd/baton-databricks/config"
	configSchema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/conductorone/baton-databricks/pkg/connector"
	"github.com/conductorone/baton-databricks/pkg/databricks"
)

var (
	connectorName = "baton-databricks"
	version       = "dev"
)

func main() {
	ctx := context.Background()
	_, cmd, err := configSchema.DefineConfiguration(ctx,
		connectorName,
		getConnector,
		config.ConfigurationSchema,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func AreTokensSet(workspaces []string, tokens []string) bool {
	return len(tokens) > 0 &&
		len(workspaces) == len(tokens)
}

func prepareClientAuth(ctx context.Context, cfg *viper.Viper) databricks.Auth {
	l := ctxzap.Extract(ctx)

	accountID := cfg.GetString(config.AccountIdField.FieldName)
	databricksClientId := cfg.GetString(config.DatabricksClientIdField.FieldName)
	databricksClientSecret := cfg.GetString(config.DatabricksClientSecretField.FieldName)
	username := cfg.GetString(config.UsernameField.FieldName)
	password := cfg.GetString(config.PasswordField.FieldName)
	workspaces := cfg.GetStringSlice(config.WorkspacesField.FieldName)
	tokens := cfg.GetStringSlice(config.TokensField.FieldName)
	accountHostname := databricks.GetAccountHostname(databricks.GetHostname(cfg))

	switch {
	case username != "" && password != "":
		l.Info(
			"using basic auth",
			zap.String("account-id", accountID),
			zap.String("username", username),
		)
		cAuth := databricks.NewBasicAuth(
			username,
			password,
		)
		return cAuth
	case databricksClientId != "" && databricksClientSecret != "":
		l.Info(
			"using oauth",
			zap.String("account-id", accountID),
			zap.String("client-id", databricksClientId),
		)
		cAuth := databricks.NewOAuth2(
			accountID,
			databricksClientId,
			databricksClientSecret,
			accountHostname,
		)
		return cAuth
	case AreTokensSet(workspaces, tokens):
		l.Info(
			"using access token",
			zap.String("account-id", accountID),
		)
		cAuth := databricks.NewTokenAuth(
			workspaces,
			tokens,
		)
		return cAuth
	default:
		return &databricks.NoAuth{}
	}
}

func getConnector(ctx context.Context, cfg *viper.Viper) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	err := config.ValidateConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	hostname := databricks.GetHostname(cfg)
	accountHostname := databricks.GetAccountHostname(databricks.GetHostname(cfg))
	auth := prepareClientAuth(ctx, cfg)
	cb, err := connector.New(
		ctx,
		hostname,
		accountHostname,
		cfg.GetString(config.AccountIdField.FieldName),
		auth,
		cfg.GetStringSlice(config.WorkspacesField.FieldName),
	)
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
