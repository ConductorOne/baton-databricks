package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/spf13/cobra"
)

// config defines the external configuration required for the connector to run.
type config struct {
	cli.BaseConfig `mapstructure:",squash"` // Puts the base config options in the same place as the connector options

	AccountId              string   `mapstructure:"account-id"`
	DatabricksClientId     string   `mapstructure:"databricks-client-id"`
	DatabricksClientSecret string   `mapstructure:"databricks-client-secret"`
	Username               string   `mapstructure:"username"`
	Password               string   `mapstructure:"password"`
	Workspaces             []string `mapstructure:"workspaces"`
	Tokens                 []string `mapstructure:"workspace-tokens"`
}

func (c *config) IsBasicAuth() bool {
	return c.Username != "" && c.Password != ""
}

func (c *config) IsOauth() bool {
	return c.DatabricksClientId != "" && c.DatabricksClientSecret != ""
}

func (c *config) AreTokensSet() bool {
	return (len(c.Tokens) > 0) && (len(c.Workspaces) == len(c.Tokens))
}

func (c *config) IsAuthReady() bool {
	return c.AreTokensSet() || c.IsOauth() || c.IsBasicAuth()
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, cfg *config) error {
	if cfg.AccountId == "" {
		return fmt.Errorf("account ID must be provided, use --help for more information")
	}

	if !cfg.IsAuthReady() {
		return fmt.Errorf("either access token along workspaces or username and password or client id and client secret must be provided, use --help for more information")
	}

	return nil
}

// cmdFlags sets the cmdFlags required for the connector.
func cmdFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("account-id", "", "The Databricks account ID used to connect to the Databricks Account and Workspace API. ($BATON_ACCOUNT_ID)")
	cmd.PersistentFlags().String(
		"databricks-client-id",
		"",
		"The Databricks service principal's client ID used to connect to the Databricks Account and Workspace API. ($BATON_DATABRICKS_CLIENT_ID)",
	)
	cmd.PersistentFlags().String(
		"databricks-client-secret",
		"",
		"The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API. ($BATON_DATABRICKS_CLIENT_SECRET)",
	)
	cmd.PersistentFlags().String("username", "", "The Databricks username used to connect to the Databricks API. ($BATON_USERNAME)")
	cmd.PersistentFlags().String("password", "", "The Databricks password used to connect to the Databricks API. ($BATON_PASSWORD)")
	cmd.PersistentFlags().StringSlice("workspaces", []string{}, "Limit syncing to the specified workspaces. ($BATON_WORKSPACES)")
	cmd.PersistentFlags().StringSlice(
		"workspace-tokens",
		[]string{},
		"The Databricks access tokens scoped to specific workspaces used to connect to the Databricks Workspace API. ($BATON_WORKSPACE_TOKENS)",
	)
}
