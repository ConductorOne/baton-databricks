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

	// Account API requires account id AND SCIM token OR username and password
	AccountId string `mapstructure:"account-id"`

	// Workspace API requires at least one workspace AND access token OR username and password
	Workspaces []string `mapstructure:"workspaces"`

	SCIMToken string `mapstructure:"scim-token"`
	Token     string `mapstructure:"token"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

func (c *config) IsBasicAuth() bool {
	return c.Username != "" && c.Password != ""
}

func (c *config) IsTokenAuth() bool {
	return c.Token != ""
}

func (c *config) IsSCIMAuth() bool {
	return c.SCIMToken != ""
}

func (c *config) IsAccountAPI() bool {
	return c.AccountId != ""
}

func (c *config) IsWorkspaceAPI() bool {
	return len(c.Workspaces) > 0
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, cfg *config) error {
	if !cfg.IsAccountAPI() && !cfg.IsWorkspaceAPI() {
		return fmt.Errorf("either account ID and SCIM token or access token must be provided, use --help for more information")
	}

	if cfg.IsAccountAPI() {
		if !cfg.IsSCIMAuth() && !cfg.IsBasicAuth() {
			return fmt.Errorf("SCIM token or username and password must be provided, use --help for more information")
		}
	}

	if cfg.IsWorkspaceAPI() {
		if !cfg.IsTokenAuth() && !cfg.IsBasicAuth() {
			return fmt.Errorf("access token or username and password must be provided, use --help for more information")
		}
	}

	return nil
}

// cmdFlags sets the cmdFlags required for the connector.
func cmdFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("scim-token", "", "The Databricks SCIM token used to connect to the Databricks Account API. ($BATON_SCIM_TOKEN)")
	cmd.PersistentFlags().String("account-id", "", "The Databricks account ID used to connect to the Databricks Account API. ($BATON_ACCOUNT_ID)")
	cmd.PersistentFlags().String("token", "", "The Databricks access token used to connect to the Databricks Workspace API. ($BATON_TOKEN)")
	cmd.PersistentFlags().StringSlice("workspaces", []string{}, "Limit syncing to the specified workspaces. ($BATON_WORKSPACES)")
	cmd.PersistentFlags().String("username", "", "The Databricks username used to connect to the Databricks API. ($BATON_USERNAME)")
	cmd.PersistentFlags().String("password", "", "The Databricks password used to connect to the Databricks API. ($BATON_PASSWORD)")
	// TODO: support token per workspace slice:
	// cmd.PersistentFlags().StringSlice("tokens", []string{}, "The Databricks access tokens used to connect to the Databricks API. ($BATON_TOKENS)")
}
