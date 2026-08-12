package config

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
)

const (
	DatabricksOAuth2Group         = "oauth2"
	DatabricksWorkspaceTokenGroup = "workspace-token"
)

var (
	AccountIdField = field.StringField(
		"account-id",
		field.WithDescription("The Databricks account ID used to connect to the Databricks Account and Workspace API"),
		field.WithRequired(true),
		field.WithDisplayName("Account ID"),
	)
	DatabricksClientIdField = field.StringField(
		"databricks-client-id",
		field.WithDescription("The Databricks service principal's client ID used to connect to the Databricks Account and Workspace API"),
		field.WithRequired(true),
		field.WithDisplayName("OAuth2 Client ID"),
	)
	DatabricksClientSecretField = field.StringField(
		"databricks-client-secret",
		field.WithDescription("The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API"),
		field.WithIsSecret(true),
		field.WithRequired(true),
		field.WithDisplayName("OAuth2 Client Secret"),
	)
	WorkspacesField = field.StringSliceField(
		"workspaces",
		field.WithDescription("Limit syncing to the specified workspaces, by deployment name, not workspace ID. Required when using workspace tokens, in the same order as workspace-tokens."),
		field.WithDisplayName("Workspaces"),
	)
	WorkspaceTokensField = field.StringSliceField(
		"workspace-tokens",
		field.WithDescription("The Databricks personal access tokens scoped to specific workspaces used to connect to the Databricks Workspace API"),
		field.WithIsSecret(true),
		field.WithRequired(true),
		field.WithDisplayName("Workspace Tokens"),
	)
	AccountHostnameField = field.StringField(
		"account-hostname",
		field.WithDescription("The hostname used to connect to the Databricks account API. If not set, it will be calculated from the hostname field."),
		field.WithDisplayName("Account Hostname"),
	)
	HostnameField = field.StringField(
		"hostname",
		field.WithDescription("The Databricks hostname used to connect to the Databricks API"),
		field.WithDefaultValue("cloud.databricks.com"),
		field.WithDisplayName("Hostname"),
	)
	BaseURLField = field.StringField(
		"base-url",
		field.WithDescription("Override the Databricks API URL (for testing)"),
		field.WithHidden(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)
	ExcludeWorkspacesField = field.StringSliceField(
		"databricks-exclude-workspaces",
		field.WithDescription("Workspaces to exclude from sync, identified by workspace name, deployment name, or numeric workspace ID"),
		field.WithDisplayName("Exclude Workspaces"),
	)
	configFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
		HostnameField,
		WorkspacesField,
		WorkspaceTokensField,
		BaseURLField,
		ExcludeWorkspacesField,
	}
)

//go:generate go run ./gen
var Config = field.NewConfiguration(
	configFields,
	field.WithConnectorDisplayName("Databricks"),
	field.WithHelpUrl("/docs/baton/databricks"),
	field.WithIconUrl("/static/app-icons/databricks.svg"),
	field.WithFieldGroups([]field.SchemaFieldGroup{
		{
			Name:        DatabricksOAuth2Group,
			DisplayName: "OAuth2",
			HelpText:    "Authenticate as a service principal using an OAuth2 client ID and secret.",
			Fields: []field.SchemaField{
				AccountIdField, DatabricksClientIdField, DatabricksClientSecretField,
				HostnameField, AccountHostnameField, WorkspacesField, BaseURLField, ExcludeWorkspacesField,
			},
			Default: true,
		},
		{
			Name:        DatabricksWorkspaceTokenGroup,
			DisplayName: "Workspace token",
			HelpText:    "Authenticate with a personal access token scoped to each workspace.",
			Fields:      []field.SchemaField{AccountIdField, WorkspacesField, WorkspaceTokensField, HostnameField, AccountHostnameField, BaseURLField, ExcludeWorkspacesField},
			Default:     false,
		},
	}),
)

// ValidateConfig checks constraints that field groups can't express: OAuth2 and
// workspace-token credentials are mutually exclusive, and a workspace token
// must be paired with the workspace it belongs to when workspace-token auth
// is the selected auth method.
func ValidateConfig(ctx context.Context, cfg *Databricks, authMethod string) error {
	if len(cfg.WorkspaceTokens) > 0 && (cfg.DatabricksClientId != "" || cfg.DatabricksClientSecret != "") {
		return fmt.Errorf("databricks-connector: databricks-client-id/databricks-client-secret and workspace-tokens are mutually exclusive")
	}

	if authMethod == DatabricksWorkspaceTokenGroup && len(cfg.Workspaces) != len(cfg.WorkspaceTokens) {
		return fmt.Errorf(
			"databricks-connector: workspaces and workspace-tokens must be the same length, got %d workspaces and %d tokens",
			len(cfg.Workspaces),
			len(cfg.WorkspaceTokens),
		)
	}

	return nil
}
