package config

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
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
		field.WithDisplayName("OAuth2 Client ID"),
	)
	DatabricksClientSecretField = field.StringField(
		"databricks-client-secret",
		field.WithDescription("The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API"),
		field.WithIsSecret(true),
		field.WithDisplayName("OAuth2 Client Secret"),
	)
	WorkspacesField = field.StringSliceField(
		"workspaces",
		field.WithDescription("Limit syncing to the specified workspaces. Required when using workspace tokens."),
		field.WithDisplayName("Workspaces"),
	)
	WorkspaceTokensField = field.StringSliceField(
		"workspace-tokens",
		field.WithDescription("The Databricks personal access tokens scoped to specific workspaces used to connect to the Databricks Workspace API"),
		field.WithIsSecret(true),
		field.WithDisplayName("Workspace Tokens"),
	)
	AccountHostnameField = field.StringField(
		"account-hostname",
		field.WithDefaultValue("accounts.cloud.databricks.com"),
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
	configFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
		HostnameField,
		WorkspacesField,
		WorkspaceTokensField,
		BaseURLField,
	}
	fieldRelationships = []field.SchemaFieldRelationship{
		field.FieldsAtLeastOneUsed(
			DatabricksClientIdField,
			WorkspaceTokensField,
		),
		field.FieldsMutuallyExclusive(
			DatabricksClientIdField,
			WorkspaceTokensField,
		),
		field.FieldsRequiredTogether(
			DatabricksClientIdField,
			DatabricksClientSecretField,
		),
		field.FieldsDependentOn(
			[]field.SchemaField{WorkspaceTokensField},
			[]field.SchemaField{WorkspacesField},
		),
	}
)

//go:generate go run ./gen
var Config = field.NewConfiguration(
	configFields,
	field.WithConstraints(fieldRelationships...),
	field.WithConnectorDisplayName("Databricks"),
	field.WithHelpUrl("/docs/baton/databricks"),
	field.WithIconUrl("/static/app-icons/databricks.svg"),
)

// ValidateConfig checks constraints that the field relationships can't express: a
// workspace token must be paired with the workspace it belongs to.
func ValidateConfig(ctx context.Context, cfg *Databricks) error {
	workspaces := cfg.Workspaces
	tokens := cfg.WorkspaceTokens

	if len(tokens) > 0 && len(workspaces) != len(tokens) {
		return fmt.Errorf(
			"databricks-connector: workspaces and workspace-tokens must be the same length, got %d workspaces and %d tokens",
			len(workspaces),
			len(tokens),
		)
	}

	return nil
}
