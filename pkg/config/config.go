package config

import (
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
		field.WithRequired(true),
	)
	// Not required: FieldsMutuallyExclusive rejects a Required field.
	DatabricksClientSecretField = field.StringField(
		"databricks-client-secret",
		field.WithDescription("The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API"),
		field.WithIsSecret(true),
		field.WithDisplayName("OAuth2 Client Secret"),
	)
	DatabricksTokenField = field.StringField(
		"databricks-token",
		field.WithDescription("An externally issued JWT identifying the workload, exchanged for a Databricks OAuth token. The connector does not refresh it, so the sync fails once the JWT expires."),
		field.WithIsSecret(true),
		field.WithDisplayName("Workload Identity Token"),
	)
	DatabricksTokenFileField = field.StringField(
		"databricks-token-file",
		field.WithDescription("Path to a file holding an externally issued JWT identifying the workload. The file must exist where the connector runs, and it is read again on every token refresh."),
		field.WithDisplayName("Workload Identity Token File"),
	)
	WorkspacesField = field.StringSliceField(
		"workspaces",
		field.WithDescription(
			"Limit syncing to the specified workspaces, identified by workspace name, deployment name, or numeric workspace ID. "+
				"Mutually exclusive with databricks-exclude-workspaces.",
		),
		field.WithDisplayName("Workspaces"),
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
		field.WithDescription("Workspaces to exclude from sync, identified by workspace name, deployment name, or numeric workspace ID. Mutually exclusive with workspaces."),
		field.WithDisplayName("Exclude Workspaces"),
	)
	configFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
		DatabricksTokenField,
		DatabricksTokenFileField,
		HostnameField,
		WorkspacesField,
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
	field.WithConstraints(
		field.FieldsMutuallyExclusive(WorkspacesField, ExcludeWorkspacesField),
		field.FieldsMutuallyExclusive(DatabricksClientSecretField, DatabricksTokenField, DatabricksTokenFileField),
		field.FieldsAtLeastOneUsed(DatabricksClientSecretField, DatabricksTokenField, DatabricksTokenFileField),
	),
)
