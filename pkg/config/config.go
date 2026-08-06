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
	DatabricksClientSecretField = field.StringField(
		"databricks-client-secret",
		field.WithDescription("The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API"),
		field.WithIsSecret(true),
		field.WithDisplayName("OAuth2 Client Secret"),
	)
	DatabricksTokenFileField = field.StringField(
		"databricks-token-file",
		field.WithDescription("Path to a file containing an external JWT for workload identity federation (e.g. a SPIFFE JWT-SVID or Kubernetes projected ServiceAccount token). The file is re-read on each token refresh to support credential rotation."),
		field.WithDisplayName("Federation Token File"),
	)
	DatabricksTokenField = field.StringField(
		"databricks-token",
		field.WithDescription("An external JWT for workload identity federation (RFC 8693 token exchange). Use --databricks-token-file for automatic credential rotation."),
		field.WithIsSecret(true),
		field.WithDisplayName("Federation Token"),
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
		DatabricksTokenFileField,
		DatabricksTokenField,
		HostnameField,
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
		field.FieldsMutuallyExclusive(DatabricksClientSecretField, DatabricksTokenFileField, DatabricksTokenField),
		field.FieldsAtLeastOneUsed(DatabricksClientSecretField, DatabricksTokenFileField, DatabricksTokenField),
	),
)
