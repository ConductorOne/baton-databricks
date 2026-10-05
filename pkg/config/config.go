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
		field.WithRequired(true),
		field.WithDisplayName("OAuth2 Client Secret"),
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
	// SQLWarehouseIDField doubles as the incremental-sync switch: set enables it, empty disables it.
	// system.access.audit is account-wide, so this warehouse can be in any workspace in the
	// account — its workspace is just query compute, not a data scope.
	SQLWarehouseIDField = field.StringField(
		"sql-warehouse-id",
		field.WithDescription(
			"Setting this enables incremental sync; leaving it empty disables it. "+
				"ID of the Databricks SQL warehouse used to query the system.access.audit log between full syncs, "+
				"so access changes show up before the next full sync (deletions are still only caught by full syncs). "+
				"The warehouse can live in any workspace; the connector discovers which one automatically.",
		),
		field.WithDisplayName("SQL Warehouse ID"),
	)
	configFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
		HostnameField,
		WorkspacesField,
		BaseURLField,
		ExcludeWorkspacesField,
		SQLWarehouseIDField,
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
	),
)
