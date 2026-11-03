package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

// Auth method names. The connector branches on the selected one, so the group
// definitions and that branch read the same constant.
const (
	ClientSecretAuthMethod         = "client-secret"
	WorkloadIdentityFileAuthMethod = "workload-identity-file"
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
	DatabricksOIDCTokenFileField = field.StringField(
		"databricks-oidc-token-file",
		field.WithDescription("Path to a file holding an externally issued JWT identifying the workload. "+
			"The file is read again on every token refresh, so an external rotation is picked up without a restart."),
		field.WithDisplayName("Workload Identity Token File"),
		field.WithRequired(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
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
	// commonFields apply whichever way the connector authenticates. Validation
	// skips any field the selected group does not list, so every group repeats
	// them: account-id is required and would stop being checked otherwise.
	commonFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		HostnameField,
		WorkspacesField,
		BaseURLField,
		ExcludeWorkspacesField,
	}

	// The client secret is the default. The token file exchanges an externally
	// issued JWT, and is read again on every exchange, so a rotated file keeps
	// working without a restart.
	fieldGroups = []field.SchemaFieldGroup{
		{
			Name:        ClientSecretAuthMethod,
			DisplayName: "Client Secret",
			HelpText:    "Authenticate with the Databricks service principal's OAuth client ID and client secret.",
			Default:     true,
			Fields:      append(append([]field.SchemaField{}, commonFields...), DatabricksClientSecretField),
		},
		{
			Name:        WorkloadIdentityFileAuthMethod,
			DisplayName: "Workload Identity (token file)",
			HelpText: "Exchange an externally issued JWT for a Databricks token. " +
				"The JWT is read from a file on every exchange, so whatever writes that file controls the lifetime.",
			ExportTarget: field.ExportTargetCLIOnly,
			Fields:       append(append([]field.SchemaField{}, commonFields...), DatabricksOIDCTokenFileField),
		},
	}

	configFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
		DatabricksOIDCTokenFileField,
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
	field.WithFieldGroups(fieldGroups),
	field.WithConstraints(
		field.FieldsMutuallyExclusive(WorkspacesField, ExcludeWorkspacesField),
	),
)
