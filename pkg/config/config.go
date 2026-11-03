package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

// Auth method names. The connector branches on the selected one, so the group
// definitions and that branch read the same constant.
const (
	ClientSecretAuthMethod         = "client-secret"
	WorkloadIdentityFileAuthMethod = "workload-identity-file"
	// WorkloadIdentityC1AuthMethod is the group a ConductorOne-hosted connector
	// selects. The name is the value ConductorOne sends as auth-method.
	WorkloadIdentityC1AuthMethod = "workload-identity-c1"
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
	// DatabricksC1TokenField is what makes a hosted runtime attach its token
	// source: the source is attached only when the selected group contains an
	// OAuth2 field. ConductorOne supplies the JWT through that source, so the
	// field holds nothing an operator sets and is not required.
	DatabricksC1TokenField = field.Oauth2Field(
		"databricks-c1-token",
		field.WithDescription("JWT ConductorOne issues for a hosted connector. The hosted runtime supplies it; leave it unset anywhere else."),
		field.WithDisplayName("ConductorOne token"),
		field.WithHidden(true),
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
	// SQLWarehouseIDField doubles as the incremental-sync switch: set enables it, empty disables it.
	// system.access.audit only has workspace-level events from the warehouse's region, so the
	// warehouse must share a region with the workspaces incremental sync should cover.
	SQLWarehouseIDField = field.StringField(
		"sql-warehouse-id",
		field.WithDescription(
			"Setting this enables incremental sync; leaving it empty disables it. "+
				"ID of the Databricks SQL warehouse used to query the system.access.audit log between full syncs, "+
				"so access changes show up before the next full sync (deletions are still only caught by full syncs). "+
				"The warehouse can be in any workspace in the same cloud region as the workspaces to update incrementally; "+
				"Databricks only exposes workspace-level audit events for that region. The connector discovers the warehouse's workspace automatically.",
		),
		field.WithDisplayName("SQL Warehouse ID"),
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
		SQLWarehouseIDField,
	}

	// The client secret is the default. The ConductorOne group is for a hosted
	// connector, which receives a JWT from ConductorOne instead of a secret.
	// The token file is for a connector you run yourself, and is read again on
	// every exchange, so a rotated file keeps working without a restart.
	fieldGroups = []field.SchemaFieldGroup{
		{
			Name:        ClientSecretAuthMethod,
			DisplayName: "Client Secret",
			HelpText:    "Authenticate with the Databricks service principal's OAuth client ID and client secret.",
			Default:     true,
			Fields:      append(append([]field.SchemaField{}, commonFields...), DatabricksClientSecretField),
		},
		{
			Name:        WorkloadIdentityC1AuthMethod,
			DisplayName: "Workload identity (ConductorOne)",
			HelpText: "For a ConductorOne-hosted connector. ConductorOne issues a short-lived JWT for this connector, " +
				"and Databricks exchanges it for an access token through a service principal federation policy. " +
				"The token's audience is the OAuth client ID.",
			Fields: append(append([]field.SchemaField{}, commonFields...), DatabricksC1TokenField),
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
		DatabricksC1TokenField,
		DatabricksOIDCTokenFileField,
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
	field.WithFieldGroups(fieldGroups),
	field.WithConstraints(
		field.FieldsMutuallyExclusive(WorkspacesField, ExcludeWorkspacesField),
	),
)
