package config

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	AccountHostnameField = field.StringField(
		"account-hostname",
		field.WithDescription("The hostname used to connect to the Databricks account API. If not set, it will be calculated from the hostname field."),
		field.WithDisplayName("Account Hostname"),
	)
	AccountIdField = field.StringField(
		"account-id",
		field.WithDescription("The Databricks account ID used to connect to the Databricks Account and Workspace API"),
		field.WithRequired(true),
		field.WithDisplayName("Account ID"),
	)
	HostnameField = field.StringField(
		"hostname",
		field.WithDescription("The Databricks hostname used to connect to the Databricks API"),
		field.WithDefaultValue("cloud.databricks.com"),
		field.WithDisplayName("Hostname"),
	)
	DatabricksClientIdField = field.StringField(
		"databricks-client-id",
		field.WithDescription("The Databricks service principal's client ID used to connect to the Databricks Account and Workspace API"),
		field.WithDisplayName("Databricks Client ID"),
	)
	DatabricksClientSecretField = field.StringField(
		"databricks-client-secret",
		field.WithDescription("The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API"),
		field.WithIsSecret(true),
		field.WithDisplayName("Databricks Client Secret"),
	)
	UsernameField = field.StringField(
		"username",
		field.WithDescription("The Databricks username used to connect to the Databricks API"),
		field.WithDisplayName("Username"),
	)
	PasswordField = field.StringField(
		"password",
		field.WithDescription("The Databricks password used to connect to the Databricks API"),
		field.WithIsSecret(true),
		field.WithDisplayName("Password"),
	)
	WorkspacesField = field.StringSliceField(
		"workspaces",
		field.WithDescription("Limit syncing to the specified workspaces"),
		field.WithDisplayName("Workspaces"),
	)
	TokensField = field.StringSliceField(
		"workspace-tokens",
		field.WithDescription("The Databricks access tokens scoped to specific workspaces used to connect to the Databricks Workspace API"),
		field.WithIsSecret(true),
		field.WithDisplayName("Workspace Tokens"),
	)
	configFields = []field.SchemaField{
		AccountHostnameField,
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
		HostnameField,
		PasswordField,
		TokensField,
		UsernameField,
		WorkspacesField,
	}
	fieldRelationships = []field.SchemaFieldRelationship{
		field.FieldsAtLeastOneUsed(
			DatabricksClientIdField,
			UsernameField,
			TokensField,
		),
		field.FieldsMutuallyExclusive(
			DatabricksClientIdField,
			UsernameField,
			TokensField,
		),
		field.FieldsRequiredTogether(
			DatabricksClientIdField,
			DatabricksClientSecretField,
		),
		field.FieldsRequiredTogether(
			UsernameField,
			PasswordField,
		),
		field.FieldsDependentOn(
			[]field.SchemaField{TokensField},
			[]field.SchemaField{WorkspacesField},
		),
	}
	fieldGroups = []field.SchemaFieldGroup{
		{
			Name:        "oauth_auth",
			DisplayName: "OAuth Authentication",
			HelpText: "Authenticate using OAuth to sync information from all Databricks workspaces. " +
				"Requires OAuth client ID and secret created following the Databricks OAuth authentication documentation.",
			Fields: []field.SchemaField{
				AccountIdField,
				DatabricksClientIdField,
				DatabricksClientSecretField,
				HostnameField,
				AccountHostnameField,
			},
			Default: true,
		},
		{
			Name:        "personal_access_token_auth",
			DisplayName: "Personal Access Token Authentication",
			HelpText:    "Authenticate using a personal access token to sync information from a single Databricks workspace. Requires a personal access token and the workspace ID.",
			Fields: []field.SchemaField{
				AccountIdField,
				TokensField,
				WorkspacesField,
				HostnameField,
				AccountHostnameField,
			},
			Default: false,
		},
		{
			Name:        "username_password_auth",
			DisplayName: "Username/Password Authentication",
			HelpText:    "Authenticate using your Databricks username and password to sync information from all Databricks workspaces.",
			Fields: []field.SchemaField{
				AccountIdField,
				UsernameField,
				PasswordField,
				HostnameField,
				AccountHostnameField,
			},
			Default: false,
		},
	}
)

//go:generate go run ./gen
var Config = field.NewConfiguration(
	configFields,
	field.WithConstraints(fieldRelationships...),
	field.WithFieldGroups(fieldGroups),
	field.WithConnectorDisplayName("Databricks"),
	field.WithHelpUrl("/docs/baton/databricks"),
	field.WithIconUrl("/static/app-icons/databricks.svg"),
)

// ValidateConfig - additional validations that cannot be encoded in relationships (yet!)
func ValidateConfig(ctx context.Context, cfg *Databricks) error {
	workspaces := cfg.Workspaces
	tokens := cfg.WorkspaceTokens

	// If there are tokens, there must be an equivalent number of workspaces.
	if len(tokens) > 0 && len(workspaces) != len(tokens) {
		return fmt.Errorf(
			"comma-separated list of workspaces and tokens must be the same length. Received %d workspaces and %d tokens",
			len(workspaces),
			len(tokens),
		)
	}

	return nil
}
