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
		field.WithDescription(
			"Limit syncing to the specified workspaces, by deployment name, not workspace ID. "+
				"Required when using workspace tokens, in the same order as workspace-tokens. "+
				"Mutually exclusive with databricks-exclude-workspaces.",
		),
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
		field.WithDescription("Workspaces to exclude from sync, identified by workspace name, deployment name, or numeric workspace ID. Mutually exclusive with workspaces."),
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
	field.WithConstraints(
		field.FieldsMutuallyExclusive(WorkspacesField, ExcludeWorkspacesField),
		field.FieldsDependentOn([]field.SchemaField{WorkspaceTokensField}, []field.SchemaField{WorkspacesField}),
	),
	field.WithFieldGroups([]field.SchemaFieldGroup{
		{
			Name:        DatabricksOAuth2Group,
			DisplayName: "OAuth2",
			HelpText:    "Authenticate as a service principal using an OAuth2 client ID and secret.",
			Fields: []field.SchemaField{
				AccountIdField, DatabricksClientIdField, DatabricksClientSecretField,
				HostnameField, AccountHostnameField, WorkspacesField, ExcludeWorkspacesField,
			},
			Default: true,
		},
		// TEMPORARILY DISABLED — workspace-token auth is not offerable through the C1 UI.
		//
		// Two defects make the hosted path unusable, neither of them in this connector's
		// auth implementation:
		//
		//  1. workspace-tokens is declared isSecret, but c1 has no secret bit for
		//     string-list fields, so the PAT is stored unencrypted and rendered in clear
		//     text. StringField secrets (databricks-client-secret) mask correctly on the
		//     same form; StringSliceField secrets do not. Tracked as CXE-1374.
		//  2. workspace-tokens is DependentOn workspaces, and workspaces renders as
		//     "(optional)", so selecting this group shows no token input at all until the
		//     user happens to commit a value in an optional field. There is no affordance
		//     telling them to.
		//
		// SCOPE — this disables workspace-token auth EVERYWHERE, not only in the C1 UI.
		//
		// With this group commented out, oauth2 is the only group left and it is
		// Default: true, so its required fields (databricks-client-id and
		// databricks-client-secret) now apply unconditionally. Passing
		// --auth-method workspace-token on the CLI fails config validation with
		// "field databricks-client-id ... is marked as required but it has a zero-value"
		// before any API call. Verified, not assumed.
		//
		// The flags and pkg/databricks/auth.go's NewTokenAuth path still exist and still
		// compile — they are simply unreachable, because no group offers them. So this is
		// a BREAKING CHANGE for any self-hosted deployment currently authenticating with
		// workspace tokens; they must move to OAuth2 or stay on the previous version.
		//
		// Restore this block once CXE-1374 ships. Do NOT delete it, and do NOT remove the
		// PAT documentation: this connector already lost PAT once in e84a1aef with the docs
		// left in place, and that mismatch is exactly what CXH-2166 was filed to fix.
		//
		// {
		// 	Name:        DatabricksWorkspaceTokenGroup,
		// 	DisplayName: "Workspace token",
		// 	HelpText: "Authenticate with a personal access token scoped to each workspace. " +
		// 		"Does not sync account-level data (account entitlements and grants, and " +
		// 		"workspace-membership entitlements); use OAuth for full account coverage.",
		// 	Fields:  []field.SchemaField{AccountIdField, WorkspacesField, WorkspaceTokensField, HostnameField, AccountHostnameField},
		// 	Default: false,
		// },
	}),
)

// ValidateConfig enforces what field groups can't: OAuth/token exclusion when no
// auth method is set, and equal-length workspaces/workspace-tokens.
func ValidateConfig(ctx context.Context, cfg *Databricks, authMethod string) error {
	// A merged/stored config can carry both groups' fields; once authMethod picks one,
	// prepareClientAuth only reads that group, so the other group's leftovers are inert.
	if authMethod == "" && len(cfg.WorkspaceTokens) > 0 && (cfg.DatabricksClientId != "" || cfg.DatabricksClientSecret != "") {
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
