package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

var (
	AccountIdField = field.StringField(
		"account-id",
		field.WithDescription("The Databricks account ID used to connect to the Databricks Account and Workspace API"),
		field.WithRequired(true),
	)
	DatabricksClientIdField = field.StringField(
		"databricks-client-id",
		field.WithDescription("The Databricks service principal's client ID used to connect to the Databricks Account and Workspace API"),
	)
	DatabricksClientSecretField = field.StringField(
		"databricks-client-secret",
		field.WithDescription("The Databricks service principal's client secret used to connect to the Databricks Account and Workspace API"),
	)
	UsernameField = field.StringField(
		"username",
		field.WithDescription("The Databricks username used to connect to the Databricks API"),
	)
	PasswordField = field.StringField(
		"password",
		field.WithDescription("The Databricks password used to connect to the Databricks API"),
	)
	WorkspacesField = field.StringSliceField(
		"workspaces",
		field.WithDescription("Limit syncing to the specified workspaces"),
	)
	TokensField = field.StringSliceField(
		"workspace-tokens",
		field.WithDescription("The Databricks access tokens scoped to specific workspaces used to connect to the Databricks Workspace API"),
	)
	configurationFields = []field.SchemaField{
		AccountIdField,
		DatabricksClientIdField,
		DatabricksClientSecretField,
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
)

// validateConfig - additional validations that cannot be encoded in relationships (yet!)
func validateConfig(ctx context.Context, cfg *viper.Viper) error {
	workspaces := cfg.GetStringSlice(WorkspacesField.FieldName)
	tokens := cfg.GetStringSlice(TokensField.FieldName)

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
