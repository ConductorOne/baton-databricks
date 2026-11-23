package config

import (
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
)

// Validation skips every field the selected group does not list, so a group
// that forgets a required field silently stops checking it.
func TestEveryAuthGroupCarriesTheRequiredCommonFields(t *testing.T) {
	for _, group := range Config.FieldGroups {
		fields := group.FieldMap()
		for _, required := range []field.SchemaField{AccountIdField, DatabricksClientIdField} {
			if _, ok := fields[required.FieldName]; !ok {
				t.Errorf("group %q omits %q, so it would no longer be validated", group.Name, required.FieldName)
			}
		}
	}
}

func TestAuthGroupsOfferOneCredentialEach(t *testing.T) {
	var defaults []string
	for _, group := range Config.FieldGroups {
		if group.Default {
			defaults = append(defaults, group.Name)
		}
	}
	if len(defaults) != 1 || defaults[0] != "client-secret" {
		t.Errorf("default groups = %v, want exactly [client-secret]", defaults)
	}

	for _, tt := range []struct {
		group string
		holds string
		omits []string
	}{
		{"client-secret", DatabricksClientSecretField.FieldName, []string{DatabricksOIDCTokenFileField.FieldName, DatabricksC1TokenField.FieldName}},
		{"workload-identity-file", DatabricksOIDCTokenFileField.FieldName, []string{DatabricksClientSecretField.FieldName, DatabricksC1TokenField.FieldName}},
		{"workload-identity-c1", DatabricksC1TokenField.FieldName, []string{DatabricksClientSecretField.FieldName, DatabricksOIDCTokenFileField.FieldName}},
	} {
		fields := Config.FieldGroupFields(tt.group)
		if _, ok := fields[tt.holds]; !ok {
			t.Errorf("group %q does not offer %q", tt.group, tt.holds)
		}
		for _, omitted := range tt.omits {
			if _, ok := fields[omitted]; ok {
				t.Errorf("group %q also offers %q, so the credentials are not separated", tt.group, omitted)
			}
		}
	}
}

// A hosted connector has no file system the tenant admin controls, so the token
// file must never reach the web form.
func TestTheTokenFileIsNotOfferedToHostedTenants(t *testing.T) {
	if DatabricksOIDCTokenFileField.ExportTarget != field.ExportTargetCLIOnly {
		t.Errorf("databricks-oidc-token-file ExportTarget = %q, want %q",
			DatabricksOIDCTokenFileField.ExportTarget, field.ExportTargetCLIOnly)
	}
	for _, group := range Config.FieldGroups {
		if group.Name != "workload-identity-file" {
			continue
		}
		if group.ExportTarget != field.ExportTargetCLIOnly {
			t.Errorf("group %q ExportTarget = %q, want %q", group.Name, group.ExportTarget, field.ExportTargetCLIOnly)
		}
	}
}

func TestCredentialValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authMethod string
		cfg        Databricks
		wantErr    string
	}{
		{
			name:       "client secret under its own method",
			authMethod: "client-secret",
			cfg:        Databricks{DatabricksClientSecret: "secret"},
		},
		{
			name:       "client secret missing under its own method",
			authMethod: "client-secret",
			cfg:        Databricks{},
			wantErr:    "databricks-client-secret",
		},
		{
			name:       "token file under its own method",
			authMethod: "workload-identity-file",
			cfg:        Databricks{DatabricksOidcTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a test fixture path, not a credential.
		},
		{
			name:       "token file missing under its own method",
			authMethod: "workload-identity-file",
			cfg:        Databricks{},
			wantErr:    "databricks-oidc-token-file",
		},
		{
			// The secret belongs to the other group, so it is not asked for here.
			name:       "token file alone does not require the secret",
			authMethod: "workload-identity-file",
			cfg:        Databricks{DatabricksOidcTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a test fixture path, not a credential.
		},
		{
			name:    "no method selected falls back to the default group",
			cfg:     Databricks{},
			wantErr: "databricks-client-secret",
		},
		{
			// The JWT arrives through the hosted token source, not this field.
			name:       "hosted workload identity requires no stored credential",
			authMethod: "workload-identity-c1",
			cfg:        Databricks{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfg
			cfg.AccountId = "acc-1"
			cfg.DatabricksClientId = "sp-client-id"

			err := field.Validate(Config, &cfg, field.WithAuthMethod(tt.authMethod))

			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("Validate returned %v, want no error", err)
			case tt.wantErr != "" && err == nil:
				t.Errorf("Validate succeeded, want an error naming %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
				t.Errorf("Validate returned %q, want it to name %q", err, tt.wantErr)
			}
		})
	}
}

// The hosted runtime attaches its token source only for an OAuth2 field, so
// this field has to stay one. A plain string would leave the connector with
// no way to receive the JWT ConductorOne signs.
func TestHostedWorkloadIdentityFieldIsOAuth2(t *testing.T) {
	if DatabricksC1TokenField.ConnectorConfig.FieldType != field.OAuth2 {
		t.Errorf("databricks-c1-token field type = %q, want %q", DatabricksC1TokenField.ConnectorConfig.FieldType, field.OAuth2)
	}
	if DatabricksC1TokenField.Required {
		t.Error("databricks-c1-token must not be required: ConductorOne supplies the JWT, the field holds no value")
	}
}

func TestCredentialFieldRequirements(t *testing.T) {
	if !DatabricksClientSecretField.Required {
		t.Error("databricks-client-secret must be required: its group offers no other credential")
	}
	if !DatabricksOIDCTokenFileField.Required {
		t.Error("databricks-oidc-token-file must be required: its group offers no other credential")
	}
	if !DatabricksClientIdField.Required {
		t.Error("databricks-client-id must stay required: federation is per service principal")
	}
}
