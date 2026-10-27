package config

import (
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
)

// An Invalid relationship makes field.Validate reject every configuration with a
// bare "invalid relationship constraint", so the connector would not start at all.
func TestCredentialConstraintsAreWellFormed(t *testing.T) {
	var mutuallyExclusive, atLeastOne int

	for _, constraint := range Config.Constraints {
		if constraint.Kind == field.Invalid {
			t.Fatalf("constraint %+v is Invalid and will never be enforced", constraint)
		}

		if !constraintCovers(constraint, DatabricksClientSecretField, DatabricksTokenField, DatabricksTokenFileField) {
			continue
		}

		switch constraint.Kind {
		case field.MutuallyExclusive:
			mutuallyExclusive++
		case field.AtLeastOne:
			atLeastOne++
		case field.Invalid, field.RequiredTogether, field.Dependents:
		}
	}

	if mutuallyExclusive != 1 {
		t.Errorf("found %d mutually exclusive credential constraints, want 1", mutuallyExclusive)
	}
	if atLeastOne != 1 {
		t.Errorf("found %d at-least-one credential constraints, want 1", atLeastOne)
	}
}

func constraintCovers(constraint field.SchemaFieldRelationship, want ...field.SchemaField) bool {
	if len(constraint.Fields) != len(want) {
		return false
	}

	present := make(map[string]bool, len(constraint.Fields))
	for _, f := range constraint.Fields {
		present[f.FieldName] = true
	}

	for _, f := range want {
		if !present[f.FieldName] {
			return false
		}
	}

	return true
}

func TestCredentialValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Databricks
		wantErr string
	}{
		{
			name: "client secret alone",
			cfg:  Databricks{DatabricksClientSecret: "secret"},
		},
		{
			name: "workload identity token alone",
			cfg:  Databricks{DatabricksToken: "subject-jwt"},
		},
		{
			name: "workload identity token file alone",
			cfg:  Databricks{DatabricksTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a test fixture path, not a credential.
		},
		{
			name:    "no credential",
			cfg:     Databricks{},
			wantErr: "at least one",
		},
		{
			name:    "client secret and token together",
			cfg:     Databricks{DatabricksClientSecret: "secret", DatabricksToken: "subject-jwt"},
			wantErr: "mutually exclusive",
		},
		{
			name:    "token and token file together",
			cfg:     Databricks{DatabricksToken: "subject-jwt", DatabricksTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: test fixtures, not credentials.
			wantErr: "mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfg
			cfg.AccountId = "acc-1"
			cfg.DatabricksClientId = "sp-client-id"

			err := field.Validate(Config, &cfg)

			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("Validate returned %v, want no error", err)
			case tt.wantErr != "" && err == nil:
				t.Errorf("Validate succeeded, want an error containing %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(strings.ToLower(err.Error()), tt.wantErr):
				t.Errorf("Validate returned %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestClientSecretIsNotRequired(t *testing.T) {
	if DatabricksClientSecretField.Required {
		t.Error("databricks-client-secret is Required, which makes the mutually exclusive constraint Invalid")
	}
	if !DatabricksClientIdField.Required {
		t.Error("databricks-client-id must stay required: federation is per service principal")
	}
}
