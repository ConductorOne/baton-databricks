package connector

import (
	"testing"

	"github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// The selected auth method decides the credential. A field left over from
// another deployment must not change it.
func TestPrepareClientAuthSelectsTheCredential(t *testing.T) {
	t.Parallel()

	const bothSet = "both fields set"

	tests := []struct {
		name       string
		authMethod string
		cfg        config.Databricks
		federated  bool
	}{
		{
			name:       "client secret",
			authMethod: config.ClientSecretAuthMethod,
			cfg:        config.Databricks{DatabricksClientSecret: "secret"},
		},
		{
			name:       "token file",
			authMethod: config.WorkloadIdentityFileAuthMethod,
			cfg:        config.Databricks{DatabricksOidcTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a fixture path, not a credential.
			federated:  true,
		},
		{
			name:       bothSet + " under the client secret method",
			authMethod: config.ClientSecretAuthMethod,
			cfg:        config.Databricks{DatabricksClientSecret: "secret", DatabricksOidcTokenFile: "/leftover/token"},
		},
		{
			name:       bothSet + " under the token file method",
			authMethod: config.WorkloadIdentityFileAuthMethod,
			cfg:        config.Databricks{DatabricksClientSecret: "secret", DatabricksOidcTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a fixture path, not a credential.
			federated:  true,
		},
		{
			// No method selected falls back to the default group, which
			// validation has already checked for a client secret.
			name: "no method selected",
			cfg:  config.Databricks{DatabricksClientSecret: "secret", DatabricksOidcTokenFile: "/leftover/token"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfg
			cfg.AccountId = "acc-1"
			cfg.DatabricksClientId = "sp-client-id"
			cfg.Hostname = "cloud.databricks.com"

			auth := prepareClientAuth(&cfg, tt.authMethod)
			_, federated := auth.(*databricks.TokenFederation)
			if federated != tt.federated {
				t.Errorf("prepareClientAuth returned %T, federated = %v, want %v", auth, federated, tt.federated)
			}
		})
	}
}
