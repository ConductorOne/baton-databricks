package connector

import (
	"testing"

	"github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// The token file is what selects workload identity federation. The static JWT
// field is gone, so a configuration without a file authenticates with the
// client secret.
func TestPrepareClientAuthSelectsTheCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       config.Databricks
		federated bool
	}{
		{
			name: "client secret",
			cfg:  config.Databricks{DatabricksClientSecret: "secret"},
		},
		{
			name:      "token file",
			cfg:       config.Databricks{DatabricksOidcTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a fixture path, not a credential.
			federated: true,
		},
		{
			// The file wins: a secret left over from an earlier configuration
			// must not silently take the federated deployment back to it.
			name:      "both",
			cfg:       config.Databricks{DatabricksClientSecret: "secret", DatabricksOidcTokenFile: "/var/run/secrets/token"}, //nolint:gosec // G101: a fixture path, not a credential.
			federated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfg
			cfg.AccountId = "acc-1"
			cfg.DatabricksClientId = "sp-client-id"
			cfg.Hostname = "cloud.databricks.com"

			auth := prepareClientAuth(&cfg)
			_, federated := auth.(*databricks.TokenFederation)
			if federated != tt.federated {
				t.Errorf("prepareClientAuth returned %T, federated = %v, want %v", auth, federated, tt.federated)
			}
		})
	}
}
