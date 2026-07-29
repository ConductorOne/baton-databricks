package connector

import (
	"testing"

	"github.com/conductorone/baton-databricks/pkg/config"
)

// Regression for CXH-2165: with account-hostname unset the Azure/GCP hostname
// calculation must run. A non-empty field default here silently masks it.
func TestGetAccountHostname(t *testing.T) {
	cases := []struct {
		name            string
		accountHostname string
		hostname        string
		want            string
	}{
		{"explicit override honored", "accounts.custom.example.com", "cloud.databricks.com", "accounts.custom.example.com"},
		{"unset azure calculates", "", "myorg.azuredatabricks.net", "accounts.azuredatabricks.net"},
		{"unset gcp calculates", "", "myorg.gcp.databricks.com", "accounts.gcp.databricks.com"},
		{"unset aws calculates", "", "cloud.databricks.com", "accounts.cloud.databricks.com"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Databricks{AccountHostname: tc.accountHostname}
			if got := getAccountHostname(cfg, tc.hostname); got != tc.want {
				t.Errorf("getAccountHostname() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAccountHostnameFieldHasNoDefault(t *testing.T) {
	if v := config.AccountHostnameField.DefaultValue; v != nil && v != "" {
		t.Errorf("account-hostname field must have no default, got %q", v)
	}
}
