package connector

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// Regression for CXH-2165: with account-hostname unset the Azure/GCP hostname
// calculation must run. A non-empty field default here silently masks it.
func TestGetAccountHostname(t *testing.T) {
	cases := []struct {
		name            string
		accountHostname string
		hostname        string
		want            string
		wantErr         bool
	}{
		{"explicit override honored", "accounts.custom.example.com", "cloud.databricks.com", "accounts.custom.example.com", false},
		{"unset azure calculates", "", "myorg.azuredatabricks.net", "accounts.azuredatabricks.net", false},
		{"unset gcp calculates", "", "myorg.gcp.databricks.com", "accounts.gcp.databricks.com", false},
		{"unset aws calculates", "", "cloud.databricks.com", "accounts.cloud.databricks.com", false},
		{"explicit valid host used verbatim", "accounts.custom.example.com", "cloud.databricks.com", "accounts.custom.example.com", false},
		{"explicit workspace host rejected", "cloud.databricks.com", "cloud.databricks.com", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Databricks{AccountHostname: tc.accountHostname}
			got, err := getAccountHostname(cfg, tc.hostname)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("getAccountHostname() error = nil, want ErrInvalidAccountHostname")
				}
				if !errors.Is(err, databricks.ErrInvalidAccountHostname) {
					t.Errorf("getAccountHostname() error = %v, want errors.Is(ErrInvalidAccountHostname)", err)
				}
				if !strings.Contains(err.Error(), "account-hostname") {
					t.Errorf("getAccountHostname() error %q does not mention account-hostname", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("getAccountHostname() error = %v, want nil", err)
			}
			if got != tc.want {
				t.Errorf("getAccountHostname() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNewConnectorRejectsWorkspaceAccountHostname is a construction-level,
// no-network regression for CXH-2569: a workspace host in account-hostname
// fails at connector construction, before New or any account-plane call.
func TestNewConnectorRejectsWorkspaceAccountHostname(t *testing.T) {
	cfg := &config.Databricks{
		Hostname:             "cloud.databricks.com",
		AccountHostname:      "cloud.databricks.com",
		AccountId:            "test-account",
		DatabricksClientId:   "id",
		DatabricksClientSecret: "secret",
	}
	_, _, err := NewConnector(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("NewConnector() error = nil, want rejection of workspace account-hostname")
	}
	if !errors.Is(err, databricks.ErrInvalidAccountHostname) {
		t.Errorf("NewConnector() error = %v, want errors.Is(ErrInvalidAccountHostname)", err)
	}
	if !strings.Contains(err.Error(), "account-hostname") || !strings.Contains(err.Error(), "accounts.") {
		t.Errorf("NewConnector() error %q does not mention account-hostname and accounts. shape", err.Error())
	}
}

func TestAccountHostnameFieldHasNoDefault(t *testing.T) {
	if v := config.AccountHostnameField.DefaultValue; v != nil && v != "" {
		t.Errorf("account-hostname field must have no default, got %q", v)
	}
}
