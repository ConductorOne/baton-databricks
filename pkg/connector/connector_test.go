package connector

import (
	"context"
	"slices"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/cli"
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

func TestWillSyncResourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts *cli.ConnectorOpts
		want map[string]bool
	}{
		{"nil opts is the capabilities prototype", nil, map[string]bool{
			metastoreResourceType.Id:        true,
			groupResourceType.Id:            true,
			servicePrincipalResourceType.Id: true,
		}},
		{"a filter leaving out group", &cli.ConnectorOpts{
			SyncResourceTypeIDs: []string{accountResourceType.Id, servicePrincipalResourceType.Id, metastoreResourceType.Id},
		}, map[string]bool{
			groupResourceType.Id:            false,
			servicePrincipalResourceType.Id: true,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := &Databricks{opts: tc.opts}
			for resourceTypeID, want := range tc.want {
				if got := d.willSyncResourceType(resourceTypeID); got != want {
					t.Errorf("willSyncResourceType(%q) = %v, want %v", resourceTypeID, got, want)
				}
			}
		})
	}
}

// The SDK derives capabilities and Grant/Revoke dispatch from the registered
// syncers, so leaving metastore out of a sync must not unregister it.
func TestResourceSyncersAlwaysRegisterTheUnityCatalogBuilders(t *testing.T) {
	t.Parallel()

	d := &Databricks{opts: &cli.ConnectorOpts{
		SyncResourceTypeIDs: []string{accountResourceType.Id, groupResourceType.Id, userResourceType.Id},
	}}

	ctx := context.Background()
	var registered []string
	for _, syncer := range d.ResourceSyncers(ctx) {
		registered = append(registered, syncer.ResourceType(ctx).GetId())
	}
	for _, want := range []string{metastoreResourceType.Id, catalogResourceType.Id} {
		if !slices.Contains(registered, want) {
			t.Errorf("registered syncers = %v, want them to include %s", registered, want)
		}
	}
}
