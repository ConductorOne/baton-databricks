package databricks

import (
	"context"
	"net/http"
	"testing"
)

func newTestClient(t *testing.T, exclude []string) *Client {
	t.Helper()
	c, err := NewClient(context.Background(), &http.Client{}, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "", nil, exclude)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestIsWorkspaceExcluded(t *testing.T) {
	ws := Workspace{ID: 12345, Name: "prod", DeploymentName: "dbc-abc"}

	tests := []struct {
		name    string
		exclude []string
		want    bool
	}{
		{"no exclusions", nil, false},
		{"by name", []string{"prod"}, true},
		{"by deployment name", []string{"dbc-abc"}, true},
		{"by numeric id", []string{"12345"}, true},
		{"no match", []string{"staging"}, false},
		{"empty and whitespace entries ignored", []string{"", "   "}, false},
		{"space-padded entry still matches", []string{" prod"}, true},
		{"name match is case-insensitive", []string{"PROD"}, true},
		{"deployment name match is case-insensitive", []string{"DBC-ABC"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.exclude)
			if _, got := c.isWorkspaceExcluded(ws); got != tt.want {
				t.Errorf("isWorkspaceExcluded(%+v) with exclude %v = %v, want %v", ws, tt.exclude, got, tt.want)
			}
		})
	}
}

func TestGetAccountHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     string
	}{
		{"azure subdomain", "myorg.azuredatabricks.net", "accounts.azuredatabricks.net"},
		{"azure bare host", "azuredatabricks.net", "accounts.azuredatabricks.net"},
		{"gcp subdomain", "myorg.gcp.databricks.com", "accounts.gcp.databricks.com"},
		{"aws falls through unnormalised", "myorg.cloud.databricks.com", "accounts.myorg.cloud.databricks.com"},
		// CXH-2349: a bare-suffix match would resolve this to the Azure account host.
		{"azure lookalike does not match", "evilazuredatabricks.net", "accounts.evilazuredatabricks.net"},
		{"gcp lookalike does not match", "evilgcp.databricks.com", "accounts.evilgcp.databricks.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetAccountHostname(tt.hostname); got != tt.want {
				t.Errorf("GetAccountHostname(%q) = %q, want %q", tt.hostname, got, tt.want)
			}
		})
	}
}

func TestIsWorkspaceExcludedReturnsEveryMatchedKey(t *testing.T) {
	ws := Workspace{ID: 12345, Name: "prod", DeploymentName: "dbc-abc"}
	c := newTestClient(t, []string{"prod", "dbc-abc", "staging"})

	keys, ok := c.isWorkspaceExcluded(ws)
	if !ok {
		t.Fatalf("isWorkspaceExcluded(%+v) = false, want true", ws)
	}
	want := []string{"prod", "dbc-abc"}
	if len(keys) != len(want) {
		t.Fatalf("isWorkspaceExcluded(%+v) matched keys = %v, want %v", ws, keys, want)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("isWorkspaceExcluded(%+v) matched keys = %v, want %v", ws, keys, want)
			break
		}
	}
}
