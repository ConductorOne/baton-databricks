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
