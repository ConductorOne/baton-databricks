package config

import (
	"context"
	"testing"
)

func TestValidateConfig(t *testing.T) {
	cases := []struct {
		name       string
		workspaces []string
		tokens     []string
		authMethod string
		wantErr    bool
	}{
		{"no tokens", nil, nil, DatabricksWorkspaceTokenGroup, false},
		{"equal length", []string{"ws-1", "ws-2"}, []string{"tok-1", "tok-2"}, DatabricksWorkspaceTokenGroup, false},
		{"more workspaces than tokens", []string{"ws-1", "ws-2"}, []string{"tok-1"}, DatabricksWorkspaceTokenGroup, true},
		{"tokens without workspaces", nil, []string{"tok-1"}, DatabricksWorkspaceTokenGroup, true},
		{"mismatched lengths ignored outside workspace-token method", []string{"ws-1", "ws-2"}, []string{"tok-1"}, DatabricksOAuth2Group, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Databricks{Workspaces: tc.workspaces, WorkspaceTokens: tc.tokens}
			err := ValidateConfig(context.Background(), cfg, tc.authMethod)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

// Both auth modes' fields live in the same struct; with no auth method selected,
// ValidateConfig can't tell which credentials would actually be used, so it must
// reject having both set.
func TestValidateConfigRejectsBothAuthModesWhenAmbiguous(t *testing.T) {
	cfg := &Databricks{
		DatabricksClientId: "client-id",
		Workspaces:         []string{"ws-1"},
		WorkspaceTokens:    []string{"tok-1"},
	}

	if err := ValidateConfig(context.Background(), cfg, ""); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestValidateConfigRejectsClientSecretWithTokensWhenAmbiguous(t *testing.T) {
	cfg := &Databricks{
		DatabricksClientSecret: "client-secret",
		Workspaces:             []string{"ws-1"},
		WorkspaceTokens:        []string{"tok-1"},
	}

	if err := ValidateConfig(context.Background(), cfg, ""); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// Once an auth method is explicitly selected, prepareClientAuth only reads that
// group's fields, so leftover values from the other group (e.g. stale OAuth
// creds after switching to workspace tokens, or vice versa) must not block startup.
func TestValidateConfigTrustsExplicitAuthMethod(t *testing.T) {
	cfg := &Databricks{
		DatabricksClientId:     "client-id",
		DatabricksClientSecret: "client-secret",
		Workspaces:             []string{"ws-1"},
		WorkspaceTokens:        []string{"tok-1"},
	}

	if err := ValidateConfig(context.Background(), cfg, DatabricksWorkspaceTokenGroup); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if err := ValidateConfig(context.Background(), cfg, DatabricksOAuth2Group); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
