package config

import (
	"context"
	"strings"
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
		// Workspace-token auth is temporarily disabled, so selecting it is rejected
		// regardless of whether the workspace/token lists are otherwise well-formed.
		// These first four previously asserted the pairing rules; they now assert the
		// disable, because it short-circuits before those rules are reached.
		{"disabled: no tokens", nil, nil, DatabricksWorkspaceTokenGroup, true},
		{"disabled: equal length", []string{"ws-1", "ws-2"}, []string{"tok-1", "tok-2"}, DatabricksWorkspaceTokenGroup, true},
		{"disabled: more workspaces than tokens", []string{"ws-1", "ws-2"}, []string{"tok-1"}, DatabricksWorkspaceTokenGroup, true},
		{"disabled: tokens without workspaces", nil, []string{"tok-1"}, DatabricksWorkspaceTokenGroup, true},
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
// group's fields, so leftover values from the other group (e.g. stale workspace
// tokens on a config that has since moved to OAuth) must not block startup.
func TestValidateConfigTrustsExplicitAuthMethod(t *testing.T) {
	cfg := &Databricks{
		DatabricksClientId:     "client-id",
		DatabricksClientSecret: "client-secret",
		Workspaces:             []string{"ws-1"},
		WorkspaceTokens:        []string{"tok-1"},
	}

	if err := ValidateConfig(context.Background(), cfg, DatabricksOAuth2Group); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// Commenting out the workspace-token field group is not enough on its own to
// disable the auth method. FieldGroupFields falls back to the default group for
// an unrecognised auth method, so a config that satisfies oauth2's required
// fields while selecting workspace-token passes field.Validate — and
// prepareClientAuth would still branch to NewTokenAuth, authenticating with PATs
// and silently ignoring the OAuth credentials supplied. ValidateConfig must
// reject the method outright. This is the regression test for that bypass.
func TestValidateConfigRejectsWorkspaceTokenEvenWithValidOAuthCreds(t *testing.T) {
	cfg := &Databricks{
		DatabricksClientId:     "client-id",
		DatabricksClientSecret: "client-secret",
		Workspaces:             []string{"ws-1"},
		WorkspaceTokens:        []string{"tok-1"},
	}

	err := ValidateConfig(context.Background(), cfg, DatabricksWorkspaceTokenGroup)
	if err == nil {
		t.Fatal("expected workspace-token auth to be rejected while disabled, got nil")
	}
	if !strings.Contains(err.Error(), "temporarily unavailable") {
		t.Fatalf("expected a 'temporarily unavailable' error, got %v", err)
	}
}
