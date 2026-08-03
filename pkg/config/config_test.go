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
		wantErr    bool
	}{
		{"no tokens", nil, nil, false},
		{"equal length", []string{"ws-1", "ws-2"}, []string{"tok-1", "tok-2"}, false},
		{"more workspaces than tokens", []string{"ws-1", "ws-2"}, []string{"tok-1"}, true},
		{"tokens without workspaces", nil, []string{"tok-1"}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Databricks{Workspaces: tc.workspaces, WorkspaceTokens: tc.tokens}
			err := ValidateConfig(context.Background(), cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

// Both auth modes' fields live in the same struct; ValidateConfig must reject
// them being set together since field groups only validate one selected group.
func TestValidateConfigRejectsBothAuthModes(t *testing.T) {
	cfg := &Databricks{
		DatabricksClientId: "client-id",
		Workspaces:         []string{"ws-1"},
		WorkspaceTokens:    []string{"tok-1"},
	}

	if err := ValidateConfig(context.Background(), cfg); err == nil {
		t.Fatal("expected error, got nil")
	}
}
