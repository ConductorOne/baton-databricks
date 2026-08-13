package connector

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// CXH-2166 regression: under token auth (no account API), groups sync parented
// under the workspace. Role grants to those groups must use the same parent, or
// the grant's principal ID references a group resource that was never synced.
func TestGroupGrantParentMatchesSyncedGroupId(t *testing.T) {
	ctx := context.Background()

	t.Run("token auth uses workspace parent", func(t *testing.T) {
		parent, err := groupGrantParent(false, "acc-1", "dbc-abc")
		if err != nil {
			t.Fatalf("groupGrantParent: %v", err)
		}

		gotResourceId, _, err := groupGrantExpansion(ctx, "group-1", parent)
		if err != nil {
			t.Fatalf("groupGrantExpansion: %v", err)
		}

		wantId := groupResourceId(ctx, "group-1", &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "dbc-abc"})
		if gotResourceId.Resource != wantId {
			t.Errorf("principal ID = %q, want %q (the ID groupBuilder emits for a workspace-parented group)", gotResourceId.Resource, wantId)
		}
	})

	t.Run("account API available uses account parent", func(t *testing.T) {
		parent, err := groupGrantParent(true, "acc-1", "dbc-abc")
		if err != nil {
			t.Fatalf("groupGrantParent: %v", err)
		}

		gotResourceId, _, err := groupGrantExpansion(ctx, "group-1", parent)
		if err != nil {
			t.Fatalf("groupGrantExpansion: %v", err)
		}

		wantId := groupResourceId(ctx, "group-1", &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acc-1"})
		if gotResourceId.Resource != wantId {
			t.Errorf("principal ID = %q, want %q (the ID groupBuilder emits for an account-parented group)", gotResourceId.Resource, wantId)
		}
	})
}
