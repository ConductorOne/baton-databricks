package connector

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
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

func TestIsGroupNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "matching group not found",
			err:  &databricks.APIError{StatusCode: http.StatusBadRequest, Message: "Group 12345 not found"},
			want: true,
		},
		{
			name: "non-matching 400",
			err:  &databricks.APIError{StatusCode: http.StatusBadRequest, Message: "invalid role name"},
			want: false,
		},
		{
			name: "unrelated not-found 400 without group in message",
			err:  &databricks.APIError{StatusCode: http.StatusBadRequest, Message: "workspace not found"},
			want: false,
		},
		{
			name: "404 status code",
			err:  &databricks.APIError{StatusCode: http.StatusNotFound, Message: "Group 12345 not found"},
			want: false,
		},
		{
			name: "mixed case still matches",
			err:  &databricks.APIError{StatusCode: http.StatusBadRequest, Message: "GROUP 12345 Not Found"},
			want: true,
		},
		{
			name: "non-APIError",
			err:  errors.New("connection reset"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isGroupNotFoundError(tt.err); got != tt.want {
				t.Errorf("isGroupNotFoundError() = %v, want %v", got, tt.want)
			}
		})
	}
}
