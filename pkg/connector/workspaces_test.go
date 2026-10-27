package connector

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func testUserAssignments(count int) []map[string]any {
	assignments := make([]map[string]any, 0, count)
	for i := range count {
		assignments = append(assignments, map[string]any{
			"principal":   map[string]any{"principal_id": 1000 + i, "user_name": "user" + strconv.Itoa(i) + "@example.com"},
			"permissions": []string{"USER"},
		})
	}
	return assignments
}

func testWorkspace(t *testing.T) *v2.Resource {
	t.Helper()
	r, err := workspaceResource(context.Background(),
		&databricks.Workspace{ID: testProvisioningWorkspaceID, Name: "ws", DeploymentName: "ws-deploy"},
		&v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId},
	)
	if err != nil {
		t.Fatalf("workspaceResource: %v", err)
	}
	return r
}

// Workspace assignments come back in one API response, so Grants must split them across pages
// without skipping or repeating a principal when assignments change between page fetches.
func TestWorkspaceGrantsPagesAssignments(t *testing.T) {
	// Every page re-fetches the assignments; disable the HTTP cache so each page sees the change.
	t.Setenv("BATON_DISABLE_HTTP_CACHE", "true")

	count := 2*int(GrantsPageSize) + 500
	m := &workspaceAssignmentsServer{assignments: shuffled(testUserAssignments(count))}
	w := newWorkspaceBuilderWithMock(t, m)
	workspace := testWorkspace(t)

	seen := map[string]int{}
	pages := 0
	token := ""
	for {
		grants, results, err := w.Grants(context.Background(), workspace, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			t.Fatalf("Grants page %d: %v", pages, err)
		}
		pages++
		if len(grants) > int(GrantsPageSize) {
			t.Errorf("page %d has %d grants, want at most %d", pages, len(grants), GrantsPageSize)
		}
		for _, gr := range grants {
			seen[gr.Principal.Id.Resource]++
		}

		token = results.NextPageToken
		if token == "" {
			break
		}
		if pages > 100 {
			t.Fatal("pagination did not terminate")
		}
		if pages == 1 {
			// Remove the first assignment, already emitted on page 1, and reshuffle.
			m.mu.Lock()
			all := testUserAssignments(count)
			m.assignments = shuffled(slices.Clone(all[1:]))
			m.mu.Unlock()
		}
	}

	if pages != 3 {
		t.Errorf("got %d pages, want 3", pages)
	}
	for i := range count {
		id := strconv.Itoa(1000 + i)
		if seen[id] != 1 {
			t.Errorf("principal %s granted %d times, want 1", id, seen[id])
		}
	}
}
