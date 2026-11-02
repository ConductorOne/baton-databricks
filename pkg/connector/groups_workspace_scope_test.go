package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// workspaceScimServer serves one workspace's SCIM plane and the account plane beside it,
// so a member can exist in the workspace and be absent from the account.
type workspaceScimServer struct {
	// adminsMembers are the $ref values the workspace reports for the admins group.
	adminsMembers []string
	// accountHolds are the ids the account plane answers for, by SCIM kind.
	accountHolds map[string][]string
}

const testAdminsGroupID = "9"

func (s *workspaceScimServer) handler(t *testing.T) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		encode := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		account := strings.Contains(r.URL.Path, "/accounts/")
		_, rest, _ := strings.Cut(r.URL.Path, "/scim/v2/")
		kind, id, _ := strings.Cut(rest, "/")

		if account {
			filter := r.URL.Query().Get("filter")
			_, value, _ := strings.Cut(filter, " eq ")
			value = strings.Trim(value, `"`)
			if slices.Contains(s.accountHolds[kind], value) {
				encode(map[string]any{"totalResults": 1, "Resources": []map[string]any{
					{"id": value, "userName": "u" + value, "displayName": "g" + value, "applicationId": "a" + value},
				}})
				return
			}
			encode(map[string]any{"totalResults": 0})
			return
		}

		if kind != "Groups" {
			t.Errorf("unexpected workspace request %s", r.URL.Path)
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
			return
		}
		members := make([]map[string]any, 0, len(s.adminsMembers))
		for _, ref := range s.adminsMembers {
			_, value, _ := strings.Cut(ref, "/")
			members = append(members, map[string]any{"$ref": ref, "value": value})
		}
		group := map[string]any{"id": testAdminsGroupID, "displayName": "admins", "members": members}
		if id != "" {
			encode(group)
			return
		}
		// A real workspace reports every group it has in totalResults, not the filtered count.
		encode(map[string]any{"totalResults": 40, "Resources": []map[string]any{group}})
	}
}

func testWorkspaceGroupResource(t *testing.T) *v2.Resource {
	t.Helper()
	r, err := rs.NewGroupResource("admins", groupResourceType,
		"workspace/wsa/group/"+testAdminsGroupID, nil,
		rs.WithParentResourceID(&v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "wsa"}))
	if err != nil {
		t.Fatalf("NewGroupResource: %v", err)
	}

	return r
}

// A workspace listing is filtered to the one admins group, so the reported total is not
// a page count and asking for a second page would re-emit the same resource.
func TestWorkspaceGroupListStopsAfterTheAdminsGroup(t *testing.T) {
	srv := &workspaceScimServer{}
	client, _ := newWorkspaceRoutedClient(t, srv.handler(t))
	g := newGroupBuilder(client)

	resources, results, err := g.List(context.Background(),
		&v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "wsa"}, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 1 || resources[0].GetId().GetResource() != "workspace/wsa/group/"+testAdminsGroupID {
		t.Fatalf("resources = %v, want the one workspace-scoped admins group", resources)
	}
	if token := results.NextPageToken; token != "" {
		t.Errorf("next page token = %q, want empty: totalResults counts every group, not the filtered one", token)
	}
}

// Only the account syncs members, so a workspace-local one has no resource to point at.
// Emitting it anyway would leave a grant on a principal the bundle never carries.
func TestWorkspaceGroupGrantsSkipMembersTheAccountDoesNotHold(t *testing.T) {
	srv := &workspaceScimServer{
		adminsMembers: []string{"Users/100", "Groups/200", "ServicePrincipals/300", "Groups/9001"},
		accountHolds: map[string][]string{
			"Users": {"100"}, "Groups": {"200"}, "ServicePrincipals": {"300"},
		},
	}
	client, _ := newWorkspaceRoutedClient(t, srv.handler(t))
	g := newGroupBuilder(client)

	grants, _, err := g.Grants(context.Background(), testWorkspaceGroupResource(t), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}

	got := make([]string, 0, len(grants))
	for _, grant := range grants {
		got = append(got, grant.GetPrincipal().GetId().GetResourceType()+"/"+grant.GetPrincipal().GetId().GetResource())
	}
	slices.Sort(got)
	want := []string{
		"group/account/" + testAccountId + "/group/200",
		"service_principal/300",
		"user/100",
	}
	if !slices.Equal(got, want) {
		t.Errorf("member grants = %v, want %v (group 9001 is workspace-local and is not synced)", got, want)
	}
}

// An identity-federated workspace answers a membership PATCH with 400 and says the group
// is managed in the account, so the connector must not offer the entitlement as
// provisionable. The account-scoped twin stays grantable.
func TestWorkspaceGroupMembershipIsNotOffered(t *testing.T) {
	srv := &workspaceScimServer{}
	client, _ := newWorkspaceRoutedClient(t, srv.handler(t))
	g := newGroupBuilder(client)

	entitlements, _, err := g.Entitlements(context.Background(), testWorkspaceGroupResource(t), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}
	if len(entitlements) != 1 {
		t.Fatalf("entitlements = %d, want only membership", len(entitlements))
	}
	if got := entitlements[0].GetGrantableTo(); len(got) != 0 {
		t.Errorf("workspace group membership is grantable to %v, want nothing", got)
	}
}
