package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
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
	// workspaceName is the name the account lists for deployment "wsa". Empty keeps the
	// default, so a test that does not care about the display name reads the same.
	workspaceName string
	// accountReads counts the filtered reads the connector sends to the account plane.
	accountReads int
	// maxFilterTerms refuses a filter wider than Databricks does. Zero accepts any width.
	maxFilterTerms int
	// pageCap serves fewer rows than matched while still reporting the true totalResults, the
	// way a server that clamps the page would. Zero serves every match.
	pageCap int
}

const testAdminsGroupID = "9"

func (s *workspaceScimServer) handler(t *testing.T) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		encode := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		account := strings.Contains(r.URL.Path, "/accounts/")
		if strings.HasSuffix(r.URL.Path, "/workspaces") {
			name := s.workspaceName
			if name == "" {
				name = "prod"
			}
			encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": name, "deployment_name": "wsa", "workspace_status": "RUNNING"},
			})
			return
		}
		_, rest, _ := strings.Cut(r.URL.Path, "/scim/v2/")
		kind, id, _ := strings.Cut(rest, "/")

		if account {
			// One row per term it holds, as the account does: matching only the first term would
			// let a caller that dropped the rest still pass.
			s.accountReads++
			terms := strings.Split(r.URL.Query().Get("filter"), " or ")
			// Databricks counts an n-term chain as 2n-1 nodes and refuses above 200.
			if s.maxFilterTerms > 0 && len(terms) > s.maxFilterTerms {
				w.WriteHeader(http.StatusBadRequest)
				encode(map[string]any{
					"schemas":  []string{"urn:ietf:params:scim:api:messages:2.0:Error"},
					"scimType": "InvalidFilter",
					"detail":   "SCIM filter exceeds max nodes: 200",
					"status":   "400",
				})
				return
			}
			var resources []map[string]any
			for _, term := range terms {
				_, value, _ := strings.Cut(term, " eq ")
				value = strings.Trim(strings.TrimSpace(value), `"`)
				if slices.Contains(s.accountHolds[kind], value) {
					resources = append(resources, map[string]any{
						"id": value, "userName": "u" + value, "displayName": "g" + value, "applicationId": "a" + value,
					})
				}
			}
			matched := len(resources)
			if s.pageCap > 0 && len(resources) > s.pageCap {
				resources = resources[:s.pageCap]
			}
			encode(map[string]any{"totalResults": matched, "Resources": resources})
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
		// A workspace holds many groups and only the filtered one may come back, so
		// the mock applies the filter: without it the listing answers with all of
		// them and a caller that dropped the filter fails instead of passing.
		all := []map[string]any{
			{"id": "wg-eng", "displayName": "engineering"},
			group,
			{"id": "wg-ds", "displayName": "data-science"},
		}
		matched := all
		if filter := r.URL.Query().Get("filter"); filter != "" {
			_, want, _ := strings.Cut(filter, " eq ")
			want = strings.Trim(want, `"`)
			matched = nil
			for _, candidate := range all {
				if candidate["displayName"] == want {
					matched = append(matched, candidate)
				}
			}
		}
		// A real workspace reports every group it has in totalResults, not the filtered count.
		encode(map[string]any{"totalResults": 40, "Resources": matched})
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

// Databricks names the built-in group "admins" in every workspace. A metastore shared by
// several of them lists one holder per workspace for the same privileges, so the name has
// to say which workspace it came from. An account group keeps the name Databricks gives it.
func TestWorkspaceGroupNameNamesItsWorkspace(t *testing.T) {
	srv := &workspaceScimServer{workspaceName: "prod"}
	client, _ := newWorkspaceRoutedClient(t, srv.handler(t))
	g := newGroupBuilder(client)

	resources, _, err := g.List(context.Background(),
		&v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "wsa"}, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("listed %d groups, want 1", len(resources))
	}
	if got := resources[0].DisplayName; got != "admins (prod)" {
		t.Errorf("display name = %q, want %q", got, "admins (prod)")
	}
	// The unqualified name stays on the resource profile for anything matching on the
	// Databricks name, and the resource ID is untouched.
	if profile, _ := rs.GetProfileStringValue(resources[0].GetProfile(), "display_name"); profile != "admins" {
		t.Errorf("profile display_name = %q, want the unqualified %q", profile, "admins")
	}
	if got := resources[0].GetId().GetResource(); got != "workspace/wsa/group/"+testAdminsGroupID {
		t.Errorf("resource id = %q, want it unchanged by the display name", got)
	}
}

// Two workspaces holding the same privileges must not read as one principal, and an
// account group must not grow a suffix.
func TestGroupNameQualifiedOnlyUnderAWorkspace(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, workspaceName, want string
		parent                    *v2.ResourceId
	}{
		{"account group", "", "admins", &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acc-1"}},
		{"workspace prod", "prod", "admins (prod)", &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "wsa"}},
		{"workspace dev", "dev", "admins (dev)", &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "wsb"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := groupResource(ctx, &databricks.Group{ID: "9", DisplayName: "admins"}, tc.parent, tc.workspaceName)
			if err != nil {
				t.Fatalf("groupResource: %v", err)
			}
			if r.DisplayName != tc.want {
				t.Errorf("display name = %q, want %q", r.DisplayName, tc.want)
			}
		})
	}
}

// Member resolution reads the account once per member type, splits a list wider than one
// filter, and fails a clamped page instead of reading it as "the account does not hold these".
func TestWorkspaceGroupGrantsResolveMembersInBatches(t *testing.T) {
	users := func(n int) ([]string, []string) {
		refs := make([]string, 0, n)
		ids := make([]string, 0, n)
		for i := range n {
			id := strconv.Itoa(1000 + i)
			refs = append(refs, "Users/"+id)
			ids = append(ids, id)
		}

		return refs, ids
	}
	manyRefs, manyIDs := users(250)

	for _, tt := range []struct {
		name       string
		members    []string
		holds      map[string][]string
		pageCap    int
		wantGrants int
		wantReads  int
		wantErr    string
	}{
		{
			name:    "one read per member type, not one per member",
			members: []string{"Users/100", "Users/101", "Users/102", "Users/103", "Groups/200", "Groups/201", "ServicePrincipals/300"},
			holds: map[string][]string{
				"Users": {"100", "101", "102", "103"}, "Groups": {"200", "201"}, "ServicePrincipals": {"300"},
			},
			wantGrants: 7,
			wantReads:  3,
		},
		{
			name:       "a list wider than one filter is split",
			members:    manyRefs,
			holds:      map[string][]string{"Users": manyIDs},
			wantGrants: 250,
			wantReads:  3,
		},
		{
			name:    "a clamped page fails instead of dropping members",
			members: []string{"Users/100", "Users/101", "Users/102"},
			holds:   map[string][]string{"Users": {"100", "101", "102"}},
			pageCap: 2,
			wantErr: "of 3 matched Users",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := &workspaceScimServer{
				adminsMembers: tt.members, accountHolds: tt.holds,
				maxFilterTerms: 100, pageCap: tt.pageCap,
			}
			client, _ := newWorkspaceRoutedClient(t, srv.handler(t))

			grants, _, err := newGroupBuilder(client).Grants(context.Background(), testWorkspaceGroupResource(t), rs.SyncOpAttrs{})

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Grants returned %d grants, want an error naming the short page", len(grants))
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %v, want it to contain %q", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("Grants: %v", err)
			}
			if len(grants) != tt.wantGrants {
				t.Errorf("grants = %d, want %d", len(grants), tt.wantGrants)
			}
			if srv.accountReads != tt.wantReads {
				t.Errorf("account reads = %d, want %d for %d members", srv.accountReads, tt.wantReads, len(tt.members))
			}
		})
	}
}
