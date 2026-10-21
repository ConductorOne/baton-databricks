package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// fakeSCIMGroup is an account group held by a test server that applies SCIM PATCH operations
// the way the account-level API does (add merges values, remove by filter drops one value) and
// records every write, so tests can check both the wire body and the resulting group state.
type fakeSCIMGroup struct {
	t      *testing.T
	path   string
	state  map[string]any
	puts   int
	writes []databricks.PatchRequest
}

var removeFilterPath = regexp.MustCompile(`^(\w+)\[value eq "([^"]*)"\]$`)

func newFakeSCIMGroup(t *testing.T, path string) *fakeSCIMGroup {
	t.Helper()
	return &fakeSCIMGroup{
		t:    t,
		path: path,
		state: map[string]any{
			"id":           "g-target",
			"displayName":  "Target Group",
			"externalId":   "ext-1",
			"members":      []any{map[string]any{"value": "existing-member-1"}, map[string]any{"value": "u-new"}},
			"roles":        []any{map[string]any{"value": AccountAdminRole}},
			"entitlements": []any{map[string]any{"value": WorkspaceAccessRole}},
			"meta":         map[string]any{"resourceType": "Group"},
		},
	}
}

func (f *fakeSCIMGroup) handler(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != f.path {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"detail":"not found"}`))
		return
	}

	switch req.Method {
	case http.MethodGet:
		writeSCIMGroup(f.t, w, req, f.state)
	case http.MethodPatch:
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var patch databricks.PatchRequest
		if err := json.Unmarshal(body, &patch); err != nil {
			f.t.Errorf("unmarshal PATCH body %s: %v", body, err)
		}
		f.writes = append(f.writes, patch)
		for _, op := range patch.Operations {
			f.apply(op)
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPut:
		f.puts++
		w.WriteHeader(http.StatusMethodNotAllowed)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeSCIMGroup) apply(op databricks.PatchOperation) {
	switch op.Op {
	case "add":
		values, _ := op.Value.([]any)
		current, _ := f.state[op.Path].([]any)
		for _, v := range values {
			if !slices.Contains(f.values(op.Path), v.(map[string]any)["value"].(string)) {
				current = append(current, v)
			}
		}
		f.state[op.Path] = current
	case "remove":
		m := removeFilterPath.FindStringSubmatch(op.Path)
		if m == nil {
			f.t.Errorf("unexpected remove path %q", op.Path)
			return
		}
		current, _ := f.state[m[1]].([]any)
		f.state[m[1]] = slices.DeleteFunc(current, func(v any) bool {
			return v.(map[string]any)["value"] == m[2]
		})
	default:
		f.t.Errorf("unexpected op %q", op.Op)
	}
}

func (f *fakeSCIMGroup) values(attribute string) []string {
	var out []string
	current, _ := f.state[attribute].([]any)
	for _, v := range current {
		out = append(out, v.(map[string]any)["value"].(string))
	}
	return out
}

func testGroupMembershipEntitlement(t *testing.T) *v2.Entitlement {
	t.Helper()
	return ent.NewAssignmentEntitlement(testTargetGroupResource(t, "g-target"), groupMemberEntitlement)
}

func testTargetGroupPrincipal(t *testing.T) *v2.Resource {
	t.Helper()
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	r, err := rs.NewGroupResource("Target Group", groupResourceType, groupResourceId(context.Background(), "g-target", parent), nil,
		rs.WithParentResourceID(parent),
	)
	if err != nil {
		t.Fatalf("NewGroupResource: %v", err)
	}
	return r
}

// TestAccountGroupWritesArePatches covers CXH-2347 item 4: each account-group write sends a
// single SCIM PATCH operation for its own change, never a full-replacement PUT, so the rest of
// the group (displayName, externalId, other members, roles, entitlements) cannot be wiped.
func TestAccountGroupWritesArePatches(t *testing.T) {
	const groupPath = "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target"

	tests := []struct {
		name      string
		run       func(t *testing.T, srv *fakeSCIMGroup) error
		wantOp    databricks.PatchOperation
		attribute string
		want      []string
	}{
		{
			name: "group membership grant",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				g := newGroupBuilderWithHandler(t, srv.handler)
				_, err := g.Grant(context.Background(), testUserPrincipal(t, "u-added"), testGroupMembershipEntitlement(t))
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "add", Path: "members", Value: []any{map[string]any{"value": "u-added"}}},
			attribute: "members",
			want:      []string{"existing-member-1", "u-new", "u-added"},
		},
		{
			name: "group membership revoke",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				g := newGroupBuilderWithHandler(t, srv.handler)
				_, err := g.Revoke(context.Background(), &v2.Grant{Principal: testUserPrincipal(t, "u-new"), Entitlement: testGroupMembershipEntitlement(t)})
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "remove", Path: `members[value eq "u-new"]`},
			attribute: "members",
			want:      []string{"existing-member-1"},
		},
		{
			name: "account role grant",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				srv.state["roles"] = []any{}
				r := newRoleBuilderWithHandler(t, srv.handler)
				_, err := r.Grant(context.Background(), testTargetGroupPrincipal(t), testRoleEntitlement(t, false))
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "add", Path: "roles", Value: []any{map[string]any{"value": AccountAdminRole}}},
			attribute: "roles",
			want:      []string{AccountAdminRole},
		},
		{
			name: "account role revoke",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				r := newRoleBuilderWithHandler(t, srv.handler)
				_, err := r.Revoke(context.Background(), &v2.Grant{Principal: testTargetGroupPrincipal(t), Entitlement: testRoleEntitlement(t, false)})
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "remove", Path: `roles[value eq "account_admin"]`},
			attribute: "roles",
			want:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeSCIMGroup(t, groupPath)
			before := map[string][]string{}
			for _, a := range []string{"members", "roles", "entitlements"} {
				before[a] = srv.values(a)
			}

			if err := tc.run(t, srv); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}

			if srv.puts != 0 {
				t.Errorf("sent %d full-replacement PUTs, want none", srv.puts)
			}
			if len(srv.writes) != 1 {
				t.Fatalf("sent %d PATCH requests, want exactly 1", len(srv.writes))
			}
			patch := srv.writes[0]
			if !slices.Equal(patch.Schemas, []string{databricks.PatchOpSchema}) {
				t.Errorf("PATCH schemas = %v, want [%s]", patch.Schemas, databricks.PatchOpSchema)
			}
			if len(patch.Operations) != 1 {
				t.Fatalf("PATCH operations = %+v, want exactly one", patch.Operations)
			}
			gotOp, _ := json.Marshal(patch.Operations[0])
			wantOp, _ := json.Marshal(tc.wantOp)
			if string(gotOp) != string(wantOp) {
				t.Errorf("PATCH operation = %s, want %s", gotOp, wantOp)
			}

			if got := srv.values(tc.attribute); !slices.Equal(got, tc.want) {
				t.Errorf("group %s after write = %v, want %v", tc.attribute, got, tc.want)
			}
			for a, v := range before {
				if a == tc.attribute {
					continue
				}
				if got := srv.values(a); !slices.Equal(got, v) {
					t.Errorf("group %s changed from %v to %v by a write that targets %s", a, v, got, tc.attribute)
				}
			}
			if srv.state["displayName"] != "Target Group" || srv.state["externalId"] != "ext-1" {
				t.Errorf("group displayName/externalId changed: %v / %v", srv.state["displayName"], srv.state["externalId"])
			}
		})
	}
}

// TestAccountGroupPatchBodyCarriesOnlyTheChange checks the raw wire body: no displayName,
// members, roles or entitlements of the rest of the group are sent alongside the operation.
func TestAccountGroupPatchBodyCarriesOnlyTheChange(t *testing.T) {
	var raw []byte
	srv := newFakeSCIMGroup(t, "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target")
	g := newGroupBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPatch {
			raw, _ = io.ReadAll(req.Body)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		srv.handler(w, req)
	})

	if _, err := g.Grant(context.Background(), testUserPrincipal(t, "u-added"), testGroupMembershipEntitlement(t)); err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	want := `{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"add","path":"members","value":[{"value":"u-added"}]}]}`
	if got := strings.TrimSpace(string(raw)); got != want {
		t.Errorf("PATCH body = %s, want %s", got, want)
	}
}

// TestAccountGroupMembershipGrantAlreadyMemberSkipsWrite keeps the read-before-write: a
// principal that is already a member is reported as granted without any write.
func TestAccountGroupMembershipGrantAlreadyMemberSkipsWrite(t *testing.T) {
	srv := newFakeSCIMGroup(t, "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target")
	g := newGroupBuilderWithHandler(t, srv.handler)

	annos, err := g.Grant(context.Background(), testUserPrincipal(t, "u-new"), testGroupMembershipEntitlement(t))
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if annos != nil {
		t.Errorf("Grant annotations = %v, want nil", annos)
	}
	if len(srv.writes) != 0 || srv.puts != 0 {
		t.Errorf("Grant wrote to the group (%d PATCH, %d PUT), want no write", len(srv.writes), srv.puts)
	}
}

// TestWorkspaceGroupWritesKeepPut pins the workspace-scoped paths to the full-replacement PUT:
// workspace-level SCIM PATCH is not verified against a live workspace, so these keep the PR 76
// behavior of writing back the full group fetched with every modeled attribute.
func TestWorkspaceGroupWritesKeepPut(t *testing.T) {
	const groupPath = "/api/2.0/preview/scim/v2/Groups/g-target"

	workspaceGroup := func(t *testing.T) *v2.Resource {
		t.Helper()
		parent := &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: testWorkspaceId}
		r, err := groupResource(context.Background(), &databricks.Group{
			BaseResponse: databricks.BaseResponse{ID: "g-target"},
			DisplayName:  "Target Group",
		}, parent)
		if err != nil {
			t.Fatalf("groupResource: %v", err)
		}
		return r
	}

	tests := []struct {
		name  string
		run   func(t *testing.T, handler http.HandlerFunc) error
		check func(t *testing.T, put capturedGroupPut)
	}{
		{
			name: "workspace group membership grant",
			run: func(t *testing.T, handler http.HandlerFunc) error {
				g := newGroupBuilderWithHandler(t, handler)
				_, err := g.Grant(context.Background(), testUserPrincipal(t, "u-added"),
					ent.NewAssignmentEntitlement(workspaceGroup(t), groupMemberEntitlement))
				return err
			},
			check: func(t *testing.T, put capturedGroupPut) {
				var members []string
				for _, m := range put.Members {
					members = append(members, m.Value)
				}
				if want := []string{"existing-member-1", "u-new", "u-added"}; !slices.Equal(members, want) {
					t.Errorf("PUT body members = %v, want %v", members, want)
				}
			},
		},
		{
			name: "workspace group membership revoke",
			run: func(t *testing.T, handler http.HandlerFunc) error {
				g := newGroupBuilderWithHandler(t, handler)
				_, err := g.Revoke(context.Background(), &v2.Grant{
					Principal:   testUserPrincipal(t, "u-new"),
					Entitlement: ent.NewAssignmentEntitlement(workspaceGroup(t), groupMemberEntitlement),
				})
				return err
			},
			check: func(t *testing.T, put capturedGroupPut) {
				if len(put.Members) != 1 || put.Members[0].Value != "existing-member-1" {
					t.Errorf("PUT body members = %+v, want only existing-member-1", put.Members)
				}
			},
		},
		{
			name: "workspace entitlement grant",
			run: func(t *testing.T, handler http.HandlerFunc) error {
				r := newRoleBuilderWithHandler(t, handler)
				_, err := r.Grant(context.Background(), testTargetGroupPrincipal(t), testRoleEntitlement(t, true))
				return err
			},
			check: func(t *testing.T, put capturedGroupPut) {
				if len(put.Members) != 2 {
					t.Errorf("PUT body members = %+v, want the group's existing members", put.Members)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeSCIMGroup(t, groupPath)
			var capturedPutBody []byte
			handler := func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodPut && req.URL.Path == groupPath {
					capturedPutBody, _ = io.ReadAll(req.Body)
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(capturedPutBody)
					return
				}
				srv.handler(w, req)
			}

			if err := tc.run(t, handler); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}
			if len(srv.writes) != 0 {
				t.Errorf("sent %d PATCH requests to a workspace group, want none", len(srv.writes))
			}
			if capturedPutBody == nil {
				t.Fatal("expected a PUT request updating the group, got none")
			}

			var put capturedGroupPut
			if err := json.Unmarshal(capturedPutBody, &put); err != nil {
				t.Fatalf("unmarshal PUT body: %v", err)
			}
			if put.DisplayName != "Target Group" {
				t.Errorf("PUT body displayName = %q, want %q", put.DisplayName, "Target Group")
			}
			if len(put.Roles) != 1 || put.Roles[0].Value != AccountAdminRole {
				t.Errorf("PUT body roles = %+v, want the group's existing role", put.Roles)
			}
			tc.check(t, put)
		})
	}
}
