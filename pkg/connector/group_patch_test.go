package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
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
	// echo answers a PATCH with 200 and the patched group, as the workspace-level API does.
	echo bool
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
		if f.echo {
			writeSCIMGroup(f.t, w, req, f.state)
			return
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
				_, err := r.Grant(context.Background(), testGroupPrincipal(t, "g-target"), testRoleEntitlement(t, false))
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
				_, err := r.Revoke(context.Background(), &v2.Grant{Principal: testGroupPrincipal(t, "g-target"), Entitlement: testRoleEntitlement(t, false)})
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
	if !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Errorf("Grant annotations = %v, want GrantAlreadyExists", annos)
	}
	if len(srv.writes) != 0 || srv.puts != 0 {
		t.Errorf("Grant wrote to the group (%d PATCH, %d PUT), want no write", len(srv.writes), srv.puts)
	}
}

// TestWorkspaceGroupMembershipGrantAlreadyMemberSkipsWrite is the workspace-scoped twin of the
// account test above: an existing member is reported as GrantAlreadyExists without any write.
func TestWorkspaceGroupMembershipGrantAlreadyMemberSkipsWrite(t *testing.T) {
	srv := newFakeSCIMGroup(t, "/api/2.0/preview/scim/v2/Groups/g-target")
	g := newGroupBuilderWithHandler(t, srv.handler)

	annos, err := g.Grant(context.Background(), testUserPrincipal(t, "u-new"),
		ent.NewAssignmentEntitlement(testWorkspaceGroup(t), groupMemberEntitlement))
	if err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Errorf("Grant annotations = %v, want GrantAlreadyExists", annos)
	}
	if len(srv.writes) != 0 || srv.puts != 0 {
		t.Errorf("Grant wrote to the group (%d PATCH, %d PUT), want no write", len(srv.writes), srv.puts)
	}
}

func testWorkspaceGroup(t *testing.T) *v2.Resource {
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

// TestWorkspaceGroupWritesArePatches covers the workspace-scoped paths: membership and workspace
// entitlement writes send a single SCIM PATCH operation to the workspace API, never a PUT, and
// leave the rest of the group as is.
func TestWorkspaceGroupWritesArePatches(t *testing.T) {
	const groupPath = "/api/2.0/preview/scim/v2/Groups/g-target"

	tests := []struct {
		name      string
		run       func(t *testing.T, srv *fakeSCIMGroup) error
		wantOp    databricks.PatchOperation
		attribute string
		want      []string
	}{
		{
			name: "workspace group membership grant",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				g := newGroupBuilderWithHandler(t, srv.handler)
				_, err := g.Grant(context.Background(), testUserPrincipal(t, "u-added"),
					ent.NewAssignmentEntitlement(testWorkspaceGroup(t), groupMemberEntitlement))
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "add", Path: "members", Value: []any{map[string]any{"value": "u-added"}}},
			attribute: "members",
			want:      []string{"existing-member-1", "u-new", "u-added"},
		},
		{
			name: "workspace group membership revoke",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				g := newGroupBuilderWithHandler(t, srv.handler)
				_, err := g.Revoke(context.Background(), &v2.Grant{
					Principal:   testUserPrincipal(t, "u-new"),
					Entitlement: ent.NewAssignmentEntitlement(testWorkspaceGroup(t), groupMemberEntitlement),
				})
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "remove", Path: `members[value eq "u-new"]`},
			attribute: "members",
			want:      []string{"existing-member-1"},
		},
		{
			name: "workspace entitlement grant",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				srv.state["entitlements"] = []any{}
				r := newRoleBuilderWithHandler(t, srv.handler)
				_, err := r.Grant(context.Background(), testGroupPrincipal(t, "g-target"), testRoleEntitlement(t, true))
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "add", Path: "entitlements", Value: []any{map[string]any{"value": WorkspaceAccessRole}}},
			attribute: "entitlements",
			want:      []string{WorkspaceAccessRole},
		},
		{
			name: "workspace entitlement revoke",
			run: func(t *testing.T, srv *fakeSCIMGroup) error {
				r := newRoleBuilderWithHandler(t, srv.handler)
				_, err := r.Revoke(context.Background(), &v2.Grant{Principal: testGroupPrincipal(t, "g-target"), Entitlement: testRoleEntitlement(t, true)})
				return err
			},
			wantOp:    databricks.PatchOperation{Op: "remove", Path: `entitlements[value eq "workspace-access"]`},
			attribute: "entitlements",
			want:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeSCIMGroup(t, groupPath)
			srv.echo = true
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
			if len(srv.writes[0].Operations) != 1 {
				t.Fatalf("PATCH operations = %+v, want exactly one", srv.writes[0].Operations)
			}
			gotOp, _ := json.Marshal(srv.writes[0].Operations[0])
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

// TestGroupRevokeAbsentValueSkipsWrite: the workspace API answers 400 to a remove on an empty
// attribute, so revoking a member, role or entitlement the group does not have must not write. It
// answers GrantAlreadyRevoked so C1 can tell the no-op from a real mutation.
func TestGroupRevokeAbsentValueSkipsWrite(t *testing.T) {
	const (
		accountPath   = "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target"
		workspacePath = "/api/2.0/preview/scim/v2/Groups/g-target"
	)
	tests := []struct {
		name string
		path string
		run  func(t *testing.T, srv *fakeSCIMGroup) (annotations.Annotations, error)
	}{
		{
			name: "membership",
			path: workspacePath,
			run: func(t *testing.T, srv *fakeSCIMGroup) (annotations.Annotations, error) {
				srv.state["members"] = []any{}
				g := newGroupBuilderWithHandler(t, srv.handler)
				return g.Revoke(context.Background(), &v2.Grant{
					Principal:   testUserPrincipal(t, "u-absent"),
					Entitlement: ent.NewAssignmentEntitlement(testWorkspaceGroup(t), groupMemberEntitlement),
				})
			},
		},
		{
			name: "workspace entitlement",
			path: workspacePath,
			run: func(t *testing.T, srv *fakeSCIMGroup) (annotations.Annotations, error) {
				srv.state["entitlements"] = []any{}
				r := newRoleBuilderWithHandler(t, srv.handler)
				return r.Revoke(context.Background(), &v2.Grant{Principal: testGroupPrincipal(t, "g-target"), Entitlement: testRoleEntitlement(t, true)})
			},
		},
		{
			name: "account role",
			path: accountPath,
			run: func(t *testing.T, srv *fakeSCIMGroup) (annotations.Annotations, error) {
				srv.state["roles"] = []any{}
				r := newRoleBuilderWithHandler(t, srv.handler)
				return r.Revoke(context.Background(), &v2.Grant{Principal: testGroupPrincipal(t, "g-target"), Entitlement: testRoleEntitlement(t, false)})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeSCIMGroup(t, tc.path)
			annos, err := tc.run(t, srv)
			if err != nil {
				t.Fatalf("Revoke returned error: %v", err)
			}
			if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
				t.Errorf("annotations = %v, want GrantAlreadyRevoked", annos)
			}
			if len(srv.writes) != 0 || srv.puts != 0 {
				t.Errorf("Revoke wrote to the group (%d PATCH, %d PUT), want no write", len(srv.writes), srv.puts)
			}
		})
	}
}

// TestGroupRevokeGroupNotFound: a group that is already gone (404 on the read) has nothing left
// to revoke, so Revoke answers GrantAlreadyRevoked; any other read failure is still an error.
func TestGroupRevokeGroupNotFound(t *testing.T) {
	tests := []struct {
		name        string
		entitlement func(t *testing.T) *v2.Entitlement
		statusCode  int
		wantErr     bool
	}{
		{name: "account level 404", entitlement: testGroupMembershipEntitlement, statusCode: http.StatusNotFound},
		{
			name: "workspace level 404",
			entitlement: func(t *testing.T) *v2.Entitlement {
				return ent.NewAssignmentEntitlement(testWorkspaceGroup(t), groupMemberEntitlement)
			},
			statusCode: http.StatusNotFound,
		},
		{name: "account level 500", entitlement: testGroupMembershipEntitlement, statusCode: http.StatusInternalServerError, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var writes int
			g := newGroupBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					writes++
				}
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(`{"detail":"Group with id g-target not found"}`))
			})
			annos, err := g.Revoke(context.Background(), &v2.Grant{
				Principal:   testUserPrincipal(t, "u-new"),
				Entitlement: tc.entitlement(t),
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Revoke returned no error, want one (annotations = %v)", annos)
				}
				return
			}
			if err != nil {
				t.Fatalf("Revoke returned error: %v", err)
			}
			if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
				t.Errorf("annotations = %v, want GrantAlreadyRevoked", annos)
			}
			if writes != 0 {
				t.Errorf("Revoke wrote %d times, want none", writes)
			}
		})
	}
}

// TestRoleRevokeGroupPrincipalNotFound: same for a role revoked from a group principal that is
// already gone. Only group principals are a no-op; user and service principal Revoke still
// return the lookup error.
func TestRoleRevokeGroupPrincipalNotFound(t *testing.T) {
	tests := []struct {
		name          string
		workspaceRole bool
	}{
		{name: "account role", workspaceRole: false},
		{name: "workspace entitlement", workspaceRole: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"detail":"Group with id g-target not found"}`))
			})
			annos, err := r.Revoke(context.Background(), &v2.Grant{
				Principal:   testGroupPrincipal(t, "g-target"),
				Entitlement: testRoleEntitlement(t, tc.workspaceRole),
			})
			if err != nil {
				t.Fatalf("Revoke returned error: %v", err)
			}
			if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
				t.Errorf("annotations = %v, want GrantAlreadyRevoked", annos)
			}
		})
	}

	t.Run("500 is still an error", func(t *testing.T) {
		r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"detail":"internal error"}`))
		})
		_, err := r.Revoke(context.Background(), &v2.Grant{Principal: testGroupPrincipal(t, "g-target"), Entitlement: testRoleEntitlement(t, false)})
		var apiErr *databricks.APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusInternalServerError {
			t.Errorf("Revoke error = %v, want APIError with status 500", err)
		}
	})
}

// TestRoleGrantToGroup: a group that already holds the role or entitlement is not patched and
// answers GrantAlreadyExists; a group that lacks it gets exactly one PATCH with the add op.
func TestRoleGrantToGroup(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		workspaceRole bool
		attribute     string
		held          string
	}{
		{name: "account role", path: "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target", attribute: "roles", held: AccountAdminRole},
		{name: "workspace entitlement", path: "/api/2.0/preview/scim/v2/Groups/g-target", workspaceRole: true, attribute: "entitlements", held: WorkspaceAccessRole},
	}

	for _, tc := range tests {
		t.Run(tc.name+" already held", func(t *testing.T) {
			srv := newFakeSCIMGroup(t, tc.path)
			r := newRoleBuilderWithHandler(t, srv.handler)

			annos, err := r.Grant(context.Background(), testGroupPrincipal(t, "g-target"), testRoleEntitlement(t, tc.workspaceRole))
			if err != nil {
				t.Fatalf("Grant returned error: %v", err)
			}
			if !annos.Contains(&v2.GrantAlreadyExists{}) {
				t.Errorf("annotations = %v, want GrantAlreadyExists", annos)
			}
			if len(srv.writes) != 0 || srv.puts != 0 {
				t.Errorf("Grant wrote to the group (%d PATCH, %d PUT), want no write", len(srv.writes), srv.puts)
			}
		})

		t.Run(tc.name+" not held", func(t *testing.T) {
			srv := newFakeSCIMGroup(t, tc.path)
			srv.state[tc.attribute] = []any{}
			r := newRoleBuilderWithHandler(t, srv.handler)

			annos, err := r.Grant(context.Background(), testGroupPrincipal(t, "g-target"), testRoleEntitlement(t, tc.workspaceRole))
			if err != nil {
				t.Fatalf("Grant returned error: %v", err)
			}
			if annos.Contains(&v2.GrantAlreadyExists{}) {
				t.Errorf("annotations = %v, want no GrantAlreadyExists", annos)
			}
			if len(srv.writes) != 1 || srv.puts != 0 {
				t.Fatalf("Grant sent %d PATCH, %d PUT, want exactly 1 PATCH", len(srv.writes), srv.puts)
			}
			ops := srv.writes[0].Operations
			if len(ops) != 1 || ops[0].Op != "add" || ops[0].Path != tc.attribute {
				t.Errorf("PATCH operations = %+v, want a single %s add", ops, tc.attribute)
			}
			if got := srv.values(tc.attribute); !slices.Equal(got, []string{tc.held}) {
				t.Errorf("group %s = %v, want [%s]", tc.attribute, got, tc.held)
			}
		})
	}
}

// TestGroupWriteReadsSkipHTTPCache: the HTTP client caches GETs, so a membership check served from
// the cache after an earlier write would skip a needed write. A revoke followed by a grant of the
// same member on one builder must send both PATCHes.
func TestGroupWriteReadsSkipHTTPCache(t *testing.T) {
	srv := newFakeSCIMGroup(t, "/api/2.0/preview/scim/v2/Groups/g-target")
	srv.echo = true
	g := newGroupBuilderWithHandler(t, srv.handler)
	entitlement := ent.NewAssignmentEntitlement(testWorkspaceGroup(t), groupMemberEntitlement)

	if _, err := g.Revoke(context.Background(), &v2.Grant{Principal: testUserPrincipal(t, "u-new"), Entitlement: entitlement}); err != nil {
		t.Fatalf("Revoke returned error: %v", err)
	}
	if _, err := g.Grant(context.Background(), testUserPrincipal(t, "u-new"), entitlement); err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	if len(srv.writes) != 2 {
		t.Fatalf("sent %d PATCH requests, want 2 (revoke then grant)", len(srv.writes))
	}
	if got := srv.values("members"); !slices.Equal(got, []string{"existing-member-1", "u-new"}) {
		t.Errorf("group members = %v, want [existing-member-1 u-new]", got)
	}
}
