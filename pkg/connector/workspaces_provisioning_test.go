package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testProvisioningWorkspaceID = 123
	testProvisioningPrincipalID = "100"

	testAssignmentsPath = "/api/2.0/accounts/" + testAccountId + "/workspaces/123/permissionassignments"
	testMemberPath      = testAssignmentsPath + "/principals/" + testProvisioningPrincipalID
)

// workspaceAssignmentsServer is a minimal mock of the workspace permission
// assignments API that records every request it receives.
type workspaceAssignmentsServer struct {
	mu          sync.Mutex
	assignments []map[string]any
	requests    []string // "METHOD path"
	putBodies   []map[string]any
}

func (m *workspaceAssignmentsServer) snapshot() ([]string, []map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.requests...), append([]map[string]any(nil), m.putBodies...)
}

func newWorkspaceBuilderWithMock(t *testing.T, m *workspaceAssignmentsServer) *workspaceBuilder {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.requests = append(m.requests, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == testAssignmentsPath:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"permission_assignments": m.assignments})
		case r.Method == http.MethodPut && r.URL.Path == testMemberPath:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			m.putBodies = append(m.putBodies, body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodDelete && r.URL.Path == testMemberPath:
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}

	httpClient := &http.Client{Transport: &hostRewriter{target: target}}
	c, err := databricks.NewClient(context.Background(), httpClient, testHostname, testAccountHost, testAccountId, "", &databricks.NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return newWorkspaceBuilder(c, nil)
}

func existingUserAssignment() []map[string]any {
	return []map[string]any{{
		"principal":   map[string]any{"principal_id": 100, "user_name": "alice@example.com"},
		"permissions": []string{"USER"},
	}}
}

func testWorkspaceMemberEntitlement(t *testing.T) *v2.Entitlement {
	t.Helper()
	r, err := workspaceResource(context.Background(),
		&databricks.Workspace{ID: testProvisioningWorkspaceID, Name: "ws", DeploymentName: "ws-deploy"},
		&v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId},
	)
	if err != nil {
		t.Fatalf("workspaceResource: %v", err)
	}
	return ent.NewAssignmentEntitlement(r, workspaceMemberEntitlement)
}

func testProvisioningUser(t *testing.T) *v2.Resource {
	t.Helper()
	r, err := rs.NewUserResource("alice@example.com", userResourceType, testProvisioningPrincipalID, nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}
	return r
}

func testProvisioningGroup(t *testing.T) *v2.Resource {
	t.Helper()
	// Group resource IDs are compound ("account/<id>/group/<id>").
	r, err := groupResource(context.Background(),
		&databricks.Group{ID: testProvisioningPrincipalID, DisplayName: "admins"},
		&v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId},
	)
	if err != nil {
		t.Fatalf("groupResource: %v", err)
	}
	return r
}

func TestWorkspaceGrantNoAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Grant(context.Background(), testProvisioningUser(t), testWorkspaceMemberEntitlement(t))
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if len(annos) != 0 {
		t.Errorf("annotations = %v, want none", annos)
	}

	requests, putBodies := m.snapshot()
	want := []string{"GET " + testAssignmentsPath, "PUT " + testMemberPath}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
	perms, _ := putBodies[0]["permissions"].([]any)
	if len(perms) != 1 || perms[0] != "USER" {
		t.Errorf("PUT permissions = %v, want [USER]", perms)
	}
}

func TestWorkspaceGrantExistingAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: existingUserAssignment()}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Grant(context.Background(), testProvisioningUser(t), testWorkspaceMemberEntitlement(t))
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Errorf("annotations = %v, want GrantAlreadyExists", annos)
	}

	requests, _ := m.snapshot()
	if len(requests) != 1 || requests[0] != "GET "+testAssignmentsPath {
		t.Errorf("requests = %v, want only the read", requests)
	}
}

func TestWorkspaceRevokeWithAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: existingUserAssignment()}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Revoke(context.Background(), &v2.Grant{Principal: testProvisioningUser(t), Entitlement: testWorkspaceMemberEntitlement(t)})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(annos) != 0 {
		t.Errorf("annotations = %v, want none", annos)
	}

	requests, _ := m.snapshot()
	want := []string{"GET " + testAssignmentsPath, "DELETE " + testMemberPath}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

func TestWorkspaceRevokeNoAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Revoke(context.Background(), &v2.Grant{Principal: testProvisioningUser(t), Entitlement: testWorkspaceMemberEntitlement(t)})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Errorf("annotations = %v, want GrantAlreadyRevoked", annos)
	}

	requests, _ := m.snapshot()
	if len(requests) != 1 || requests[0] != "GET "+testAssignmentsPath {
		t.Errorf("requests = %v, want only the read", requests)
	}
}

func existingGroupAssignment() []map[string]any {
	return []map[string]any{{
		"principal":   map[string]any{"principal_id": 100, "display_name": "admins"},
		"permissions": []string{"USER"},
	}}
}

func testWorkspaceGroupPrincipal(t *testing.T, resourceID string) *v2.Resource {
	t.Helper()
	return &v2.Resource{Id: &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: resourceID}}
}

func TestWorkspaceGrantGroupNoAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Grant(context.Background(), testProvisioningGroup(t), testWorkspaceMemberEntitlement(t))
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if len(annos) != 0 {
		t.Errorf("annotations = %v, want none", annos)
	}

	requests, putBodies := m.snapshot()
	want := []string{"GET " + testAssignmentsPath, "PUT " + testMemberPath}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
	perms, _ := putBodies[0]["permissions"].([]any)
	if len(perms) != 1 || perms[0] != "USER" {
		t.Errorf("PUT permissions = %v, want [USER]", perms)
	}
}

func TestWorkspaceGrantGroupExistingAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: existingGroupAssignment()}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Grant(context.Background(), testProvisioningGroup(t), testWorkspaceMemberEntitlement(t))
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Errorf("annotations = %v, want GrantAlreadyExists", annos)
	}

	requests, _ := m.snapshot()
	if len(requests) != 1 || requests[0] != "GET "+testAssignmentsPath {
		t.Errorf("requests = %v, want only the read", requests)
	}
}

func TestWorkspaceRevokeGroupWithAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: existingGroupAssignment()}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Revoke(context.Background(), &v2.Grant{Principal: testProvisioningGroup(t), Entitlement: testWorkspaceMemberEntitlement(t)})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(annos) != 0 {
		t.Errorf("annotations = %v, want none", annos)
	}

	requests, _ := m.snapshot()
	want := []string{"GET " + testAssignmentsPath, "DELETE " + testMemberPath}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

func TestWorkspaceRevokeGroupAdminAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: []map[string]any{{
		"principal":   map[string]any{"principal_id": 100, "display_name": "admins"},
		"permissions": []string{"ADMIN"},
	}}}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Revoke(context.Background(), &v2.Grant{Principal: testProvisioningGroup(t), Entitlement: testWorkspaceMemberEntitlement(t)})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(annos) != 0 {
		t.Errorf("annotations = %v, want none", annos)
	}

	requests, _ := m.snapshot()
	want := []string{"GET " + testAssignmentsPath, "DELETE " + testMemberPath}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

// The principal is built the way Grants() emits group principals, so a change to the
// compound ID format must keep Grant/Revoke hitting the numeric principal path.
func TestWorkspaceGroupPrincipalRoundTrip(t *testing.T) {
	ctx := context.Background()
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	groupID, _, err := groupGrantExpansion(ctx, testProvisioningPrincipalID, parent)
	if err != nil {
		t.Fatalf("groupGrantExpansion: %v", err)
	}
	principal := &v2.Resource{Id: groupID}

	t.Run("grant", func(t *testing.T) {
		m := &workspaceAssignmentsServer{}
		w := newWorkspaceBuilderWithMock(t, m)
		if _, err := w.Grant(ctx, principal, testWorkspaceMemberEntitlement(t)); err != nil {
			t.Fatalf("Grant(%q): %v", groupID.Resource, err)
		}
		requests, _ := m.snapshot()
		want := []string{"GET " + testAssignmentsPath, "PUT " + testMemberPath}
		if !slices.Equal(requests, want) {
			t.Fatalf("requests = %v, want %v", requests, want)
		}
	})

	t.Run("revoke", func(t *testing.T) {
		m := &workspaceAssignmentsServer{assignments: existingGroupAssignment()}
		w := newWorkspaceBuilderWithMock(t, m)
		if _, err := w.Revoke(ctx, &v2.Grant{Principal: principal, Entitlement: testWorkspaceMemberEntitlement(t)}); err != nil {
			t.Fatalf("Revoke(%q): %v", groupID.Resource, err)
		}
		requests, _ := m.snapshot()
		want := []string{"GET " + testAssignmentsPath, "DELETE " + testMemberPath}
		if !slices.Equal(requests, want) {
			t.Fatalf("requests = %v, want %v", requests, want)
		}
	})
}

func TestWorkspaceRevokeGroupNoAssignment(t *testing.T) {
	m := &workspaceAssignmentsServer{}
	w := newWorkspaceBuilderWithMock(t, m)

	annos, err := w.Revoke(context.Background(), &v2.Grant{Principal: testProvisioningGroup(t), Entitlement: testWorkspaceMemberEntitlement(t)})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Errorf("annotations = %v, want GrantAlreadyRevoked", annos)
	}

	requests, _ := m.snapshot()
	if len(requests) != 1 || requests[0] != "GET "+testAssignmentsPath {
		t.Errorf("requests = %v, want only the read", requests)
	}
}

func TestWorkspaceGroupPrincipalInvalidIDRejected(t *testing.T) {
	for name, id := range map[string]string{
		"bare group id":         "group/100",
		"bare numeric":          "100",
		"non-numeric group id":  "account/" + testAccountId + "/group/abc",
		"workspace parent":      "workspace/ws-deploy/group/100",
		"other account":         "account/other-" + testAccountId + "/group/100",
		"wrong child type":      "account/" + testAccountId + "/user/100",
		"too many segments":     "account/" + testAccountId + "/group/100/extra",
		"empty":                 "",
		"empty numeric segment": "account/" + testAccountId + "/group/",
	} {
		t.Run(name, func(t *testing.T) {
			m := &workspaceAssignmentsServer{}
			w := newWorkspaceBuilderWithMock(t, m)
			principal := testWorkspaceGroupPrincipal(t, id)

			_, err := w.Grant(context.Background(), principal, testWorkspaceMemberEntitlement(t))
			if code := status.Code(err); code != codes.InvalidArgument {
				t.Errorf("Grant status code = %v, want InvalidArgument (err: %v)", code, err)
			}
			_, err = w.Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: testWorkspaceMemberEntitlement(t)})
			if code := status.Code(err); code != codes.InvalidArgument {
				t.Errorf("Revoke status code = %v, want InvalidArgument (err: %v)", code, err)
			}
			if requests, _ := m.snapshot(); len(requests) != 0 {
				t.Errorf("requests = %v, want none", requests)
			}
		})
	}
}

func TestWorkspaceGrantServicePrincipal(t *testing.T) {
	m := &workspaceAssignmentsServer{}
	w := newWorkspaceBuilderWithMock(t, m)

	sp, err := rs.NewResource("sp", servicePrincipalResourceType, testProvisioningPrincipalID)
	if err != nil {
		t.Fatalf("NewResource: %v", err)
	}
	if _, err := w.Grant(context.Background(), sp, testWorkspaceMemberEntitlement(t)); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	requests, _ := m.snapshot()
	want := []string{"GET " + testAssignmentsPath, "PUT " + testMemberPath}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

func TestWorkspaceGrantsSkipAssignmentsWithoutUsablePrincipal(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: []map[string]any{
		{}, // no principal key at all
		{"principal": map[string]any{}},
		{"error": "failed to resolve principal 200"},
		{"principal": map[string]any{"principal_id": 100, "user_name": "alice@example.com"}, "permissions": []string{"USER"}},
		{"principal": map[string]any{"principal_id": 500, "group_name": "admins"}, "permissions": []string{"USER"}},
	}}
	w := newWorkspaceBuilderWithMock(t, m)

	core, logs := observer.New(zapcore.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))

	// The package-level warn sampler persists across tests in one binary.
	resetWorkspaceSkipWarnOccurrencesForTest()

	resource, err := workspaceResource(ctx,
		&databricks.Workspace{ID: testProvisioningWorkspaceID, Name: "ws", DeploymentName: "ws-deploy"},
		&v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId},
	)
	if err != nil {
		t.Fatalf("workspaceResource: %v", err)
	}

	grants, results, err := w.Grants(ctx, resource, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}
	if len(grants) != 2 {
		t.Fatalf("expected 2 grants, got %d", len(grants))
	}

	userID, err := rs.NewResourceID(userResourceType, 100)
	if err != nil {
		t.Fatalf("NewResourceID user: %v", err)
	}
	// The group grant goes through groupGrantExpansion, the exact production path.
	rawGroupID, err := rs.NewResourceID(groupResourceType, 500)
	if err != nil {
		t.Fatalf("NewResourceID group: %v", err)
	}
	expandedGroupID, _, err := groupGrantExpansion(ctx, rawGroupID.Resource, resource.ParentResourceId)
	if err != nil {
		t.Fatalf("groupGrantExpansion: %v", err)
	}
	wantIDs := []*v2.ResourceId{userID, expandedGroupID}
	wantEntitlementID := testWorkspaceMemberEntitlement(t).GetId()
	for i, g := range grants {
		if g.GetPrincipal().GetId().GetResourceType() != wantIDs[i].GetResourceType() ||
			g.GetPrincipal().GetId().GetResource() != wantIDs[i].GetResource() {
			t.Errorf("grant %d: got principal %v, want %v", i, g.GetPrincipal().GetId(), wantIDs[i])
		}
		if g.GetEntitlement().GetId() != wantEntitlementID {
			t.Errorf("grant %d: entitlement = %q, want %q", i, g.GetEntitlement().GetId(), wantEntitlementID)
		}
	}

	// Grants() returns a nil SyncOpResults on success — no annotation kinds added (I8).
	if results != nil {
		t.Errorf("results = %v, want nil (no annotations added)", results)
	}

	// Per-entry skip Debug lines: exactly one per skipped entry, in order.
	skipLogs := logs.FilterMessage("skipping workspace permission assignment without a usable principal").All()
	if len(skipLogs) != 3 {
		t.Fatalf("expected 3 skip Debug lines, got %d", len(skipLogs))
	}
	for i, e := range skipLogs {
		if e.Level != zapcore.DebugLevel {
			t.Errorf("skip log %d: level = %v, want Debug", i, e.Level)
		}
		gotIndex := -1
		wantError := ""
		for _, f := range e.Context {
			switch f.Key {
			case "assignment_index":
				gotIndex = int(f.Integer)
			case "assignment_error":
				wantError = f.String
			}
		}
		if gotIndex != i {
			t.Errorf("skip log %d: assignment_index = %d", i, gotIndex)
		}
		wantErrors := []string{"", "", "failed to resolve principal 200"}
		if wantError != wantErrors[i] {
			t.Errorf("skip log %d: assignment_error = %q, want %q", i, wantError, wantErrors[i])
		}
	}

	// Exactly one Warn summary with the locked fields.
	warns := logs.FilterMessage("skipped workspace permission assignments without a usable principal")
	if warns.Len() != 1 {
		t.Fatalf("expected exactly 1 Warn summary, got %d", warns.Len())
	}
	we := warns.All()[0]
	if we.Level != zapcore.WarnLevel {
		t.Errorf("summary level = %v, want Warn", we.Level)
	}
	var skippedCount, assignmentsCount int
	var totalOccurrences int64
	var wsID, wsName string
	for _, f := range we.Context {
		switch f.Key {
		case "skipped_count":
			skippedCount = int(f.Integer)
		case "assignments_count":
			assignmentsCount = int(f.Integer)
		case "total_occurrences":
			totalOccurrences = f.Integer
		case "workspace_id":
			wsID = f.String
		case "workspace_name":
			wsName = f.String
		}
	}
	if skippedCount != 3 || assignmentsCount != 5 || totalOccurrences != 1 || wsID != "123" || wsName != "ws" {
		t.Errorf("Warn summary fields: skipped_count=%d assignments_count=%d total_occurrences=%d workspace_id=%q workspace_name=%q",
			skippedCount, assignmentsCount, totalOccurrences, wsID, wsName)
	}
}

// TestWorkspaceGrantsAllAssignmentsUnusable: when EVERY permission assignment
// in the API response lacks a usable principal, Grants() fails instead of
// silently emitting zero grants (which C1 would bucket as deletions).
func TestWorkspaceGrantsAllAssignmentsUnusable(t *testing.T) {
	m := &workspaceAssignmentsServer{assignments: []map[string]any{
		{}, // no principal key at all
		{"principal": map[string]any{}},
		{"error": "failed to resolve principal 200"}, // principal-less despite the Error field
	}}
	w := newWorkspaceBuilderWithMock(t, m)

	core, logs := observer.New(zapcore.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))
	resetWorkspaceSkipWarnOccurrencesForTest()

	resource, err := workspaceResource(ctx,
		&databricks.Workspace{ID: testProvisioningWorkspaceID, Name: "ws", DeploymentName: "ws-deploy"},
		&v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId},
	)
	if err != nil {
		t.Fatalf("workspaceResource: %v", err)
	}

	grants, results, err := w.Grants(ctx, resource, rs.SyncOpAttrs{})
	if err == nil {
		t.Fatalf("expected error when all assignments are principal-less, got grants=%d results=%v", len(grants), results)
	}
	if !strings.HasPrefix(err.Error(), "databricks-connector: ") {
		t.Errorf("error = %q, want databricks-connector: prefix", err.Error())
	}
	if !strings.Contains(err.Error(), "all 3 permission assignments lack a usable principal") {
		t.Errorf("error = %q, want all-assignments-unusable message", err.Error())
	}
	if len(grants) != 0 {
		t.Errorf("grants = %d, want 0 (nothing emitted)", len(grants))
	}

	// The Warn summary still fired for the sampled skip count before the guard.
	warns := logs.FilterMessage("skipped workspace permission assignments without a usable principal")
	if warns.Len() != 1 {
		t.Fatalf("expected exactly 1 Warn summary, got %d", warns.Len())
	}
}
