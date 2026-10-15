package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// workspaceMembersHostRewriter sends every request to the test server,
// regardless of the host the client computed internally.
type workspaceMembersHostRewriter struct {
	target *url.URL
}

func (h *workspaceMembersHostRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = h.target.Scheme
	req.URL.Host = h.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

// recordedRequest captures what the client actually sent, so tests can assert
// on HTTP method and request body rather than just the final return value.
type recordedRequest struct {
	method string
	path   string
	body   map[string]interface{}
}

// newWorkspaceMembersTestClient starts a mock server that answers the list
// (GET) endpoint with currentAssignments and records every PUT it receives
// into *puts, so the test can assert the exact method/body the fix sends.
func newWorkspaceMembersTestClient(
	t *testing.T,
	currentAssignments []WorkspaceAssignment,
	puts *[]recordedRequest,
) *Client {
	t.Helper()

	listPath := fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, testAccountId, testWorkspaceId)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == listPath:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(struct {
				Assignments []WorkspaceAssignment `json:"permission_assignments"`
			}{Assignments: currentAssignments})
			return
		case r.Method == http.MethodPut:
			var body map[string]interface{}
			if r.Body != nil {
				_ = json.NewDecoder(r.Body).Decode(&body)
			}
			*puts = append(*puts, recordedRequest{method: r.Method, path: r.URL.Path, body: body})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"detail":"unexpected request"}`))
		}
	}))
	t.Cleanup(srv.Close)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}

	httpClient := &http.Client{Transport: &workspaceMembersHostRewriter{target: target}}
	c, err := NewClient(context.Background(), httpClient, testHostnameForWorkspaceMembers, testAccountHostForWorkspaceMembers, testAccountId, "", &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

const (
	testAccountId                      = "acc-1"
	testWorkspaceId                    = "ws1"
	testHostnameForWorkspaceMembers    = "example.cloud.databricks.com"
	testAccountHostForWorkspaceMembers = "accounts.cloud.databricks.com"
	testPrincipalId                    = "100"
)

func permissionsFromBody(t *testing.T, body map[string]interface{}) []string {
	t.Helper()
	raw, ok := body["permissions"]
	if !ok {
		t.Fatalf("request body has no \"permissions\" field: %v", body)
	}
	rawSlice, ok := raw.([]interface{})
	if !ok {
		t.Fatalf("\"permissions\" field is not an array: %v", raw)
	}
	out := make([]string, 0, len(rawSlice))
	for _, v := range rawSlice {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("permissions entry is not a string: %v", v)
		}
		out = append(out, s)
	}
	return out
}

func containsAll(got []string, want ...string) bool {
	set := make(map[string]struct{}, len(got))
	for _, g := range got {
		set[g] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; !ok {
			return false
		}
	}
	return true
}

// TestCreateOrUpdateWorkspaceMember_PreservesExistingAdmin: granting "member"
// to a principal that already holds ADMIN must not demote them to USER-only.
func TestCreateOrUpdateWorkspaceMember_PreservesExistingAdmin(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
		{
			Principal:   &WorkspacePrincipal{ID: 100, UserName: "admin@example.com"},
			Permissions: []string{"ADMIN"},
		},
	}, &puts)

	alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
	}
	if alreadyGranted {
		t.Errorf("alreadyGranted = true, want false (ADMIN-only principal does not already hold USER)")
	}

	if len(puts) != 1 {
		t.Fatalf("expected exactly 1 PUT request, got %d: %+v", len(puts), puts)
	}
	got := puts[0]
	if got.method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got.method)
	}
	permissions := permissionsFromBody(t, got.body)
	if !containsAll(permissions, "ADMIN", "USER") {
		t.Errorf("permissions sent = %v, want to contain both ADMIN and USER (ADMIN must survive the member grant)", permissions)
	}
}

// TestCreateOrUpdateWorkspaceMember_AlreadyGranted: a principal that already
// holds USER needs no PUT; the grant reports GrantAlreadyExists upstream.
func TestCreateOrUpdateWorkspaceMember_AlreadyGranted(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
		{
			Principal:   &WorkspacePrincipal{ID: 100, UserName: "user@example.com"},
			Permissions: []string{"USER"},
		},
	}, &puts)

	alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
	}
	if !alreadyGranted {
		t.Errorf("alreadyGranted = false, want true (principal already holds USER)")
	}
	if len(puts) != 0 {
		t.Fatalf("expected no PUT requests, got %d: %+v", len(puts), puts)
	}
}

// TestCreateOrUpdateWorkspaceMember_NoExistingAssignment covers a brand new
// principal who has no prior assignment at all.
func TestCreateOrUpdateWorkspaceMember_NoExistingAssignment(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, nil, &puts)

	alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
	}
	if alreadyGranted {
		t.Errorf("alreadyGranted = true, want false (no prior assignment)")
	}

	if len(puts) != 1 {
		t.Fatalf("expected exactly 1 PUT request, got %d: %+v", len(puts), puts)
	}
	permissions := permissionsFromBody(t, puts[0].body)
	if len(permissions) != 1 || permissions[0] != "USER" {
		t.Errorf("permissions sent = %v, want exactly [USER]", permissions)
	}
}

// TestRemoveWorkspaceMember_PreservesExistingAdmin: revoking "member" from a
// principal holding both USER and ADMIN must leave ADMIN intact.
func TestRemoveWorkspaceMember_PreservesExistingAdmin(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
		{
			Principal:   &WorkspacePrincipal{ID: 100, UserName: "admin@example.com"},
			Permissions: []string{"USER", "ADMIN"},
		},
	}, &puts)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if alreadyRevoked {
		t.Errorf("alreadyRevoked = true, want false (a PUT was needed to drop USER)")
	}

	if len(puts) != 1 {
		t.Fatalf("expected exactly 1 PUT request, got %d: %+v", len(puts), puts)
	}
	got := puts[0]
	if got.method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got.method)
	}
	permissions := permissionsFromBody(t, got.body)
	if len(permissions) != 1 || permissions[0] != "ADMIN" {
		t.Errorf("permissions sent = %v, want exactly [ADMIN] (USER removed, ADMIN preserved)", permissions)
	}
}

// TestRemoveWorkspaceMember_UserOnlyEndsUpWithNoPermissions is the
// non-regression companion: a principal with only USER ends up with an empty
// permissions set after revoke (matching prior full-removal behavior).
func TestRemoveWorkspaceMember_UserOnlyEndsUpWithNoPermissions(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
		{
			Principal:   &WorkspacePrincipal{ID: 100, UserName: "user@example.com"},
			Permissions: []string{"USER"},
		},
	}, &puts)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if alreadyRevoked {
		t.Errorf("alreadyRevoked = true, want false (a PUT was needed to drop USER)")
	}

	if len(puts) != 1 {
		t.Fatalf("expected exactly 1 PUT request, got %d: %+v", len(puts), puts)
	}
	permissions := permissionsFromBody(t, puts[0].body)
	if len(permissions) != 0 {
		t.Errorf("permissions sent = %v, want empty", permissions)
	}
}

// TestRemoveWorkspaceMember_AdminOnlyFailsPrecondition: a principal who holds
// ADMIN but not USER can't be "revoked" from member without silently leaving
// ADMIN in place, so this must error instead of PUTting.
func TestRemoveWorkspaceMember_AdminOnlyFailsPrecondition(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
		{
			Principal:   &WorkspacePrincipal{ID: 100, UserName: "admin@example.com"},
			Permissions: []string{"ADMIN"},
		},
	}, &puts)

	_, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err == nil {
		t.Fatal("RemoveWorkspaceMember: expected error, got nil")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("status code = %v, want FailedPrecondition", code)
	}
	if len(puts) != 0 {
		t.Fatalf("expected no PUT requests, got %d: %+v", len(puts), puts)
	}
}

// TestRemoveWorkspaceMember_AlreadyRevoked: a principal who holds nothing at
// all needs no PUT; the revoke reports GrantAlreadyRevoked upstream.
func TestRemoveWorkspaceMember_AlreadyRevoked(t *testing.T) {
	var puts []recordedRequest
	c := newWorkspaceMembersTestClient(t, nil, &puts)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if !alreadyRevoked {
		t.Errorf("alreadyRevoked = false, want true (principal holds nothing)")
	}
	if len(puts) != 0 {
		t.Fatalf("expected no PUT requests, got %d: %+v", len(puts), puts)
	}
}

func TestWorkspaceAssignmentHasAdmin(t *testing.T) {
	tests := []struct {
		name        string
		permissions []string
		want        bool
	}{
		{"nil permissions", nil, false},
		{"user only", []string{"USER"}, false},
		{"admin only", []string{"ADMIN"}, true},
		{"user and admin", []string{"USER", "ADMIN"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := WorkspaceAssignment{Permissions: tt.permissions}
			if got := a.HasAdmin(); got != tt.want {
				t.Errorf("HasAdmin() with permissions %v = %v, want %v", tt.permissions, got, tt.want)
			}
		})
	}
}
