package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
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
// (GET) endpoint with currentAssignments and records every PUT and DELETE it
// receives into *writes, so the test can assert the exact method/body sent.
func newWorkspaceMembersTestClient(
	t *testing.T,
	currentAssignments []WorkspaceAssignment,
	writes *[]recordedRequest,
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
			*writes = append(*writes, recordedRequest{method: r.Method, path: r.URL.Path, body: body})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
			return
		case r.Method == http.MethodDelete:
			*writes = append(*writes, recordedRequest{method: r.Method, path: r.URL.Path})
			w.WriteHeader(http.StatusOK)
			return
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
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

var testPrincipalPath = fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, testAccountId, testWorkspaceId) + "/principals/" + testPrincipalId

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

// TestCreateOrUpdateWorkspaceMember_NoExistingAssignment: a principal with no
// assignment at all gets exactly one PUT with [USER].
func TestCreateOrUpdateWorkspaceMember_NoExistingAssignment(t *testing.T) {
	var writes []recordedRequest
	c := newWorkspaceMembersTestClient(t, nil, &writes)

	alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
	}
	if alreadyGranted {
		t.Errorf("alreadyGranted = true, want false (no prior assignment)")
	}

	if len(writes) != 1 {
		t.Fatalf("expected exactly 1 write request, got %d: %+v", len(writes), writes)
	}
	got := writes[0]
	if got.method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got.method)
	}
	if got.path != testPrincipalPath {
		t.Errorf("path = %s, want %s", got.path, testPrincipalPath)
	}
	permissions := permissionsFromBody(t, got.body)
	if len(permissions) != 1 || permissions[0] != "USER" {
		t.Errorf("permissions sent = %v, want exactly [USER]", permissions)
	}
}

// TestCreateOrUpdateWorkspaceMember_ExistingAssignment: any existing
// assignment already grants workspace access, so nothing is written and an
// existing ADMIN is left untouched.
func TestCreateOrUpdateWorkspaceMember_ExistingAssignment(t *testing.T) {
	tests := []struct {
		name        string
		permissions []string
	}{
		{"user only", []string{"USER"}},
		{"admin only", []string{"ADMIN"}},
		{"user and admin", []string{"USER", "ADMIN"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writes []recordedRequest
			c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
				{
					Principal:   &WorkspacePrincipal{ID: 100, UserName: "user@example.com"},
					Permissions: tt.permissions,
				},
			}, &writes)

			alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
			if err != nil {
				t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
			}
			if !alreadyGranted {
				t.Errorf("alreadyGranted = false, want true (principal already holds %v)", tt.permissions)
			}
			if len(writes) != 0 {
				t.Fatalf("expected no write requests, got %d: %+v", len(writes), writes)
			}
		})
	}
}

// TestRemoveWorkspaceMember_ExistingAssignment: revoke deletes the whole
// assignment, whatever it holds.
func TestRemoveWorkspaceMember_ExistingAssignment(t *testing.T) {
	tests := []struct {
		name        string
		permissions []string
	}{
		{"user only", []string{"USER"}},
		{"admin only", []string{"ADMIN"}},
		{"user and admin", []string{"USER", "ADMIN"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writes []recordedRequest
			c := newWorkspaceMembersTestClient(t, []WorkspaceAssignment{
				{
					Principal:   &WorkspacePrincipal{ID: 100, UserName: "user@example.com"},
					Permissions: tt.permissions,
				},
			}, &writes)

			alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
			if err != nil {
				t.Fatalf("RemoveWorkspaceMember: %v", err)
			}
			if alreadyRevoked {
				t.Errorf("alreadyRevoked = true, want false (a DELETE was needed)")
			}

			if len(writes) != 1 {
				t.Fatalf("expected exactly 1 write request, got %d: %+v", len(writes), writes)
			}
			got := writes[0]
			if got.method != http.MethodDelete {
				t.Errorf("method = %s, want DELETE", got.method)
			}
			if got.path != testPrincipalPath {
				t.Errorf("path = %s, want %s", got.path, testPrincipalPath)
			}
		})
	}
}

// TestRemoveWorkspaceMember_AlreadyRevoked: a principal who holds nothing at
// all needs no request; the revoke reports GrantAlreadyRevoked upstream.
func TestRemoveWorkspaceMember_AlreadyRevoked(t *testing.T) {
	var writes []recordedRequest
	c := newWorkspaceMembersTestClient(t, nil, &writes)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if !alreadyRevoked {
		t.Errorf("alreadyRevoked = false, want true (principal holds nothing)")
	}
	if len(writes) != 0 {
		t.Fatalf("expected no write requests, got %d: %+v", len(writes), writes)
	}
}

// workspaceAssignmentsMock is a mutex-guarded mock of the assignments API. It
// records the headers of every list GET and the method of every write, and can
// answer DELETE with a configurable status.
type workspaceAssignmentsMock struct {
	mu           sync.Mutex
	assignments  []WorkspaceAssignment
	deleteStatus int // 0 means 200 OK
	listGets     []http.Header
	writes       []string
}

func (m *workspaceAssignmentsMock) setAssignments(assignments []WorkspaceAssignment) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignments = assignments
}

func (m *workspaceAssignmentsMock) snapshot() ([]http.Header, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]http.Header(nil), m.listGets...), append([]string(nil), m.writes...)
}

func newWorkspaceAssignmentsMockClient(t *testing.T, m *workspaceAssignmentsMock) *Client {
	t.Helper()

	listPath := fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, testAccountId, testWorkspaceId)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == listPath:
			m.listGets = append(m.listGets, r.Header.Clone())
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(struct {
				Assignments []WorkspaceAssignment `json:"permission_assignments"`
			}{Assignments: m.assignments})
		case r.Method == http.MethodPut && r.URL.Path == testPrincipalPath:
			m.writes = append(m.writes, r.Method)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodDelete && r.URL.Path == testPrincipalPath:
			m.writes = append(m.writes, r.Method)
			if m.deleteStatus != 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(m.deleteStatus)
				_, _ = w.Write([]byte(`{}`))
				return
			}
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
	httpClient := &http.Client{Transport: &workspaceMembersHostRewriter{target: target}}
	c, err := NewClient(context.Background(), httpClient, testHostnameForWorkspaceMembers, testAccountHostForWorkspaceMembers, testAccountId, "", &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

var testUserAssignment = []WorkspaceAssignment{
	{
		Principal:   &WorkspacePrincipal{ID: 100, UserName: "user@example.com"},
		Permissions: []string{"USER"},
	},
}

// TestWorkspaceMember_ReadsAreNotCached: grant then revoke on the same client
// must each see the live assignment list, not a cached one, and each
// read-before-write GET must carry Cache-Control: no-cache.
func TestWorkspaceMember_ReadsAreNotCached(t *testing.T) {
	m := &workspaceAssignmentsMock{}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
	}
	if alreadyGranted {
		t.Errorf("alreadyGranted = true, want false (empty list)")
	}

	m.setAssignments(testUserAssignment)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if alreadyRevoked {
		t.Errorf("alreadyRevoked = true, want false (stale cached list)")
	}

	listGets, writes := m.snapshot()
	if len(listGets) != 2 {
		t.Fatalf("list GETs = %d, want 2", len(listGets))
	}
	for i, h := range listGets {
		if got := h.Get("Cache-Control"); got != "no-cache" {
			t.Errorf("list GET %d Cache-Control = %q, want \"no-cache\"", i, got)
		}
	}
	if len(writes) != 2 || writes[0] != http.MethodPut || writes[1] != http.MethodDelete {
		t.Errorf("writes = %v, want [PUT DELETE]", writes)
	}
}

// TestRemoveWorkspaceMember_DeleteNotFound: an assignment removed between the
// read and the DELETE is already revoked, not an error.
func TestRemoveWorkspaceMember_DeleteNotFound(t *testing.T) {
	m := &workspaceAssignmentsMock{assignments: testUserAssignment, deleteStatus: http.StatusNotFound}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if !alreadyRevoked {
		t.Errorf("alreadyRevoked = false, want true (DELETE returned 404)")
	}
	if _, writes := m.snapshot(); len(writes) != 1 || writes[0] != http.MethodDelete {
		t.Errorf("writes = %v, want exactly [DELETE]", writes)
	}
}

// TestRemoveWorkspaceMember_DeleteServerError: any other DELETE failure is
// returned, not swallowed.
func TestRemoveWorkspaceMember_DeleteServerError(t *testing.T) {
	m := &workspaceAssignmentsMock{assignments: testUserAssignment, deleteStatus: http.StatusInternalServerError}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err == nil {
		t.Fatal("RemoveWorkspaceMember: expected error, got nil")
	}
	if alreadyRevoked {
		t.Errorf("alreadyRevoked = true, want false (DELETE failed)")
	}
}

// TestWorkspaceMember_NoCacheReadDoesNotOverwriteSyncCache: a read-before-write
// must see live state without replacing the cached entry that sync's
// ListWorkspaceMembers reads from.
func TestWorkspaceMember_NoCacheReadDoesNotOverwriteSyncCache(t *testing.T) {
	m := &workspaceAssignmentsMock{}
	c := newWorkspaceAssignmentsMockClient(t, m)
	ctx := context.Background()

	// State A (empty), cached by sync.
	first, _, err := c.ListWorkspaceMembers(ctx, testWorkspaceId)
	if err != nil {
		t.Fatalf("ListWorkspaceMembers: %v", err)
	}
	if len(first) != 0 {
		t.Fatalf("first ListWorkspaceMembers = %v, want empty", first)
	}

	// State B: the no-cache read must see it.
	m.setAssignments(testUserAssignment)
	alreadyGranted, _, err := c.CreateOrUpdateWorkspaceMember(ctx, testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("CreateOrUpdateWorkspaceMember: %v", err)
	}
	if !alreadyGranted {
		t.Errorf("alreadyGranted = false, want true (read-before-write saw stale state)")
	}

	// Sync's cached entry must still be state A, served without a new GET.
	second, _, err := c.ListWorkspaceMembers(ctx, testWorkspaceId)
	if err != nil {
		t.Fatalf("ListWorkspaceMembers: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("second ListWorkspaceMembers = %v, want cached empty list (cache entry overwritten)", second)
	}

	listGets, writes := m.snapshot()
	if len(listGets) != 2 {
		t.Errorf("list GETs = %d, want 2 (cached sync read, no-cache read)", len(listGets))
	}
	if len(writes) != 0 {
		t.Errorf("writes = %v, want none", writes)
	}
}

// newNoRequestsTestClient fails the test if any HTTP request reaches the
// server, so callers can assert a validation error short-circuits before I/O.
func newNoRequestsTestClient(t *testing.T) *Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
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

// TestCreateOrUpdateWorkspaceMember_RejectsCompositePrincipalID: a group's
// composite resource ID must never reach the permission-assignments API.
func TestCreateOrUpdateWorkspaceMember_RejectsCompositePrincipalID(t *testing.T) {
	c := newNoRequestsTestClient(t)

	_, _, err := c.CreateOrUpdateWorkspaceMember(context.Background(), testWorkspaceId, "account/abc/group/100")
	if err == nil {
		t.Fatal("CreateOrUpdateWorkspaceMember: expected error, got nil")
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("status code = %v, want InvalidArgument", code)
	}
}

// TestRemoveWorkspaceMember_RejectsCompositePrincipalID: same validation on
// revoke, so a compound group ID can't be silently read as "already revoked".
func TestRemoveWorkspaceMember_RejectsCompositePrincipalID(t *testing.T) {
	c := newNoRequestsTestClient(t)

	_, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, "account/abc/group/100")
	if err == nil {
		t.Fatal("RemoveWorkspaceMember: expected error, got nil")
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("status code = %v, want InvalidArgument", code)
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
