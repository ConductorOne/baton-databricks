package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

const (
	testAccountId                      = "acc-1"
	testWorkspaceId                    = "ws1"
	testHostnameForWorkspaceMembers    = "example.cloud.databricks.com"
	testAccountHostForWorkspaceMembers = "accounts.cloud.databricks.com"
	testPrincipalId                    = "100"
)

var testPrincipalPath = fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, testAccountId, testWorkspaceId) + "/principals/" + testPrincipalId

// recordedRequest captures what the client actually sent, so tests can assert
// on HTTP method, headers and request body rather than just the final return value.
type recordedRequest struct {
	method string
	path   string
	header http.Header
	body   map[string]interface{}
}

// workspaceAssignmentsMock is a mutex-guarded mock of the assignments API. It
// records every list GET and every write, and answers DELETE with a
// configurable status.
type workspaceAssignmentsMock struct {
	mu           sync.Mutex
	assignments  []WorkspaceAssignment
	deleteStatus int // 0 means 200 OK
	listGets     []http.Header
	writes       []recordedRequest
}

func (m *workspaceAssignmentsMock) setAssignments(assignments []WorkspaceAssignment) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignments = assignments
}

func (m *workspaceAssignmentsMock) snapshot() ([]http.Header, []recordedRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]http.Header(nil), m.listGets...), append([]recordedRequest(nil), m.writes...)
}

func (m *workspaceAssignmentsMock) writeMethods() []string {
	_, writes := m.snapshot()
	methods := make([]string, 0, len(writes))
	for _, w := range writes {
		methods = append(methods, w.method)
	}
	return methods
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
			var reqBody map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&reqBody)
			m.writes = append(m.writes, recordedRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: reqBody})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodDelete && r.URL.Path == testPrincipalPath:
			m.writes = append(m.writes, recordedRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone()})
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

// testAssignment returns a fresh assignment for the target principal (ID 100).
func testAssignment(permissions ...string) WorkspaceAssignment {
	return WorkspaceAssignment{
		Principal:   &WorkspacePrincipal{ID: 100, UserName: "user@example.com"},
		Permissions: permissions,
	}
}

// testUserAssignment returns a fresh list holding a USER assignment for the target principal.
func testUserAssignment() []WorkspaceAssignment {
	return []WorkspaceAssignment{testAssignment("USER")}
}

// otherPrincipalAssignmentWithError is an unreadable entry for a principal other than the target.
func otherPrincipalAssignmentWithError() WorkspaceAssignment {
	return WorkspaceAssignment{
		Principal: &WorkspacePrincipal{ID: 200, UserName: "other@example.com"},
		Error:     "failed to resolve principal 200",
	}
}

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

// assertNoRequests fails if the mock saw any read or write.
func assertNoRequests(t *testing.T, m *workspaceAssignmentsMock) {
	t.Helper()
	listGets, writes := m.snapshot()
	if len(listGets) != 0 || len(writes) != 0 {
		t.Fatalf("expected no requests, got %d list GETs and %d writes: %+v", len(listGets), len(writes), writes)
	}
}

// assertNoWrites fails if the mock saw any PUT or DELETE.
func assertNoWrites(t *testing.T, m *workspaceAssignmentsMock) {
	t.Helper()
	if _, writes := m.snapshot(); len(writes) != 0 {
		t.Fatalf("expected no write requests, got %d: %+v", len(writes), writes)
	}
}

// TestAddWorkspaceMember_NoExistingAssignment: a principal with no
// assignment at all gets exactly one PUT with [USER].
func TestAddWorkspaceMember_NoExistingAssignment(t *testing.T) {
	m := &workspaceAssignmentsMock{}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyGranted, _, err := c.AddWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("AddWorkspaceMember: %v", err)
	}
	if alreadyGranted {
		t.Errorf("alreadyGranted = true, want false (no prior assignment)")
	}

	_, writes := m.snapshot()
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

// TestAddWorkspaceMember_ExistingAssignment: any existing
// assignment already grants workspace access, so nothing is written and an
// existing ADMIN is left untouched.
func TestAddWorkspaceMember_ExistingAssignment(t *testing.T) {
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
			m := &workspaceAssignmentsMock{assignments: []WorkspaceAssignment{testAssignment(tt.permissions...)}}
			c := newWorkspaceAssignmentsMockClient(t, m)

			alreadyGranted, _, err := c.AddWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
			if err != nil {
				t.Fatalf("AddWorkspaceMember: %v", err)
			}
			if !alreadyGranted {
				t.Errorf("alreadyGranted = false, want true (principal already holds %v)", tt.permissions)
			}
			assertNoWrites(t, m)
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
			m := &workspaceAssignmentsMock{assignments: []WorkspaceAssignment{testAssignment(tt.permissions...)}}
			c := newWorkspaceAssignmentsMockClient(t, m)

			alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
			if err != nil {
				t.Fatalf("RemoveWorkspaceMember: %v", err)
			}
			if alreadyRevoked {
				t.Errorf("alreadyRevoked = true, want false (a DELETE was needed)")
			}

			_, writes := m.snapshot()
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
	m := &workspaceAssignmentsMock{}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if !alreadyRevoked {
		t.Errorf("alreadyRevoked = false, want true (principal holds nothing)")
	}
	assertNoWrites(t, m)
}

// TestWorkspaceMember_AssignmentError: an entry for the target principal that
// carries an error could not be read properly, so neither grant nor revoke may
// write on top of it.
func TestWorkspaceMember_AssignmentError(t *testing.T) {
	errAssignment := func() []WorkspaceAssignment {
		a := testAssignment("USER")
		a.Error = "principal lookup failed"
		return []WorkspaceAssignment{a}
	}
	tests := []struct {
		name string
		call func(c *Client) error
	}{
		{"grant", func(c *Client) error {
			_, _, err := c.AddWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
			return err
		}},
		{"revoke", func(c *Client) error {
			_, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &workspaceAssignmentsMock{assignments: errAssignment()}
			c := newWorkspaceAssignmentsMockClient(t, m)

			err := tt.call(c)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			for _, want := range []string{testWorkspaceId, testPrincipalId, "principal lookup failed"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			assertNoWrites(t, m)
		})
	}
}

// TestWorkspaceMember_OtherPrincipalAssignmentErrorDoesNotBlock: an error on
// another principal's entry must not affect the target.
func TestWorkspaceMember_OtherPrincipalAssignmentErrorDoesNotBlock(t *testing.T) {
	t.Run("grant", func(t *testing.T) {
		m := &workspaceAssignmentsMock{assignments: []WorkspaceAssignment{otherPrincipalAssignmentWithError()}}
		c := newWorkspaceAssignmentsMockClient(t, m)

		alreadyGranted, _, err := c.AddWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
		if err != nil {
			t.Fatalf("AddWorkspaceMember: %v", err)
		}
		if alreadyGranted {
			t.Errorf("alreadyGranted = true, want false")
		}
		if got := m.writeMethods(); len(got) != 1 || got[0] != http.MethodPut {
			t.Errorf("writes = %v, want [PUT]", got)
		}
	})
	t.Run("revoke", func(t *testing.T) {
		m := &workspaceAssignmentsMock{assignments: append(testUserAssignment(), otherPrincipalAssignmentWithError())}
		c := newWorkspaceAssignmentsMockClient(t, m)

		alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
		if err != nil {
			t.Fatalf("RemoveWorkspaceMember: %v", err)
		}
		if alreadyRevoked {
			t.Errorf("alreadyRevoked = true, want false")
		}
		if got := m.writeMethods(); len(got) != 1 || got[0] != http.MethodDelete {
			t.Errorf("writes = %v, want [DELETE]", got)
		}
	})
}

// TestWorkspaceMember_ReadsAreNotCached: grant then revoke on the same client
// must each see the live assignment list, not a cached one, and each
// read-before-write GET must carry Cache-Control: no-cache.
func TestWorkspaceMember_ReadsAreNotCached(t *testing.T) {
	m := &workspaceAssignmentsMock{}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyGranted, _, err := c.AddWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("AddWorkspaceMember: %v", err)
	}
	if alreadyGranted {
		t.Errorf("alreadyGranted = true, want false (empty list)")
	}

	m.setAssignments(testUserAssignment())

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if alreadyRevoked {
		t.Errorf("alreadyRevoked = true, want false (stale cached list)")
	}

	listGets, _ := m.snapshot()
	if len(listGets) != 2 {
		t.Fatalf("list GETs = %d, want 2", len(listGets))
	}
	for i, h := range listGets {
		if got := h.Get("Cache-Control"); got != "no-cache" {
			t.Errorf("list GET %d Cache-Control = %q, want \"no-cache\"", i, got)
		}
	}
	if got := m.writeMethods(); len(got) != 2 || got[0] != http.MethodPut || got[1] != http.MethodDelete {
		t.Errorf("writes = %v, want [PUT DELETE]", got)
	}
}

// TestRemoveWorkspaceMember_DeleteNotFound: an assignment removed between the
// read and the DELETE is already revoked, not an error.
func TestRemoveWorkspaceMember_DeleteNotFound(t *testing.T) {
	m := &workspaceAssignmentsMock{assignments: testUserAssignment(), deleteStatus: http.StatusNotFound}
	c := newWorkspaceAssignmentsMockClient(t, m)

	alreadyRevoked, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("RemoveWorkspaceMember: %v", err)
	}
	if !alreadyRevoked {
		t.Errorf("alreadyRevoked = false, want true (DELETE returned 404)")
	}
	if got := m.writeMethods(); len(got) != 1 || got[0] != http.MethodDelete {
		t.Errorf("writes = %v, want exactly [DELETE]", got)
	}
}

// TestRemoveWorkspaceMember_DeleteServerError: any other DELETE failure is
// returned, not swallowed.
func TestRemoveWorkspaceMember_DeleteServerError(t *testing.T) {
	m := &workspaceAssignmentsMock{assignments: testUserAssignment(), deleteStatus: http.StatusInternalServerError}
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
	m.setAssignments(testUserAssignment())
	alreadyGranted, _, err := c.AddWorkspaceMember(ctx, testWorkspaceId, testPrincipalId)
	if err != nil {
		t.Fatalf("AddWorkspaceMember: %v", err)
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

// TestWorkspaceMember_RejectsCompositePrincipalID: a group's composite
// resource ID must never reach the permission-assignments API, on grant or
// revoke (so it can't be silently read as "already revoked").
func TestWorkspaceMember_RejectsCompositePrincipalID(t *testing.T) {
	tests := []struct {
		name string
		call func(c *Client) error
	}{
		{"grant", func(c *Client) error {
			_, _, err := c.AddWorkspaceMember(context.Background(), testWorkspaceId, "account/abc/group/100")
			return err
		}},
		{"revoke", func(c *Client) error {
			_, _, err := c.RemoveWorkspaceMember(context.Background(), testWorkspaceId, "account/abc/group/100")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &workspaceAssignmentsMock{}
			c := newWorkspaceAssignmentsMockClient(t, m)

			err := tt.call(c)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if code := status.Code(err); code != codes.InvalidArgument {
				t.Errorf("status code = %v, want InvalidArgument", code)
			}
			assertNoRequests(t, m)
		})
	}
}
