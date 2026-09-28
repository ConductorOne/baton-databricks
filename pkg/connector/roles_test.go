package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testAccountId   = "acc-1"
	testWorkspaceId = "ws1"
	testHostname    = "example.cloud.databricks.com"
	testAccountHost = "accounts.cloud.databricks.com"
)

// hostRewriter sends every request to the test server.
type hostRewriter struct {
	target *url.URL
}

func (h *hostRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = h.target.Scheme
	req.URL.Host = h.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

// newNotFoundRoleBuilder returns a role builder whose Databricks API answers 404 to every request.
func newNotFoundRoleBuilder(t *testing.T) *roleBuilder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"detail":"User with id u1 not found"}`))
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
	return newRoleBuilder(c)
}

func testUser(t *testing.T) *v2.Resource {
	t.Helper()
	r, err := rs.NewUserResource("alice@example.com", userResourceType, "u1", nil,
		rs.WithParentResourceID(&v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}),
	)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}
	return r
}

func testRoleEntitlement(t *testing.T, workspaceRole bool) *v2.Entitlement {
	t.Helper()
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	role := AccountAdminRole
	if workspaceRole {
		parent = &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: testWorkspaceId}
		role = WorkspaceAccessRole
	}
	r, err := roleResource(context.Background(), role, parent)
	if err != nil {
		t.Fatalf("roleResource: %v", err)
	}
	return ent.NewAssignmentEntitlement(r, RoleMemberEntitlement)
}

func assertNotInWorkspaceError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("got nil error, want FailedPrecondition")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.FailedPrecondition {
		t.Errorf("error = %v, want FailedPrecondition", err)
	}
	if !strings.Contains(err.Error(), "not assigned to workspace "+testWorkspaceId) {
		t.Errorf("error %q does not explain the missing workspace membership", err)
	}
}

func TestRoleGrantPrincipalNotInWorkspace(t *testing.T) {
	r := newNotFoundRoleBuilder(t)

	_, err := r.Grant(context.Background(), testUser(t), testRoleEntitlement(t, true))
	assertNotInWorkspaceError(t, err)
}

func TestRoleRevokePrincipalNotInWorkspace(t *testing.T) {
	r := newNotFoundRoleBuilder(t)

	_, err := r.Revoke(context.Background(), &v2.Grant{Principal: testUser(t), Entitlement: testRoleEntitlement(t, true)})
	assertNotInWorkspaceError(t, err)
}

func TestRoleGrantAccountPrincipalNotFound(t *testing.T) {
	r := newNotFoundRoleBuilder(t)

	_, err := r.Grant(context.Background(), testUser(t), testRoleEntitlement(t, false))
	if err == nil {
		t.Fatal("Grant succeeded for a principal missing from the account")
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
		t.Errorf("account role error = %v, want a plain lookup failure", err)
	}
	if !strings.Contains(err.Error(), "failed to get user") {
		t.Errorf("error %q lost the original lookup context", err)
	}
}
