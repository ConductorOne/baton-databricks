package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPrincipalNotFoundError(t *testing.T) {
	err := principalNotFoundError("user", "", "u-1")

	if !errors.Is(err, errPrincipalNotFound) {
		t.Errorf("errors.Is(err, errPrincipalNotFound) = false for %v", err)
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("status.Code(err) = %s, want %s", got, codes.NotFound)
	}
	if want := "rpc error: code = NotFound desc = databricks-connector: user /u-1 not found"; err.Error() != want {
		t.Errorf("message = %q, want %q", err.Error(), want)
	}
}

// TestRevokeMissingPrincipal covers Revoke for a principal that no longer exists in Databricks:
// an empty lookup means the entitlement is already gone, so Revoke succeeds with
// GrantAlreadyRevoked and never writes the rule set. A lookup that fails (here an HTTP 404 from
// the API) is not the same thing and must still surface as an error.
func TestRevokeMissingPrincipal(t *testing.T) {
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}

	tests := []struct {
		name   string
		revoke func(t *testing.T, c *databricks.Client, principal *v2.Resource) (annotations.Annotations, error)
	}{
		{
			name: "group",
			revoke: func(t *testing.T, c *databricks.Client, principal *v2.Resource) (annotations.Annotations, error) {
				entitlement := ent.NewAssignmentEntitlement(testTargetGroupResource(t, "g-target"), groupManagerEntitlement)
				return newGroupBuilder(c).Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: entitlement})
			},
		},
		{
			name: "account",
			revoke: func(t *testing.T, c *databricks.Client, principal *v2.Resource) (annotations.Annotations, error) {
				a := newAccountBuilder(c)
				accountResource, err := a.accountResource(context.Background())
				if err != nil {
					t.Fatalf("accountResource: %v", err)
				}
				entitlement := ent.NewAssignmentEntitlement(accountResource, "account_admin")
				return a.Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: entitlement})
			},
		},
		{
			name: "service principal",
			revoke: func(t *testing.T, c *databricks.Client, principal *v2.Resource) (annotations.Annotations, error) {
				s := newServicePrincipalBuilder(c)
				spResource, err := s.servicePrincipalResource(context.Background(),
					&databricks.ServicePrincipal{BaseResponse: databricks.BaseResponse{ID: "sp-target"}, ApplicationID: "app-target"}, parent)
				if err != nil {
					t.Fatalf("servicePrincipalResource: %v", err)
				}
				entitlement := ent.NewAssignmentEntitlement(spResource, "service.principal.manager")
				return s.Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: entitlement})
			},
		},
	}

	for _, tc := range tests {
		for _, lookup := range []struct {
			name       string
			statusCode int
			body       string
			wantRevoke bool
		}{
			{name: "empty lookup", statusCode: http.StatusOK, body: `{"Resources":[],"totalResults":0}`, wantRevoke: true},
			{name: "lookup 404", statusCode: http.StatusNotFound, body: `{"detail":"not found"}`},
		} {
			t.Run(tc.name+"/"+lookup.name, func(t *testing.T) {
				putCount := 0

				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Users":
						w.WriteHeader(lookup.statusCode)
						_, _ = w.Write([]byte(lookup.body))
					case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/preview/accounts/acc-1/access-control/rule-sets":
						_, _ = w.Write([]byte(`{"grant_rules":[{"role":"roles/group.manager","principals":["users/alice@example.com"]}],"etag":"etag-1"}`))
					case req.Method == http.MethodPut:
						putCount++
						_, _ = w.Write([]byte(`{}`))
					default:
						w.WriteHeader(http.StatusNotFound)
						_, _ = w.Write([]byte(`{"detail":"not found"}`))
					}
				}))
				t.Cleanup(srv.Close)

				target, err := url.Parse(srv.URL)
				if err != nil {
					t.Fatalf("parse server url: %v", err)
				}
				c, err := databricks.NewClient(context.Background(), &http.Client{Transport: &hostRewriter{target: target}},
					testHostname, testAccountHost, testAccountId, "", &databricks.NoAuth{}, nil)
				if err != nil {
					t.Fatalf("NewClient: %v", err)
				}

				annos, err := tc.revoke(t, c, testUserPrincipal(t, "u-1"))

				if putCount != 0 {
					t.Errorf("expected no rule-set PUT, got %d", putCount)
				}
				if !lookup.wantRevoke {
					if err == nil {
						t.Fatal("Revoke returned nil error for a failed lookup, want an error")
					}
					if errors.Is(err, errPrincipalNotFound) || annos.Contains(&v2.GrantAlreadyRevoked{}) {
						t.Errorf("failed lookup treated as already revoked: err=%v annotations=%v", err, annos)
					}
					return
				}
				if err != nil {
					t.Fatalf("Revoke returned error: %v", err)
				}
				if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
					t.Errorf("annotations = %v, want GrantAlreadyRevoked", annos)
				}
			})
		}
	}
}
