package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
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

// newRoleBuilderWithHandler returns a role builder whose Databricks API is a test server
// driven by handler, so tests can assert on the exact requests (path, method, body) made.
func newRoleBuilderWithHandler(t *testing.T, handler http.HandlerFunc) *roleBuilder {
	t.Helper()
	srv := httptest.NewServer(handler)
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

// testGroupPrincipal builds a group resource whose Id.Resource is the compound
// "account/<acct>/group/<nativeID>" form produced by groupResourceId, mirroring what a
// real account-level group resource looks like as a Grant/Revoke principal.
func testGroupPrincipal(t *testing.T, nativeGroupID string) *v2.Resource {
	t.Helper()
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	compoundID := groupResourceId(context.Background(), nativeGroupID, parent)
	r, err := rs.NewGroupResource("Group "+nativeGroupID, groupResourceType, compoundID, nil,
		rs.WithParentResourceID(parent),
	)
	if err != nil {
		t.Fatalf("NewGroupResource: %v", err)
	}
	return r
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

// TestRoleGrantGroupPrincipalParsesCompoundID is the regression test for CXH-2347 symptom B:
// granting a role to a group principal must parse the compound resource id down to the
// native group id before calling GetGroup/PatchGroup (the mock server here only answers
// requests for the native id "g-native"; sending the unparsed compound id 404s). It also
// asserts the outgoing PATCH adds only the granted role.
func TestRoleGrantGroupPrincipalParsesCompoundID(t *testing.T) {
	var capturedPatch *databricks.PatchRequest

	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-native":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"g-native","displayName":"Native Group","members":[{"value":"existing-member-1","display":"Existing","$ref":"Users/existing-member-1"}],"roles":[]}`))
		case req.Method == http.MethodPatch && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-native":
			capturedPatch = &databricks.PatchRequest{}
			if err := json.NewDecoder(req.Body).Decode(capturedPatch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"detail":"not found"}`))
		}
	})

	principal := testGroupPrincipal(t, "g-native")
	_, err := r.Grant(context.Background(), principal, testRoleEntitlement(t, false))
	if err != nil {
		t.Fatalf("Grant returned error (likely means the compound id reached GetGroup unparsed): %v", err)
	}

	if capturedPatch == nil {
		t.Fatal("expected a PATCH request updating the group, got none")
	}

	if len(capturedPatch.Operations) != 1 || capturedPatch.Operations[0].Op != "add" || capturedPatch.Operations[0].Path != "roles" {
		t.Fatalf("PATCH operations = %+v, want a single roles add", capturedPatch.Operations)
	}
	added, _ := capturedPatch.Operations[0].Value.([]any)
	if len(added) != 1 || added[0].(map[string]any)["value"] != AccountAdminRole {
		t.Errorf("PATCH added roles = %+v, want only %s", added, AccountAdminRole)
	}
}

// TestRoleGrantUserPrincipalStillWorks is the user-principal control (PROV-01): the
// group-principal fix in roles.go must not change behavior for user principals, which
// never carry a compound resource id.
func TestRoleGrantUserPrincipalStillWorks(t *testing.T) {
	var capturedPutBody []byte

	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Users/u1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"u1","userName":"alice@example.com","roles":[]}`))
		case req.Method == http.MethodPut && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Users/u1":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			capturedPutBody = body
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"detail":"not found"}`))
		}
	})

	_, err := r.Grant(context.Background(), testUser(t), testRoleEntitlement(t, false))
	if err != nil {
		t.Fatalf("Grant for user principal failed: %v", err)
	}

	if capturedPutBody == nil {
		t.Fatal("expected a PUT request updating the user, got none")
	}

	var put struct {
		Roles []struct {
			Value string `json:"value"`
		} `json:"roles"`
	}
	if err := json.Unmarshal(capturedPutBody, &put); err != nil {
		t.Fatalf("unmarshal PUT body: %v", err)
	}

	found := false
	for _, role := range put.Roles {
		if role.Value == AccountAdminRole {
			found = true
		}
	}
	if !found {
		t.Errorf("PUT body missing the granted role for user principal, roles = %+v", put.Roles)
	}
}

// TestRoleGroupPrincipalRequestsUpdateAttributes checks role Grant and Revoke to an account
// group read the group with the update attributes and then PATCH only the role, without
// sending the group's members, displayName or other permissions back.
func TestRoleGroupPrincipalRequestsUpdateAttributes(t *testing.T) {
	tests := []struct {
		name string
		// roles are the group's current roles: a grant needs a group that lacks the role, a revoke one that holds it.
		roles []map[string]any
		run   func(r *roleBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error
	}{
		{
			name:  "grant",
			roles: []map[string]any{},
			run: func(r *roleBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error {
				_, err := r.Grant(context.Background(), principal, entitlement)
				return err
			},
		},
		{
			name:  "revoke",
			roles: []map[string]any{{"value": AccountAdminRole}},
			run: func(r *roleBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error {
				_, err := r.Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: entitlement})
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotAttrs []string
			var capturedPatchBody []byte

			r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-native":
					gotAttrs = req.URL.Query()["attributes"]
					writeSCIMGroup(t, w, req, map[string]any{
						"id":          "g-native",
						"displayName": "Native Group",
						"members":     []map[string]any{{"value": "existing-member-1"}},
						"roles":       tc.roles,
						"meta":        map[string]any{"resourceType": "Group"},
					})
				case req.Method == http.MethodPatch && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-native":
					body, err := io.ReadAll(req.Body)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					capturedPatchBody = body
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"detail":"not found"}`))
				}
			})

			if err := tc.run(r, testGroupPrincipal(t, "g-native"), testRoleEntitlement(t, false)); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}

			if want := databricks.NewGroupUpdateAttrVars().Attrs; !slices.Equal(gotAttrs, want) {
				t.Errorf("GetGroup attributes = %v, want %v", gotAttrs, want)
			}

			if capturedPatchBody == nil {
				t.Fatal("expected a PATCH request updating the group, got none")
			}

			var patch capturedGroupPut
			if err := json.Unmarshal(capturedPatchBody, &patch); err != nil {
				t.Fatalf("unmarshal PATCH body: %v", err)
			}
			if patch.DisplayName != "" || patch.Members != nil || patch.Roles != nil || patch.Entitlements != nil {
				t.Errorf("PATCH body carries group attributes beside its operation: %s", capturedPatchBody)
			}
		})
	}
}

// TestRoleGrantsSyncRequestsOnlyRoleAttributes guards the sync path: listing groups for role
// grants must not request members, which would download every group's member list per page.
func TestRoleGrantsSyncRequestsOnlyRoleAttributes(t *testing.T) {
	var groupListAttrs [][]string

	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"detail":"not found"}`))
			return
		}
		if req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups" {
			groupListAttrs = append(groupListAttrs, req.URL.Query()["attributes"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Resources":[],"totalResults":0}`))
	})

	role := testRoleEntitlement(t, false).Resource
	token := ""
	for i := 0; i < 10; i++ {
		_, res, err := r.Grants(context.Background(), role, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			t.Fatalf("Grants returned error: %v", err)
		}
		token = res.NextPageToken
		if token == "" {
			break
		}
	}

	want := []string{"roles", "entitlements", "meta"}
	if len(groupListAttrs) != 1 || !slices.Equal(groupListAttrs[0], want) {
		t.Errorf("group list attributes = %v, want a single request with %v", groupListAttrs, want)
	}
}
