package connector

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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

// TestRoleGroupPrincipalRequestsRolesAttributes checks role Grant and Revoke to an account
// group read the group with the roles attributes and then PATCH only the role, without
// sending the group's members, displayName or other permissions back.
func TestRoleGroupPrincipalRequestsRolesAttributes(t *testing.T) {
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

			if len(gotAttrs) != 1 {
				t.Errorf("GetGroup attributes = %v, want a single attributes key", gotAttrs)
			} else if parts := strings.Split(gotAttrs[0], ","); !slices.Equal(parts, []string{"roles", "entitlements", "meta"}) {
				t.Errorf("GetGroup attributes = %v, want roles,entitlements,meta", gotAttrs)
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
	if len(groupListAttrs) != 1 {
		t.Errorf("group list attributes = %v, want a single request with %v", groupListAttrs, want)
	} else if len(groupListAttrs[0]) != 1 {
		t.Errorf("group list attributes = %v, want a single attributes key", groupListAttrs)
	} else if parts := strings.Split(groupListAttrs[0][0], ","); !slices.Equal(parts, want) {
		t.Errorf("group list attributes = %v, want %v", groupListAttrs, want)
	}
}

// A group page whose JSON carries no meta key must fail the sync (fail-closed),
// not silently complete with zero grants.
func TestGrantsFailsOnUnknownMeta(t *testing.T) {
	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Resources":[{"id":"g1","displayName":"no-meta-group"}],"totalResults":1}`))
	})

	role := testRoleEntitlement(t, false).Resource
	token := ""
	var err error
	for range 10 {
		var res *rs.SyncOpResults
		_, res, err = r.Grants(context.Background(), role, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			break
		}
		token = res.NextPageToken
		if token == "" {
			break
		}
	}
	if err == nil {
		t.Fatal("Grants returned nil error, want failure on missing meta")
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("error code = %s, want %s", status.Code(err), codes.Internal)
	}
	if !strings.Contains(err.Error(), "databricks-connector:") || !strings.Contains(err.Error(), "missing meta.resourceType") {
		t.Errorf("error %q does not explain the missing meta.resourceType", err)
	}
}

// An account group (meta.resourceType "Group") that holds the role must emit a
// grant from Grants; a regression in the AccountGroup branch would otherwise go
// unnoticed because workspace groups are skipped.
func TestGrantsEmitsGrantForAccountGroupWithRole(t *testing.T) {
	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Resources":[{"id":"g1","displayName":"admins","meta":{"resourceType":"Group"},"roles":[{"value":"` + AccountAdminRole + `"}]}],"totalResults":1}`))
	})

	role := testRoleEntitlement(t, false).Resource
	grants, err := collectGrants(context.Background(), t, r, role)
	if err != nil {
		t.Fatalf("Grants returned error: %v", err)
	}
	// The same JSON doubles as the user/sp pages, so filter to group principals.
	var groupGrants int
	for _, g := range grants {
		if g.Principal.Id.ResourceType == groupResourceType.Id {
			groupGrants++
		}
	}
	if groupGrants != 1 {
		t.Errorf("Grants returned %d group-principal grants for an account group with the role, want 1", groupGrants)
	}
}

// Stripped multi-group list responses ({"id","displayName"} only) must be classified via
// the fallback read chain instead of failing the sync or silently dropping grants.
// Vendor evidence (CI tenant runs 45719189560/45720792196): the account plane strips the
// explicit attributes projection from lists/filtered lists/GETs, and even an id-filtered
// default-projection read came back stripped — but a displayName-filtered default-
// projection read returned meta twice, and the id,displayName projection is honored.
func TestGrantsClassifiesStrippedListViaFallbackChain(t *testing.T) {
	roleWithAdmin := `{"id":"g1","displayName":"admins","meta":{"resourceType":"Group"},"roles":[{"value":"` + AccountAdminRole + `"}]}`
	roleWithout := `{"id":"g1","displayName":"admins","meta":{"resourceType":"Group"},"roles":[]}`
	workspaceGroup := `{"id":"g1","displayName":"ws-group","meta":{"resourceType":"WorkspaceGroup"}}`
	noMeta := `{"id":"g1","displayName":""}`

	tests := []struct {
		name           string
		idDefaultBody  string
		idDefaultTotal int
		namedBody      string
		nameBody       string
		nameTotal      int
		wantGrants     int
		wantFallback   int
		wantErr        bool
		wantErrContain string
	}{
		{
			name:           "id-filtered read returns a different group: fail closed",
			idDefaultBody:  `{"id":"g2","displayName":"admins","meta":{"resourceType":"Group"},"roles":[]}`,
			idDefaultTotal: 1,
			wantFallback:   1,
			wantErr:        true,
			wantErrContain: "returned group g2 instead",
		},
		{
			name:           "name-filtered read returns a different group: fail closed",
			idDefaultBody:  noMeta,
			idDefaultTotal: 1,
			namedBody:      `{"id":"g1","displayName":"admins"}`,
			nameBody:       `{"id":"g2","displayName":"admins","meta":{"resourceType":"Group"},"roles":[]}`,
			nameTotal:      1,
			wantFallback:   3,
			wantErr:        true,
			wantErrContain: "returned group g2 instead",
		},
		{
			name:           "id-filtered default read carries meta and the role",
			idDefaultBody:  roleWithAdmin,
			idDefaultTotal: 1,
			wantGrants:     1,
			wantFallback:   1,
		},
		{
			name:           "id-filtered default read carries meta, workspace group",
			idDefaultBody:  workspaceGroup,
			idDefaultTotal: 1,
			wantGrants:     0,
			wantFallback:   1,
		},
		{
			name:           "id-filtered default read carries meta, no role",
			idDefaultBody:  roleWithout,
			idDefaultTotal: 1,
			wantGrants:     0,
			wantFallback:   1,
		},
		{
			name:           "id-filtered stripped, name-filtered default read carries meta with role",
			idDefaultBody:  noMeta,
			idDefaultTotal: 1,
			namedBody:      `{"id":"g1","displayName":"admins"}`,
			nameBody:       roleWithAdmin,
			nameTotal:      1,
			wantGrants:     1,
			wantFallback:   3,
		},
		{
			name:           "id-filtered stripped, name-filtered carries meta without role",
			idDefaultBody:  noMeta,
			idDefaultTotal: 1,
			namedBody:      `{"id":"g1","displayName":"admins"}`,
			nameBody:       roleWithout,
			nameTotal:      1,
			wantGrants:     0,
			wantFallback:   3,
		},
		{
			name:           "id-filtered stripped, name contains a single quote, matched with the quote left raw",
			idDefaultBody:  noMeta,
			idDefaultTotal: 1,
			namedBody:      `{"id":"g1","displayName":"Bob's team"}`,
			nameBody:       roleWithout,
			nameTotal:      1,
			wantGrants:     0,
			wantFallback:   3,
		},
		{
			name:           "id-filtered stripped and name-filtered still no meta: fail closed",
			idDefaultBody:  noMeta,
			idDefaultTotal: 1,
			namedBody:      `{"id":"g1","displayName":"admins"}`,
			nameBody:       noMeta,
			nameTotal:      1,
			wantFallback:   3,
			wantErr:        true,
		},
		{
			name:           "id-filtered empty, group vanished mid-sync: skip",
			idDefaultTotal: 0,
			wantGrants:     0,
			wantFallback:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fallbackReads int
			r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				filter := req.URL.Query().Get("filter")
				attrs := req.URL.Query().Get("attributes")
				switch {
				case filter == "id eq 'g1'" && attrs == "":
					fallbackReads++
					if attrs2 := req.URL.Query().Get("attributes"); attrs2 != "" {
						t.Errorf("default-projection read sent attributes=%q, want none", attrs2)
					}
					_, _ = fmt.Fprintf(w, `{"Resources":[%s],"totalResults":%d}`, tt.idDefaultBody, tt.idDefaultTotal)
				case filter == "id eq 'g1'" && attrs == "id,displayName":
					fallbackReads++
					_, _ = fmt.Fprintf(w, `{"Resources":[%s],"totalResults":1}`, tt.namedBody)
				case filter == "displayName eq 'Bob's team'" && attrs == "":
					fallbackReads++
					_, _ = fmt.Fprintf(w, `{"Resources":[%s],"totalResults":%d}`, tt.nameBody, tt.nameTotal)
				case filter == "displayName eq 'admins'" && attrs == "":
					fallbackReads++
					_, _ = fmt.Fprintf(w, `{"Resources":[%s],"totalResults":%d}`, tt.nameBody, tt.nameTotal)
				default:
					// Every group list page is stripped: id and displayName only.
					_, _ = w.Write([]byte(`{"Resources":[{"id":"g1","displayName":""}],"totalResults":1}`))
				}
			})

			role := testRoleEntitlement(t, false).Resource
			grants, err := collectGrants(context.Background(), t, r, role)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Grants returned nil error, want fail-closed on still-unknown meta")
				}
				if code := status.Code(err); code != codes.Internal {
					t.Errorf("error code = %s, want %s", code, codes.Internal)
				}
				if tt.wantErrContain != "" {
					if !strings.Contains(err.Error(), tt.wantErrContain) {
						t.Errorf("error %q does not contain %q", err, tt.wantErrContain)
					}
					return
				}
				if !strings.Contains(err.Error(), "missing meta.resourceType") {
					t.Errorf("error %q does not explain the missing meta.resourceType", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Grants returned error: %v", err)
			}
			var groupGrants int
			for _, g := range grants {
				if g.Principal.Id.ResourceType == groupResourceType.Id {
					groupGrants++
				}
			}
			if groupGrants != tt.wantGrants {
				t.Errorf("Grants returned %d group-principal grants, want %d", groupGrants, tt.wantGrants)
			}
			if fallbackReads != tt.wantFallback {
				t.Errorf("fallback read count = %d, want %d", fallbackReads, tt.wantFallback)
			}
		})
	}
}

// Groups whose list object already carries meta must be classified from the list alone,
// without any fallback read.
func TestGrantsDoesNotFetchWhenListHasMeta(t *testing.T) {
	var fallbackReads int
	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.URL.Query().Get("filter") == "id eq 'g1'" {
			fallbackReads++
		}
		_, _ = w.Write([]byte(`{"Resources":[{"id":"g1","displayName":"admins","meta":{"resourceType":"Group"},"roles":[]}],"totalResults":1}`))
	})

	role := testRoleEntitlement(t, false).Resource
	if _, err := collectGrants(context.Background(), t, r, role); err != nil {
		t.Fatalf("Grants returned error: %v", err)
	}
	if fallbackReads != 0 {
		t.Errorf("filtered single-group read count = %d, want 0 for a meta-bearing list page", fallbackReads)
	}
}

// collectGrants drives Grants to completion, following next-page tokens, and
// returns every grant emitted.
func collectGrants(ctx context.Context, t *testing.T, r *roleBuilder, role *v2.Resource) ([]*v2.Grant, error) {
	t.Helper()
	token := ""
	var all []*v2.Grant
	for range 10 {
		grants, res, err := r.Grants(ctx, role, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			return nil, err
		}
		all = append(all, grants...)
		token = res.NextPageToken
		if token == "" {
			break
		}
	}
	return all, nil
}

// Known workspace-scoped groups are skipped with a Debug log, and the sync succeeds.
func TestGrantsSkipsWorkspaceGroups(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))

	r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Resources":[{"id":"g1","displayName":"admins","meta":{"resourceType":"WorkspaceGroup"}}],"totalResults":1}`))
	})

	role := testRoleEntitlement(t, false).Resource
	token := ""
	for range 10 {
		grants, res, err := r.Grants(ctx, role, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			t.Fatalf("Grants returned error: %v", err)
		}
		if len(grants) != 0 {
			t.Errorf("Grants returned %d grants for a workspace group, want 0", len(grants))
		}
		token = res.NextPageToken
		if token == "" {
			break
		}
	}
	if logs.FilterMessage("databricks-connector: skipping workspace-scoped group").Len() != 1 {
		t.Errorf("want exactly one skip Debug log, got %d", logs.FilterMessage("databricks-connector: skipping workspace-scoped group").Len())
	}
}

// A fallback-classified group whose read carries no permission attributes must emit
// the sampled Warn (with total_occurrences) while the sync succeeds; a fallback
// group whose read DOES carry the role must not warn.
func TestGrantsWarnsOnUnverifiableFallbackGroup(t *testing.T) {
	tests := []struct {
		name      string
		idDefault string
		wantWarn  int
		wantGrant int
	}{
		{
			name:      "fallback read stripped of roles: sampled Warn fires",
			idDefault: `{"id":"g1","displayName":"admins","meta":{"resourceType":"Group"},"roles":[]}`,
			wantWarn:  1,
			wantGrant: 0,
		},
		{
			name:      "fallback read carries the role: no Warn",
			idDefault: `{"id":"g1","displayName":"admins","meta":{"resourceType":"Group"},"roles":[{"value":"` + AccountAdminRole + `"}]}`,
			wantWarn:  0,
			wantGrant: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			ctx := ctxzap.ToContext(context.Background(), zap.New(core))

			r := newRoleBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if req.URL.Query().Get("filter") == "id eq 'g1'" && req.URL.Query().Get("attributes") == "" {
					_, _ = fmt.Fprintf(w, `{"Resources":[%s],"totalResults":1}`, tt.idDefault)
					return
				}
				// Every group list page is stripped: id and displayName only.
				_, _ = w.Write([]byte(`{"Resources":[{"id":"g1","displayName":""}],"totalResults":1}`))
			})

			role := testRoleEntitlement(t, false).Resource
			grants, err := collectGrants(ctx, t, r, role)
			if err != nil {
				t.Fatalf("Grants returned error: %v", err)
			}
			var groupGrants int
			for _, g := range grants {
				if g.Principal.Id.ResourceType == groupResourceType.Id {
					groupGrants++
				}
			}
			if groupGrants != tt.wantGrant {
				t.Errorf("Grants returned %d group-principal grants, want %d", groupGrants, tt.wantGrant)
			}
			warns := logs.FilterMessage("databricks-connector: group carries no permission attributes in the vendor response; " +
				"its grants cannot be verified on this tenant and may be reported as deleted (no read shape on this tenant " +
				"returns roles/entitlements; remediation: re-enumerate group-held roles via the account access-control grant_rules API)")
			if warns.Len() != tt.wantWarn {
				t.Fatalf("unverifiable-grants Warn count = %d, want %d (all logs: %d)", warns.Len(), tt.wantWarn, logs.Len())
			}
			if tt.wantWarn == 1 {
				occ := warns.All()[0].ContextMap()["total_occurrences"]
				if occ != int64(1) {
					t.Errorf("warn total_occurrences = %v, want 1", occ)
				}
			}
		})
	}
}
