package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
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

// newGroupBuilderWithHandler returns a group builder whose Databricks API is a test server
// driven by handler, so tests can assert on the exact requests (path, method, body) made.
func newGroupBuilderWithHandler(t *testing.T, handler http.HandlerFunc) *groupBuilder {
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
	return newGroupBuilder(c)
}

func testTargetGroupResource(t *testing.T, nativeGroupID string) *v2.Resource {
	t.Helper()
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	group := &databricks.Group{
		BaseResponse: databricks.BaseResponse{ID: nativeGroupID},
		DisplayName:  "Target Group",
	}
	r, err := groupResource(context.Background(), group, parent)
	if err != nil {
		t.Fatalf("groupResource: %v", err)
	}
	return r
}

// TestGroupGrantMembershipUsesNativePrincipalID is the regression test for CXH-2347 symptom A:
// granting group membership to a group principal must write the member using the parsed
// native id, not the compound "account/<acct>/group/<id>" resource id, so the write matches
// the already-exists check a few lines above it.
func TestGroupGrantMembershipUsesNativePrincipalID(t *testing.T) {
	var capturedPutBody []byte

	g := newGroupBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"g-target","displayName":"Target Group","members":[]}`))
		case req.Method == http.MethodPut && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target":
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

	targetGroup := testTargetGroupResource(t, "g-target")
	entitlement := ent.NewAssignmentEntitlement(targetGroup, groupMemberEntitlement)

	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	compoundPrincipalID := groupResourceId(context.Background(), "g-member-native", parent)
	principal, err := rs.NewGroupResource("Member Group", groupResourceType, compoundPrincipalID, nil,
		rs.WithParentResourceID(parent),
	)
	if err != nil {
		t.Fatalf("NewGroupResource: %v", err)
	}

	if _, err := g.Grant(context.Background(), principal, entitlement); err != nil {
		t.Fatalf("Grant returned error: %v", err)
	}

	if capturedPutBody == nil {
		t.Fatal("expected a PUT request updating the group, got none")
	}

	var put struct {
		Members []struct {
			Value string `json:"value"`
		} `json:"members"`
	}
	if err := json.Unmarshal(capturedPutBody, &put); err != nil {
		t.Fatalf("unmarshal PUT body: %v", err)
	}

	if len(put.Members) != 1 || put.Members[0].Value != "g-member-native" {
		t.Errorf("PUT body members = %+v, want a single member with native id %q (not the compound principal id %q)",
			put.Members, "g-member-native", compoundPrincipalID)
	}
}

// writeSCIMGroup writes group as JSON, keeping only the attributes listed in the request's
// "attributes" query (all of them when none are listed), like the real SCIM API does. This
// is what makes a read-modify-write that under-requests attributes visibly drop them.
func writeSCIMGroup(t *testing.T, w http.ResponseWriter, req *http.Request, group map[string]any) {
	t.Helper()
	out := group
	if attrs := req.URL.Query()["attributes"]; len(attrs) > 0 {
		out = map[string]any{}
		for _, a := range attrs {
			if v, ok := group[a]; ok {
				out[a] = v
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		t.Errorf("encode group: %v", err)
	}
}

// testSCIMGroupWithPermissions is an account group that already has a member, a role and an
// entitlement, so tests can check a membership PUT carries all of them back.
func testSCIMGroupWithPermissions() map[string]any {
	return map[string]any{
		"id":           "g-target",
		"displayName":  "Target Group",
		"members":      []map[string]any{{"value": "existing-member-1"}, {"value": "u-new"}},
		"roles":        []map[string]any{{"value": "account_admin"}},
		"entitlements": []map[string]any{{"value": "workspace-access"}},
		"meta":         map[string]any{"resourceType": "Group"},
	}
}

type capturedGroupPut struct {
	DisplayName string `json:"displayName"`
	Members     []struct {
		Value string `json:"value"`
	} `json:"members"`
	Roles []struct {
		Value string `json:"value"`
	} `json:"roles"`
	Entitlements []struct {
		Value string `json:"value"`
	} `json:"entitlements"`
}

func testUserPrincipal(t *testing.T, id string) *v2.Resource {
	t.Helper()
	r, err := rs.NewUserResource(id, userResourceType, id, nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}
	return r
}

// TestGroupMembershipPutPreservesPermissions covers the CXH-2347 PUT-wipe: membership Grant
// and Revoke read the group and PUT it back in full, so the read must request roles and
// entitlements too or the PUT silently strips them from the group.
func TestGroupMembershipPutPreservesPermissions(t *testing.T) {
	tests := []struct {
		name        string
		principalID string
		run         func(g *groupBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error
		wantMembers []string
	}{
		{
			name:        "grant",
			principalID: "u-added",
			run: func(g *groupBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error {
				_, err := g.Grant(context.Background(), principal, entitlement)
				return err
			},
			wantMembers: []string{"existing-member-1", "u-new", "u-added"},
		},
		{
			name:        "revoke",
			principalID: "u-new",
			run: func(g *groupBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error {
				_, err := g.Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: entitlement})
				return err
			},
			wantMembers: []string{"existing-member-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedPutBody []byte

			g := newGroupBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target":
					writeSCIMGroup(t, w, req, testSCIMGroupWithPermissions())
				case req.Method == http.MethodPut && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups/g-target":
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

			targetGroup := testTargetGroupResource(t, "g-target")
			entitlement := ent.NewAssignmentEntitlement(targetGroup, groupMemberEntitlement)

			if err := tc.run(g, testUserPrincipal(t, tc.principalID), entitlement); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
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

			var members []string
			for _, m := range put.Members {
				members = append(members, m.Value)
			}
			if !slices.Equal(members, tc.wantMembers) {
				t.Errorf("PUT body members = %v, want %v", members, tc.wantMembers)
			}

			if len(put.Roles) != 1 || put.Roles[0].Value != "account_admin" {
				t.Errorf("PUT body roles = %+v, want the group's existing role account_admin", put.Roles)
			}
			if len(put.Entitlements) != 1 || put.Entitlements[0].Value != "workspace-access" {
				t.Errorf("PUT body entitlements = %+v, want the group's existing entitlement workspace-access", put.Entitlements)
			}
		})
	}
}

// TestGroupRuleSetCompoundGroupPrincipal covers the CXH-2347 rule-set path: granting or
// revoking a group's manager role to a group principal must look the principal up by its
// native id (the filter "id eq 'account/...'" matches nothing) and write it to the rule set
// as "groups/<display name>".
func TestGroupRuleSetCompoundGroupPrincipal(t *testing.T) {
	const wantPrincipal = "groups/Member Group"

	tests := []struct {
		name           string
		existingRules  string
		run            func(g *groupBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error
		wantPrincipals []string
	}{
		{
			name:          "grant",
			existingRules: `[]`,
			run: func(g *groupBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error {
				_, err := g.Grant(context.Background(), principal, entitlement)
				return err
			},
			wantPrincipals: []string{wantPrincipal},
		},
		{
			name:          "revoke",
			existingRules: `[{"role":"roles/group.manager","principals":["users/alice@example.com","groups/Member Group"]}]`,
			run: func(g *groupBuilder, principal *v2.Resource, entitlement *v2.Entitlement) error {
				_, err := g.Revoke(context.Background(), &v2.Grant{Principal: principal, Entitlement: entitlement})
				return err
			},
			wantPrincipals: []string{"users/alice@example.com"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var filters []string
			var capturedPutBody []byte

			g := newGroupBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/accounts/acc-1/scim/v2/Groups":
					filter := req.URL.Query().Get("filter")
					filters = append(filters, filter)
					w.Header().Set("Content-Type", "application/json")
					if filter == "id eq 'g-member-native'" {
						_, _ = w.Write([]byte(`{"Resources":[{"id":"g-member-native","displayName":"Member Group"}],"totalResults":1}`))
						return
					}
					_, _ = w.Write([]byte(`{"Resources":[],"totalResults":0}`))
				case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/preview/accounts/acc-1/access-control/rule-sets":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"grant_rules":` + tc.existingRules + `,"etag":"etag-1"}`))
				case req.Method == http.MethodPut && req.URL.Path == "/api/2.0/preview/accounts/acc-1/access-control/rule-sets":
					body, err := io.ReadAll(req.Body)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					capturedPutBody = body
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{}`))
				default:
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"detail":"not found"}`))
				}
			})

			targetGroup := testTargetGroupResource(t, "g-target")
			entitlement := ent.NewAssignmentEntitlement(targetGroup, groupManagerEntitlement)

			parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			compoundPrincipalID := groupResourceId(context.Background(), "g-member-native", parent)
			principal, err := rs.NewGroupResource("Member Group", groupResourceType, compoundPrincipalID, nil,
				rs.WithParentResourceID(parent),
			)
			if err != nil {
				t.Fatalf("NewGroupResource: %v", err)
			}

			if err := tc.run(g, principal, entitlement); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}

			if !slices.Equal(filters, []string{"id eq 'g-member-native'"}) {
				t.Errorf("group lookup filters = %q, want a single lookup by the native id (never %q)", filters, compoundPrincipalID)
			}

			if capturedPutBody == nil {
				t.Fatal("expected a PUT request updating the rule set, got none")
			}

			var put struct {
				RuleSet struct {
					GrantRules []databricks.RuleSet `json:"grant_rules"`
				} `json:"rule_set"`
			}
			if err := json.Unmarshal(capturedPutBody, &put); err != nil {
				t.Fatalf("unmarshal PUT body: %v", err)
			}

			if len(put.RuleSet.GrantRules) != 1 ||
				put.RuleSet.GrantRules[0].Role != groupManagerEntitlement ||
				!slices.Equal(put.RuleSet.GrantRules[0].Principals, tc.wantPrincipals) {
				t.Errorf("PUT body grant_rules = %+v, want role %q with principals %v",
					put.RuleSet.GrantRules, groupManagerEntitlement, tc.wantPrincipals)
			}
		})
	}
}

// TestGroupRuleSetPrincipalLookup covers rule-set grants for every principal type: a principal
// the SCIM lookup cannot find must fail with NotFound before any rule-set PUT (instead of
// writing an empty "users/", "groups/" or "servicePrincipals/" principal), and a found one must
// be written with its type prefix.
func TestGroupRuleSetPrincipalLookup(t *testing.T) {
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}

	tests := []struct {
		name          string
		scimPath      string
		nativeID      string
		found         string
		principal     func(t *testing.T) *v2.Resource
		wantPrincipal string
	}{
		{
			name:     "user",
			scimPath: "/api/2.0/accounts/acc-1/scim/v2/Users",
			nativeID: "u-1",
			found:    `{"id":"u-1","userName":"alice@example.com"}`,
			principal: func(t *testing.T) *v2.Resource {
				return testUserPrincipal(t, "u-1")
			},
			wantPrincipal: "users/alice@example.com",
		},
		{
			name:     "group",
			scimPath: "/api/2.0/accounts/acc-1/scim/v2/Groups",
			nativeID: "g-member-native",
			found:    `{"id":"g-member-native","displayName":"Member Group"}`,
			principal: func(t *testing.T) *v2.Resource {
				r, err := rs.NewGroupResource("Member Group", groupResourceType,
					groupResourceId(context.Background(), "g-member-native", parent), nil,
					rs.WithParentResourceID(parent),
				)
				if err != nil {
					t.Fatalf("NewGroupResource: %v", err)
				}
				return r
			},
			wantPrincipal: "groups/Member Group",
		},
		{
			name:     "service principal",
			scimPath: "/api/2.0/accounts/acc-1/scim/v2/ServicePrincipals",
			nativeID: "sp-1",
			found:    `{"id":"sp-1","applicationId":"app-1"}`,
			principal: func(t *testing.T) *v2.Resource {
				r, err := rs.NewResource("Service Principal", servicePrincipalResourceType, "sp-1")
				if err != nil {
					t.Fatalf("NewResource: %v", err)
				}
				return r
			},
			wantPrincipal: "servicePrincipals/app-1",
		},
	}

	for _, tc := range tests {
		for _, exists := range []bool{false, true} {
			name := tc.name + "/missing"
			if exists {
				name = tc.name + "/found"
			}
			t.Run(name, func(t *testing.T) {
				var capturedPutBody []byte
				putCount := 0

				g := newGroupBuilderWithHandler(t, func(w http.ResponseWriter, req *http.Request) {
					switch {
					case req.Method == http.MethodGet && req.URL.Path == tc.scimPath:
						w.Header().Set("Content-Type", "application/json")
						if exists && req.URL.Query().Get("filter") == "id eq '"+tc.nativeID+"'" {
							_, _ = w.Write([]byte(`{"Resources":[` + tc.found + `],"totalResults":1}`))
							return
						}
						_, _ = w.Write([]byte(`{"Resources":[],"totalResults":0}`))
					case req.Method == http.MethodGet && req.URL.Path == "/api/2.0/preview/accounts/acc-1/access-control/rule-sets":
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{"grant_rules":[],"etag":"etag-1"}`))
					case req.Method == http.MethodPut && req.URL.Path == "/api/2.0/preview/accounts/acc-1/access-control/rule-sets":
						putCount++
						body, err := io.ReadAll(req.Body)
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						capturedPutBody = body
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{}`))
					default:
						w.WriteHeader(http.StatusNotFound)
						_, _ = w.Write([]byte(`{"detail":"not found"}`))
					}
				})

				targetGroup := testTargetGroupResource(t, "g-target")
				entitlement := ent.NewAssignmentEntitlement(targetGroup, groupManagerEntitlement)

				_, err := g.Grant(context.Background(), tc.principal(t), entitlement)

				if !exists {
					if status.Code(err) != codes.NotFound {
						t.Fatalf("Grant error = %v (code %s), want code %s", err, status.Code(err), codes.NotFound)
					}
					if putCount != 0 {
						t.Fatalf("expected no rule-set PUT for a missing principal, got %d (last body %s)", putCount, capturedPutBody)
					}
					return
				}

				if err != nil {
					t.Fatalf("Grant returned error: %v", err)
				}
				if putCount != 1 {
					t.Fatalf("expected one rule-set PUT, got %d", putCount)
				}

				var put struct {
					RuleSet struct {
						GrantRules []databricks.RuleSet `json:"grant_rules"`
					} `json:"rule_set"`
				}
				if err := json.Unmarshal(capturedPutBody, &put); err != nil {
					t.Fatalf("unmarshal PUT body: %v", err)
				}

				if len(put.RuleSet.GrantRules) != 1 ||
					!slices.Equal(put.RuleSet.GrantRules[0].Principals, []string{tc.wantPrincipal}) {
					t.Errorf("PUT body grant_rules = %+v, want principals [%s]", put.RuleSet.GrantRules, tc.wantPrincipal)
				}
			})
		}
	}
}

// shuffled returns a shuffled copy of items, mimicking Databricks returning members in a different
// order on every call.
func shuffled[T any](items []T) []T {
	out := slices.Clone(items)
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] }) // #nosec G404 -- test data order, not security sensitive
	return out
}

func testGroupMembers(count int) []databricks.Member {
	members := make([]databricks.Member, 0, count)
	for i := range count {
		id := fmt.Sprintf("%05d", i)
		members = append(members, databricks.Member{ID: id, Ref: "Users/" + id})
	}
	return members
}

// newTestGroupBuilder backs a group builder with a test server serving an account group with
// *members as its members and one rule set granting a service principal the manager role.
// Tests may change *members between pages. It counts the rule set requests so tests can assert
// they are only made once.
func newTestGroupBuilder(t *testing.T, members *[]databricks.Member, ruleSetCalls *int) *groupBuilder {
	t.Helper()
	// Every page re-fetches the group. Disable the HTTP cache so each page sees a fresh, reshuffled
	// response, as it does when pages run in separate Lambda invocations.
	t.Setenv("BATON_DISABLE_HTTP_CACHE", "true")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/scim/v2/Groups/g1"):
			_ = json.NewEncoder(w).Encode(databricks.Group{ID: "g1", DisplayName: "all users", Members: shuffled(*members)})
		case strings.HasSuffix(r.URL.Path, "/access-control/rule-sets"):
			*ruleSetCalls++
			_, _ = w.Write([]byte(`{"grant_rules":[{"role":"roles/group.manager","principals":["servicePrincipals/app-1"]}],"etag":"e1"}`))
		case strings.HasSuffix(r.URL.Path, "/scim/v2/ServicePrincipals"):
			_, _ = w.Write([]byte(`{"Resources":[{"id":"sp-1","applicationId":"app-1"}],"totalResults":1}`))
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
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
	return newGroupBuilder(c)
}

func testAccountGroup(t *testing.T) *v2.Resource {
	t.Helper()
	parent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	g, err := groupResource(context.Background(), &databricks.Group{ID: "g1", DisplayName: "all users"}, parent)
	if err != nil {
		t.Fatalf("groupResource: %v", err)
	}
	return g
}

// groupGrantPages pages through Grants for group until the page token is empty, calling
// afterPage after each page. It returns how many times each member was granted, the manager
// grant count and the page count, and checks no page exceeds GrantsPageSize member grants.
func groupGrantPages(t *testing.T, g *groupBuilder, group *v2.Resource, afterPage func(page int)) (map[string]int, int, int) {
	t.Helper()
	seen := map[string]int{}
	managerGrants := 0
	pages := 0
	token := ""
	for {
		grants, results, err := g.Grants(context.Background(), group, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			t.Fatalf("Grants page %d: %v", pages, err)
		}
		pages++

		memberGrants := 0
		for _, gr := range grants {
			switch {
			case strings.HasSuffix(gr.Entitlement.Id, ":"+groupMemberEntitlement):
				memberGrants++
				seen[gr.Principal.Id.Resource]++
			case strings.HasSuffix(gr.Entitlement.Id, ":"+groupManagerEntitlement):
				managerGrants++
			}
		}
		if memberGrants > int(GrantsPageSize) {
			t.Errorf("page %d has %d member grants, want at most %d", pages, memberGrants, GrantsPageSize)
		}

		token = results.NextPageToken
		if token == "" {
			return seen, managerGrants, pages
		}
		if pages > 100 {
			t.Fatal("pagination did not terminate")
		}
		if afterPage != nil {
			afterPage(pages)
		}
	}
}

// A group's members come back in one API response, so Grants must split them across pages:
// a single page for a group with thousands of members exceeds the 6MB Lambda response limit.
func TestGroupGrantsPagesMembers(t *testing.T) {
	memberCount := 2*int(GrantsPageSize) + 500
	members := testGroupMembers(memberCount)
	ruleSetCalls := 0
	g := newTestGroupBuilder(t, &members, &ruleSetCalls)

	seen, managerGrants, pages := groupGrantPages(t, g, testAccountGroup(t), nil)

	if pages != 3 {
		t.Errorf("got %d pages, want 3", pages)
	}
	if len(seen) != memberCount {
		t.Errorf("got %d distinct member grants, want %d", len(seen), memberCount)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("member %s granted %d times, want 1", id, n)
		}
	}
	if managerGrants != 1 || ruleSetCalls != 1 {
		t.Errorf("got %d manager grants from %d rule set calls, want 1 from 1", managerGrants, ruleSetCalls)
	}
}

// Members removed or added between page fetches must not shift the next page's start, or a
// member near the boundary is skipped (C1 reads its grant as revoked) or granted twice.
func TestGroupGrantsMembershipChurnBetweenPages(t *testing.T) {
	memberCount := 2*int(GrantsPageSize) + 500
	tests := []struct {
		name   string
		change func([]databricks.Member) []databricks.Member
	}{
		{
			// With an offset cursor, every later member moves up one slot and the first member
			// of page 2 is never granted.
			name:   "member already emitted is removed",
			change: func(m []databricks.Member) []databricks.Member { return slices.Clone(m[1:]) },
		},
		{
			// With an offset cursor, every later member moves down one slot and the last member
			// of page 1 is granted again on page 2.
			name: "member sorting before the cursor is added",
			change: func(m []databricks.Member) []databricks.Member {
				return append(slices.Clone(m), databricks.Member{ID: "00000a", Ref: "Users/00000a"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members := testGroupMembers(memberCount)
			ruleSetCalls := 0
			g := newTestGroupBuilder(t, &members, &ruleSetCalls)

			seen, _, _ := groupGrantPages(t, g, testAccountGroup(t), func(page int) {
				if page == 1 {
					members = tt.change(members)
				}
			})

			// Every original member is granted exactly once; the removed one on page 1, before
			// its removal. A member added before the cursor is picked up by the next sync.
			for _, m := range testGroupMembers(memberCount) {
				if seen[m.ID] != 1 {
					t.Errorf("member %s granted %d times, want 1", m.ID, seen[m.ID])
				}
			}
		})
	}
}

func TestGroupGrantsSmallGroupSinglePage(t *testing.T) {
	members := testGroupMembers(3)
	ruleSetCalls := 0
	g := newTestGroupBuilder(t, &members, &ruleSetCalls)

	grants, results, err := g.Grants(context.Background(), testAccountGroup(t), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}
	if results.NextPageToken != "" {
		t.Errorf("got next page token %q, want none", results.NextPageToken)
	}
	if len(grants) != 4 {
		t.Errorf("got %d grants, want 3 members + 1 manager", len(grants))
	}
}
