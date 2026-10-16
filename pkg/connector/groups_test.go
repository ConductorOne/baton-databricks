package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
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
