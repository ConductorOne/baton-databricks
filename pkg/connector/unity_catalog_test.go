package connector

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"slices"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type statusRT struct {
	status int
	body   string
}

func (rt *statusRT) RoundTrip(r *http.Request) (*http.Response, error) {
	body := rt.body
	if body == "" {
		body = "{}"
	}
	return &http.Response{
		StatusCode: rt.status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Request:    r,
	}, nil
}

func newErrClient(t *testing.T, status int) *databricks.Client {
	t.Helper()
	c, err := databricks.NewClient(
		context.Background(),
		&http.Client{Transport: &statusRT{status: status, body: `{"message":"denied"}`}},
		"example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "",
		&databricks.NoAuth{}, nil,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// A workspace without Unity Catalog, or a service principal lacking UC access,
// must not fail the sync — the catalog List degrades to empty instead of
// returning an error that aborts every other resource type.
func TestCatalogListDegradesOnForbidden(t *testing.T) {
	b := newCatalogBuilder(newUCResolver(newErrClient(t, http.StatusForbidden)))
	parent := &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "dbc-abc"}

	resources, results, err := b.List(context.Background(), parent, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("List returned error on 403, want graceful skip: %v", err)
	}
	if len(resources) != 0 {
		t.Fatalf("List returned %d resources on 403, want 0", len(resources))
	}
	if results != nil && results.NextPageToken != "" {
		t.Fatalf("List returned next page token on 403, want none")
	}
}

// Context cancellation must propagate rather than be swallowed as degradation.
func TestCatalogListPropagatesContextCancellation(t *testing.T) {
	b := newCatalogBuilder(newUCResolver(newErrClient(t, http.StatusForbidden)))
	parent := &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "dbc-abc"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := b.List(ctx, parent, rs.SyncOpAttrs{}); err == nil {
		t.Fatalf("List swallowed cancelled context, want error")
	}
}

func childResourceTypeIDs(t *testing.T, r *v2.Resource) []string {
	t.Helper()
	var out []string
	for _, a := range r.GetAnnotations() {
		crt := &v2.ChildResourceType{}
		if a.MessageIs(crt) {
			if err := a.UnmarshalTo(crt); err != nil {
				t.Fatalf("unmarshal child resource type: %v", err)
			}
			out = append(out, crt.ResourceTypeId)
		}
	}
	return out
}

// Unity Catalog is opt-in: the catalog child (and thus UC sync) must be absent
// unless enabled, and table sync is separately gated on the schema child.
func TestUnityCatalogSyncIsGated(t *testing.T) {
	ctx := context.Background()
	ws := &databricks.Workspace{Name: "prod", DeploymentName: "dbc-abc", ID: 1}
	acct := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acc-1"}

	off, err := workspaceResource(ctx, ws, acct, false)
	if err != nil {
		t.Fatalf("workspaceResource: %v", err)
	}
	if slices.Contains(childResourceTypeIDs(t, off), catalogResourceType.Id) {
		t.Fatalf("catalog child present when UC sync disabled")
	}

	on, err := workspaceResource(ctx, ws, acct, true)
	if err != nil {
		t.Fatalf("workspaceResource: %v", err)
	}
	if !slices.Contains(childResourceTypeIDs(t, on), catalogResourceType.Id) {
		t.Fatalf("catalog child missing when UC sync enabled")
	}

	catParent := &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: makeSecurableID("dbc-abc", "prod_evaluation")}
	schema := &databricks.Schema{Name: "reports", FullName: "prod_evaluation.reports"}

	noTables, err := schemaResource("dbc-abc", schema, catParent, false)
	if err != nil {
		t.Fatalf("schemaResource: %v", err)
	}
	if slices.Contains(childResourceTypeIDs(t, noTables), tableResourceType.Id) {
		t.Fatalf("table child present when table sync disabled")
	}

	withTables, err := schemaResource("dbc-abc", schema, catParent, true)
	if err != nil {
		t.Fatalf("schemaResource: %v", err)
	}
	if !slices.Contains(childResourceTypeIDs(t, withTables), tableResourceType.Id) {
		t.Fatalf("table child missing when table sync enabled")
	}
}
