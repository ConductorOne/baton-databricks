package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// workspaceRouter dispatches on the deployment-name label of the request host,
// the way Databricks routes an account whose workspaces sit on separate hosts.
type workspaceRouter struct {
	target string
	hits   map[string]int
}

func (r *workspaceRouter) RoundTrip(req *http.Request) (*http.Response, error) {
	workspace, _, _ := strings.Cut(req.URL.Host, ".")
	r.hits[workspace]++

	routed := req.Clone(req.Context())
	routed.URL.Scheme = "http"
	routed.URL.Host = r.target
	routed.Header.Set("X-Test-Workspace", workspace)

	return http.DefaultTransport.RoundTrip(routed)
}

func newWorkspaceRoutedClient(t *testing.T, handler http.HandlerFunc) (*databricks.Client, *workspaceRouter) {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	router := &workspaceRouter{target: strings.TrimPrefix(srv.URL, "http://"), hits: map[string]int{}}
	client, err := databricks.NewClient(context.Background(), &http.Client{Transport: router},
		testHostname, testAccountHost, testAccountId, "", &databricks.NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	return client, router
}

// These run at the cache's default settings on purpose: BATON_HTTP_CACHE_TTL is
// deliberately not set, because turning the cache off is what hid this and a test
// that turns it off guards nothing.

// Every workspace answers /api/2.1/unity-catalog/catalogs with the same path and
// query, so a cache entry shared between hosts files a workspace under a metastore
// it cannot reach and routes every securable read below it to the wrong host.
func TestWorkspaceMetastoreLookupIsNotAnsweredByAnotherWorkspacesCachedListing(t *testing.T) {
	served := map[string]string{"wsa": "ms-a", "wsb": "ms-b"}

	client, router := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		workspace := r.Header.Get("X-Test-Workspace")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"catalogs": []map[string]any{{
				"name":         "main",
				"metastore_id": served[workspace],
			}},
		})
	})

	uc := newUnityCatalog(client, "app-1")
	ctx := context.Background()

	for _, workspace := range []string{"wsa", "wsb"} {
		metastoreIDs, _, err := uc.metastoresServedBy(ctx, workspace)
		if err != nil {
			t.Fatalf("metastoresServedBy(%s): %v", workspace, err)
		}

		if router.hits[workspace] == 0 {
			t.Errorf("workspace %s was never asked for its catalogs; hits = %v", workspace, router.hits)
		}
		if want := []string{served[workspace]}; !slices.Equal(metastoreIDs, want) {
			t.Errorf("workspace %s serves %v, want %v: a catalog listing crossed between workspace hosts",
				workspace, metastoreIDs, want)
		}
	}
}

// `principal` holds a userName, a displayName or an applicationId with no type
// discriminator, so falling back to the name when the id does not resolve
// attributes the grant to whichever principal happens to share the string.
func TestLookupDoesNotFallBackFromAnUnknownID(t *testing.T) {
	t.Parallel()

	index := &principalIndex{
		byID: map[string]*v2.ResourceId{
			"10": {ResourceType: "user", Resource: "10"},
		},
		byName: map[string]*v2.ResourceId{
			"shared": {ResourceType: "user", Resource: "10"},
		},
	}

	if _, ok := index.lookup(databricks.PrivilegeAssignment{
		Principal: "shared", PrincipalID: json.Number("99"),
	}); ok {
		t.Fatal("an unknown principal id must not resolve through a shared name")
	}

	got, ok := index.lookup(databricks.PrivilegeAssignment{
		Principal: "shared", PrincipalID: json.Number("10"),
	})
	if !ok || got.GetResource() != "10" {
		t.Fatalf("known id lookup = %v %v", got, ok)
	}
}
