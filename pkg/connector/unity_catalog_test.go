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

	uc := newUnityCatalog(client, "app-1", nil)
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

// routingHandler answers the three account-plane calls buildRouting makes. Each
// workspace's metastore assignment is whatever assignments names it, and a status
// code there stands in for a credential that cannot read that workspace.
func routingHandler(t *testing.T, assignments map[string]any, probed map[string]int) http.HandlerFunc {
	t.Helper()

	return routingHandlerWithCatalogs(t, assignments, nil, probed)
}

// catalogStatus, keyed by deployment name, makes the catalog listing fail for a
// workspace, which is how a credential that is not a member of it answers.
func routingHandlerWithCatalogs(
	t *testing.T,
	assignments map[string]any,
	catalogStatus map[string]int,
	probed map[string]int,
) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/metastores"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metastores": []map[string]any{
					{"metastore_id": "ms-a", "name": "main"},
					{"metastore_id": "ms-b", "name": "other"},
				},
			})
		case strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
			})
		case strings.HasSuffix(path, "/metastore"):
			parts := strings.Split(path, "/")
			workspaceID := parts[len(parts)-2]
			probed[workspaceID]++
			switch assigned := assignments[workspaceID].(type) {
			case int:
				http.Error(w, `{"message":"denied"}`, assigned)
			case string:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"metastore_assignment": map[string]any{"metastore_id": assigned},
				})
			default:
				http.Error(w, `{"message":"missing"}`, http.StatusNotFound)
			}
		default:
			workspace := r.Header.Get("X-Test-Workspace")
			probed[workspace]++
			if status, ok := catalogStatus[workspace]; ok {
				http.Error(w, `{"message":"denied"}`, status)

				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"catalogs": []map[string]any{}})
		}
	}
}

// The allowlist is what the operator scoped the sync to, but ListWorkspaces only
// applies the exclude list. Unity Catalog reads are addressed to a workspace, so
// routing has to filter too or the connector reads the account through a
// deployment that never appears in the sync.
func TestRoutingSkipsAWorkspaceOutsideTheConfiguredAllowlist(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a", "2": "ms-a"}, probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if probed["2"] != 0 {
		t.Errorf("the out-of-scope workspace was asked for its metastore assignment %d time(s)", probed["2"])
	}
	if want := []string{"wsa"}; !slices.Equal(snap.workspaces["ms-a"], want) {
		t.Errorf("ms-a routes through %v, want %v", snap.workspaces["ms-a"], want)
	}
}

// Scoping the sync to one workspace must not fail it for the metastores the other
// workspaces hold. A metastore only the excluded workspace can reach has to read
// as absent, the way a filtered catalog does: left in the snapshot it reaches
// usableWorkspaces, which answers FailedPrecondition, and that is not a warning
// to the SDK, so every metastore-enabled sync would fail.
func TestAMetastoreOnlyAnOutOfScopeWorkspaceReachesIsAbsentRatherThanFatal(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a", "2": "ms-b"}, probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if _, ok := snap.metastores["ms-a"]; !ok {
		t.Error("ms-a is missing from the snapshot, so the in-scope metastore would not sync")
	}
	if _, ok := snap.metastores["ms-b"]; ok {
		t.Error("ms-b is still in the snapshot; its Grants() would fail the whole sync with FailedPrecondition")
	}

	workspace, exists, _, err := workspaceFromSnapshot(snap, "ms-b", nil)
	if err != nil {
		t.Fatalf("resolving the out-of-scope metastore errored instead of reporting it absent: %v", err)
	}
	if exists || workspace != "" {
		t.Errorf("ms-b resolved to workspace %q (exists=%v), want absent", workspace, exists)
	}
}

// Dropping an unrouted metastore is only safe when the workspaces left out are
// the reason it is unrouted. An in-scope workspace that answered neither its
// assignment nor a catalog listing may be exactly the one serving it, so pruning
// there would report the metastore deleted and take its grants with it. Failing
// the sync is recoverable; a deletion is not.
func TestAnUnreadableInScopeWorkspaceStopsTheMetastoreFromBeingPruned(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandlerWithCatalogs(t,
		map[string]any{"1": "ms-a", "2": http.StatusForbidden},
		map[string]int{"wsb": http.StatusForbidden},
		probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod", "dev"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if !slices.Contains(snap.unreadable, "wsb") {
		t.Fatalf("unreadable = %v, want it to name wsb", snap.unreadable)
	}
	if _, ok := snap.metastores["ms-b"]; !ok {
		t.Fatal("ms-b was pruned while an in-scope workspace was unreadable, so C1 would delete it and its grants")
	}

	if _, _, _, err := workspaceFromSnapshot(snap, "ms-b", nil); err == nil {
		t.Error("ms-b resolved without an error, want the sync to fail loudly rather than report an absence")
	}
}

// The catalogs endpoint returns only the catalogs the caller may use, so an empty
// listing from a credential that was also refused the assignment read is equally
// explained by missing USE_CATALOG. That pair settles nothing, and treating it as
// "this workspace carries nothing" would prune a metastore it does serve. Only a
// 404 on the assignment plus an empty listing reads as outside Unity Catalog.
func TestADeniedAssignmentWithAnEmptyCatalogListingIsNotEvidenceOfNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		assignment   any
		wantPruned   bool
		wantUnreadab bool
	}{
		"denied assignment": {assignment: http.StatusForbidden, wantPruned: false, wantUnreadab: true},
		"no assignment":     {assignment: http.StatusNotFound, wantPruned: true, wantUnreadab: false},
	} {
		t.Run(name, func(t *testing.T) {
			probed := map[string]int{}
			client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
				map[string]any{"1": "ms-a", "2": tc.assignment}, probed))

			uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod", "dev"}))
			snap, _, err := uc.buildRouting(context.Background())
			if err != nil {
				t.Fatalf("buildRouting: %v", err)
			}

			if got := slices.Contains(snap.unreadable, "wsb"); got != tc.wantUnreadab {
				t.Errorf("unreadable names wsb = %v, want %v (unreadable = %v)", got, tc.wantUnreadab, snap.unreadable)
			}
			_, stillListed := snap.metastores["ms-b"]
			if pruned := !stillListed; pruned != tc.wantPruned {
				t.Errorf("ms-b pruned = %v, want %v", pruned, tc.wantPruned)
			}
		})
	}
}

// Only codes.NotFound is a warning to the SDK; every other code fails the whole
// sync. A credential that is not a member of one workspace answers 401 or 403
// there, which says nothing about the rest of the account, so letting it escape
// buildRouting would take down the resource types that do work.
func TestRoutingSetsAsideAWorkspaceTheCredentialCannotRead(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			probed := map[string]int{}
			client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
				map[string]any{"1": "ms-a", "2": status}, probed))

			uc := newUnityCatalog(client, "app-1", nil)
			snap, _, err := uc.buildRouting(context.Background())
			if err != nil {
				t.Fatalf("one unreadable workspace failed the whole snapshot: %v", err)
			}

			if want := []string{"wsa"}; !slices.Equal(snap.workspaces["ms-a"], want) {
				t.Errorf("ms-a routes through %v, want %v", snap.workspaces["ms-a"], want)
			}
			if !slices.Contains(snap.unusable, "wsb") {
				t.Errorf("unusable = %v, want it to name wsb", snap.unusable)
			}
		})
	}
}
