package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// Every workspace answers /api/2.1/unity-catalog/permissions with the same path
// and query, so a cache shared between hosts would misattribute privileges.
func TestPermissionsListingIsNotAnsweredByAnotherWorkspacesCachedPage(t *testing.T) {
	held := map[string]string{"wsa": "alice", "wsb": "bob"}

	client, router := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		workspace := r.Header.Get("X-Test-Workspace")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"privilege_assignments": []map[string]any{{
				"principal":  held[workspace],
				"privileges": []string{"CREATE_CATALOG"},
			}},
		})
	})

	ctx := context.Background()
	for _, workspace := range []string{"wsa", "wsb"} {
		assignments, _, _, err := client.ListPermissions(ctx, workspace, databricks.SecurableMetastore, "ms-1", "", "")
		if err != nil {
			t.Fatalf("ListPermissions(%s): %v", workspace, err)
		}

		if router.hits[workspace] == 0 {
			t.Errorf("workspace %s was never asked for its permissions; hits = %v", workspace, router.hits)
		}
		if len(assignments) != 1 || assignments[0].Principal != held[workspace] {
			t.Errorf("workspace %s answered %+v, want %s: a permissions page crossed between workspace hosts",
				workspace, assignments, held[workspace])
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

// routingHandler answers ListMetastores, ListWorkspaces, and
// ListMetastoreWorkspaces. assignments maps workspace id → metastore id (or an
// HTTP status that ListMetastoreWorkspaces should return for that metastore).
func routingHandler(t *testing.T, assignments map[string]any, probed map[string]int) http.HandlerFunc {
	t.Helper()

	return routingHandlerWith(t, assignments, probed, []map[string]any{
		{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
		{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
	})
}

// routingHandlerWith is routingHandler over a given workspace listing. Any other
// request counts as a probe of its workspace and answers 404, which is what the
// permissions endpoint returns for a metastore that is gone.
func routingHandlerWith(t *testing.T, assignments map[string]any, probed map[string]int, workspaces []map[string]any) http.HandlerFunc {
	t.Helper()

	byMetastore := map[string][]int{}
	statusByMetastore := map[string]int{}
	for workspaceID, assigned := range assignments {
		switch value := assigned.(type) {
		case int:
			statusByMetastore[workspaceID] = value
		case string:
			id, _ := strconv.Atoi(workspaceID)
			byMetastore[value] = append(byMetastore[value], id)
		}
	}

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
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			parts := strings.Split(strings.Trim(path, "/"), "/")
			metastoreID := parts[len(parts)-2]
			probed[metastoreID]++
			if status, ok := statusByMetastore[metastoreID]; ok {
				http.Error(w, `{"message":"denied"}`, status)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": byMetastore[metastoreID]})
		case strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode(workspaces)
		default:
			workspace := r.Header.Get("X-Test-Workspace")
			probed[workspace]++
			http.Error(w, `{"error_code":"METASTORE_DOES_NOT_EXIST","message":"not found"}`, http.StatusNotFound)
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

	if want := []string{"wsa"}; !slices.Equal(snap.workspaces["ms-a"], want) {
		t.Errorf("ms-a routes through %v, want %v", snap.workspaces["ms-a"], want)
	}
}

// Scoping the sync to one workspace must not fail it for the metastores the other
// workspaces hold. A metastore only the excluded workspace can reach is left out
// of scope, the way a filtered catalog is: left in the snapshot it reaches
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

	workspaces, state, err := usableWorkspaces(snap, "ms-b")
	if err != nil {
		t.Fatalf("resolving the out-of-scope metastore errored instead of reporting it out of scope: %v", err)
	}
	if state != metastoreOutOfScope || len(workspaces) != 0 {
		t.Errorf("ms-b resolved to workspaces %v (state=%v), want out of scope", workspaces, state)
	}
}

// Dropping an unrouted metastore is only safe when the workspaces left out are
// the reason it is unrouted. An unreadable assignment list may hide an in-scope
// workspace, and pruning would read as a deletion in C1.
func TestAnUnreadableMetastoreAssignmentListStopsTheMetastoreFromBeingPruned(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a", "ms-b": http.StatusForbidden}, probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod", "dev"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if !slices.Contains(snap.unreadable, "ms-b") {
		t.Fatalf("unreadable = %v, want it to name ms-b", snap.unreadable)
	}
	if _, ok := snap.metastores["ms-b"]; !ok {
		t.Fatal("ms-b was pruned while its workspace list was unreadable, so C1 would delete it and its grants")
	}

	if _, _, err := usableWorkspaces(snap, "ms-b"); err == nil {
		t.Error("ms-b resolved without an error, want the sync to fail loudly rather than report an absence")
	}
}

// A metastore whose readable assignment list names no in-scope workspace is out
// of scope under an allowlist.
func TestAMetastoreMissingFromEveryAssignmentListIsPrunedUnderAnAllowlist(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a"}, probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod", "dev"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if _, ok := snap.metastores["ms-b"]; ok {
		t.Fatal("ms-b is still in the snapshot even though no workspace is assigned to it")
	}
}

// Only codes.NotFound is a warning to the SDK; every other code fails the whole
// sync. A credential that is not a member of one workspace answers 401 or 403
// there, which says nothing about the rest of the account, so letting it escape
// buildRouting would take down the resource types that do work.
func TestRoutingSetsAsideAMetastoreWhoseAssignmentListCannotBeRead(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			probed := map[string]int{}
			client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
				map[string]any{"1": "ms-a", "ms-b": status}, probed))

			uc := newUnityCatalog(client, "app-1", nil)
			snap, _, err := uc.buildRouting(context.Background())
			if err != nil {
				t.Fatalf("one unreadable metastore failed the whole snapshot: %v", err)
			}

			if want := []string{"wsa"}; !slices.Equal(snap.workspaces["ms-a"], want) {
				t.Errorf("ms-a routes through %v, want %v", snap.workspaces["ms-a"], want)
			}
			if !slices.Contains(snap.unreadable, "ms-b") {
				t.Errorf("unreadable = %v, want it to name ms-b", snap.unreadable)
			}
		})
	}
}

// A PROVISIONING workspace may serve the unrouted metastores once RUNNING, so it
// blocks the prune like an unreadable assignment list.
func TestANotRunningInScopeWorkspaceStopsTheMetastoreFromBeingPruned(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandlerWith(t,
		map[string]any{"1": "ms-a", "2": "ms-b"}, probed, []map[string]any{
			{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
			{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "PROVISIONING"},
		}))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod", "dev"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if !slices.Equal(snap.pending["ms-b"], []string{"wsb (PROVISIONING)"}) {
		t.Fatalf("pending[ms-b] = %v, want [wsb (PROVISIONING)]", snap.pending["ms-b"])
	}
	if _, ok := snap.metastores["ms-b"]; !ok {
		t.Fatal("ms-b was pruned while an in-scope workspace was still PROVISIONING, so C1 would delete it and its grants")
	}
	if _, _, err := usableWorkspaces(snap, "ms-b"); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "PROVISIONING") {
		t.Errorf("resolving ms-b = %v, want FailedPrecondition naming PROVISIONING", err)
	}
}

// CANCELLING is in the documented workspace status set and means the cancellation
// is still running. Pruning it would delete the metastore before that finishes.
func TestACancellingWorkspaceStopsTheMetastoreFromBeingPruned(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandlerWith(t,
		map[string]any{"1": "ms-a", "2": "ms-b"}, probed, []map[string]any{
			{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
			{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "CANCELLING"},
		}))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod", "dev"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}

	if !slices.Equal(snap.pending["ms-b"], []string{"wsb (CANCELLING)"}) {
		t.Fatalf("pending[ms-b] = %v, want [wsb (CANCELLING)]", snap.pending["ms-b"])
	}
	if _, ok := snap.metastores["ms-b"]; !ok {
		t.Fatal("ms-b was pruned while its only in-scope workspace was CANCELLING, so C1 would delete it and its grants")
	}
	if _, _, err := usableWorkspaces(snap, "ms-b"); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "CANCELLING") {
		t.Errorf("resolving ms-b = %v, want FailedPrecondition naming CANCELLING", err)
	}
}

// An allowlist that names no workspace on the account is a configuration error,
// not an account with no metastores: pruning would delete every metastore in C1.
func TestAnAllowlistMatchingNoWorkspaceFailsInsteadOfPruningEverything(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a", "2": "ms-b"}, probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"does-not-exist"}))
	_, _, err := uc.buildRouting(context.Background())
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("buildRouting = %v, want FailedPrecondition", err)
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("error %q does not name the allowlist", err)
	}
}

// Scoping the sync to a workspace that is not on Unity Catalog leaves every
// metastore out of scope. That is an empty result, not a failed sync.
func TestAnAllowlistedWorkspaceWithoutUnityCatalogPrunesTheMetastores(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a"}, probed))

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"dev"}))
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}
	if len(snap.metastores) != 0 {
		t.Fatalf("metastores = %v, want none: no allowlisted workspace is attached to any", snap.metastores)
	}
}

// Without an allowlist, a metastore with no usable workspace would still fail its
// Grants with FailedPrecondition and, with it, the whole sync.
func TestAnUnroutableMetastoreIsPrunedWithoutAnAllowlist(t *testing.T) {
	tests := []struct {
		name        string
		assignments map[string]any
	}{
		{"no workspace assigned", map[string]any{"1": "ms-a"}},
		{"only an excluded workspace assigned", map[string]any{"1": "ms-a", "3": "ms-b"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probed := map[string]int{}
			client, _ := newWorkspaceRoutedClient(t, routingHandler(t, test.assignments, probed))

			uc := newUnityCatalog(client, "app-1", nil)
			snap, _, err := uc.buildRouting(context.Background())
			if err != nil {
				t.Fatalf("buildRouting: %v", err)
			}
			if _, ok := snap.metastores["ms-a"]; !ok {
				t.Error("ms-a is missing from the snapshot")
			}
			if _, ok := snap.metastores["ms-b"]; ok {
				t.Error("ms-b is still in the snapshot; its Grants would fail the whole sync with FailedPrecondition")
			}
		})
	}
}

// Admin on any attached workspace is enough, so Grants tries the next workspace
// before reporting PermissionDenied.
func TestGrantsRoutesThroughTheFirstWorkspaceWhereTheProbePasses(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var permissionsHosts []string
	client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/workspaces/1/permissionassignments"):
			_ = json.NewEncoder(w).Encode(map[string]any{"permission_assignments": []any{}})
		case strings.HasSuffix(path, "/workspaces/2/permissionassignments"):
			_ = json.NewEncoder(w).Encode(map[string]any{"permission_assignments": []map[string]any{{
				"principal":   map[string]any{"service_principal_name": "app-1", "principal_id": 7},
				"permissions": []string{"ADMIN"},
			}}})
		case strings.HasSuffix(path, "/metastores"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metastores": []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "someone-else"}},
			})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1, 2}})
		case strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
			})
		case strings.Contains(path, "/unity-catalog/permissions/"):
			permissionsHosts = append(permissionsHosts, r.Header.Get("X-Test-Workspace"))
			_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": []any{}})
		default:
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	})
	m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})

	ctx := context.Background()
	account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	metastore, err := metastoreResource(ctx, &databricks.Metastore{MetastoreID: "ms-a"}, account, "", "", "")
	if err != nil {
		t.Fatalf("metastoreResource: %v", err)
	}

	if _, _, err := m.Grants(ctx, metastore, rs.SyncOpAttrs{}); err != nil {
		t.Fatalf("Grants: %v", err)
	}
	if !slices.Equal(permissionsHosts, []string{"wsb"}) {
		t.Errorf("permissions read through %v, want [wsb], the workspace where the connector is Admin", permissionsHosts)
	}
}

// Databricks does not document a 404 on the assignment list, so it must not be
// read as a deletion: pruning on it would make C1 delete the metastore's grants.
func TestA404OnTheAssignmentListIsUnknownRatherThanDeleted(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a", "ms-b": http.StatusNotFound}, probed))

	uc := newUnityCatalog(client, "app-1", nil)
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}
	if _, ok := snap.metastores["ms-b"]; !ok {
		t.Error("ms-b was dropped after its assignment list answered 404, so C1 would delete it and its grants")
	}
	if !slices.Contains(snap.unreadable, "ms-b") {
		t.Errorf("unreadable = %v, want it to name ms-b", snap.unreadable)
	}
}

// The profile workspace says nothing about whether the metastore still exists;
// without asking, Revoke on a deleted metastore fails on every retry.
func TestProvisioningOnADeletedMetastoreDoesNotTrustTheProfile(t *testing.T) {
	tests := []struct {
		name   string
		change func(ctx context.Context, m *metastoreBuilder, principal *v2.Resource, g *v2.Grant) (annotations.Annotations, error)
		code   codes.Code
	}{
		{"revoke", func(ctx context.Context, m *metastoreBuilder, _ *v2.Resource, g *v2.Grant) (annotations.Annotations, error) {
			return m.Revoke(ctx, g)
		}, codes.OK},
		{"grant", func(ctx context.Context, m *metastoreBuilder, principal *v2.Resource, g *v2.Grant) (annotations.Annotations, error) {
			return m.Grant(ctx, principal, g.GetEntitlement())
		}, codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probed := map[string]int{}
			client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
				map[string]any{"1": "ms-a", "2": "ms-b"}, probed))
			m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})

			ctx := context.Background()
			account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			metastore, err := metastoreResource(ctx, &databricks.Metastore{MetastoreID: "ms-gone", Owner: "app-1"}, account, "wsa", "1", "prod")
			if err != nil {
				t.Fatalf("metastoreResource: %v", err)
			}
			principal := &v2.Resource{Id: &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "1"}}
			g := grant.NewGrant(metastore, "CREATE_CATALOG", principal.GetId())

			annos, err := test.change(ctx, m, principal, g)
			if status.Code(err) != test.code {
				t.Fatalf("err = %v, want %s", err, test.code)
			}
			if test.code == codes.OK && !annos.Contains(&v2.GrantAlreadyRevoked{}) {
				t.Error("revoke on a deleted metastore did not report GrantAlreadyRevoked")
			}
			if probed["wsa"] != 0 {
				t.Errorf("the profile workspace was asked %d time(s) about a metastore the account no longer lists", probed["wsa"])
			}
		})
	}
}

// A pruned metastore still holds its privileges, so a revoke cannot be reported
// done, and a grant has no workspace this sync may address.
func TestProvisioningOnAnOutOfScopeMetastoreFailsWithoutWriting(t *testing.T) {
	tests := []struct {
		name   string
		change func(ctx context.Context, m *metastoreBuilder, principal *v2.Resource, g *v2.Grant) (annotations.Annotations, error)
	}{
		{"revoke", func(ctx context.Context, m *metastoreBuilder, _ *v2.Resource, g *v2.Grant) (annotations.Annotations, error) {
			return m.Revoke(ctx, g)
		}},
		{"grant", func(ctx context.Context, m *metastoreBuilder, principal *v2.Resource, g *v2.Grant) (annotations.Annotations, error) {
			return m.Grant(ctx, principal, g.GetEntitlement())
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probed := map[string]int{}
			var writes []string
			routing := routingHandler(t, map[string]any{"1": "ms-a", "3": "ms-b"}, probed)
			client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes = append(writes, r.Method+" "+r.URL.Path)
				}
				routing(w, r)
			})
			m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})

			ctx := context.Background()
			account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			metastore, err := metastoreResource(ctx, &databricks.Metastore{MetastoreID: "ms-b", Owner: "app-1"}, account, "wsb", "2", "dev")
			if err != nil {
				t.Fatalf("metastoreResource: %v", err)
			}
			principal := &v2.Resource{Id: &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "1"}}
			g := grant.NewGrant(metastore, "CREATE_CATALOG", principal.GetId())

			annos, err := test.change(ctx, m, principal, g)
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("err = %v, want FailedPrecondition", err)
			}
			if !strings.Contains(err.Error(), "ms-b") || !strings.Contains(err.Error(), "outside the sync scope") {
				t.Errorf("error %q does not name the metastore as outside the sync scope", err)
			}
			if annos.Contains(&v2.GrantAlreadyRevoked{}) {
				t.Error("an out-of-scope metastore was reported as already revoked while it still holds the privilege")
			}
			if len(writes) != 0 {
				t.Errorf("sent %v to an out-of-scope metastore, want no write", writes)
			}
			if probed["wsa"] != 0 || probed["wsb"] != 0 {
				t.Errorf("a workspace was asked about an out-of-scope metastore: %v", probed)
			}
		})
	}
}

// List leaves a pruned metastore out, so Grants on it is an empty page, not an error.
func TestGrantsOnAnOutOfScopeMetastoreIsEmpty(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t, map[string]any{"1": "ms-a", "3": "ms-b"}, probed))
	m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})

	ctx := context.Background()
	account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	metastore, err := metastoreResource(ctx, &databricks.Metastore{MetastoreID: "ms-b"}, account, "", "", "")
	if err != nil {
		t.Fatalf("metastoreResource: %v", err)
	}

	grants, _, err := m.Grants(ctx, metastore, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}
	if len(grants) != 0 {
		t.Errorf("Grants = %v, want none", grants)
	}
}

// A profile workspace the credential can no longer read, or one the allowlist
// now leaves out, is a stale hint: Grants rebuilds the routing instead of failing.
func TestGrantsFallsBackWhenTheProfileWorkspaceCannotBeUsed(t *testing.T) {
	tests := []struct {
		name       string
		workspaces []string
		profileWS  string
		profileID  string
	}{
		{"probe answers 403", nil, "wsb", "2"},
		{"workspace left out of the allowlist", []string{"prod"}, "wsb", "2"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("BATON_HTTP_CACHE_TTL", "0")

			var permissionsHosts []string
			client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				path := r.URL.Path
				switch {
				case strings.HasSuffix(path, "/permissionassignments"):
					http.Error(w, `{"message":"forbidden"}`, http.StatusForbidden)
				case strings.HasSuffix(path, "/metastores"):
					_ = json.NewEncoder(w).Encode(map[string]any{
						"metastores": []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "app-1"}},
					})
				case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1, 2}})
				case strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode([]map[string]any{
						{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
						{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
					})
				case strings.Contains(path, "/unity-catalog/permissions/"):
					permissionsHosts = append(permissionsHosts, r.Header.Get("X-Test-Workspace"))
					_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": []any{}})
				default:
					http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
				}
			})
			m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", configuredWorkspaceSet(test.workspaces)), syncPrincipals{})

			ctx := context.Background()
			account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			metastore, err := metastoreResource(ctx, &databricks.Metastore{MetastoreID: "ms-a"}, account, test.profileWS, test.profileID, "dev")
			if err != nil {
				t.Fatalf("metastoreResource: %v", err)
			}

			if _, _, err := m.Grants(ctx, metastore, rs.SyncOpAttrs{}); err != nil {
				t.Fatalf("Grants: %v", err)
			}
			if !slices.Equal(permissionsHosts, []string{"wsa"}) {
				t.Errorf("permissions read through %v, want [wsa] from the rebuilt routing", permissionsHosts)
			}
		})
	}
}

func TestAFailedWorkspaceDoesNotKeepItsMetastorePending(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandlerWith(t,
		map[string]any{"1": "ms-a", "2": "ms-b"}, probed, []map[string]any{
			{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
			{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "FAILED"},
		}))

	uc := newUnityCatalog(client, "app-1", nil)
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}
	if _, ok := snap.metastores["ms-b"]; ok {
		t.Fatal("ms-b stayed in the snapshot while its only workspace is FAILED, so every sync would fail")
	}
	if len(snap.pending["ms-b"]) != 0 {
		t.Fatalf("pending[ms-b] = %v, want none", snap.pending["ms-b"])
	}
	if _, ok := snap.metastores["ms-a"]; !ok {
		t.Fatal("ms-a was dropped along with the FAILED workspace's metastore")
	}
}

func TestAnAllowlistThatIsAlsoExcludedNamesTheExcludeList(t *testing.T) {
	probed := map[string]int{}
	handler := routingHandler(t, map[string]any{"1": "ms-a", "2": "ms-b"}, probed)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	router := &workspaceRouter{target: strings.TrimPrefix(srv.URL, "http://"), hits: map[string]int{}}
	client, err := databricks.NewClient(context.Background(), &http.Client{Transport: router},
		testHostname, testAccountHost, testAccountId, "", &databricks.NoAuth{}, []string{"prod"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	uc := newUnityCatalog(client, "app-1", configuredWorkspaceSet([]string{"prod"}))
	_, _, err = uc.buildRouting(context.Background())
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("buildRouting = %v, want FailedPrecondition", err)
	}
	if !strings.Contains(err.Error(), "databricks-exclude-workspaces") || strings.Contains(err.Error(), "not on the account") {
		t.Errorf("error %q does not say the allowlisted workspace was excluded", err)
	}
}

func TestAnUnreadableAssignmentListNamesThatFailure(t *testing.T) {
	probed := map[string]int{}
	client, _ := newWorkspaceRoutedClient(t, routingHandler(t,
		map[string]any{"1": "ms-a", "ms-b": http.StatusForbidden}, probed))

	uc := newUnityCatalog(client, "app-1", nil)
	snap, _, err := uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}
	_, _, err = usableWorkspaces(snap, "ms-b")
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "assignment list") {
		t.Fatalf("resolving ms-b = %v, want FailedPrecondition about the assignment list", err)
	}
	if strings.Contains(err.Error(), "databricks-exclude-workspaces") {
		t.Errorf("error %q sends the operator to check the exclude list", err)
	}
}

func TestAPermissions404WithoutTheGoneCodeIsNotAWarning(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	tests := []struct {
		name string
		body string
		code codes.Code
	}{
		{"routing miss", `{"message":"nope"}`, codes.FailedPrecondition},
		// The account still lists the metastore, so the error code alone is not absence.
		{"still listed", `{"error_code":"METASTORE_DOES_NOT_EXIST","message":"No metastore assigned for the current workspace"}`, codes.FailedPrecondition},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				path := r.URL.Path
				switch {
				case strings.HasSuffix(path, "/metastores"):
					_ = json.NewEncoder(w).Encode(map[string]any{
						"metastores": []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "app-1"}},
					})
				case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1}})
				case strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode([]map[string]any{
						{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
					})
				case strings.Contains(path, "/unity-catalog/permissions/"):
					http.Error(w, test.body, http.StatusNotFound)
				default:
					http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
				}
			})
			m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})
			account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			metastore, err := metastoreResource(context.Background(), &databricks.Metastore{MetastoreID: "ms-a", Owner: "app-1"}, account, "", "", "")
			if err != nil {
				t.Fatalf("metastoreResource: %v", err)
			}

			_, _, err = m.Grants(context.Background(), metastore, rs.SyncOpAttrs{})
			if status.Code(err) != test.code {
				t.Fatalf("Grants = %v, want %s", err, test.code)
			}
		})
	}
}

func TestListRecordsTheWorkspaceWhereTheProbePasses(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/workspaces/1/permissionassignments"):
			_ = json.NewEncoder(w).Encode(map[string]any{"permission_assignments": []any{}})
		case strings.HasSuffix(path, "/workspaces/2/permissionassignments"):
			_ = json.NewEncoder(w).Encode(map[string]any{"permission_assignments": []map[string]any{{
				"principal":   map[string]any{"service_principal_name": "app-1", "principal_id": 7},
				"permissions": []string{"ADMIN"},
			}}})
		case strings.HasSuffix(path, "/metastores"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metastores": []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "someone-else"}},
			})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1, 2}})
		case strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
			})
		default:
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	})
	m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})
	account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
	resources, _, err := m.List(context.Background(), account, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("List returned %d resources, want 1", len(resources))
	}
	workspace, _ := rs.GetProfileStringValue(rs.GetProfile(resources[0]), profileKeyWorkspace)
	if workspace != "wsb" {
		t.Errorf("profile workspace = %q, want wsb, the workspace where the connector is Admin", workspace)
	}
}

func TestMetastoreDoesNotExistIsAbsenceOnlyWhenTheAccountDropsIt(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	tests := []struct {
		name   string
		listed bool
		want   codes.Code
	}{
		{"account still lists it", true, codes.FailedPrecondition},
		{"account dropped it", false, codes.OK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				path := r.URL.Path
				switch {
				case strings.HasSuffix(path, "/metastores"):
					metastores := []map[string]any{}
					if test.listed {
						metastores = append(metastores, map[string]any{"metastore_id": "ms-a", "name": "main", "owner": "app-1"})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metastores": metastores})
				case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1}})
				case strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode([]map[string]any{
						{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
					})
				case strings.Contains(path, "/unity-catalog/permissions/"):
					http.Error(w, `{"error_code":"METASTORE_DOES_NOT_EXIST","message":"No metastore assigned for the current workspace"}`, http.StatusNotFound)
				default:
					http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
				}
			})
			m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})
			account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			metastore, err := metastoreResource(context.Background(), &databricks.Metastore{MetastoreID: "ms-a", Owner: "app-1"}, account, "wsa", "1", "prod")
			if err != nil {
				t.Fatalf("metastoreResource: %v", err)
			}

			_, _, err = m.Grants(context.Background(), metastore, rs.SyncOpAttrs{})
			if status.Code(err) != test.want {
				t.Fatalf("Grants = %v, want %s", err, test.want)
			}
		})
	}
}

func TestListSkipsAProfileProbeThatIsNotARefusal(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	tests := []struct {
		name      string
		firstCode int
		want      string
	}{
		{"one workspace returns 500", http.StatusInternalServerError, "wsb"},
		{"every probe fails", http.StatusInternalServerError, "wsa"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				path := r.URL.Path
				switch {
				case strings.HasSuffix(path, "/workspaces/1/permissionassignments"):
					http.Error(w, `{"message":"unavailable"}`, test.firstCode)
				case strings.HasSuffix(path, "/workspaces/2/permissionassignments"):
					if test.want == "wsa" {
						http.Error(w, `{"message":"unavailable"}`, http.StatusInternalServerError)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"permission_assignments": []map[string]any{{
						"principal":   map[string]any{"service_principal_name": "app-1", "principal_id": 7},
						"permissions": []string{"ADMIN"},
					}}})
				case strings.HasSuffix(path, "/metastores"):
					_ = json.NewEncoder(w).Encode(map[string]any{
						"metastores": []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "someone-else"}},
					})
				case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1, 2}})
				case strings.HasSuffix(path, "/workspaces"):
					_ = json.NewEncoder(w).Encode([]map[string]any{
						{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
						{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
					})
				default:
					http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
				}
			})
			m := newMetastoreBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})
			account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: testAccountId}
			resources, _, err := m.List(context.Background(), account, rs.SyncOpAttrs{})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			workspace, _ := rs.GetProfileStringValue(rs.GetProfile(resources[0]), profileKeyWorkspace)
			if workspace != test.want {
				t.Errorf("profile workspace = %q, want %q", workspace, test.want)
			}
		})
	}
}
