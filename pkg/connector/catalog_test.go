package connector

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// fakeCatalogAccount is one metastore (ms-1) served by two RUNNING workspaces,
// wsa (id 1) and wsb (id 2). Each field shapes one answer; the counters record
// what the connector asked.
type fakeCatalogAccount struct {
	metastoreOwner string
	attached       []int
	// listed maps a workspace to the catalogs its listing returns.
	listed map[string][]map[string]any
	// listStatus fails a workspace's catalog listing with that status.
	listStatus map[string]int
	// unbound is what include_unbound=true returns.
	unbound []map[string]any
	// manage names the catalogs the connector principal holds MANAGE on.
	manage map[string]bool

	routing          int
	catalogPages     int
	patches          int
	patchBodies      []string
	permissionsHosts []string
	// assignments is what an unfiltered permissions read returns per catalog.
	assignments map[string][]map[string]any
	// echo is what a PATCH returns; nil echoes assignments.
	echo        []map[string]any
	patchStatus int
	// users, groups and servicePrincipals are account SCIM, keyed by id. The value
	// is the name a privilege assignment carries: userName, displayName, applicationId.
	users             map[string]string
	groups            map[string]string
	servicePrincipals map[string]string
}

// scim answers account SCIM by id (/Users/{id}) and by filter (id or name eq).
func (f *fakeCatalogAccount) scim(w http.ResponseWriter, r *http.Request) bool {
	kinds := []struct {
		segment, nameField string
		byID               map[string]string
	}{
		{"/scim/v2/Users", "userName", f.users},
		{"/scim/v2/Groups", "displayName", f.groups},
		{"/scim/v2/ServicePrincipals", "applicationId", f.servicePrincipals},
	}
	for _, kind := range kinds {
		idx := strings.Index(r.URL.Path, kind.segment)
		if idx < 0 {
			continue
		}
		rest := strings.TrimPrefix(r.URL.Path[idx+len(kind.segment):], "/")
		if rest != "" {
			name, ok := kind.byID[rest]
			if !ok {
				http.Error(w, `{"detail":"not found"}`, http.StatusNotFound)
				return true
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": rest, kind.nameField: name})
			return true
		}

		filter := r.URL.Query().Get("filter")
		var found []map[string]any
		for id, name := range kind.byID {
			if strings.Contains(filter, strconv.Quote(id)) || strings.Contains(filter, strconv.Quote(name)) {
				found = append(found, map[string]any{"id": id, kind.nameField: name})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Resources": found, "totalResults": len(found)})
		return true
	}

	return false
}

func (f *fakeCatalogAccount) handler(t *testing.T) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		workspace := r.Header.Get("X-Test-Workspace")
		if f.scim(w, r) {
			return
		}

		switch {
		case strings.HasSuffix(path, "/metastores"):
			f.routing++
			_ = json.NewEncoder(w).Encode(map[string]any{"metastores": []map[string]any{
				{"metastore_id": "ms-1", "name": "main", "owner": f.metastoreOwner},
			}})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": f.attached})
		case strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
			})
		case strings.HasSuffix(path, "/unity-catalog/catalogs"):
			if r.URL.Query().Get("include_unbound") == "true" {
				_ = json.NewEncoder(w).Encode(map[string]any{"catalogs": f.unbound})
				return
			}
			f.catalogPages++
			if code, ok := f.listStatus[workspace]; ok {
				http.Error(w, `{"message":"denied"}`, code)
				return
			}
			all := f.listed[workspace]
			start, _ := strconv.Atoi(r.URL.Query().Get("page_token"))
			size, _ := strconv.Atoi(r.URL.Query().Get("max_results"))
			if size <= 0 || size > len(all)-start {
				size = len(all) - start
			}
			next := ""
			if start+size < len(all) {
				next = strconv.Itoa(start + size)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"catalogs": all[start : start+size], "next_page_token": next})
		case strings.Contains(path, "/unity-catalog/permissions/catalog/"):
			name := path[strings.LastIndex(path, "/")+1:]
			if r.Method == http.MethodPatch {
				f.patches++
				body, _ := io.ReadAll(r.Body)
				f.patchBodies = append(f.patchBodies, string(body))
				if f.patchStatus != 0 {
					http.Error(w, `{"message":"forbidden"}`, f.patchStatus)
					return
				}
				echo := f.echo
				if echo == nil {
					echo = f.assignments[name]
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": echo})
				return
			}
			if r.URL.Query().Get("principal") != "" {
				var assignments []map[string]any
				if f.manage[name] {
					assignments = []map[string]any{{"principal": "app-1", "privileges": []string{"MANAGE"}}}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": assignments})
				return
			}
			f.permissionsHosts = append(f.permissionsHosts, workspace)
			_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": f.assignments[name]})
		default:
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	}
}

func catalogEntry(name, owner string) map[string]any {
	return map[string]any{"name": name, "metastore_id": "ms-1", "owner": owner}
}

func newFakeCatalogBuilder(t *testing.T, f *fakeCatalogAccount, workspaces []string) *catalogBuilder {
	t.Helper()

	return newFakeCatalogBuilderSyncing(t, f, workspaces, syncPrincipals{})
}

func newFakeCatalogBuilderSyncing(t *testing.T, f *fakeCatalogAccount, workspaces []string, sync syncPrincipals) *catalogBuilder {
	t.Helper()
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newCatalogBuilder(client, newUnityCatalog(client, "app-1", configuredWorkspaceSet(workspaces)), sync)
}

// profiledCatalog is a catalog resource as List writes it, routed through wsa.
func profiledCatalog(t *testing.T, name, owner string) *v2.Resource {
	t.Helper()

	resource, err := newSecurableResource(catalogResourceType, securableTypeCatalog, nil, securable{
		name:  name,
		owner: owner,
		profile: map[string]any{
			profileKeyMetastoreOwner: "app-1",
			profileKeyWorkspace:      "wsa",
			profileKeyWorkspaceID:    "1",
			profileKeyWorkspaceName:  "prod",
		},
	}, securableRef{metastoreID: "ms-1", parts: []string{name}}, &v2.ResourceId{ResourceType: metastoreResourceType.Id, Resource: "ms-1"})
	if err != nil {
		t.Fatalf("newSecurableResource: %v", err)
	}

	return resource
}

func catalogProvisioningAccount() *fakeCatalogAccount {
	return &fakeCatalogAccount{
		metastoreOwner: "app-1",
		attached:       []int{1},
		listed:         map[string][]map[string]any{"wsa": {catalogEntry("main", "someone")}},
		users:          map[string]string{"1": "alice@example.com"},
		assignments:    map[string][]map[string]any{},
	}
}

func catalogGrantTo(t *testing.T, privilege string) (*v2.Resource, *v2.Grant) {
	t.Helper()

	principal := &v2.Resource{Id: &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "1"}}

	return principal, grant.NewGrant(profiledCatalog(t, "main", "someone"), privilege, principal.GetId())
}

// Databricks no-ops a privilege already held, so the pre-read is the only signal
// for GrantAlreadyExists, and the echo is the only proof the PATCH landed.
func TestCatalogGrantAddsThePrivilegeOnceAndConfirmsIt(t *testing.T) {
	f := catalogProvisioningAccount()
	f.echo = []map[string]any{{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"USE_CATALOG"}}}
	b := newFakeCatalogBuilder(t, f, nil)
	principal, g := catalogGrantTo(t, "USE_CATALOG")

	annos, err := b.Grant(context.Background(), principal, g.GetEntitlement())
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Error("a fresh grant reported GrantAlreadyExists")
	}
	if f.patches != 1 || !strings.Contains(f.patchBodies[0], `"add":["USE_CATALOG"]`) || !strings.Contains(f.patchBodies[0], "alice@example.com") {
		t.Fatalf("PATCH bodies = %v, want one add of USE_CATALOG for alice", f.patchBodies)
	}

	f.assignments["main"] = f.echo
	annos, err = b.Grant(context.Background(), principal, g.GetEntitlement())
	if err != nil {
		t.Fatalf("second Grant: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Error("granting a held privilege did not report GrantAlreadyExists")
	}
	if f.patches != 1 {
		t.Errorf("granting a held privilege sent a PATCH; patches = %d", f.patches)
	}
}

func TestCatalogGrantFailsWhenTheEchoDoesNotCarryThePrivilege(t *testing.T) {
	f := catalogProvisioningAccount()
	f.echo = []map[string]any{}
	b := newFakeCatalogBuilder(t, f, nil)
	principal, g := catalogGrantTo(t, "USE_CATALOG")

	_, err := b.Grant(context.Background(), principal, g.GetEntitlement())
	if status.Code(err) != codes.Internal {
		t.Fatalf("Grant = %v, want Internal", err)
	}
}

// A principal that is gone from SCIM still carries its numeric id on the row, so
// removal goes by principal_id, and an absent privilege is already revoked.
func TestCatalogRevokeRemovesByIDAndIsIdempotent(t *testing.T) {
	f := catalogProvisioningAccount()
	f.assignments["main"] = []map[string]any{{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"USE_CATALOG"}}}
	f.echo = []map[string]any{}
	b := newFakeCatalogBuilder(t, f, nil)
	_, g := catalogGrantTo(t, "USE_CATALOG")

	annos, err := b.Revoke(context.Background(), g)
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Error("revoking a held privilege reported GrantAlreadyRevoked")
	}
	if f.patches != 1 || !strings.Contains(f.patchBodies[0], `"principal_id":1`) {
		t.Fatalf("PATCH bodies = %v, want one removal by principal_id", f.patchBodies)
	}

	f.assignments["main"] = nil
	annos, err = b.Revoke(context.Background(), g)
	if err != nil {
		t.Fatalf("second Revoke: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Error("revoking an absent privilege did not report GrantAlreadyRevoked")
	}
	if f.patches != 1 {
		t.Errorf("revoking an absent privilege sent a PATCH; patches = %d", f.patches)
	}
}

func TestCatalogWriteRefusedByDatabricksNamesTheRemedy(t *testing.T) {
	f := catalogProvisioningAccount()
	f.patchStatus = http.StatusForbidden
	b := newFakeCatalogBuilder(t, f, nil)
	principal, g := catalogGrantTo(t, "USE_CATALOG")

	_, err := b.Grant(context.Background(), principal, g.GetEntitlement())
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), catalogTrustRemedy) {
		t.Fatalf("Grant = %v, want PermissionDenied naming %q", err, catalogTrustRemedy)
	}
}

func TestCatalogGrantRejectsWhatCannotBeProvisioned(t *testing.T) {
	for _, privilege := range []string{ownerEntitlement, "CREATE", "CREATE_CATALOG"} {
		t.Run(privilege, func(t *testing.T) {
			f := catalogProvisioningAccount()
			b := newFakeCatalogBuilder(t, f, nil)
			principal, g := catalogGrantTo(t, privilege)

			_, err := b.Grant(context.Background(), principal, g.GetEntitlement())
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Grant(%s) = %v, want InvalidArgument", privilege, err)
			}
			if f.patches != 0 {
				t.Errorf("Grant(%s) sent a PATCH", privilege)
			}
		})
	}
}

// A group's grant is expandable to its members; the owner's is immutable and only
// on the first page; a principal type the sync leaves out and a privilege the
// catalog does not offer are not emitted.
func TestCatalogGrantsEmission(t *testing.T) {
	f := catalogProvisioningAccount()
	f.groups = map[string]string{"11": "data-eng"}
	f.servicePrincipals = map[string]string{"21": "app-2"}
	f.assignments["main"] = []map[string]any{
		{"principal": "data-eng", "principal_id": 11, "privileges": []string{"USE_CATALOG", "NOT_A_PRIVILEGE"}},
		{"principal": "app-2", "principal_id": 21, "privileges": []string{"SELECT"}},
	}
	b := newFakeCatalogBuilderSyncing(t, f, nil, syncPrincipals{groups: true})

	grants, _, err := b.Grants(context.Background(), profiledCatalog(t, "main", "alice@example.com"), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}

	got := map[string]*v2.Grant{}
	for _, g := range grants {
		got[g.GetEntitlement().GetId()+" -> "+g.GetPrincipal().GetId().GetResourceType()] = g
	}
	if len(got) != 2 {
		t.Fatalf("grants = %v, want the group's USE_CATALOG and the owner", slices.Collect(maps.Keys(got)))
	}

	groupGrant, ok := got["catalog:ms-1::main:USE_CATALOG -> group"]
	if !ok {
		t.Fatalf("missing the group's USE_CATALOG grant; got %v", slices.Collect(maps.Keys(got)))
	}
	groupAnnos := annotations.Annotations(groupGrant.GetAnnotations())
	expandable := &v2.GrantExpandable{}
	if ok, err := groupAnnos.Pick(expandable); err != nil || !ok {
		t.Fatalf("the group's grant is not expandable (err %v)", err)
	}
	if len(expandable.GetEntitlementIds()) != 1 || !strings.HasSuffix(expandable.GetEntitlementIds()[0], ":"+groupMemberEntitlement) || expandable.GetShallow() {
		t.Errorf("GrantExpandable = %v, want the group's member entitlement, not shallow", expandable)
	}

	ownerGrant, ok := got["catalog:ms-1::main:owner -> user"]
	if !ok {
		t.Fatalf("missing the owner grant; got %v", slices.Collect(maps.Keys(got)))
	}
	ownerAnnos := annotations.Annotations(ownerGrant.GetAnnotations())
	if !ownerAnnos.Contains(&v2.GrantImmutable{}) {
		t.Error("the owner grant is not immutable")
	}

	next, _, err := b.Grants(context.Background(), profiledCatalog(t, "main", "alice@example.com"),
		rs.SyncOpAttrs{PageToken: pagination.Token{Token: "page-2"}})
	if err != nil {
		t.Fatalf("Grants page 2: %v", err)
	}
	for _, g := range next {
		if g.GetEntitlement().GetId() == "catalog:ms-1::main:owner" {
			t.Error("the owner grant was emitted again on a later page")
		}
	}
}

// listAllCatalogs drives List the way the SDK does, page token by page token.
func listAllCatalogs(t *testing.T, b *catalogBuilder) ([]*v2.Resource, error) {
	t.Helper()

	var all []*v2.Resource
	token := ""
	for range 1000 {
		resources, results, err := b.List(context.Background(), &v2.ResourceId{ResourceType: metastoreResourceType.Id, Resource: "ms-1"},
			rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			return all, err
		}
		all = append(all, resources...)
		if results == nil || results.NextPageToken == "" {
			return all, nil
		}
		token = results.NextPageToken
	}
	t.Fatal("List never returned an empty page token")

	return nil, nil
}

func manyCatalogs(prefix string, n int) []map[string]any {
	catalogs := make([]map[string]any, 0, n)
	for i := range n {
		catalogs = append(catalogs, catalogEntry(fmt.Sprintf("%s_%03d", prefix, i), "someone"))
	}

	return catalogs
}

// Every List page used to rebuild the routing and drain every workspace's catalogs.
// Paging per workspace reads each API page once and the routing once per metastore.
func TestCatalogListReadsEachWorkspacePageOnce(t *testing.T) {
	shared := manyCatalogs("shared", 120)
	f := &fakeCatalogAccount{
		attached: []int{1, 2},
		listed: map[string][]map[string]any{
			"wsa": shared,
			"wsb": append(slices.Clone(shared), manyCatalogs("only_b", 30)...),
		},
	}
	b := newFakeCatalogBuilder(t, f, nil)

	resources, err := listAllCatalogs(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	unique := map[string]struct{}{}
	for _, resource := range resources {
		unique[resource.GetId().GetResource()] = struct{}{}
	}
	t.Logf("resources emitted=%d unique=%d routing=%d catalog pages read=%d", len(resources), len(unique), f.routing, f.catalogPages)
	if len(unique) != 150 {
		t.Fatalf("unique catalogs = %d, want 150", len(unique))
	}
	if f.routing != 1 {
		t.Errorf("routing rebuilt %d times, want once per metastore", f.routing)
	}
	if want := 3 + 3; f.catalogPages != want {
		t.Errorf("catalog pages read = %d, want %d (each workspace's pages once)", f.catalogPages, want)
	}
}

// An ISOLATED catalog is listed only by the workspaces bound to it, so every usable
// workspace has to be asked, and the catalog has to keep the workspace that lists it.
func TestCatalogsAreUnionedAcrossTheWorkspacesThatListThem(t *testing.T) {
	f := &fakeCatalogAccount{
		attached: []int{1, 2},
		listed: map[string][]map[string]any{
			"wsa": {catalogEntry("shared", "someone"), catalogEntry("only_a", "someone")},
			"wsb": {catalogEntry("shared", "someone"), catalogEntry("only_b", "someone")},
		},
	}
	b := newFakeCatalogBuilder(t, f, nil)

	snap, _, err := b.uc.buildRouting(context.Background())
	if err != nil {
		t.Fatalf("buildRouting: %v", err)
	}
	facts, state, _, err := b.uc.catalogsOf(context.Background(), snap, "ms-1")
	if err != nil || state != securableInScope {
		t.Fatalf("catalogsOf = %v, %v", state, err)
	}

	want := map[string][]string{"shared": {"wsa", "wsb"}, "only_a": {"wsa"}, "only_b": {"wsb"}}
	for name, workspaces := range want {
		if got := facts[name].workspaces; !slices.Equal(got, workspaces) {
			t.Errorf("catalog %s is listed by %v, want %v", name, got, workspaces)
		}
	}
}

// Leaving a workspace out of the sync is the operator scoping its content out.
// A catalog only that workspace lists is not synced, the same rule as a metastore.
func TestACatalogOnlyAnOutOfScopeWorkspaceListsIsNotSynced(t *testing.T) {
	f := &fakeCatalogAccount{
		attached: []int{1, 2},
		listed: map[string][]map[string]any{
			"wsa": {catalogEntry("main", "someone")},
			"wsb": {catalogEntry("isolated_dev", "someone")},
		},
	}
	b := newFakeCatalogBuilder(t, f, []string{"prod"})

	resources, err := listAllCatalogs(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	names := make([]string, 0, len(resources))
	for _, resource := range resources {
		names = append(names, resource.GetDisplayName())
	}
	if !slices.Equal(names, []string{"main"}) {
		t.Errorf("listed %v, want only main", names)
	}
}

// A workspace whose catalog listing is refused leaves the catalog set unknown.
// Emitting what the other workspaces list would read as deleting the rest.
func TestAnUnreadableCatalogListingFailsInsteadOfEmittingAShortList(t *testing.T) {
	f := &fakeCatalogAccount{
		attached: []int{1, 2},
		listed:   map[string][]map[string]any{"wsa": {catalogEntry("main", "someone")}},
		listStatus: map[string]int{
			"wsb": http.StatusForbidden,
		},
	}
	b := newFakeCatalogBuilder(t, f, nil)

	_, err := listAllCatalogs(t, b)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("List = %v, want FailedPrecondition", err)
	}
}

// Databricks shows a catalog's full grant list only to the metastore admin, the
// owner, or a holder of MANAGE. Anyone else gets their own grants with a 200.
func TestCatalogGrantsNeedOwnershipOrManage(t *testing.T) {
	tests := []struct {
		name           string
		metastoreOwner string
		catalogOwner   string
		manage         bool
		wantCode       codes.Code
	}{
		{"metastore owner", "app-1", "someone", false, codes.OK},
		{"catalog owner", "someone", "app-1", false, codes.OK},
		{"MANAGE on the catalog", "someone", "someone", true, codes.OK},
		{"none of them", "someone", "someone", false, codes.PermissionDenied},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeCatalogAccount{
				metastoreOwner: test.metastoreOwner,
				attached:       []int{1},
				listed:         map[string][]map[string]any{"wsa": {catalogEntry("main", test.catalogOwner)}},
				manage:         map[string]bool{"main": test.manage},
			}
			b := newFakeCatalogBuilder(t, f, nil)

			resource := &v2.Resource{Id: &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: "ms-1::main"}}
			_, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("Grants = %v, want %s", err, test.wantCode)
			}
			if test.wantCode == codes.PermissionDenied && !strings.Contains(err.Error(), "MANAGE") {
				t.Errorf("error %q does not name the alternatives", err)
			}
		})
	}
}

// The workspace written on the catalog profile spares Grants the routing rebuild.
func TestCatalogGrantsReadThroughTheProfileWorkspace(t *testing.T) {
	f := &fakeCatalogAccount{metastoreOwner: "app-1", attached: []int{1}}
	b := newFakeCatalogBuilder(t, f, nil)

	ref := securableRef{metastoreID: "ms-1", parts: []string{"main"}}
	resource, err := newSecurableResource(catalogResourceType, securableTypeCatalog, nil, securable{
		name: "main",
		profile: map[string]any{
			profileKeyMetastoreOwner: "app-1",
			profileKeyWorkspace:      "wsa",
			profileKeyWorkspaceID:    "1",
			profileKeyWorkspaceName:  "prod",
		},
	}, ref, &v2.ResourceId{ResourceType: metastoreResourceType.Id, Resource: "ms-1"})
	if err != nil {
		t.Fatalf("newSecurableResource: %v", err)
	}

	if _, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{}); err != nil {
		t.Fatalf("Grants: %v", err)
	}
	if f.routing != 0 {
		t.Errorf("routing was rebuilt %d time(s) despite a usable profile workspace", f.routing)
	}
	if !slices.Equal(f.permissionsHosts, []string{"wsa"}) {
		t.Errorf("permissions read through %v, want [wsa]", f.permissionsHosts)
	}
}

// A catalog no in-scope workspace lists is either deleted or bound only to
// workspaces outside the sync. Revoke reports it gone only when include_unbound,
// read by the metastore owner, proves it; otherwise the privilege may still be held.
func TestRevokeOnAnUnlistedCatalogNeedsProofOfDeletion(t *testing.T) {
	tests := []struct {
		name           string
		metastoreOwner string
		unbound        []map[string]any
		wantCode       codes.Code
		wantRevoked    bool
	}{
		{"owner sees it is gone", "app-1", nil, codes.OK, true},
		{"owner sees it bound elsewhere", "app-1", []map[string]any{catalogEntry("isolated", "someone")}, codes.FailedPrecondition, false},
		{"not the owner, cannot tell", "someone", nil, codes.FailedPrecondition, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeCatalogAccount{
				metastoreOwner: test.metastoreOwner,
				attached:       []int{1},
				listed:         map[string][]map[string]any{"wsa": {catalogEntry("main", "someone")}},
				unbound:        test.unbound,
			}
			b := newFakeCatalogBuilder(t, f, nil)

			catalog := &v2.Resource{Id: &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: "ms-1::isolated"}}
			principal := &v2.Resource{Id: &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "1"}}
			annos, err := b.Revoke(context.Background(), grant.NewGrant(catalog, "USE_CATALOG", principal.GetId()))
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("Revoke = %v, want %s", err, test.wantCode)
			}
			if got := annos.Contains(&v2.GrantAlreadyRevoked{}); got != test.wantRevoked {
				t.Errorf("GrantAlreadyRevoked = %v, want %v", got, test.wantRevoked)
			}
			if f.patches != 0 {
				t.Errorf("Revoke sent %d PATCH(es) for a catalog it could not reach", f.patches)
			}
		})
	}
}

// Catalog names repeat across metastores, and the permissions path is the same on
// every workspace host, so a cache shared between hosts would hand one metastore's
// grants to another.
func TestSameNamedCatalogsInTwoMetastoresDoNotShareGrants(t *testing.T) {
	client, router := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		workspace := r.Header.Get("X-Test-Workspace")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"privilege_assignments": []map[string]any{{
				"principal":  "analyst_of_" + workspace,
				"privileges": []string{"SELECT"},
			}},
		})
	})

	ctx := context.Background()
	for _, workspace := range []string{"wsa", "wsb"} {
		assignments, _, _, err := client.ListPermissions(ctx, workspace, databricks.SecurableCatalog, "main", "", "")
		if err != nil {
			t.Fatalf("ListPermissions through %s: %v", workspace, err)
		}

		if router.hits[workspace] == 0 {
			t.Errorf("workspace %s was never asked for the grants on main; hits = %v", workspace, router.hits)
		}
		if len(assignments) != 1 {
			t.Fatalf("workspace %s returned %d assignments, want 1", workspace, len(assignments))
		}

		want := "analyst_of_" + workspace
		if got := assignments[0].Principal; got != want {
			t.Errorf("catalog main through %s is held by %s, want %s: a grant crossed between metastores", workspace, got, want)
		}
	}
}

func TestHoldsManageIgnoresAllPrivileges(t *testing.T) {
	t.Parallel()

	if holdsManage([]databricks.PrivilegeAssignment{{
		Principal:  "sp",
		Privileges: []string{privilegeAllPrivileges, "SELECT"},
	}}) {
		t.Fatal("ALL_PRIVILEGES must not count as full grant visibility")
	}
	if !holdsManage([]databricks.PrivilegeAssignment{{
		Principal:  "group",
		Privileges: []string{privilegeManage},
	}}) {
		t.Fatal("MANAGE must count as full grant visibility")
	}
}
