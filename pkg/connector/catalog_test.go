package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// fakeCatalogAccount is one metastore (ms-1) served by the RUNNING workspaces
// wsa (id 1, named prod) and wsb (id 2, named dev). Each field shapes one
// answer; the counters record what the connector asked.
type fakeCatalogAccount struct {
	attached []int
	// listed maps a workspace to the catalogs its listing returns.
	listed map[string][]map[string]any
	// listStatus fails a workspace's catalog listing with that status.
	listStatus map[string]int
	// assignments maps a catalog to the privilege assignments on it.
	assignments map[string][]map[string]any
	permStatus  int
	patchStatus int
	users       map[string]string
	groups      map[string]string

	routed       int
	catalogPages int
	patches      []map[string]any
	permHosts    []string
}

func (f *fakeCatalogAccount) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		encode := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		switch {
		case strings.Contains(path, "/scim/v2/"):
			serveSCIM(w, r, f.users, f.groups, nil, nil)
		case strings.HasSuffix(path, "/metastores"):
			encode(map[string]any{"metastores": []map[string]any{{"metastore_id": "ms-1", "name": "main", "owner": "eng"}}})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			encode(map[string]any{"workspace_ids": f.attached})
		case strings.HasSuffix(path, "/workspaces"):
			f.routed++
			encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
			})
		case strings.HasSuffix(path, "/unity-catalog/catalogs"):
			f.serveCatalogs(w, r)
		case strings.Contains(path, "/unity-catalog/permissions/catalog/"):
			f.servePermissions(w, r, path[strings.LastIndex(path, "/")+1:])
		default:
			t.Errorf("unexpected request %s %s", r.Method, path)
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	}
}

func (f *fakeCatalogAccount) serveCatalogs(w http.ResponseWriter, r *http.Request) {
	workspace := r.Header.Get("X-Test-Workspace")
	if code, ok := f.listStatus[workspace]; ok {
		http.Error(w, `{"message":"denied"}`, code)
		return
	}

	f.catalogPages++
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
}

func (f *fakeCatalogAccount) servePermissions(w http.ResponseWriter, r *http.Request, catalog string) {
	f.permHosts = append(f.permHosts, r.Header.Get("X-Test-Workspace"))
	if f.permStatus != 0 {
		http.Error(w, `{"message":"nope"}`, f.permStatus)
		return
	}
	if r.Method == http.MethodPatch {
		if f.patchStatus != 0 {
			http.Error(w, `{"message":"denied"}`, f.patchStatus)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var changes struct {
			Changes []map[string]any `json:"changes"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.UseNumber()
		_ = decoder.Decode(&changes)
		for _, change := range changes.Changes {
			f.patches = append(f.patches, change)
			f.apply(catalog, change)
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": f.assignments[catalog]})
}

func (f *fakeCatalogAccount) apply(catalog string, change map[string]any) {
	added, _ := change["add"].([]any)
	for _, privilege := range added {
		f.assignments[catalog] = append(f.assignments[catalog], map[string]any{
			"principal": change["principal"], "principal_id": 1, "privileges": []string{privilege.(string)},
		})
	}
	remove, ok := change["remove"].([]any)
	if !ok {
		return
	}
	kept := f.assignments[catalog][:0]
	for _, assignment := range f.assignments[catalog] {
		if fmt.Sprint(assignment["principal_id"]) != fmt.Sprint(change["principal_id"]) ||
			!slices.Contains(assignment["privileges"].([]string), remove[0].(string)) {
			kept = append(kept, assignment)
		}
	}
	f.assignments[catalog] = kept
}

func newTestCatalogBuilder(t *testing.T, f *fakeCatalogAccount, allowlist []string, sync syncPrincipals) *catalogBuilder {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newCatalogBuilder(client, configuredWorkspaceSet(allowlist), sync)
}

var testMetastoreResourceID = &v2.ResourceId{ResourceType: metastoreResourceType.Id, Resource: "ms-1"}

func catalogEntry(name, owner string) map[string]any {
	return map[string]any{"name": name, "metastore_id": "ms-1", "owner": owner}
}

func manyCatalogs(prefix string, n int) []map[string]any {
	catalogs := make([]map[string]any, 0, n)
	for i := range n {
		catalogs = append(catalogs, catalogEntry(fmt.Sprintf("%s_%03d", prefix, i), "someone"))
	}

	return catalogs
}

// testCatalog is a catalog resource as List writes it, routed through wsa.
func testCatalog(t *testing.T, name, owner, workspace string) *v2.Resource {
	t.Helper()
	r, err := catalogResource(&databricks.Catalog{Name: name, MetastoreID: "ms-1", Owner: owner}, "ms-1", testMetastoreResourceID,
		catalogRoute{Workspace: workspace, WorkspaceID: "1", WorkspaceName: "prod"})
	if err != nil {
		t.Fatalf("catalogResource: %v", err)
	}

	return r
}

// listAllCatalogs drives List the way the SDK does, page token by page token.
func listAllCatalogs(t *testing.T, b *catalogBuilder) ([]*v2.Resource, error) {
	t.Helper()

	var all []*v2.Resource
	token := ""
	for range 1000 {
		resources, results, err := b.List(context.Background(), testMetastoreResourceID, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
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

// A catalog bound to a subset of the workspaces is listed only by the ones it is
// bound to, so every usable workspace is asked. Paging per workspace reads each
// API page once and resolves the route once per metastore.
func TestCatalogListPagesEachWorkspaceOnceAndUnionsThem(t *testing.T) {
	shared := manyCatalogs("shared", 120)
	f := &fakeCatalogAccount{
		attached: []int{1, 2},
		listed: map[string][]map[string]any{
			"wsa": shared,
			"wsb": append(slices.Clone(shared), manyCatalogs("only_b", 30)...),
		},
	}
	b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})

	resources, err := listAllCatalogs(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	unique := map[string]*v2.Resource{}
	for _, resource := range resources {
		unique[resource.GetId().GetResource()] = resource
	}
	if len(unique) != 150 {
		t.Fatalf("unique catalogs = %d, want 150", len(unique))
	}
	if f.routed != 1 {
		t.Errorf("the route was resolved %d times, want once per metastore", f.routed)
	}
	if want := 3 + 3; f.catalogPages != want {
		t.Errorf("catalog pages read = %d, want %d (each workspace's pages once)", f.catalogPages, want)
	}

	// The resource id keys the catalog by its metastore, and the profile carries
	// the workspace Grants and Grant/Revoke are addressed to.
	first, ok := unique["ms-1::shared_000"]
	if !ok {
		t.Fatalf("no resource with id ms-1::shared_000")
	}
	if first.GetDisplayName() != "shared_000" {
		t.Errorf("display name = %q, want shared_000", first.GetDisplayName())
	}
	profile := rs.GetProfile(first)
	for key, want := range map[string]string{
		profileKeyMetastoreID: "ms-1", profileKeyFullName: "shared_000", profileKeyCatalogName: "shared_000",
		profileKeyOwner: "someone", profileKeySecurableType: securableTypeCatalog,
	} {
		if got, _ := rs.GetProfileStringValue(profile, key); got != want {
			t.Errorf("profile[%s] = %q, want %q", key, got, want)
		}
	}
	// A catalog both workspaces list keeps whichever one emitted it last.
	workspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	if workspace != "wsa" && workspace != "wsb" {
		t.Errorf("profile[%s] = %q, want a workspace that lists the catalog", profileKeyWorkspace, workspace)
	}
	if only, ok := unique["ms-1::only_b_000"]; ok {
		if got, _ := rs.GetProfileStringValue(rs.GetProfile(only), profileKeyWorkspace); got != "wsb" {
			t.Errorf("a catalog only wsb lists is routed through %q, want wsb", got)
		}
	} else {
		t.Error("the catalog only wsb lists was not emitted")
	}
}

// Leaving a workspace out of the sync is the operator scoping its content out,
// so a catalog only that workspace lists is not synced.
func TestCatalogListSkipsAWorkspaceTheAllowlistLeavesOut(t *testing.T) {
	f := &fakeCatalogAccount{
		attached: []int{1, 2},
		listed: map[string][]map[string]any{
			"wsa": {catalogEntry("main", "someone")},
			"wsb": {catalogEntry("isolated_dev", "someone")},
		},
	}
	b := newTestCatalogBuilder(t, f, []string{"prod"}, syncPrincipals{})

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

// A workspace that refuses the catalog listing leaves the catalog set unknown.
// Emitting what the other workspaces list would read as deleting the rest.
func TestCatalogListFailsInsteadOfEmittingAShortList(t *testing.T) {
	// A refusal is reported as FailedPrecondition rather than NotFound, which
	// the SDK would read as the page being empty.
	for code, want := range map[int]codes.Code{
		http.StatusForbidden:           codes.FailedPrecondition,
		http.StatusNotFound:            codes.FailedPrecondition,
		http.StatusInternalServerError: codes.Unavailable,
	} {
		f := &fakeCatalogAccount{
			attached:   []int{1, 2},
			listed:     map[string][]map[string]any{"wsa": {catalogEntry("main", "someone")}},
			listStatus: map[string]int{"wsb": code},
		}
		b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})

		_, err := listAllCatalogs(t, b)
		if err == nil {
			t.Errorf("List over a workspace answering %d succeeded", code)
			continue
		}
		if got := status.Code(err); got != want {
			t.Errorf("List over a workspace answering %d = %v, want %s", code, err, want)
		}
	}
}

// A group's privilege is expandable to its members; the owner's is immutable and
// emitted only with the first page; a principal type the sync leaves out and a
// privilege the catalog does not offer are not emitted.
func TestCatalogGrants(t *testing.T) {
	f := &fakeCatalogAccount{
		users:  map[string]string{"1": "alice@example.com"},
		groups: map[string]string{"11": "data-eng"},
		assignments: map[string][]map[string]any{"main": {
			{"principal": "data-eng", "principal_id": 11, "privileges": []string{"USE_CATALOG", "NOT_A_PRIVILEGE"}},
			{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
		}},
	}
	b := newTestCatalogBuilder(t, f, nil, syncPrincipals{groups: true})
	resource := testCatalog(t, "main", "alice@example.com", "wsa")

	grants, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}

	group := "account/" + testAccountId + "/group/11"
	want := []string{
		"1 catalog:ms-1::main:SELECT",
		"1 catalog:ms-1::main:owner",
		group + " catalog:ms-1::main:USE_CATALOG",
	}
	slices.Sort(want)
	if got := grantKeys(grants); !slices.Equal(got, want) {
		t.Fatalf("grants = %v, want %v", got, want)
	}
	if !slices.Equal(f.permHosts, []string{"wsa"}) {
		t.Errorf("permissions read through %v, want [wsa]", f.permHosts)
	}

	for _, g := range grants {
		annos := annotations.Annotations(g.GetAnnotations())
		switch {
		case strings.HasSuffix(g.GetEntitlement().GetId(), ":owner"):
			if !annos.Contains(&v2.GrantImmutable{}) {
				t.Error("the owner grant is not immutable")
			}
		case g.GetPrincipal().GetId().GetResourceType() == groupResourceType.Id:
			expandable := &v2.GrantExpandable{}
			if ok, err := annos.Pick(expandable); err != nil || !ok {
				t.Fatalf("the group's grant is not expandable (err %v)", err)
			}
			if len(expandable.GetEntitlementIds()) != 1 ||
				!strings.HasSuffix(expandable.GetEntitlementIds()[0], ":"+groupMemberEntitlement) || expandable.GetShallow() {
				t.Errorf("GrantExpandable = %v, want the group's member entitlement, not shallow", expandable)
			}
		}
	}

	next, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{PageToken: pagination.Token{Token: "page-2"}})
	if err != nil {
		t.Fatalf("Grants page 2: %v", err)
	}
	for _, g := range next {
		if strings.HasSuffix(g.GetEntitlement().GetId(), ":owner") {
			t.Error("the owner grant was emitted again on a later page")
		}
	}
}

// A permissions read that fails is never an absence: returning no grants would
// revoke every privilege on the catalog in ConductorOne, and NotFound is what
// the SDK reads as "no grants".
func TestCatalogGrantsFailOnErrors(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusInternalServerError} {
		f := &fakeCatalogAccount{permStatus: code}
		b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})

		_, _, err := b.Grants(context.Background(), testCatalog(t, "main", "", "wsa"), rs.SyncOpAttrs{})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("Grants on %d = %v, want FailedPrecondition", code, err)
		}
	}
}

func catalogProvisioningAccount() *fakeCatalogAccount {
	return &fakeCatalogAccount{
		attached:    []int{1},
		listed:      map[string][]map[string]any{"wsa": {catalogEntry("main", "someone")}},
		users:       map[string]string{"1": "alice@example.com"},
		assignments: map[string][]map[string]any{},
	}
}

// Databricks no-ops a privilege already held, so the pre-read is the only signal
// for GrantAlreadyExists and the echo is the only proof the PATCH landed.
func TestCatalogGrantAddsThePrivilegeOnceAndConfirmsIt(t *testing.T) {
	f := catalogProvisioningAccount()
	b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})
	entitlement := grant.NewGrant(testCatalog(t, "main", "someone", "wsa"), "USE_CATALOG", alice.GetId()).GetEntitlement()

	annos, err := b.Grant(context.Background(), alice, entitlement)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Error("a fresh grant reported GrantAlreadyExists")
	}
	if len(f.patches) != 1 || f.patches[0]["principal"] != "alice@example.com" {
		t.Fatalf("patches = %v, want one add for alice@example.com", f.patches)
	}

	annos, err = b.Grant(context.Background(), alice, entitlement)
	if err != nil || !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Fatalf("second Grant = %v, %v; want GrantAlreadyExists", annos, err)
	}
	if len(f.patches) != 1 {
		t.Errorf("a held privilege was patched again: %v", f.patches)
	}
}

// A principal that is gone from SCIM still carries its numeric id on the row, so
// the removal goes by principal_id; an absent privilege is already revoked.
func TestCatalogRevokeRemovesByIDAndIsIdempotent(t *testing.T) {
	f := catalogProvisioningAccount()
	f.assignments["main"] = []map[string]any{{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"USE_CATALOG"}}}
	b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testCatalog(t, "main", "someone", "wsa"), "USE_CATALOG", alice.GetId())

	annos, err := b.Revoke(context.Background(), g)
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Error("revoking a held privilege reported GrantAlreadyRevoked")
	}
	if len(f.patches) != 1 || fmt.Sprint(f.patches[0]["principal_id"]) != "1" {
		t.Fatalf("patches = %v, want one removal by principal_id 1", f.patches)
	}

	annos, err = b.Revoke(context.Background(), g)
	if err != nil || !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Fatalf("second Revoke = %v, %v; want GrantAlreadyRevoked", annos, err)
	}
}

func TestCatalogGrantRejections(t *testing.T) {
	tests := []struct {
		name        string
		workspace   string
		privilege   string
		patchStatus int
		code        codes.Code
	}{
		{"owner", "wsa", ownerEntitlement, 0, codes.InvalidArgument},
		{"legacy alias", "wsa", "USAGE", 0, codes.InvalidArgument},
		{"not offered", "wsa", "CREATE_CATALOG", 0, codes.InvalidArgument},
		{"no workspace on the profile", "", "USE_CATALOG", 0, codes.FailedPrecondition},
		{"forbidden", "wsa", "USE_CATALOG", http.StatusForbidden, codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := catalogProvisioningAccount()
			f.patchStatus = test.patchStatus
			b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})
			g := grant.NewGrant(testCatalog(t, "main", "someone", test.workspace), test.privilege, alice.GetId())

			_, err := b.Grant(context.Background(), alice, g.GetEntitlement())
			if status.Code(err) != test.code {
				t.Fatalf("Grant = %v, want %s", err, test.code)
			}
			if test.code == codes.PermissionDenied && !strings.Contains(err.Error(), catalogWriteRemedy) {
				t.Errorf("error %q does not name the remedy", err)
			}
		})
	}
}
