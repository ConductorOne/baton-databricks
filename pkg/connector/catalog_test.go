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
	// attachedStatus fails the metastore's assignment list with that status.
	attachedStatus int
	// listed maps a workspace to the catalogs its listing returns.
	listed map[string][]map[string]any
	// listStatus fails a workspace's catalog listing with that status.
	listStatus map[string]int
	// assignments maps a catalog to the privilege assignments on it.
	assignments map[string][]map[string]any
	// ownPrivileges answers a read filtered by principal, which is how the
	// connector asks for its own effective privileges on a catalog.
	ownPrivileges []map[string]any
	// workspaceMembers maps a workspace id to its permission assignments, or to
	// the HTTP status the account API answers the list with.
	workspaceMembers map[string]any
	permStatus       int
	// permErrorCode is the vendor error_code that rides along with permStatus.
	permErrorCode string
	patchStatus   int
	users         map[string]string
	groups        map[string]string

	routed        int
	catalogPages  int
	patches       []map[string]any
	permHosts     []string
	memberLists   []string
	filteredReads []string
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
			if f.attachedStatus != 0 {
				http.Error(w, `{"message":"denied"}`, f.attachedStatus)

				return
			}
			encode(map[string]any{"workspace_ids": f.attached})
		case strings.HasSuffix(path, "/permissionassignments"):
			parts := strings.Split(path, "/")
			workspaceID := parts[len(parts)-2]
			f.memberLists = append(f.memberLists, workspaceID)
			switch v := f.workspaceMembers[workspaceID].(type) {
			case int:
				http.Error(w, `{"message":"denied"}`, v)
			default:
				encode(map[string]any{"permission_assignments": v})
			}
		case strings.HasSuffix(path, "/workspaces"):
			f.routed++
			encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
			})
		case strings.HasSuffix(path, "/unity-catalog/catalogs"):
			f.serveCatalogs(w, r)
		case strings.Contains(path, "/unity-catalog/catalogs/"):
			f.serveCatalog(w, r, path[strings.LastIndex(path, "/")+1:])
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

// serveCatalog answers GET /catalogs/{name} from the same per-workspace listing,
// so a catalog bound elsewhere 404s here exactly as the workspace would answer.
func (f *fakeCatalogAccount) serveCatalog(w http.ResponseWriter, r *http.Request, name string) {
	workspace := r.Header.Get("X-Test-Workspace")
	if code, ok := f.listStatus[workspace]; ok {
		http.Error(w, `{"message":"denied"}`, code)

		return
	}
	for _, catalog := range f.listed[workspace] {
		if catalog["name"] == name {
			_ = json.NewEncoder(w).Encode(catalog)

			return
		}
	}

	http.Error(w, `{"error_code":"CATALOG_DOES_NOT_EXIST","message":"not found"}`, http.StatusNotFound)
}

func (f *fakeCatalogAccount) servePermissions(w http.ResponseWriter, r *http.Request, catalog string) {
	f.permHosts = append(f.permHosts, r.Header.Get("X-Test-Workspace"))
	if f.permStatus != 0 {
		http.Error(w, fmt.Sprintf(`{"error_code":%q,"message":"nope"}`, f.permErrorCode), f.permStatus)
		return
	}
	if principal := r.URL.Query().Get("principal"); principal != "" {
		f.filteredReads = append(f.filteredReads, principal)
		_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": f.ownPrivileges})

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
	return newTestCatalogBuilderAs(t, f, allowlist, sync, testOwnPrincipal)
}

func newTestCatalogBuilderAs(t *testing.T, f *fakeCatalogAccount, allowlist []string, sync syncPrincipals, clientID string) *catalogBuilder {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newCatalogBuilder(client, clientID, configuredWorkspaceSet(allowlist), sync)
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

// testCatalog is a catalog resource as List writes it, routed through wsa, in a
// metastore the connector's own principal owns. That is the live shape: ownership
// settles grant visibility with no probe call at all.
func testCatalog(t *testing.T, name, owner, workspace string) *v2.Resource {
	t.Helper()

	return testCatalogOf(t, name, owner, testOwnPrincipal, workspace)
}

func testCatalogOf(t *testing.T, name, owner, metastoreOwner, workspace string) *v2.Resource {
	t.Helper()
	r, err := catalogResource(&databricks.Catalog{Name: name, MetastoreID: "ms-1", Owner: owner}, "ms-1", testMetastoreResourceID,
		catalogRoute{Workspace: workspace, WorkspaceID: "1", WorkspaceName: "prod", MetastoreOwner: metastoreOwner})
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

// Catalogs are read through every running in-scope workspace. A catalog bound
// to only one of them is absent from the others, so listing a single workspace
// would drop it. A catalog both of them list is emitted twice, once per route,
// and the store collapses the copies on (external_id, sync_id).
func TestCatalogListReadsEveryWorkspace(t *testing.T) {
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
		t.Fatalf("unique catalogs = %d, want the 120 shared catalogs plus the 30 bound only to wsb", len(unique))
	}
	if _, ok := unique["ms-1::only_b_000"]; !ok {
		t.Fatal("no resource with id ms-1::only_b_000")
	}
	if f.routed != 1 {
		t.Errorf("the route was resolved %d times, want once per metastore", f.routed)
	}
	if want := 6; f.catalogPages != want {
		t.Errorf("catalog pages read = %d, want %d (both workspaces, once each)", f.catalogPages, want)
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
		// Grants reads the metastore's owner off the profile, so List writes it down.
		profileKeyMetastoreOwner: "eng",
	} {
		if got, _ := rs.GetProfileStringValue(profile, key); got != want {
			t.Errorf("profile[%s] = %q, want %q", key, got, want)
		}
	}
	// Grants reads the workspace the route picked. A catalog listed by both
	// workspaces keeps the last route to emit it, since that is the copy the
	// store's upsert leaves behind. Either one serves the read: a route only
	// emits a catalog it lists and can prove it reads every privilege on.
	workspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	if workspace != "wsb" {
		t.Errorf("profile[%s] = %q, want wsb", profileKeyWorkspace, workspace)
	}
	if onlyB, ok := unique["ms-1::only_b_000"]; ok {
		workspace, _ := rs.GetProfileStringValue(rs.GetProfile(onlyB), profileKeyWorkspace)
		if workspace != "wsb" {
			t.Errorf("profile[%s] = %q, want wsb for a catalog only wsb lists", profileKeyWorkspace, workspace)
		}
	}
}

// A catalog whose privilege list is partial fails the sync instead of being left
// out. Leaving it out is not the quiet half: ConductorOne deletes every resource
// a sync stops emitting, so the catalog, its grants and its whole subtree would
// go, where failing keeps the state C1 already holds.
func TestCatalogListFailsOnACatalogItCannotFullyRead(t *testing.T) {
	f := &fakeCatalogAccount{
		attached: []int{1},
		listed:   map[string][]map[string]any{"wsa": {catalogEntry("main", "someone")}},
		workspaceMembers: map[string]any{"1": []map[string]any{{
			"principal":   map[string]any{"service_principal_name": "app-9"},
			"permissions": []string{"ADMIN"},
		}}},
	}
	b := newTestCatalogBuilderAs(t, f, nil, syncPrincipals{}, "app-9")

	resources, err := listAllCatalogs(t, b)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("List = %v, want PermissionDenied", err)
	}
	if !strings.Contains(err.Error(), "main") || !strings.Contains(err.Error(), catalogTrustRemedy) {
		t.Errorf("error %q does not name the catalog and the remedy", err)
	}
	if len(resources) != 0 {
		t.Errorf("listed %d catalogs, want none", len(resources))
	}
}

// system and samples are built in, owned by System user, and Databricks refuses
// a MANAGE grant on them. A connector that does not own the metastore cannot
// prove their privilege list, so they are left out. A catalog beside them that
// the caller owns still syncs. When the connector does own the metastore, the
// same catalogs are synced, because ownership makes the read complete.
func TestCatalogListSkipsBuiltInCatalogs(t *testing.T) {
	f := &fakeCatalogAccount{
		attached: []int{1},
		listed: map[string][]map[string]any{"wsa": {
			catalogEntry(systemCatalog, "System user"),
			{"name": "samples", "metastore_id": "ms-1", "owner": "System user", "catalog_type": "SYSTEM_CATALOG"},
			catalogEntry("owned", "app-9"),
		}},
		workspaceMembers: map[string]any{"1": []map[string]any{{
			"principal":   map[string]any{"service_principal_name": "app-9"},
			"permissions": []string{"ADMIN"},
		}}},
	}
	b := newTestCatalogBuilderAs(t, f, nil, syncPrincipals{}, "app-9")

	resources, err := listAllCatalogs(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 1 || resources[0].GetDisplayName() != "owned" {
		t.Fatalf("listed %d catalogs, want only the one it owns", len(resources))
	}
}

// Which workspaces serve the metastore decides which catalogs are in scope, so an
// assignment list the connector cannot read is unknown, not empty. Listing no
// catalogs would make ConductorOne delete the metastore's whole subtree. Only the
// NotFound has to be re-coded: the SDK reads that one as "this parent has no
// children" and skips the List as a warning. A 403 already fails the sync.
func TestCatalogListFailsWhenTheAssignmentListCannotBeRead(t *testing.T) {
	for code, want := range map[int]codes.Code{
		http.StatusNotFound:  codes.FailedPrecondition,
		http.StatusForbidden: codes.PermissionDenied,
	} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			f := &fakeCatalogAccount{attached: []int{1}, attachedStatus: code}
			b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})

			resources, err := listAllCatalogs(t, b)
			if status.Code(err) != want {
				t.Fatalf("List = %v, want %s", err, want)
			}
			if !strings.Contains(err.Error(), "ms-1") {
				t.Errorf("error %q does not name the metastore", err)
			}
			if len(resources) != 0 {
				t.Errorf("listed %d catalogs, want none", len(resources))
			}
		})
	}
}

// Grants reports the assignments stored on the catalog. List is what refuses a
// partial privilege list, so Grants does not prove visibility again.
func TestCatalogGrantsReportStoredAssignments(t *testing.T) {
	adminOfTheWorkspace := []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"ADMIN"}},
	}
	// The principal filter answers with the group the privilege is inherited
	// through, so the row names the group and not the connector's principal.
	manageThroughAGroup := []map[string]any{
		{"principal": "data-eng", "principal_id": 11, "privileges": []string{"MANAGE"}},
	}
	allPrivileges := []map[string]any{
		{"principal": "app-9", "principal_id": 3, "privileges": []string{"ALL_PRIVILEGES"}},
	}

	tests := []struct {
		name              string
		clientID          string
		metastoreOwner    string
		catalogOwner      string
		members           any
		ownPrivileges     []map[string]any
		wantGrants        bool
		wantMemberLists   int
		wantFilteredReads int
	}{
		{
			name: "owner of the metastore", clientID: "eng",
			metastoreOwner: "eng", catalogOwner: "someone",
			wantGrants: true,
		},
		{
			name: "owner of the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "app-9",
			wantGrants: true,
		},
		{
			name: "manage inherited through a group", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", ownPrivileges: manageThroughAGroup,
			wantGrants: true,
		},
		{
			name: "admin of the workspace whose admins group owns the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_prod_1", members: adminOfTheWorkspace,
			wantGrants: true,
		},
		{
			name: "admins group of another workspace owns the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_dev_2", members: adminOfTheWorkspace,
			wantGrants: true,
		},
		{
			name: "all privileges without manage", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", ownPrivileges: allPrivileges,
			wantGrants: true,
		},
		{
			name: "none of them", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone",
			wantGrants: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeCatalogAccount{
				users:            map[string]string{"1": "alice@example.com"},
				workspaceMembers: map[string]any{"1": test.members},
				ownPrivileges:    test.ownPrivileges,
				assignments: map[string][]map[string]any{"main": {
					{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
				}},
			}
			b := newTestCatalogBuilderAs(t, f, nil, syncPrincipals{}, test.clientID)
			resource := testCatalogOf(t, "main", test.catalogOwner, test.metastoreOwner, "wsa")

			grants, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
			switch {
			case test.wantGrants && err != nil:
				t.Fatalf("Grants: %v", err)
			case test.wantGrants && len(grants) == 0:
				t.Fatal("Grants returned nothing, want the privileges on the catalog")
			case !test.wantGrants:
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("Grants = %v, want PermissionDenied", err)
				}
				if len(grants) != 0 {
					t.Errorf("grants = %v, want none", grantKeys(grants))
				}
				if !strings.Contains(err.Error(), "main") || !strings.Contains(err.Error(), catalogTrustRemedy) {
					t.Errorf("error %q does not name the catalog and the remedy", err)
				}
			}
			if len(f.memberLists) != test.wantMemberLists {
				t.Errorf("workspace assignments listed %d times, want %d", len(f.memberLists), test.wantMemberLists)
			}
			if len(f.filteredReads) != test.wantFilteredReads {
				t.Errorf("own privileges read %d times, want %d", len(f.filteredReads), test.wantFilteredReads)
			}
		})
	}
}

// A privilege list the connector cannot fully read cannot be written to either:
// the change would be decided from the subset Databricks chose to show.
func TestCatalogProvisioningRefusesAPartialPrivilegeList(t *testing.T) {
	for _, verb := range []string{"grant", "revoke"} {
		t.Run(verb, func(t *testing.T) {
			f := catalogProvisioningAccount()
			f.assignments["main"] = []map[string]any{
				{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"USE_CATALOG"}},
			}
			b := newTestCatalogBuilderAs(t, f, nil, syncPrincipals{}, "app-9")
			g := grant.NewGrant(testCatalogOf(t, "main", "someone", "eng", "wsa"), "USE_CATALOG", alice.GetId())

			var err error
			if verb == "grant" {
				_, err = b.Grant(context.Background(), alice, g.GetEntitlement())
			} else {
				_, err = b.Revoke(context.Background(), g)
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("%s = %v, want PermissionDenied", verb, err)
			}
			if len(f.patches) != 0 {
				t.Errorf("patches = %v, want none", f.patches)
			}
		})
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

// Only the error_code proves the catalog is gone. A bare 404 on the privilege
// read also comes back from a workspace the catalog is not bound to, where the
// privilege is still held, so that one stays an error.
func TestCatalogRevokeTellsADeletedCatalogFromAnUnreachableOne(t *testing.T) {
	tests := []struct {
		name      string
		errorCode string
		wantGone  bool
	}{
		{name: "deleted catalog", errorCode: errorCodeCatalogGone, wantGone: true},
		{name: "unbound workspace", errorCode: ""},
		{name: "some other 404", errorCode: "RESOURCE_DOES_NOT_EXIST"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := catalogProvisioningAccount()
			f.permStatus = http.StatusNotFound
			f.permErrorCode = test.errorCode
			b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})
			g := grant.NewGrant(testCatalog(t, "main", "someone", "wsa"), "USE_CATALOG", alice.GetId())

			annos, err := b.Revoke(context.Background(), g)
			switch {
			case test.wantGone:
				if err != nil || !annos.Contains(&v2.GrantAlreadyRevoked{}) {
					t.Fatalf("Revoke = %v, %v; want GrantAlreadyRevoked", annos, err)
				}
			default:
				if err == nil {
					t.Fatalf("Revoke returned %v and no error; want the revoke to fail", annos)
				}
				if annos.Contains(&v2.GrantAlreadyRevoked{}) {
					t.Error("a 404 without the gone code reported GrantAlreadyRevoked")
				}
			}
			if len(f.patches) != 0 {
				t.Errorf("patches = %v, want none", f.patches)
			}
		})
	}
}

// A catalog no workspace in scope lists is not a catalog proven gone: an ISOLATED
// catalog bound to an excluded workspace, and one the connector lost USE_CATALOG
// on, both read the same way and still hold the privilege. Reporting
// GrantAlreadyRevoked would leave that privilege in Databricks while
// ConductorOne recorded it as revoked.
func TestCatalogRevokeOnAnUnlistedCatalogDoesNotReportAlreadyRevoked(t *testing.T) {
	f := catalogProvisioningAccount()
	// 403, not a 404 naming the catalog: this workspace does not serve the
	// catalog, which says nothing about whether it exists.
	f.listStatus = map[string]int{"wsa": http.StatusForbidden}
	b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testCatalog(t, "main", "someone", "wsa"), "USE_CATALOG", alice.GetId())

	annos, err := b.Revoke(context.Background(), g)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Revoke = %v, want FailedPrecondition", err)
	}
	if annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Error("an unlisted catalog reported GrantAlreadyRevoked")
	}
	if len(f.patches) != 0 {
		t.Errorf("patches = %v, want none", f.patches)
	}
}
