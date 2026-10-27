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
	patchStatus      int
	users            map[string]string
	groups           map[string]string

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
// would drop it. Names repeat across workspaces and are emitted once.
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
	// Grants reads the workspace the route picked.
	workspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	if workspace != "wsa" {
		t.Errorf("profile[%s] = %q, want wsa", profileKeyWorkspace, workspace)
	}
}

// A catalog the caller cannot fully read is left out. Failing the sync here
// would take users, groups and every other catalog down with it.
func TestCatalogListSkipsACatalogItCannotFullyRead(t *testing.T) {
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
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 0 {
		t.Fatalf("listed %d catalogs, want none", len(resources))
	}
}

// Which workspaces serve the metastore decides which catalogs are in scope, so an
// assignment list the connector cannot read is unknown, not empty. Listing no
// catalogs would make ConductorOne delete the metastore's whole subtree.
func TestCatalogListFailsWhenTheAssignmentListCannotBeRead(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			f := &fakeCatalogAccount{attached: []int{1}, attachedStatus: code}
			b := newTestCatalogBuilder(t, f, nil, syncPrincipals{})

			resources, err := listAllCatalogs(t, b)
			if err == nil {
				t.Fatalf("List returned %d resources and no error, want the sync to fail", len(resources))
			}
			if !strings.Contains(err.Error(), "ms-1") {
				t.Errorf("error %q does not name the metastore", err)
			}
		})
	}
}

// Databricks answers an under-permissioned read of a catalog's privileges with
// 200 and only the rows the caller may see, which is indistinguishable from a
// catalog nobody holds a privilege on. The sync reports them only when it can
// prove it reads all of them, and fails on a catalog it cannot prove rather than
// dropping it: a dropped catalog takes its whole subtree out of ConductorOne.
func TestCatalogGrantsRequireFullVisibility(t *testing.T) {
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
			wantGrants: true, wantFilteredReads: 1,
		},
		{
			name: "admin of the workspace whose admins group owns the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_prod_1", members: adminOfTheWorkspace,
			wantGrants: true, wantMemberLists: 1,
		},
		{
			// The admins group of another workspace is not this read's authority,
			// so the name has to end in the id of the workspace it goes through.
			name: "admins group of another workspace owns the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_dev_2", members: adminOfTheWorkspace,
			wantGrants: false, wantFilteredReads: 1,
		},
		{
			// Databricks leaves MANAGE out of ALL_PRIVILEGES, so its holder still
			// reads a partial list.
			name: "all privileges without manage", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", ownPrivileges: allPrivileges,
			wantGrants: false, wantFilteredReads: 1,
		},
		{
			name: "none of them", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone",
			wantGrants: false, wantFilteredReads: 1,
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

// A catalog no workspace in scope lists is not a catalog proven gone: an ISOLATED
// catalog bound to an excluded workspace, and one the connector lost USE_CATALOG
// on, both read the same way and still hold the privilege. Reporting
// GrantAlreadyRevoked would leave that privilege in Databricks while
// ConductorOne recorded it as revoked.
func TestCatalogRevokeOnAnUnlistedCatalogDoesNotReportAlreadyRevoked(t *testing.T) {
	f := catalogProvisioningAccount()
	f.listed = map[string][]map[string]any{"wsa": {}}
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
