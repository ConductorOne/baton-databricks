package connector

import (
	"context"
	"encoding/json"
	"fmt"
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

// fakeSchemaAccount is one metastore (ms-1) holding the catalog main, served by
// the RUNNING workspace wsa (id 1, named prod) and, when second is set, by wsb
// (id 2, named other). Each field shapes one answer; the counters record what the
// connector asked.
type fakeSchemaAccount struct {
	metastoreOwner string
	catalogOwner   string
	second         bool

	// schemas is what the schemas listing returns, paged by max_results.
	schemas []map[string]any
	// schemaStatus fails the schemas listing with that status.
	schemaStatus int
	// assignments maps a securable's full name to the privilege assignments on it.
	assignments map[string][]map[string]any
	// ownPrivileges answers a read filtered by principal, keyed by
	// "{securable_type}/{full_name}", which is how the connector asks for its own
	// effective privileges on a securable.
	ownPrivileges map[string][]map[string]any
	// permStatus fails the permissions endpoint on that deployment name.
	permStatus map[string]int
	// workspaceMembers maps a workspace id to its permission assignments, or to
	// the HTTP status the account API answers the list with.
	workspaceMembers map[string]any
	patchStatus      int
	users            map[string]string
	groups           map[string]string

	routed        int
	schemaPages   int
	catalogPages  int
	patches       []map[string]any
	permHosts     []string
	memberLists   []string
	filteredReads []string
}

func (f *fakeSchemaAccount) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		encode := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		switch {
		case strings.Contains(path, "/scim/v2/"):
			serveSCIM(w, r, f.users, f.groups, nil, nil)
		case strings.HasSuffix(path, "/metastores"):
			encode(map[string]any{"metastores": []map[string]any{
				{"metastore_id": "ms-1", "name": "main", "owner": f.metastoreOwner},
			}})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			ids := []int{1}
			if f.second {
				ids = append(ids, 2)
			}
			encode(map[string]any{"workspace_ids": ids})
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
			listed := []map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
			}
			if f.second {
				listed = append(listed, map[string]any{
					"workspace_id": 2, "workspace_name": "other", "deployment_name": "wsb", "workspace_status": "RUNNING",
				})
			}
			encode(listed)
		case strings.HasSuffix(path, "/unity-catalog/catalogs"):
			f.catalogPages++
			encode(map[string]any{"catalogs": []map[string]any{
				{"name": "main", "metastore_id": "ms-1", "owner": f.catalogOwner},
			}})
		case strings.HasSuffix(path, "/unity-catalog/schemas"):
			f.serveSchemas(w, r)
		case strings.Contains(path, "/unity-catalog/permissions/"):
			_, securable, _ := strings.Cut(path, "/unity-catalog/permissions/")
			f.servePermissions(w, r, securable)
		default:
			t.Errorf("unexpected request %s %s", r.Method, path)
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	}
}

func (f *fakeSchemaAccount) serveSchemas(w http.ResponseWriter, r *http.Request) {
	if f.schemaStatus != 0 {
		http.Error(w, `{"message":"denied"}`, f.schemaStatus)

		return
	}
	if got := r.URL.Query().Get("catalog_name"); got != "main" {
		http.Error(w, `{"message":"catalog_name is required"}`, http.StatusBadRequest)

		return
	}

	f.schemaPages++
	all := f.schemas
	start, _ := strconv.Atoi(r.URL.Query().Get("page_token"))
	size, _ := strconv.Atoi(r.URL.Query().Get("max_results"))
	if size <= 0 || size > len(all)-start {
		size = len(all) - start
	}
	next := ""
	if start+size < len(all) {
		next = strconv.Itoa(start + size)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"schemas": all[start : start+size], "next_page_token": next})
}

func (f *fakeSchemaAccount) servePermissions(w http.ResponseWriter, r *http.Request, securable string) {
	workspace := r.Header.Get("X-Test-Workspace")
	f.permHosts = append(f.permHosts, workspace)
	if code, ok := f.permStatus[workspace]; ok {
		http.Error(w, `{"message":"refused"}`, code)

		return
	}
	if principal := r.URL.Query().Get("principal"); principal != "" {
		f.filteredReads = append(f.filteredReads, securable)
		_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": f.ownPrivileges[securable]})

		return
	}

	_, fullName, _ := strings.Cut(securable, "/")
	if r.Method == http.MethodPatch {
		if f.patchStatus != 0 {
			http.Error(w, `{"message":"denied"}`, f.patchStatus)

			return
		}
		var body struct {
			Changes []map[string]any `json:"changes"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		_ = decoder.Decode(&body)
		for _, change := range body.Changes {
			f.patches = append(f.patches, change)
			f.apply(fullName, change)
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": f.assignments[fullName]})
}

func (f *fakeSchemaAccount) apply(fullName string, change map[string]any) {
	added, _ := change["add"].([]any)
	for _, privilege := range added {
		f.assignments[fullName] = append(f.assignments[fullName], map[string]any{
			"principal": change["principal"], "principal_id": 1, "privileges": []string{privilege.(string)},
		})
	}
	remove, ok := change["remove"].([]any)
	if !ok {
		return
	}
	kept := f.assignments[fullName][:0]
	for _, assignment := range f.assignments[fullName] {
		if fmt.Sprint(assignment["principal_id"]) != fmt.Sprint(change["principal_id"]) ||
			!slices.Contains(assignment["privileges"].([]string), remove[0].(string)) {
			kept = append(kept, assignment)
		}
	}
	f.assignments[fullName] = kept
}

// newTestSchemaBuilder authenticates as the owner of the metastore, which is the
// live shape: ownership settles every trust rule with no probe call at all.
func newTestSchemaBuilder(t *testing.T, f *fakeSchemaAccount) *schemaBuilder {
	return newTestSchemaBuilderAs(t, f, testOwnPrincipal)
}

func newTestSchemaBuilderAs(t *testing.T, f *fakeSchemaAccount, clientID string) *schemaBuilder {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	if f.metastoreOwner == "" {
		f.metastoreOwner = testOwnPrincipal
	}
	if f.assignments == nil {
		f.assignments = map[string][]map[string]any{}
	}
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newSchemaBuilder(client, clientID, nil, syncPrincipals{groups: true})
}

var testCatalogResourceID = &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: "ms-1::main"}

// testSchema is a schema resource as List writes it, routed through wsa.
func testSchema(t *testing.T, name, owner, catalogOwner, metastoreOwner, workspace string) *v2.Resource {
	t.Helper()
	r, err := schemaResource(&databricks.Schema{Name: name, Owner: owner}, "ms-1", "main", testCatalogResourceID,
		schemaRoute{Workspace: workspace, WorkspaceID: "1", WorkspaceName: "prod", MetastoreOwner: metastoreOwner, CatalogOwner: catalogOwner})
	if err != nil {
		t.Fatalf("schemaResource: %v", err)
	}

	return r
}

// listAllSchemas drives List the way the SDK does, page token by page token.
func listAllSchemas(t *testing.T, b *schemaBuilder) ([]*v2.Resource, error) {
	t.Helper()

	var all []*v2.Resource
	token := ""
	for range 1000 {
		resources, results, err := b.List(context.Background(), testCatalogResourceID, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
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

func manySchemas(prefix string, n int) []map[string]any {
	schemas := make([]map[string]any, 0, n)
	for i := range n {
		schemas = append(schemas, map[string]any{"name": fmt.Sprintf("%s_%03d", prefix, i), "owner": "someone"})
	}

	return schemas
}

// A schema is keyed by its metastore and its dotted full name, which extends the
// catalog's id, and carries the route and the owners its Grants are decided from.
// information_schema is Databricks' own read-only schema and is left out.
func TestSchemaListEmitsTheSchemasOfItsCatalog(t *testing.T) {
	f := &fakeSchemaAccount{
		catalogOwner: "someone",
		schemas: append(manySchemas("sales", 120),
			map[string]any{"name": "information_schema", "owner": "System user"}),
	}
	b := newTestSchemaBuilder(t, f)

	resources, err := listAllSchemas(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 120 {
		t.Fatalf("listed %d schemas, want 120 (information_schema left out)", len(resources))
	}
	if f.schemaPages != 3 {
		t.Errorf("schema pages read = %d, want 3", f.schemaPages)
	}
	// Later pages carry the route, so neither the workspace nor the catalog is
	// resolved again.
	if f.routed != 1 || f.catalogPages != 1 {
		t.Errorf("the route was resolved %d times over %d catalog pages, want once per catalog", f.routed, f.catalogPages)
	}

	first := resources[0]
	if got := first.GetId().GetResource(); got != "ms-1::main.sales_000" {
		t.Errorf("resource id = %q, want ms-1::main.sales_000", got)
	}
	if first.GetDisplayName() != "main.sales_000" {
		t.Errorf("display name = %q, want main.sales_000", first.GetDisplayName())
	}
	if first.GetParentResourceId().GetResource() != "ms-1::main" {
		t.Errorf("parent = %v, want the catalog ms-1::main", first.GetParentResourceId())
	}
	profile := rs.GetProfile(first)
	for key, want := range map[string]string{
		profileKeyMetastoreID: "ms-1", profileKeyFullName: "main.sales_000",
		profileKeyCatalogName: "main", profileKeySchemaName: "sales_000",
		profileKeyOwner: "someone", profileKeySecurableType: securableTypeSchema,
		profileKeyWorkspace: "wsa", profileKeyWorkspaceID: "1", profileKeyWorkspaceName: "prod",
		// Grants decides visibility from the owners above the schema, so List
		// writes both of them down.
		profileKeyMetastoreOwner: testOwnPrincipal, profileKeyCatalogOwner: "someone",
	} {
		if got, _ := rs.GetProfileStringValue(profile, key); got != want {
			t.Errorf("profile[%s] = %q, want %q", key, got, want)
		}
	}
}

// Databricks answers a caller without catalog-wide visibility with only the
// schemas it owns or can use, at 200, which is indistinguishable from a catalog
// that holds just those. A short list read as the whole one makes ConductorOne
// delete the schemas it did not see and every grant under them, so the sync fails
// on a catalog whose listing it cannot prove complete.
func TestSchemaListRequiresACompleteListing(t *testing.T) {
	adminOfTheWorkspace := []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"ADMIN"}},
	}
	// The principal filter answers with the group the privilege is inherited
	// through, so the row names the group and not the connector's principal.
	heldThroughAGroup := func(privilege string) map[string][]map[string]any {
		return map[string][]map[string]any{
			"catalog/main": {{"principal": "data-eng", "principal_id": 11, "privileges": []string{privilege}}},
		}
	}

	tests := []struct {
		name              string
		clientID          string
		metastoreOwner    string
		catalogOwner      string
		members           any
		ownPrivileges     map[string][]map[string]any
		wantSchemas       bool
		wantMemberLists   int
		wantFilteredReads int
	}{
		{
			name: "owner of the metastore", clientID: "eng",
			metastoreOwner: "eng", catalogOwner: "someone",
			wantSchemas: true,
		},
		{
			name: "owner of the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "app-9",
			wantSchemas: true,
		},
		{
			name: "manage on the catalog, inherited through a group", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", ownPrivileges: heldThroughAGroup("MANAGE"),
			wantSchemas: true, wantFilteredReads: 1,
		},
		{
			// Measured: a catalog grant of USE_SCHEMA does list a schema owned by
			// someone else, even though it does not make the privilege list complete.
			name: "use schema on the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", ownPrivileges: heldThroughAGroup("USE_SCHEMA"),
			wantSchemas: true, wantFilteredReads: 1,
		},
		{
			name: "all privileges on the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", ownPrivileges: heldThroughAGroup("ALL_PRIVILEGES"),
			wantSchemas: true, wantFilteredReads: 1,
		},
		{
			name: "admin of the workspace whose admins group owns the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_prod_1", members: adminOfTheWorkspace,
			wantSchemas: true, wantMemberLists: 1,
		},
		{
			// Workspace Admin on its own is not an authority: the system catalog
			// came back with 2 schemas out of 15 for one.
			name: "admin of a workspace that does not own the catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", members: adminOfTheWorkspace,
			wantSchemas: false, wantFilteredReads: 1,
		},
		{
			name: "none of them", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone",
			wantSchemas: false, wantFilteredReads: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaAccount{
				metastoreOwner:   test.metastoreOwner,
				catalogOwner:     test.catalogOwner,
				workspaceMembers: map[string]any{"1": test.members},
				ownPrivileges:    test.ownPrivileges,
				schemas:          []map[string]any{{"name": "sales", "owner": "someone"}},
			}
			b := newTestSchemaBuilderAs(t, f, test.clientID)

			resources, err := listAllSchemas(t, b)
			switch {
			case test.wantSchemas && err != nil:
				t.Fatalf("List: %v", err)
			case test.wantSchemas && len(resources) != 1:
				t.Fatalf("listed %d schemas, want 1", len(resources))
			case !test.wantSchemas:
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("List = %v, want PermissionDenied", err)
				}
				if len(resources) != 0 {
					t.Errorf("listed %d schemas, want none", len(resources))
				}
				if !strings.Contains(err.Error(), "main") || !strings.Contains(err.Error(), schemaListRemedy) {
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

// Visibility through any one attached workspace is enough, and nothing in the
// listing order says which one that is, so a workspace that refuses the probe is
// not the end of the walk. A refusal is 401 or 403; anything else is the account
// plane failing, which must not be read as an absence of access.
func TestSchemaListTriesTheNextWorkspaceWhenOneRefusesTheProbe(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		wantRouting string
	}{
		{"unauthorized", http.StatusUnauthorized, "wsb"},
		{"forbidden", http.StatusForbidden, "wsb"},
		{"server error", http.StatusInternalServerError, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaAccount{
				metastoreOwner: "eng",
				catalogOwner:   "someone",
				second:         true,
				permStatus:     map[string]int{"wsa": test.status},
				ownPrivileges: map[string][]map[string]any{
					"catalog/main": {{"principal": "data-eng", "principal_id": 11, "privileges": []string{"MANAGE"}}},
				},
				schemas: []map[string]any{{"name": "sales", "owner": "someone"}},
			}
			b := newTestSchemaBuilderAs(t, f, "app-9")

			resources, err := listAllSchemas(t, b)
			if test.wantRouting == "" {
				if err == nil {
					t.Fatalf("List succeeded over a workspace answering %d", test.status)
				}
				if status.Code(err) == codes.PermissionDenied {
					t.Errorf("List = %v, want the probe failure rather than a refusal", err)
				}

				return
			}
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(resources) != 1 {
				t.Fatalf("listed %d schemas, want 1", len(resources))
			}
			got, _ := rs.GetProfileStringValue(rs.GetProfile(resources[0]), profileKeyWorkspace)
			if got != test.wantRouting {
				t.Errorf("schemas routed through %q, want %q", got, test.wantRouting)
			}
		})
	}
}

// A listing the workspace refuses leaves which schemas exist unknown. Emitting
// nothing would read as deleting them all, and a NotFound is what the SDK reads
// as a securable with no children.
func TestSchemaListFailsInsteadOfEmittingAShortList(t *testing.T) {
	for code, want := range map[int]codes.Code{
		http.StatusUnauthorized:        codes.FailedPrecondition,
		http.StatusForbidden:           codes.FailedPrecondition,
		http.StatusNotFound:            codes.FailedPrecondition,
		http.StatusInternalServerError: codes.Unavailable,
	} {
		f := &fakeSchemaAccount{catalogOwner: "someone", schemaStatus: code}
		b := newTestSchemaBuilder(t, f)

		resources, err := listAllSchemas(t, b)
		if err == nil {
			t.Errorf("List over a schemas listing answering %d returned %d resources and no error", code, len(resources))

			continue
		}
		if got := status.Code(err); got != want {
			t.Errorf("List over a schemas listing answering %d = %v, want %s", code, err, want)
		}
	}
}

// A group's privilege is expandable to its members; the owner's is immutable and
// emitted only with the first page; a privilege a schema does not offer is not
// emitted at all.
func TestSchemaGrants(t *testing.T) {
	f := &fakeSchemaAccount{
		catalogOwner: "someone",
		users:        map[string]string{"1": "alice@example.com"},
		groups:       map[string]string{"11": "data-eng"},
		assignments: map[string][]map[string]any{"main.sales": {
			{"principal": "data-eng", "principal_id": 11, "privileges": []string{"USE_SCHEMA", "USE_CATALOG"}},
			{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
		}},
	}
	b := newTestSchemaBuilder(t, f)
	resource := testSchema(t, "sales", "alice@example.com", "someone", testOwnPrincipal, "wsa")

	grants, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}

	group := "account/" + testAccountId + "/group/11"
	want := []string{
		"1 schema:ms-1::main.sales:SELECT",
		"1 schema:ms-1::main.sales:owner",
		group + " schema:ms-1::main.sales:USE_SCHEMA",
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

// A privilege read a schema's own access cannot account for is refused rather
// than reported: Databricks answers it with 200 and only the rows the caller may
// see, and reporting that subset would revoke the access ConductorOne cannot see.
// Dropping the schema instead would delete it and its whole subtree.
func TestSchemaGrantsRequireFullVisibility(t *testing.T) {
	adminOfTheWorkspace := []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"ADMIN"}},
	}
	manage := func(securable string) map[string][]map[string]any {
		return map[string][]map[string]any{
			securable: {{"principal": "data-eng", "principal_id": 11, "privileges": []string{"MANAGE"}}},
		}
	}

	tests := []struct {
		name              string
		clientID          string
		metastoreOwner    string
		catalogOwner      string
		schemaOwner       string
		members           any
		ownPrivileges     map[string][]map[string]any
		wantGrants        bool
		wantMemberLists   int
		wantFilteredReads []string
	}{
		{
			name: "owner of the metastore", clientID: "eng",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone",
			wantGrants: true,
		},
		{
			name: "owner of the parent catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "app-9", schemaOwner: "someone",
			wantGrants: true,
		},
		{
			name: "owner of the schema", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "app-9",
			wantGrants: true,
		},
		{
			name: "manage on the schema, inherited through a group", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone",
			ownPrivileges: manage("schema/main.sales"),
			wantGrants:    true, wantFilteredReads: []string{"schema/main.sales"},
		},
		{
			name: "manage on the parent catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone",
			ownPrivileges: manage("catalog/main"),
			wantGrants:    true, wantFilteredReads: []string{"schema/main.sales", "catalog/main"},
		},
		{
			name: "admin of the workspace whose admins group owns the parent catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_prod_1", schemaOwner: "someone",
			members:    adminOfTheWorkspace,
			wantGrants: true, wantMemberLists: 1,
		},
		{
			// Databricks leaves MANAGE out of ALL_PRIVILEGES, so its holder still
			// reads a partial list.
			name: "all privileges on the schema", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone",
			ownPrivileges: map[string][]map[string]any{
				"schema/main.sales": {{"principal": "app-9", "principal_id": 3, "privileges": []string{"ALL_PRIVILEGES"}}},
			},
			wantGrants: false, wantFilteredReads: []string{"schema/main.sales", "catalog/main"},
		},
		{
			name: "none of them", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone",
			wantGrants: false, wantFilteredReads: []string{"schema/main.sales", "catalog/main"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaAccount{
				metastoreOwner:   test.metastoreOwner,
				catalogOwner:     test.catalogOwner,
				users:            map[string]string{"1": "alice@example.com"},
				workspaceMembers: map[string]any{"1": test.members},
				ownPrivileges:    test.ownPrivileges,
				assignments: map[string][]map[string]any{"main.sales": {
					{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
				}},
			}
			b := newTestSchemaBuilderAs(t, f, test.clientID)
			resource := testSchema(t, "sales", test.schemaOwner, test.catalogOwner, test.metastoreOwner, "wsa")

			grants, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
			switch {
			case test.wantGrants && err != nil:
				t.Fatalf("Grants: %v", err)
			case test.wantGrants && len(grants) == 0:
				t.Fatal("Grants returned nothing, want the privileges on the schema")
			case !test.wantGrants:
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("Grants = %v, want PermissionDenied", err)
				}
				if len(grants) != 0 {
					t.Errorf("grants = %v, want none", grantKeys(grants))
				}
				if !strings.Contains(err.Error(), "main.sales") || !strings.Contains(err.Error(), schemaTrustRemedy) {
					t.Errorf("error %q does not name the schema and the remedy", err)
				}
			}
			if len(f.memberLists) != test.wantMemberLists {
				t.Errorf("workspace assignments listed %d times, want %d", len(f.memberLists), test.wantMemberLists)
			}
			if !slices.Equal(f.filteredReads, test.wantFilteredReads) {
				t.Errorf("own privileges read on %v, want %v", f.filteredReads, test.wantFilteredReads)
			}
		})
	}
}

// Databricks no-ops a privilege already held or already absent, so the pre-read
// is the only signal for the idempotency annotations and the echo is the only
// proof the PATCH landed. A principal gone from SCIM still carries its numeric id
// on the row, so the removal goes by principal_id.
func TestSchemaGrantAndRevokeAreIdempotent(t *testing.T) {
	f := &fakeSchemaAccount{catalogOwner: "someone", users: map[string]string{"1": "alice@example.com"}}
	b := newTestSchemaBuilder(t, f)
	g := grant.NewGrant(testSchema(t, "sales", "someone", "someone", testOwnPrincipal, "wsa"), "SELECT", alice.GetId())

	annos, err := b.Grant(context.Background(), alice, g.GetEntitlement())
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Error("a fresh grant reported GrantAlreadyExists")
	}
	if len(f.patches) != 1 || f.patches[0]["principal"] != "alice@example.com" {
		t.Fatalf("patches = %v, want one add for alice@example.com", f.patches)
	}

	annos, err = b.Grant(context.Background(), alice, g.GetEntitlement())
	if err != nil || !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Fatalf("second Grant = %v, %v; want GrantAlreadyExists", annos, err)
	}

	if _, err := b.Revoke(context.Background(), g); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(f.patches) != 2 || fmt.Sprint(f.patches[1]["principal_id"]) != "1" {
		t.Fatalf("patches = %v, want a removal by principal_id 1", f.patches)
	}

	annos, err = b.Revoke(context.Background(), g)
	if err != nil || !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Fatalf("second Revoke = %v, %v; want GrantAlreadyRevoked", annos, err)
	}
}

func TestSchemaGrantRejections(t *testing.T) {
	tests := []struct {
		name        string
		workspace   string
		privilege   string
		patchStatus int
		code        codes.Code
	}{
		{"owner", "wsa", ownerEntitlement, 0, codes.InvalidArgument},
		{"legacy alias", "wsa", "USAGE", 0, codes.InvalidArgument},
		// USE_CATALOG stops at the catalog, so a schema never offers it.
		{"stops at the catalog", "wsa", "USE_CATALOG", 0, codes.InvalidArgument},
		{"no workspace on the profile", "", "SELECT", 0, codes.FailedPrecondition},
		{"forbidden", "wsa", "SELECT", http.StatusForbidden, codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaAccount{
				catalogOwner: "someone",
				users:        map[string]string{"1": "alice@example.com"},
				patchStatus:  test.patchStatus,
			}
			b := newTestSchemaBuilder(t, f)
			g := grant.NewGrant(testSchema(t, "sales", "someone", "someone", testOwnPrincipal, test.workspace), test.privilege, alice.GetId())

			_, err := b.Grant(context.Background(), alice, g.GetEntitlement())
			if status.Code(err) != test.code {
				t.Fatalf("Grant = %v, want %s", err, test.code)
			}
			if test.code == codes.PermissionDenied && !strings.Contains(err.Error(), schemaWriteRemedy) {
				t.Errorf("error %q does not name the remedy", err)
			}
		})
	}
}

// A privilege list the connector cannot fully read cannot be written to either:
// the change would be decided from the subset Databricks chose to show.
func TestSchemaProvisioningRefusesAPartialPrivilegeList(t *testing.T) {
	for _, verb := range []string{"grant", "revoke"} {
		t.Run(verb, func(t *testing.T) {
			f := &fakeSchemaAccount{
				metastoreOwner: "eng",
				catalogOwner:   "someone",
				users:          map[string]string{"1": "alice@example.com"},
				assignments: map[string][]map[string]any{"main.sales": {
					{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
				}},
			}
			b := newTestSchemaBuilderAs(t, f, "app-9")
			g := grant.NewGrant(testSchema(t, "sales", "someone", "someone", "eng", "wsa"), "SELECT", alice.GetId())

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
