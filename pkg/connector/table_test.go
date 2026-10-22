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
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// fakeSchemaChildAccount is one metastore (ms-1) holding the catalog main and
// the schema main.sales, served by the RUNNING workspace wsa (id 1, named prod)
// and, when second is set, by wsb (id 2, named other). Each field shapes one
// answer; the counters record what the connector asked. It serves both the table
// and the volume syncer, which list through the same route.
type fakeSchemaChildAccount struct {
	metastoreOwner string
	catalogOwner   string
	schemaOwner    string
	second         bool

	// tables and volumes are what the listings return, paged by max_results.
	tables  []map[string]any
	volumes []map[string]any
	// childStatus fails the tables and volumes listings with that status.
	childStatus int
	// assignments maps a securable's full name to the privilege assignments on it.
	assignments map[string][]map[string]any
	// ownPrivileges answers a read filtered by principal, keyed by
	// "{securable_type}/{full_name}", which is how the connector asks for its own
	// effective privileges on a securable.
	ownPrivileges map[string][]map[string]any
	// workspaceMembers maps a workspace id to its permission assignments, or to
	// the HTTP status the account API answers the list with.
	workspaceMembers map[string]any
	patchStatus      int
	users            map[string]string
	groups           map[string]string

	routed        int
	childPages    int
	schemaPages   int
	catalogPages  int
	patches       []map[string]any
	permHosts     []string
	memberLists   []string
	filteredReads []string
}

func (f *fakeSchemaChildAccount) handler(t *testing.T) http.HandlerFunc {
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
			f.schemaPages++
			encode(map[string]any{"schemas": []map[string]any{
				{"name": "sales", "owner": f.schemaOwner},
			}})
		case strings.HasSuffix(path, "/unity-catalog/tables"):
			f.serveChildren(w, r, "tables", f.tables)
		case strings.HasSuffix(path, "/unity-catalog/volumes"):
			f.serveChildren(w, r, "volumes", f.volumes)
		case strings.Contains(path, "/unity-catalog/permissions/"):
			_, securable, _ := strings.Cut(path, "/unity-catalog/permissions/")
			f.servePermissions(w, r, securable)
		default:
			t.Errorf("unexpected request %s %s", r.Method, path)
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	}
}

func (f *fakeSchemaChildAccount) serveChildren(w http.ResponseWriter, r *http.Request, key string, all []map[string]any) {
	if f.childStatus != 0 {
		http.Error(w, `{"message":"denied"}`, f.childStatus)

		return
	}
	if r.URL.Query().Get("catalog_name") != "main" || r.URL.Query().Get("schema_name") != "sales" {
		http.Error(w, `{"message":"catalog_name and schema_name are required"}`, http.StatusBadRequest)

		return
	}

	f.childPages++
	start, _ := strconv.Atoi(r.URL.Query().Get("page_token"))
	size, _ := strconv.Atoi(r.URL.Query().Get("max_results"))
	if size <= 0 || size > len(all)-start {
		size = len(all) - start
	}
	next := ""
	if start+size < len(all) {
		next = strconv.Itoa(start + size)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{key: all[start : start+size], "next_page_token": next})
}

func (f *fakeSchemaChildAccount) servePermissions(w http.ResponseWriter, r *http.Request, securable string) {
	workspace := r.Header.Get("X-Test-Workspace")
	f.permHosts = append(f.permHosts, workspace)
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

func (f *fakeSchemaChildAccount) apply(fullName string, change map[string]any) {
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

func (f *fakeSchemaChildAccount) client(t *testing.T) *databricks.Client {
	t.Helper()
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	if f.metastoreOwner == "" {
		f.metastoreOwner = testOwnPrincipal
	}
	if f.assignments == nil {
		f.assignments = map[string][]map[string]any{}
	}
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return client
}

// newTestTableBuilder authenticates as the owner of the metastore, which is the
// live shape: ownership settles every trust rule with no probe call at all.
func newTestTableBuilder(t *testing.T, f *fakeSchemaChildAccount) *tableBuilder {
	return newTestTableBuilderAs(t, f, testOwnPrincipal)
}

func newTestTableBuilderAs(t *testing.T, f *fakeSchemaChildAccount, clientID string) *tableBuilder {
	return newTableBuilder(f.client(t), clientID, nil, syncPrincipals{groups: true})
}

var testSchemaResourceID = &v2.ResourceId{ResourceType: schemaResourceType.Id, Resource: "ms-1::main.sales"}

// testTable is a table resource as List writes it, routed through wsa.
func testTable(t *testing.T, name, tableType, owner, schemaOwner, catalogOwner, metastoreOwner, workspace string) *v2.Resource {
	t.Helper()
	r, err := tableResource(&databricks.Table{Name: name, Owner: owner, TableType: tableType}, "ms-1", "main", "sales", testSchemaResourceID,
		schemaChildRoute{
			Workspace: workspace, WorkspaceID: "1", WorkspaceName: "prod",
			MetastoreOwner: metastoreOwner, CatalogOwner: catalogOwner, SchemaOwner: schemaOwner,
		})
	if err != nil {
		t.Fatalf("tableResource: %v", err)
	}

	return r
}

// listAll drives List the way the SDK does, page token by page token.
func listAll(
	t *testing.T,
	list func(context.Context, *v2.ResourceId, rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error),
) ([]*v2.Resource, error) {
	t.Helper()

	var all []*v2.Resource
	token := ""
	for range 1000 {
		resources, results, err := list(context.Background(), testSchemaResourceID, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
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

func manyTables(prefix string, n int) []map[string]any {
	tables := make([]map[string]any, 0, n)
	for i := range n {
		tables = append(tables, map[string]any{
			"name": fmt.Sprintf("%s_%03d", prefix, i), "owner": "someone", "table_type": tableTypeManaged,
		})
	}

	return tables
}

// A table is keyed by its metastore and its dotted full name, which extends the
// schema's id, and carries the route and the owners its Grants are decided from.
func TestTableListEmitsTheTablesOfItsSchema(t *testing.T) {
	f := &fakeSchemaChildAccount{
		catalogOwner: "someone",
		schemaOwner:  "someone-else",
		tables:       manyTables("orders", 120),
	}
	b := newTestTableBuilder(t, f)

	resources, err := listAll(t, b.List)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 120 {
		t.Fatalf("listed %d tables, want 120", len(resources))
	}
	if f.childPages != 3 {
		t.Errorf("table pages read = %d, want 3", f.childPages)
	}
	// Later pages carry the route, so neither the workspace nor the catalog nor
	// the schema is resolved again.
	if f.routed != 1 || f.catalogPages != 1 || f.schemaPages != 1 {
		t.Errorf("the route was resolved %d times over %d catalog and %d schema pages, want once each",
			f.routed, f.catalogPages, f.schemaPages)
	}

	first := resources[0]
	if got := first.GetId().GetResource(); got != "ms-1::main.sales.orders_000" {
		t.Errorf("resource id = %q, want ms-1::main.sales.orders_000", got)
	}
	if first.GetDisplayName() != "main.sales.orders_000" {
		t.Errorf("display name = %q, want main.sales.orders_000", first.GetDisplayName())
	}
	if first.GetParentResourceId().GetResource() != "ms-1::main.sales" {
		t.Errorf("parent = %v, want the schema ms-1::main.sales", first.GetParentResourceId())
	}
	profile := rs.GetProfile(first)
	for key, want := range map[string]string{
		profileKeyMetastoreID: "ms-1", profileKeyFullName: "main.sales.orders_000",
		profileKeyCatalogName: "main", profileKeySchemaName: "sales",
		profileKeyTableName: "orders_000", profileKeyTableType: tableTypeManaged,
		profileKeyOwner: "someone", profileKeySecurableType: securableTypeTable,
		profileKeyWorkspace: "wsa", profileKeyWorkspaceID: "1", profileKeyWorkspaceName: "prod",
		// Grants decides visibility from the owners above the table, so List
		// writes all three of them down.
		profileKeyMetastoreOwner: testOwnPrincipal, profileKeyCatalogOwner: "someone",
		profileKeySchemaOwner: "someone-else",
	} {
		if got, _ := rs.GetProfileStringValue(profile, key); got != want {
			t.Errorf("profile[%s] = %q, want %q", key, got, want)
		}
	}
}

// A listing the workspace refuses leaves which tables exist unknown. Emitting
// nothing would read as deleting them all, and a NotFound is what the SDK reads
// as a securable with no children.
func TestTableListFailsInsteadOfEmittingAShortList(t *testing.T) {
	for code, want := range map[int]codes.Code{
		http.StatusUnauthorized:        codes.FailedPrecondition,
		http.StatusForbidden:           codes.FailedPrecondition,
		http.StatusNotFound:            codes.FailedPrecondition,
		http.StatusInternalServerError: codes.Unavailable,
	} {
		f := &fakeSchemaChildAccount{catalogOwner: "someone", schemaOwner: "someone", childStatus: code}
		b := newTestTableBuilder(t, f)

		resources, err := listAll(t, b.List)
		if err == nil {
			t.Errorf("List over a tables listing answering %d returned %d resources and no error", code, len(resources))

			continue
		}
		if got := status.Code(err); got != want {
			t.Errorf("List over a tables listing answering %d = %v, want %s", code, err, want)
		}
	}
}

// A workspace that cannot see every schema in the catalog cannot be trusted to
// list every table under one either, so the sync fails rather than emitting the
// subset Databricks chose to show.
func TestTableListRequiresACompleteSchemaListing(t *testing.T) {
	f := &fakeSchemaChildAccount{
		metastoreOwner: "eng",
		catalogOwner:   "someone",
		schemaOwner:    "someone",
		tables:         manyTables("orders", 1),
	}
	b := newTestTableBuilderAs(t, f, "app-9")

	resources, err := listAll(t, b.List)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("List = %v, want PermissionDenied", err)
	}
	if len(resources) != 0 {
		t.Errorf("listed %d tables, want none", len(resources))
	}
	if !strings.Contains(err.Error(), "main.sales") || !strings.Contains(err.Error(), schemaListRemedy) {
		t.Errorf("error %q does not name the schema and the remedy", err)
	}
}

// table_type is the only field that says which privilege set a table row takes:
// a view and a materialized view are both securable_type "table". A privilege
// the variant does not take is not reported, so ConductorOne never offers it,
// and one it does take is reported even when the grants API refuses to add it.
func TestTablePrivilegesFollowTableType(t *testing.T) {
	// Every privilege any table variant takes, as if SQL had granted them all.
	held := slices.Clone(anyTablePrivileges)

	tests := []struct {
		name      string
		tableType string
		want      []string
	}{
		{"managed", tableTypeManaged, tablePrivileges},
		{"external", tableTypeExternal, tablePrivileges},
		{"managed shallow clone", tableTypeManagedShallowClone, tablePrivileges},
		{"external shallow clone", tableTypeExternalShallowClone, tablePrivileges},
		{"foreign is read-only", tableTypeForeign, foreignTablePrivileges},
		{"view", tableTypeView, viewPrivileges},
		{"padded view", " view ", viewPrivileges},
		{"metric view", tableTypeMetricView, viewPrivileges},
		{"materialized view takes refresh", tableTypeMaterializedView, materializedViewPrivileges},
		{"streaming table", tableTypeStreamingTable, streamingTablePrivileges},
		{"a type Databricks added later", "SOMETHING_NEW", anyTablePrivileges},
		{"no type at all", "", anyTablePrivileges},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaChildAccount{
				catalogOwner: "someone",
				schemaOwner:  "someone",
				users:        map[string]string{"1": "alice@example.com"},
				assignments: map[string][]map[string]any{"main.sales.orders": {
					{"principal": "alice@example.com", "principal_id": 1, "privileges": held},
				}},
			}
			b := newTestTableBuilder(t, f)
			resource := testTable(t, "orders", test.tableType, "someone", "someone", "someone", testOwnPrincipal, "wsa")

			grants, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
			if err != nil {
				t.Fatalf("Grants: %v", err)
			}

			want := make([]string, 0, len(test.want))
			for _, privilege := range test.want {
				want = append(want, "1 table:ms-1::main.sales.orders:"+privilege)
			}
			slices.Sort(want)
			if got := grantKeys(grants); !slices.Equal(got, want) {
				t.Errorf("grants = %v, want %v", got, want)
			}
		})
	}
}

// Two privileges a table reports cannot be asked for: the grants API answers an
// added ALL_PRIVILEGES on a view, and an added REFRESH on anything other than a
// materialized view or a streaming table, with a 400. Both are refused before
// the PATCH, and a privilege outside the variant's set is refused with it.
func TestTableGrantRejectsPrivilegesTheGrantsApiRefuses(t *testing.T) {
	tests := []struct {
		name      string
		tableType string
		privilege string
		wantCode  codes.Code
	}{
		{"all privileges on a view", tableTypeView, privilegeAllPrivileges, codes.InvalidArgument},
		{"all privileges on a metric view", tableTypeMetricView, privilegeAllPrivileges, codes.InvalidArgument},
		{"all privileges on a managed table", tableTypeManaged, privilegeAllPrivileges, codes.OK},
		{"refresh on a managed table", tableTypeManaged, privilegeRefresh, codes.InvalidArgument},
		{"refresh on a foreign table", tableTypeForeign, privilegeRefresh, codes.InvalidArgument},
		{"refresh on a type Databricks added later", "SOMETHING_NEW", privilegeRefresh, codes.InvalidArgument},
		{"refresh on a materialized view", tableTypeMaterializedView, privilegeRefresh, codes.OK},
		{"refresh on a streaming table", tableTypeStreamingTable, privilegeRefresh, codes.OK},
		{"modify on a foreign table", tableTypeForeign, "MODIFY", codes.InvalidArgument},
		{"modify on a managed table", tableTypeManaged, "MODIFY", codes.OK},
		{"a catalog-only privilege", tableTypeManaged, "USE_CATALOG", codes.InvalidArgument},
		{"ownership", tableTypeManaged, ownerEntitlement, codes.InvalidArgument},
		{"a legacy alias", tableTypeManaged, "USAGE", codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaChildAccount{
				catalogOwner: "someone",
				schemaOwner:  "someone",
				users:        map[string]string{"1": "alice@example.com"},
			}
			b := newTestTableBuilder(t, f)
			resource := testTable(t, "orders", test.tableType, "someone", "someone", "someone", testOwnPrincipal, "wsa")
			g := grant.NewGrant(resource, test.privilege, alice.GetId())

			_, err := b.Grant(context.Background(), alice, g.GetEntitlement())
			if status.Code(err) != test.wantCode {
				t.Fatalf("Grant = %v, want %s", err, test.wantCode)
			}
			if test.wantCode == codes.OK {
				if len(f.patches) != 1 {
					t.Errorf("patches = %v, want the one add", f.patches)
				}

				return
			}
			if len(f.patches) != 0 {
				t.Errorf("patches = %v, want none: the grants API would reject the write", f.patches)
			}
		})
	}
}

// A privilege read a table's own access cannot account for is refused rather
// than reported: Databricks answers it with 200 and only the rows the caller may
// see, and reporting that subset would revoke the access ConductorOne cannot see.
// Dropping the table instead would delete it and its grants.
func TestTableGrantsRequireFullVisibility(t *testing.T) {
	adminOfTheWorkspace := []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"ADMIN"}},
	}
	// The principal filter answers with the group the privilege is inherited
	// through, so the row names the group and not the connector's principal.
	manage := func(securable string) map[string][]map[string]any {
		return map[string][]map[string]any{
			securable: {{"principal": "data-eng", "principal_id": 11, "privileges": []string{privilegeManage}}},
		}
	}
	// The read walks the securable, then the schema, then the catalog.
	everyRead := []string{"table/main.sales.orders", "schema/main.sales", "catalog/main"}

	tests := []struct {
		name              string
		clientID          string
		metastoreOwner    string
		catalogOwner      string
		schemaOwner       string
		tableOwner        string
		members           any
		ownPrivileges     map[string][]map[string]any
		wantGrants        bool
		wantMemberLists   int
		wantFilteredReads []string
	}{
		{
			name: "owner of the metastore", clientID: "eng",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "someone",
			wantGrants: true,
		},
		{
			name: "owner of the parent catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "app-9", schemaOwner: "someone", tableOwner: "someone",
			wantGrants: true,
		},
		{
			name: "owner of the parent schema", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "app-9", tableOwner: "someone",
			wantGrants: true,
		},
		{
			name: "owner of the table", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "app-9",
			wantGrants: true,
		},
		{
			name: "manage on the table, inherited through a group", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "someone",
			ownPrivileges: manage("table/main.sales.orders"),
			wantGrants:    true, wantFilteredReads: everyRead[:1],
		},
		{
			name: "manage on the parent schema", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "someone",
			ownPrivileges: manage("schema/main.sales"),
			wantGrants:    true, wantFilteredReads: everyRead[:2],
		},
		{
			name: "manage on the parent catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "someone",
			ownPrivileges: manage("catalog/main"),
			wantGrants:    true, wantFilteredReads: everyRead,
		},
		{
			name: "admin of the workspace whose admins group owns the parent catalog", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "_workspace_admins_prod_1", schemaOwner: "someone", tableOwner: "someone",
			members:    adminOfTheWorkspace,
			wantGrants: true, wantMemberLists: 1,
		},
		{
			// Databricks leaves MANAGE out of ALL_PRIVILEGES, so its holder still
			// reads a partial list.
			name: "all privileges on the table", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "someone",
			ownPrivileges: map[string][]map[string]any{
				"table/main.sales.orders": {{"principal": "app-9", "principal_id": 3, "privileges": []string{privilegeAllPrivileges}}},
			},
			wantGrants: false, wantFilteredReads: everyRead,
		},
		{
			name: "none of them", clientID: "app-9",
			metastoreOwner: "eng", catalogOwner: "someone", schemaOwner: "someone", tableOwner: "someone",
			wantGrants: false, wantFilteredReads: everyRead,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaChildAccount{
				metastoreOwner:   test.metastoreOwner,
				catalogOwner:     test.catalogOwner,
				schemaOwner:      test.schemaOwner,
				users:            map[string]string{"1": "alice@example.com"},
				workspaceMembers: map[string]any{"1": test.members},
				ownPrivileges:    test.ownPrivileges,
				assignments: map[string][]map[string]any{"main.sales.orders": {
					{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
				}},
			}
			b := newTestTableBuilderAs(t, f, test.clientID)
			resource := testTable(t, "orders", tableTypeManaged, test.tableOwner, test.schemaOwner, test.catalogOwner, test.metastoreOwner, "wsa")

			grants, _, err := b.Grants(context.Background(), resource, rs.SyncOpAttrs{})
			switch {
			case test.wantGrants && err != nil:
				t.Fatalf("Grants: %v", err)
			case test.wantGrants && len(grants) == 0:
				t.Fatal("Grants returned nothing, want the privileges on the table")
			case !test.wantGrants:
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("Grants = %v, want PermissionDenied", err)
				}
				if len(grants) != 0 {
					t.Errorf("grants = %v, want none", grantKeys(grants))
				}
				if !strings.Contains(err.Error(), "main.sales.orders") || !strings.Contains(err.Error(), tableTrustRemedy) {
					t.Errorf("error %q does not name the table and the remedy", err)
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
// proof the PATCH landed.
func TestTableGrantAndRevokeAreIdempotent(t *testing.T) {
	f := &fakeSchemaChildAccount{catalogOwner: "someone", schemaOwner: "someone", users: map[string]string{"1": "alice@example.com"}}
	b := newTestTableBuilder(t, f)
	resource := testTable(t, "orders", tableTypeManaged, "someone", "someone", "someone", testOwnPrincipal, "wsa")
	g := grant.NewGrant(resource, "SELECT", alice.GetId())

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

// A privilege list the connector cannot fully read cannot be written to either:
// the change would be decided from the subset Databricks chose to show.
func TestTableProvisioningRefusesAPartialPrivilegeList(t *testing.T) {
	for _, verb := range []string{"grant", "revoke"} {
		t.Run(verb, func(t *testing.T) {
			f := &fakeSchemaChildAccount{
				metastoreOwner: "eng",
				catalogOwner:   "someone",
				schemaOwner:    "someone",
				users:          map[string]string{"1": "alice@example.com"},
				assignments: map[string][]map[string]any{"main.sales.orders": {
					{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"SELECT"}},
				}},
			}
			b := newTestTableBuilderAs(t, f, "app-9")
			g := grant.NewGrant(testTable(t, "orders", tableTypeManaged, "someone", "someone", "someone", "eng", "wsa"), "SELECT", alice.GetId())

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
