package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

// fakeAccount serves the account, Unity Catalog and SCIM endpoints the metastore syncer calls.
type fakeAccount struct {
	metastores  []map[string]any
	assignments map[string]any // metastore id -> []int workspace ids, or an HTTP status
	workspaces  []map[string]any
	permissions []map[string]any
	permStatus  int
	patchStatus int
	users       map[string]string // id -> userName
	groups      map[string]string // id -> displayName
	sps         map[string]string // id -> applicationId

	// workspaceMembers maps a workspace id to its permission assignments, or to
	// the HTTP status the account API answers the list with.
	workspaceMembers map[string]any

	permHosts   []string
	patches     []map[string]any
	scimGets    map[string]int
	memberLists []string
}

func defaultFakeAccount() *fakeAccount {
	return &fakeAccount{
		metastores:  []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "eng", "region": "us-east-1"}},
		assignments: map[string]any{"ms-a": []int{1}},
		workspaces: []map[string]any{
			{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
		},
		users:    map[string]string{"1": "alice@example.com"},
		groups:   map[string]string{"2": "eng"},
		sps:      map[string]string{"3": "app-9"},
		scimGets: map[string]int{},
	}
}

func scimList(field string, records map[string]string, filter string) []map[string]any {
	key, value, _ := strings.Cut(filter, " eq ")
	value, _ = strconv.Unquote(value)
	var out []map[string]any
	for id, name := range records {
		if (key == "id" && id == value) || (key != "id" && name == value) {
			out = append(out, map[string]any{"id": id, field: name})
		}
	}

	return out
}

func (f *fakeAccount) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		encode := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		switch {
		case strings.HasSuffix(path, "/metastores"):
			encode(map[string]any{"metastores": f.metastores})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			parts := strings.Split(path, "/")
			switch v := f.assignments[parts[len(parts)-2]].(type) {
			case int:
				http.Error(w, `{"message":"denied"}`, v)
			default:
				encode(map[string]any{"workspace_ids": v})
			}
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
			encode(f.workspaces)
		case strings.Contains(path, "/unity-catalog/permissions/metastore/"):
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
				var body struct {
					Changes []map[string]any `json:"changes"`
				}
				decoder := json.NewDecoder(r.Body)
				decoder.UseNumber()
				_ = decoder.Decode(&body)
				for _, change := range body.Changes {
					f.patches = append(f.patches, change)
					f.apply(change)
				}
			}
			encode(map[string]any{"privilege_assignments": f.permissions})
		case strings.Contains(path, "/scim/v2/"):
			f.scim(w, r)
		default:
			t.Errorf("unexpected request %s %s", r.Method, path)
			http.Error(w, `{"message":"unexpected"}`, http.StatusInternalServerError)
		}
	}
}

func (f *fakeAccount) scim(w http.ResponseWriter, r *http.Request) {
	serveSCIM(w, r, f.users, f.groups, f.sps, f.scimGets)
}

// serveSCIM answers account SCIM by id (/Users/{id}) and by filter (id or name eq).
// gets, when given, counts the reads by id.
func serveSCIM(w http.ResponseWriter, r *http.Request, users, groups, sps map[string]string, gets map[string]int) {
	_, rest, _ := strings.Cut(r.URL.Path, "/scim/v2/")
	kind, id, _ := strings.Cut(rest, "/")
	field, records := map[string]string{"Users": "userName", "Groups": "displayName", "ServicePrincipals": "applicationId"}[kind],
		map[string]map[string]string{"Users": users, "Groups": groups, "ServicePrincipals": sps}[kind]
	if id != "" {
		if gets != nil {
			gets[kind+"/"+id]++
		}
		if name, ok := records[id]; ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": id, field: name})
			return
		}
		http.Error(w, `{"message":"missing"}`, http.StatusNotFound)
		return
	}
	found := scimList(field, records, r.URL.Query().Get("filter"))
	_ = json.NewEncoder(w).Encode(map[string]any{"totalResults": len(found), "Resources": found})
}

// principalID is the SCIM id the account holds for a principal name.
func (f *fakeAccount) principalID(name string) int {
	for _, records := range []map[string]string{f.users, f.groups, f.sps} {
		for id, recorded := range records {
			if recorded == name {
				numeric, _ := strconv.Atoi(id)
				return numeric
			}
		}
	}

	return 0
}

func (f *fakeAccount) apply(change map[string]any) {
	added, _ := change["add"].([]any)
	for _, privilege := range added {
		f.permissions = append(f.permissions, map[string]any{
			"principal":    change["principal"],
			"principal_id": f.principalID(fmt.Sprint(change["principal"])),
			"privileges":   []string{privilege.(string)},
		})
	}
	if remove, ok := change["remove"].([]any); ok {
		kept := f.permissions[:0]
		for _, p := range f.permissions {
			if fmt.Sprint(p["principal_id"]) != fmt.Sprint(change["principal_id"]) || !slices.Contains(p["privileges"].([]string), remove[0].(string)) {
				kept = append(kept, p)
			}
		}
		f.permissions = kept
	}
}

// testOwnPrincipal is the applicationId the connector authenticates as. It is
// the owner of the metastore in defaultFakeAccount, which is the live shape:
// ownership settles grant visibility without any probe call.
const testOwnPrincipal = "eng"

func newTestMetastoreBuilder(t *testing.T, f *fakeAccount, allowlist []string, sync syncPrincipals) *metastoreBuilder {
	return newTestMetastoreBuilderAs(t, f, allowlist, sync, testOwnPrincipal)
}

func newTestMetastoreBuilderAs(t *testing.T, f *fakeAccount, allowlist []string, sync syncPrincipals, clientID string) *metastoreBuilder {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newMetastoreBuilder(client, clientID, configuredWorkspaceSet(allowlist), sync)
}

var testAccountResourceID = &v2.ResourceId{ResourceType: "account", Resource: testAccountId}

// testSyncedMetastore is the resource List emits, route included.
func testSyncedMetastore(t *testing.T, workspace string) *v2.Resource {
	t.Helper()
	r, err := metastoreResource(context.Background(), &databricks.Metastore{MetastoreID: "ms-a", Name: "main", Owner: "eng"},
		testAccountResourceID, workspace, "1", "prod")
	if err != nil {
		t.Fatalf("metastoreResource: %v", err)
	}

	return r
}

// testMetastore is the resource a provisioning call carries: an id, no profile.
func testMetastore(t *testing.T) *v2.Resource {
	t.Helper()
	r, err := rs.NewAppResource("main", metastoreResourceType, "ms-a", nil, rs.WithParentResourceID(testAccountResourceID))
	if err != nil {
		t.Fatalf("NewAppResource: %v", err)
	}

	return r
}

// Which workspaces serve a metastore decides whether it syncs at all, so an
// assignment list the connector cannot read is unknown, not absent. Skipping it
// would drop the metastore and make ConductorOne remove its grants. Only the
// NotFound has to be re-coded: the SDK runs the metastore List with
// allowWarning, reads NotFound as "the account has no metastores" and lets the
// sync succeed with none. A 401 or 403 already fails it.
func TestMetastoreListFailsWhenAnAssignmentListCannotBeRead(t *testing.T) {
	for code, want := range map[int]codes.Code{
		http.StatusNotFound:     codes.FailedPrecondition,
		http.StatusForbidden:    codes.PermissionDenied,
		http.StatusUnauthorized: codes.Unauthenticated,
	} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			f := defaultFakeAccount()
			f.assignments = map[string]any{"ms-a": code}
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

			resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
			if status.Code(err) != want {
				t.Fatalf("List = %v, want %s", err, want)
			}
			if !strings.Contains(err.Error(), "ms-a") {
				t.Errorf("error %q does not name the metastore", err)
			}
			if len(resources) != 0 {
				t.Errorf("listed %d metastores, want none", len(resources))
			}
		})
	}
}

// A workspace that will not become RUNNING leaves the metastore out of the sync
// instead of failing it on every run; one that can still change keeps it waiting.
func TestMetastoreListSeparatesTerminalFromPendingWorkspaces(t *testing.T) {
	tests := []struct {
		workspaceStatus string
		wantErr         bool
		wantSynced      bool
	}{
		{"RUNNING", false, true},
		{"PROVISIONING", true, false},
		{"NOT_PROVISIONED", true, false},
		{"CANCELLING", true, false},
		{"FAILED", false, false},
		{"BANNED", false, false},
		{"SOMETHING_NEW", false, false},
	}
	for _, test := range tests {
		t.Run(test.workspaceStatus, func(t *testing.T) {
			f := defaultFakeAccount()
			f.assignments = map[string]any{"ms-a": []int{1}}
			f.workspaces = []map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": test.workspaceStatus},
			}
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

			resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
			if test.wantErr != (err != nil) {
				t.Fatalf("List error = %v, want error: %v", err, test.wantErr)
			}
			if got := len(resources) > 0; got != test.wantSynced {
				t.Errorf("metastore synced = %v, want %v", got, test.wantSynced)
			}
		})
	}
}

// An owner sees every privilege, so the route is the first running workspace in scope.
func TestMetastoreListRoutesTheOwnerThroughTheFirstScopedWorkspace(t *testing.T) {
	tests := []struct {
		name      string
		allowlist []string
		want      map[string]string // metastore id -> profile workspace
	}{
		{"no allowlist", nil, map[string]string{"ms-a": "wsa"}},
		{"allowlist by name", []string{"third"}, map[string]string{"ms-a": "wsc"}},
		{"allowlist matching nothing", []string{"nope"}, map[string]string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			f.metastores = append(f.metastores, map[string]any{"metastore_id": "ms-elsewhere"})
			f.assignments = map[string]any{
				"ms-a":         []int{2, 1, 3},
				"ms-elsewhere": []int{4},
			}
			f.workspaces = []map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
				{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "PROVISIONING"},
				{"workspace_id": 3, "workspace_name": "third", "deployment_name": "wsc", "workspace_status": "RUNNING"},
			}
			m := newTestMetastoreBuilder(t, f, test.allowlist, syncPrincipals{})

			resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			got := map[string]string{}
			for _, r := range resources {
				got[r.GetId().GetResource()], _ = rs.GetProfileStringValue(rs.GetProfile(r), profileKeyWorkspace)
			}
			if len(got) != len(test.want) {
				t.Fatalf("List = %v, want %v", got, test.want)
			}
			for id, workspace := range test.want {
				if got[id] != workspace {
					t.Errorf("metastore %s routed through %q, want %q", id, got[id], workspace)
				}
			}
		})
	}
}

func grantKeys(grants []*v2.Grant) []string {
	keys := make([]string, 0, len(grants))
	for _, g := range grants {
		keys = append(keys, g.GetPrincipal().GetId().GetResource()+" "+g.GetEntitlement().GetId())
	}
	slices.Sort(keys)

	return keys
}

var (
	adminByAppID = []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"ADMIN"}},
	}
	adminByScimID = []map[string]any{
		{"principal": map[string]any{"principal_id": 3}, "permissions": []string{"ADMIN"}},
	}
	notAdmin = []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"USER"}},
		{"principal": map[string]any{"group_name": "admins"}, "permissions": []string{"ADMIN"}},
	}
)

// A read without standing returns 200 with a partial list, so List routes only
// through a workspace it can fully read.
func TestMetastoreListRoutesOnlyThroughAWorkspaceItCanFullyRead(t *testing.T) {
	tests := []struct {
		name       string
		clientID   string
		members    any
		wantRouted bool
		wantProbes int
	}{
		{"owner of the metastore", testOwnPrincipal, nil, true, 0},
		{"direct workspace admin", "app-9", adminByAppID, true, 1},
		{"direct workspace admin matched by scim id", "app-9", adminByScimID, true, 1},
		{"admin held only through a group", "app-9", notAdmin, false, 1},
		{"assignment list denied", "app-9", http.StatusForbidden, false, 1},
		{"no principal to compare", "", adminByAppID, false, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			f.workspaceMembers = map[string]any{"1": test.members}
			m := newTestMetastoreBuilderAs(t, f, nil, syncPrincipals{}, test.clientID)

			resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
			switch {
			case test.wantRouted && err != nil:
				t.Fatalf("List: %v", err)
			case test.wantRouted && len(resources) != 1:
				t.Fatalf("List returned %d resources, want the metastore", len(resources))
			case !test.wantRouted:
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("List = %v, want PermissionDenied", err)
				}
				if len(resources) != 0 {
					t.Errorf("resources = %v, want none", resources)
				}
				if !strings.Contains(err.Error(), "ms-a") {
					t.Errorf("error %q does not name the metastore", err)
				}
			}
			if len(f.memberLists) != test.wantProbes {
				t.Errorf("workspace assignments listed %d times, want %d", len(f.memberLists), test.wantProbes)
			}
		})
	}
}

// The route is where standing holds, not the first workspace the assignment list names.
func TestMetastoreListRoutesThroughTheWorkspaceWhereStandingHolds(t *testing.T) {
	f := defaultFakeAccount()
	f.metastores = []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "someone-else"}}
	f.assignments = map[string]any{"ms-a": []int{1, 2}}
	f.workspaces = []map[string]any{
		{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
		{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
	}
	f.workspaceMembers = map[string]any{"1": nil, "2": adminByAppID}
	m := newTestMetastoreBuilderAs(t, f, nil, syncPrincipals{}, "app-9")

	resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
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
	if !slices.Equal(f.memberLists, []string{"1", "2"}) {
		t.Errorf("probed %v, want both workspaces in order until one holds Admin", f.memberLists)
	}
}

// Grants uses the route List already chose and does not probe again.
func TestMetastoreGrantsReadThroughTheRoutedWorkspace(t *testing.T) {
	f := defaultFakeAccount()
	f.permissions = []map[string]any{
		{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG"}},
	}
	m := newTestMetastoreBuilderAs(t, f, nil, syncPrincipals{}, "app-9")

	grants, _, err := m.Grants(context.Background(), testSyncedMetastore(t, "wsb"), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}
	if want := []string{"1 metastore:ms-a:CREATE_CATALOG"}; !slices.Equal(grantKeys(grants), want) {
		t.Errorf("grants = %v, want %v", grantKeys(grants), want)
	}
	if !slices.Equal(f.permHosts, []string{"wsb"}) {
		t.Errorf("permissions read through %v, want the routed workspace wsb", f.permHosts)
	}
	if len(f.memberLists) != 0 {
		t.Errorf("workspace assignments listed %d times, want none: List already settled the route", len(f.memberLists))
	}
}

// A NotFound must not look like an empty grant page, and a throttle must stay
// retryable. Wrapping every permissions error as FailedPrecondition does both wrong.
func TestMetastoreGrantsKeepThePermissionsStatus(t *testing.T) {
	tests := []struct {
		name string
		code int
		want codes.Code
	}{
		{name: "not found", code: http.StatusNotFound, want: codes.FailedPrecondition},
		{name: "throttle", code: http.StatusTooManyRequests, want: codes.Unavailable},
		{name: "unavailable", code: http.StatusServiceUnavailable, want: codes.Unavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			f.permStatus = test.code
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

			grants, _, err := m.Grants(context.Background(), testSyncedMetastore(t, "wsa"), rs.SyncOpAttrs{})
			if status.Code(err) != test.want {
				t.Fatalf("Grants = %v, want %s", err, test.want)
			}
			if len(grants) != 0 {
				t.Errorf("grants = %v, want none", grantKeys(grants))
			}
		})
	}
}

// No grants and no error would revoke every privilege, so an unrouted resource fails.
func TestMetastoreGrantsFailWithoutARouteOnTheProfile(t *testing.T) {
	f := defaultFakeAccount()
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

	grants, _, err := m.Grants(context.Background(), testMetastore(t), rs.SyncOpAttrs{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Grants = %v, want FailedPrecondition", err)
	}
	if len(grants) != 0 {
		t.Errorf("grants = %v, want none", grantKeys(grants))
	}
}

var alice = &v2.Resource{Id: &v2.ResourceId{ResourceType: "user", Resource: "1"}}

func TestMetastoreGrantRejections(t *testing.T) {
	tests := []struct {
		name        string
		privilege   string
		patchStatus int
		code        codes.Code
	}{
		{"owner", ownerEntitlement, 0, codes.InvalidArgument},
		{"legacy alias", "USAGE", 0, codes.InvalidArgument},
		{"not offered", "MANAGE", 0, codes.InvalidArgument},
		{"forbidden", "CREATE_CATALOG", http.StatusForbidden, codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
			g := grant.NewGrant(testMetastore(t), test.privilege, alice.GetId())
			f.patchStatus = test.patchStatus

			_, err := m.Grant(context.Background(), alice, g.GetEntitlement())
			if status.Code(err) != test.code {
				t.Fatalf("Grant = %v, want %s", err, test.code)
			}
			if len(f.patches) != 0 {
				t.Errorf("patches = %v, want none", f.patches)
			}
		})
	}
}

// Grant and Revoke resolve the route from the metastore id.
func TestMetastoreProvisioningRoutesFromTheMetastoreID(t *testing.T) {
	f := defaultFakeAccount()
	f.metastores = []map[string]any{{"metastore_id": "ms-a", "name": "main", "owner": "someone-else"}}
	f.assignments = map[string]any{"ms-a": []int{1, 2}}
	f.workspaces = []map[string]any{
		{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
		{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING"},
	}
	f.workspaceMembers = map[string]any{"1": nil, "2": adminByAppID}
	m := newTestMetastoreBuilderAs(t, f, nil, syncPrincipals{}, "app-9")
	g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

	if _, err := m.Grant(context.Background(), alice, g.GetEntitlement()); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if len(f.patches) != 1 {
		t.Fatalf("patches = %v, want one", f.patches)
	}
	for _, host := range f.permHosts {
		if host != "wsb" {
			t.Fatalf("permissions addressed %v, want only wsb, the workspace where the connector is Admin", f.permHosts)
		}
	}
}

// A deleted metastore has nothing to grant and nothing left to revoke.
func TestMetastoreProvisioningOnADeletedMetastore(t *testing.T) {
	newBuilder := func(t *testing.T) (*metastoreBuilder, *fakeAccount) {
		f := defaultFakeAccount()
		f.metastores = []map[string]any{{"metastore_id": "ms-other", "name": "other", "owner": "eng"}}

		return newTestMetastoreBuilder(t, f, nil, syncPrincipals{}), f
	}

	t.Run("grant", func(t *testing.T) {
		m, f := newBuilder(t)
		g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

		_, err := m.Grant(context.Background(), alice, g.GetEntitlement())
		if status.Code(err) != codes.NotFound {
			t.Fatalf("Grant = %v, want NotFound", err)
		}
		if len(f.patches) != 0 {
			t.Errorf("patches = %v, want none", f.patches)
		}
	})

	t.Run("revoke", func(t *testing.T) {
		m, f := newBuilder(t)
		g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

		annos, err := m.Revoke(context.Background(), g)
		if err != nil || !annos.Contains(&v2.GrantAlreadyRevoked{}) {
			t.Fatalf("Revoke = %v, %v; want GrantAlreadyRevoked", annos, err)
		}
		if len(f.patches) != 0 {
			t.Errorf("patches = %v, want none", f.patches)
		}
	})
}

func TestMetastoreRevoke(t *testing.T) {
	f := defaultFakeAccount()
	f.permissions = []map[string]any{{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG"}}}
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

	if _, err := m.Revoke(context.Background(), g); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(f.patches) != 1 || fmt.Sprint(f.patches[0]["principal_id"]) != "1" {
		t.Fatalf("patches = %v, want one removal by principal_id 1", f.patches)
	}

	annos, err := m.Revoke(context.Background(), g)
	if err != nil || !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Fatalf("second Revoke = %v, %v; want GrantAlreadyRevoked", annos, err)
	}
}

// A row the pre-read missed may be a partial read, so the removal still goes out.
func TestMetastoreRevokeWritesWhenThePreReadMissesTheRow(t *testing.T) {
	f := defaultFakeAccount()
	f.permissions = nil
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

	annos, err := m.Revoke(context.Background(), g)
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(f.patches) != 1 {
		t.Fatalf("patches = %v, want the removal to go out anyway", f.patches)
	}
	if got := f.patches[0]["remove"]; fmt.Sprint(got) != "[CREATE_CATALOG]" {
		t.Errorf("removal = %v, want CREATE_CATALOG", got)
	}
	if got := f.patches[0]["principal"]; fmt.Sprint(got) != "alice@example.com" {
		t.Errorf("removal addressed %v, want the principal name alice@example.com", got)
	}
	if _, byID := f.patches[0]["principal_id"]; byID {
		t.Errorf("removal = %v, want no principal_id: the pre-read saw no row to take one from", f.patches[0])
	}
	if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Error("want GrantAlreadyRevoked: the echo proves the privilege is not held")
	}
}

// A deleted principal's rows are only addressable by id.
func TestMetastoreRevokeRemovesADeletedPrincipalByID(t *testing.T) {
	f := defaultFakeAccount()
	f.users = map[string]string{}
	f.permissions = []map[string]any{
		{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG"}},
	}
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

	if _, err := m.Revoke(context.Background(), g); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if len(f.patches) != 1 {
		t.Fatalf("patches = %v, want one removal", f.patches)
	}
	if got := f.patches[0]["principal_id"]; fmt.Sprint(got) != "1" {
		t.Errorf("removal addressed %v, want principal_id 1", got)
	}
	if _, byName := f.patches[0]["principal"]; byName {
		t.Errorf("removal = %v, want no principal name: Databricks no longer has one", f.patches[0])
	}
}

// A grant to a deleted principal would write an empty name.
func TestMetastoreGrantToADeletedPrincipalFails(t *testing.T) {
	f := defaultFakeAccount()
	f.users = map[string]string{}
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

	_, err := m.Grant(context.Background(), alice, g.GetEntitlement())
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Grant = %v, want NotFound", err)
	}
	if len(f.patches) != 0 {
		t.Errorf("patches = %v, want none", f.patches)
	}
}

// A privilege list the connector cannot fully read cannot be written to either:
// the change would be decided from the subset Databricks chose to show.
func TestMetastoreProvisioningRefusesAPartialPrivilegeList(t *testing.T) {
	for _, verb := range []string{"grant", "revoke"} {
		t.Run(verb, func(t *testing.T) {
			f := defaultFakeAccount()
			f.permissions = []map[string]any{
				{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG"}},
			}
			m := newTestMetastoreBuilderAs(t, f, nil, syncPrincipals{}, "app-9")
			g := grant.NewGrant(testMetastore(t), "CREATE_CATALOG", alice.GetId())

			var err error
			if verb == "grant" {
				_, err = m.Grant(context.Background(), alice, g.GetEntitlement())
			} else {
				_, err = m.Revoke(context.Background(), g)
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
