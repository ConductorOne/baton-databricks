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
	_, rest, _ := strings.Cut(r.URL.Path, "/scim/v2/")
	kind, id, _ := strings.Cut(rest, "/")
	field, records := map[string]string{"Users": "userName", "Groups": "displayName", "ServicePrincipals": "applicationId"}[kind],
		map[string]map[string]string{"Users": f.users, "Groups": f.groups, "ServicePrincipals": f.sps}[kind]
	if id != "" {
		f.scimGets[kind+"/"+id]++
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

func (f *fakeAccount) apply(change map[string]any) {
	added, _ := change["add"].([]any)
	for _, privilege := range added {
		f.permissions = append(f.permissions, map[string]any{
			"principal": change["principal"], "principal_id": 1, "privileges": []string{privilege.(string)},
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

func testMetastore(t *testing.T, workspace string) *v2.Resource {
	t.Helper()
	r, err := metastoreResource(context.Background(), &databricks.Metastore{MetastoreID: "ms-a", Name: "main", Owner: "eng"},
		testAccountResourceID, workspace, "1", "prod")
	if err != nil {
		t.Fatalf("metastoreResource: %v", err)
	}

	return r
}

// Which workspaces serve a metastore decides whether it syncs at all, so an
// assignment list the connector cannot read is unknown, not absent. Skipping it
// would drop the metastore and make ConductorOne remove its grants.
func TestMetastoreListFailsWhenAnAssignmentListCannotBeRead(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusUnauthorized} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			f := defaultFakeAccount()
			f.assignments = map[string]any{"ms-a": code}
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

			resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
			if err == nil {
				t.Fatalf("List returned %d resources and no error, want the sync to fail", len(resources))
			}
			if !strings.Contains(err.Error(), "ms-a") {
				t.Errorf("error %q does not name the metastore", err)
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

func TestMetastoreListPicksTheFirstUsableWorkspace(t *testing.T) {
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

// Databricks answers an under-permissioned read of the permissions endpoint
// with 200 and only the privileges the caller may see, which is indistinguishable
// from a metastore with no grants. The sync therefore reports grants only when
// it can prove it reads all of them.
func TestMetastoreGrantsRequireFullVisibility(t *testing.T) {
	adminByAppID := []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"ADMIN"}},
	}
	adminByScimID := []map[string]any{
		{"principal": map[string]any{"principal_id": 3}, "permissions": []string{"ADMIN"}},
	}
	notAdmin := []map[string]any{
		{"principal": map[string]any{"service_principal_name": "app-9"}, "permissions": []string{"USER"}},
		{"principal": map[string]any{"group_name": "admins"}, "permissions": []string{"ADMIN"}},
	}

	tests := []struct {
		name       string
		clientID   string
		members    any
		wantGrants bool
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
			f.permissions = []map[string]any{
				{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG"}},
			}
			m := newTestMetastoreBuilderAs(t, f, nil, syncPrincipals{}, test.clientID)

			grants, _, err := m.Grants(context.Background(), testMetastore(t, "wsa"), rs.SyncOpAttrs{})
			switch {
			case test.wantGrants && err != nil:
				t.Fatalf("Grants: %v", err)
			case test.wantGrants && len(grants) == 0:
				t.Fatal("Grants returned nothing, want the privileges it can read")
			case !test.wantGrants:
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("Grants = %v, want PermissionDenied", err)
				}
				if len(grants) != 0 {
					t.Errorf("grants = %v, want none", grantKeys(grants))
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

var alice = &v2.Resource{Id: &v2.ResourceId{ResourceType: "user", Resource: "1"}}

func TestMetastoreGrantRejections(t *testing.T) {
	tests := []struct {
		name        string
		workspace   string
		privilege   string
		patchStatus int
		code        codes.Code
	}{
		{"owner", "wsa", ownerEntitlement, 0, codes.InvalidArgument},
		{"legacy alias", "wsa", "USAGE", 0, codes.InvalidArgument},
		{"not offered", "wsa", "MANAGE", 0, codes.InvalidArgument},
		{"no workspace on the profile", "", "CREATE_CATALOG", 0, codes.FailedPrecondition},
		{"forbidden", "wsa", "CREATE_CATALOG", http.StatusForbidden, codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
			g := grant.NewGrant(testMetastore(t, test.workspace), test.privilege, alice.GetId())
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

func TestMetastoreRevoke(t *testing.T) {
	f := defaultFakeAccount()
	f.permissions = []map[string]any{{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG"}}}
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testMetastore(t, "wsa"), "CREATE_CATALOG", alice.GetId())

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
			g := grant.NewGrant(testMetastore(t, "wsa"), "CREATE_CATALOG", alice.GetId())

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
