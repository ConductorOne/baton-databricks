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

	permHosts []string
	patches   []map[string]any
	scimGets  map[string]int
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

func newTestMetastoreBuilder(t *testing.T, f *fakeAccount, allowlist []string, sync syncPrincipals) *metastoreBuilder {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newMetastoreBuilder(client, configuredWorkspaceSet(allowlist), sync)
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

func TestMetastoreListProfile(t *testing.T) {
	m := newTestMetastoreBuilder(t, defaultFakeAccount(), nil, syncPrincipals{})
	resources, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
	if err != nil || len(resources) != 1 {
		t.Fatalf("List = %v, %v", resources, err)
	}
	profile := rs.GetProfile(resources[0])
	for key, want := range map[string]string{
		profileKeyWorkspace: "wsa", profileKeyWorkspaceID: "1", profileKeyWorkspaceName: "prod",
		profileKeyOwner: "eng", profileKeyMetastoreID: "ms-a", profileKeySecurableType: securableTypeMetastore,
	} {
		if got, _ := rs.GetProfileStringValue(profile, key); got != want {
			t.Errorf("profile[%s] = %q, want %q", key, got, want)
		}
	}
	if resources[0].GetDisplayName() != "main" {
		t.Errorf("display name = %q, want main", resources[0].GetDisplayName())
	}
}

func TestMetastoreListFailsWhenItCannotRoute(t *testing.T) {
	tests := []struct {
		name       string
		assignment any
	}{
		{"assignment list answers 403", http.StatusForbidden},
		{"assignment list answers 500", http.StatusInternalServerError},
		{"only in-scope workspace is not running", []int{2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			f.assignments["ms-a"] = test.assignment
			f.workspaces = append(f.workspaces,
				map[string]any{"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "PROVISIONING"})
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

			if _, _, err := m.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{}); err == nil {
				t.Fatal("List succeeded")
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

func TestMetastoreGrants(t *testing.T) {
	f := defaultFakeAccount()
	f.permissions = []map[string]any{
		{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"CREATE_CATALOG", "NOT_OFFERED"}},
		{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"USE_SHARE"}},
		{"principal": "eng", "principal_id": 2, "privileges": []string{"READ_METADATA"}},
		{"principal": "app-9", "principal_id": 3, "privileges": []string{"CREATE_SHARE"}},
		{"principal": "", "principal_id": 99, "privileges": []string{"CREATE_SHARE"}},
	}
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{groups: true})

	grants, _, err := m.Grants(context.Background(), testMetastore(t, "wsa"), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}
	group := "account/" + testAccountId + "/group/2"
	want := []string{
		"1 metastore:ms-a:CREATE_CATALOG",
		"1 metastore:ms-a:USE_SHARE",
		group + " metastore:ms-a:READ_METADATA",
		group + " metastore:ms-a:owner",
	}
	slices.Sort(want)
	if got := grantKeys(grants); !slices.Equal(got, want) {
		t.Fatalf("grants = %v, want %v", got, want)
	}
	if !slices.Equal(f.permHosts, []string{"wsa"}) {
		t.Errorf("permissions read through %v, want [wsa]", f.permHosts)
	}
	if f.scimGets["Users/1"] != 1 {
		t.Errorf("user 1 resolved %d times, want 1", f.scimGets["Users/1"])
	}
	for _, g := range grants {
		annos := g.GetAnnotations()
		if !strings.HasSuffix(g.GetEntitlement().GetId(), ":owner") {
			continue
		}
		if len(annos) != 2 {
			t.Errorf("owner grant annotations = %v, want expandable and immutable", annos)
		}
	}
}

func TestMetastoreGrantsReturnNothing(t *testing.T) {
	tests := []struct {
		name       string
		workspace  string
		permStatus int
	}{
		{"no workspace on the profile", "", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := defaultFakeAccount()
			f.permStatus = test.permStatus
			m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{groups: true})

			grants, _, err := m.Grants(context.Background(), testMetastore(t, test.workspace), rs.SyncOpAttrs{})
			if err != nil || len(grants) != 0 {
				t.Fatalf("Grants = %v, %v; want none and no error", grants, err)
			}
		})
	}
}

func TestMetastoreGrantsFailOnErrors(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		f := defaultFakeAccount()
		f.permStatus = code
		m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})

		_, _, err := m.Grants(context.Background(), testMetastore(t, "wsa"), rs.SyncOpAttrs{})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("Grants on %d = %v, want FailedPrecondition", code, err)
		}
	}
}

var alice = &v2.Resource{Id: &v2.ResourceId{ResourceType: "user", Resource: "1"}}

func TestMetastoreGrant(t *testing.T) {
	f := defaultFakeAccount()
	m := newTestMetastoreBuilder(t, f, nil, syncPrincipals{})
	g := grant.NewGrant(testMetastore(t, "wsa"), "CREATE_CATALOG", alice.GetId())

	if _, err := m.Grant(context.Background(), alice, g.GetEntitlement()); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if len(f.patches) != 1 || f.patches[0]["principal"] != "alice@example.com" {
		t.Fatalf("patches = %v, want one add for alice@example.com", f.patches)
	}

	annos, err := m.Grant(context.Background(), alice, g.GetEntitlement())
	if err != nil || !annos.Contains(&v2.GrantAlreadyExists{}) {
		t.Fatalf("second Grant = %v, %v; want GrantAlreadyExists", annos, err)
	}
	if len(f.patches) != 1 {
		t.Errorf("a held privilege was patched again: %v", f.patches)
	}
}

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
			t.Errorf("workspace %s answered %+v, want %s", workspace, assignments, held[workspace])
		}
	}
}
