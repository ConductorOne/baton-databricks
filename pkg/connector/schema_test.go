package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeSchemaAccount serves one metastore, one workspace and one catalog. schemaPages
// is what ListSchemas returns; schemaStatus forces that call's HTTP status.
type fakeSchemaAccount struct {
	metastoreOwner string
	catalogOwner   string
	schemas        []map[string]any
	schemaStatus   int
	schemaCode     string
	// manage is the effective catalog privilege set for the connector principal.
	manage []string

	routing int
	patches int
	users   map[string]string
}

func (f *fakeSchemaAccount) handler(t *testing.T) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/metastores"):
			f.routing++
			_ = json.NewEncoder(w).Encode(map[string]any{"metastores": []map[string]any{
				{"metastore_id": "ms-1", "name": "main", "owner": f.metastoreOwner},
			}})
		case strings.Contains(path, "/metastores/") && strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace_ids": []int{1}})
		case strings.HasSuffix(path, "/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": "RUNNING"},
			})
		case strings.HasSuffix(path, "/unity-catalog/catalogs"):
			_ = json.NewEncoder(w).Encode(map[string]any{"catalogs": []map[string]any{
				{"name": "main", "metastore_id": "ms-1", "owner": f.catalogOwner},
			}})
		case strings.HasSuffix(path, "/unity-catalog/schemas"):
			if f.schemaStatus != 0 {
				body := `{"message":"missing"}`
				if f.schemaCode != "" {
					body = fmt.Sprintf(`{"error_code":%q,"message":"missing"}`, f.schemaCode)
				}
				http.Error(w, body, f.schemaStatus)
				return
			}
			all := f.schemas
			start, _ := strconv.Atoi(r.URL.Query().Get("page_token"))
			size, _ := strconv.Atoi(r.URL.Query().Get("max_results"))
			if size <= 0 || size > len(all)-start {
				size = len(all) - start
			}
			if start > len(all) {
				start = len(all)
				size = 0
			}
			next := ""
			if start+size < len(all) {
				next = strconv.Itoa(start + size)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"schemas": all[start : start+size], "next_page_token": next})
		case strings.Contains(path, "/unity-catalog/permissions/"):
			if r.Method == http.MethodPatch {
				f.patches++
				_, _ = io.Copy(io.Discard, r.Body)
			}
			if f.schemaCode != "" && strings.Contains(path, "/permissions/schema/") {
				http.Error(w, fmt.Sprintf(`{"error_code":%q,"message":"gone"}`, f.schemaCode), http.StatusNotFound)
				return
			}
			if r.URL.Query().Get("principal") != "" {
				var assignments []map[string]any
				if len(f.manage) > 0 {
					assignments = []map[string]any{{"principal": "app-1", "privileges": f.manage}}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": assignments})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"privilege_assignments": []map[string]any{}})
		case strings.Contains(path, "/scim/v2/Users"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Resources": []map[string]any{
				{"id": "1", "userName": f.users["1"]},
			}, "totalResults": 1})
		default:
			http.Error(w, `{"message":"unexpected `+path+`"}`, http.StatusInternalServerError)
		}
	}
}

func newFakeSchemaBuilder(t *testing.T, f *fakeSchemaAccount) *schemaBuilder {
	t.Helper()
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newSchemaBuilder(client, newUnityCatalog(client, "app-1", nil), syncPrincipals{})
}

func listSchemas(t *testing.T, b *schemaBuilder) ([]*v2.Resource, error) {
	t.Helper()

	parent := &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: "ms-1::main"}
	var all []*v2.Resource
	token := ""
	for range 10 {
		resources, results, err := b.List(context.Background(), parent, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		if err != nil {
			return all, err
		}
		all = append(all, resources...)
		if results == nil || results.NextPageToken == "" {
			return all, nil
		}
		token = results.NextPageToken
	}
	t.Fatal("List never finished")

	return nil, nil
}

func TestSchemaListSkipsInformationSchema(t *testing.T) {
	f := &fakeSchemaAccount{
		metastoreOwner: "app-1",
		catalogOwner:   "someone",
		schemas: []map[string]any{
			{"name": "sales", "owner": "someone"},
			{"name": "information_schema", "owner": "System user"},
		},
	}
	b := newFakeSchemaBuilder(t, f)

	resources, err := listSchemas(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 1 || resources[0].GetDisplayName() != "main.sales" {
		t.Fatalf("listed %#v, want main.sales", resources)
	}
}

func TestSchemaListFailsClosedWhenTheListingWouldBePartial(t *testing.T) {
	f := &fakeSchemaAccount{
		metastoreOwner: "someone",
		catalogOwner:   "someone",
		schemas:        []map[string]any{{"name": "sales", "owner": "someone"}},
	}
	b := newFakeSchemaBuilder(t, f)

	_, err := listSchemas(t, b)
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), schemaListRemedy) {
		t.Fatalf("List = %v, want PermissionDenied naming the remedy", err)
	}
}

func TestSchemaListTrustsManageOnTheCatalog(t *testing.T) {
	f := &fakeSchemaAccount{
		metastoreOwner: "someone",
		catalogOwner:   "someone",
		manage:         []string{"MANAGE"},
		schemas:        []map[string]any{{"name": "sales", "owner": "someone"}},
	}
	b := newFakeSchemaBuilder(t, f)

	resources, err := listSchemas(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("listed %d schemas, want 1", len(resources))
	}
}

func TestSchemaListDoesNotRebuildRoutingOnLaterPages(t *testing.T) {
	schemas := make([]map[string]any, 0, 51)
	for i := range 51 {
		schemas = append(schemas, map[string]any{"name": fmt.Sprintf("s%02d", i), "owner": "someone"})
	}
	f := &fakeSchemaAccount{metastoreOwner: "app-1", catalogOwner: "someone", schemas: schemas}
	b := newFakeSchemaBuilder(t, f)

	resources, err := listSchemas(t, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 51 {
		t.Fatalf("listed %d schemas, want 51", len(resources))
	}
	if f.routing != 1 {
		t.Errorf("routing ran %d times, want 1", f.routing)
	}
}

func TestSchemaListNotFoundIsNotAWarning(t *testing.T) {
	f := &fakeSchemaAccount{metastoreOwner: "app-1", catalogOwner: "someone", schemaStatus: http.StatusNotFound}
	b := newFakeSchemaBuilder(t, f)

	_, err := listSchemas(t, b)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("List = %v, want Unavailable", err)
	}
}

func TestRevokeOnADroppedSchemaIsAlreadyRevoked(t *testing.T) {
	f := &fakeSchemaAccount{
		metastoreOwner: "app-1",
		catalogOwner:   "someone",
		schemaCode:     "SCHEMA_DOES_NOT_EXIST",
		users:          map[string]string{"1": "alice@example.com"},
	}
	b := newFakeSchemaBuilder(t, f)
	principal := &v2.Resource{Id: &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "1"}}
	resource, err := newSecurableResource(schemaResourceType, securableTypeSchema, nil, securable{
		name: "sales", owner: "someone",
		profile: map[string]any{
			profileKeyMetastoreOwner: "app-1",
			profileKeyWorkspace:      "wsa",
			profileKeyWorkspaceID:    "1",
			profileKeyWorkspaceName:  "prod",
		},
	}, securableRef{metastoreID: "ms-1", parts: []string{"main", "sales"}}, &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: "ms-1::main"})
	if err != nil {
		t.Fatalf("resource: %v", err)
	}

	annos, err := b.Revoke(context.Background(), grant.NewGrant(resource, "SELECT", principal.GetId()))
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if !annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Fatal("dropped schema did not report GrantAlreadyRevoked")
	}
	if f.patches != 0 {
		t.Errorf("dropped schema sent %d PATCHes", f.patches)
	}
}

func TestRevokeOnARoute404StillFails(t *testing.T) {
	f := &fakeSchemaAccount{
		metastoreOwner: "app-1",
		catalogOwner:   "someone",
		schemaCode:     "RESOURCE_DOES_NOT_EXIST",
		users:          map[string]string{"1": "alice@example.com"},
	}
	b := newFakeSchemaBuilder(t, f)
	principal := &v2.Resource{Id: &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "1"}}
	resource, err := newSecurableResource(schemaResourceType, securableTypeSchema, nil, securable{
		name: "sales",
		profile: map[string]any{
			profileKeyMetastoreOwner: "app-1",
			profileKeyWorkspace:      "wsa",
			profileKeyWorkspaceID:    "1",
			profileKeyWorkspaceName:  "prod",
		},
	}, securableRef{metastoreID: "ms-1", parts: []string{"main", "sales"}}, &v2.ResourceId{ResourceType: catalogResourceType.Id, Resource: "ms-1::main"})
	if err != nil {
		t.Fatalf("resource: %v", err)
	}

	annos, err := b.Revoke(context.Background(), grant.NewGrant(resource, "SELECT", principal.GetId()))
	if err == nil || annos.Contains(&v2.GrantAlreadyRevoked{}) {
		t.Fatalf("route 404 = %v annos already-revoked %v, want an error", err, annos.Contains(&v2.GrantAlreadyRevoked{}))
	}
}
