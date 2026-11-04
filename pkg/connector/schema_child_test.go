package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serveSchema answers GET /schemas/{full_name}.
func (f *fakeSchemaAccount) serveSchema(w http.ResponseWriter, r *http.Request) {
	workspace := r.Header.Get("X-Test-Workspace")
	f.schemaReads = append(f.schemaReads, workspace)
	if code, ok := f.schemaGetStatus[workspace]; ok {
		body := `{"message":"refused"}`
		if f.schemaGetErrorCode != "" {
			body = fmt.Sprintf(`{"error_code":%q,"message":"refused"}`, f.schemaGetErrorCode)
		}
		http.Error(w, body, code)

		return
	}

	owner := "someone"
	if len(f.schemas) > 0 {
		owner, _ = f.schemas[0]["owner"].(string)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"name": "sales", "full_name": "main.sales", "owner": owner})
}

func (f *fakeSchemaAccount) serveSchemaChildren(w http.ResponseWriter, r *http.Request, key string, rows []map[string]any) {
	f.childReads = append(f.childReads, key)
	if got := r.URL.Query().Get("schema_name"); got == "" {
		http.Error(w, `{"message":"schema_name is required"}`, http.StatusBadRequest)

		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{key: rows})
}

var testSchemaResourceID = &v2.ResourceId{ResourceType: schemaResourceType.Id, Resource: "ms-1::main.sales"}

func newTestChildBuildersAs(t *testing.T, f *fakeSchemaAccount, clientID string) (*tableBuilder, *volumeBuilder) {
	t.Helper()
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	if f.metastoreOwner == "" {
		f.metastoreOwner = testOwnPrincipal
	}
	if f.assignments == nil {
		f.assignments = map[string][]map[string]any{}
	}
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	return newTableBuilder(client, clientID, nil, syncPrincipals{groups: true}),
		newVolumeBuilder(client, clientID, nil, syncPrincipals{groups: true})
}

func listAllChildren(t *testing.T,
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

// A schema's children are listed after reading that one schema, not after
// walking the catalog's whole schema listing: the walk runs once per schema and
// again per child type, which is quadratic in the catalog's schema count.
func TestSchemaChildListReadsOneSchemaInsteadOfDrainingTheCatalog(t *testing.T) {
	// sales sits last, so walking the listing pays for every page of the catalog.
	manySchemas := make([]map[string]any, 0, 300)
	for i := range 299 {
		manySchemas = append(manySchemas, map[string]any{"name": fmt.Sprintf("s%03d", i), "owner": "eng"})
	}
	manySchemas = append(manySchemas, map[string]any{"name": "sales", "owner": "eng"})

	f := &fakeSchemaAccount{
		metastoreOwner: "eng",
		catalogOwner:   "eng",
		schemas:        manySchemas,
		tables:         []map[string]any{{"name": "orders", "owner": "eng", "table_type": "MANAGED"}},
		volumes:        []map[string]any{{"name": "files", "owner": "eng", "volume_type": "MANAGED"}},
	}
	tables, volumes := newTestChildBuildersAs(t, f, "eng")

	listedTables, err := listAllChildren(t, tables.List)
	if err != nil {
		t.Fatalf("table List: %v", err)
	}
	listedVolumes, err := listAllChildren(t, volumes.List)
	if err != nil {
		t.Fatalf("volume List: %v", err)
	}

	if len(listedTables) != 1 || len(listedVolumes) != 1 {
		t.Fatalf("listed %d table(s) and %d volume(s), want 1 of each", len(listedTables), len(listedVolumes))
	}
	if f.schemaPages != 0 {
		t.Errorf("the schemas listing was paged %d time(s); listing a schema's children must not walk the catalog's schemas", f.schemaPages)
	}
	if want := []string{"wsa", "wsa"}; !slices.Equal(f.schemaReads, want) {
		t.Errorf("single-schema reads = %v, want %v: one per child type", f.schemaReads, want)
	}
}

// A schema named as missing is proof of deletion only for a caller that sees the
// whole catalog. Reading the other case as "no children" would make ConductorOne
// delete every table and volume under it.
func TestSchemaChildListTellsADeletedSchemaFromAnInvisibleOne(t *testing.T) {
	tests := []struct {
		name         string
		clientID     string
		catalogOwner string
		errorCode    string
		wantErr      codes.Code
	}{
		{
			name:     "the connector owns the catalog, so the schema is gone",
			clientID: "eng", catalogOwner: "eng", errorCode: errorCodeSchemaGone,
		},
		{
			name:     "the connector cannot see the whole catalog, so the sync fails",
			clientID: "app-9", catalogOwner: "someone", errorCode: errorCodeSchemaGone,
			wantErr: codes.PermissionDenied,
		},
		{
			// A 404 Databricks does not put a code on says nothing about the
			// schema. Read as an empty result it would delete every table and
			// volume under it, so the sync fails instead.
			name:     "a 404 naming nothing is not proof the schema is gone",
			clientID: "eng", catalogOwner: "eng",
			wantErr: codes.FailedPrecondition,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeSchemaAccount{
				metastoreOwner:     "someone",
				catalogOwner:       test.catalogOwner,
				schemas:            []map[string]any{{"name": "sales", "owner": "someone"}},
				schemaGetStatus:    map[string]int{"wsa": http.StatusNotFound},
				schemaGetErrorCode: test.errorCode,
			}
			tables, _ := newTestChildBuildersAs(t, f, test.clientID)

			listed, err := listAllChildren(t, tables.List)
			if test.wantErr != codes.OK {
				if status.Code(err) != test.wantErr {
					t.Fatalf("List error = %v, want %s", err, test.wantErr)
				}

				return
			}
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(listed) != 0 {
				t.Errorf("listed %d table(s), want none under a schema that is gone", len(listed))
			}
			// The empty result must come from the schema read proving the
			// absence, not from an earlier silent exit.
			if len(f.schemaReads) == 0 || len(f.childReads) != 0 {
				t.Errorf("schema reads = %v, child listings = %v: want the empty result to come from the schema read",
					f.schemaReads, f.childReads)
			}
		})
	}
}
