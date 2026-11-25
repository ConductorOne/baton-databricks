package connector

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// TestSchemaChildGoneNeedsTheParentServed pins the rule that decides whether C1
// keeps or removes a table's and a volume's grants: a 404 is proof of deletion
// only from a workspace that has just answered 200 for the parent schema. A
// workspace that cannot serve the catalog answers the same codes, so believing
// one of those empties a securable that still exists.
func TestSchemaChildGoneNeedsTheParentServed(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	const (
		schemaPath = "/api/2.1/unity-catalog/schemas/main.sales"
		tablePath  = "/api/2.1/unity-catalog/tables/main.sales.orders"
		volumePath = "/api/2.1/unity-catalog/volumes/main.sales.files"
	)

	leafBodies := map[string]string{
		tablePath:  `{"name":"orders","full_name":"main.sales.orders","owner":"eng","table_type":"MANAGED"}`,
		volumePath: `{"name":"files","full_name":"main.sales.files","owner":"eng","volume_type":"MANAGED"}`,
	}

	for _, tc := range []struct {
		name         string
		schemaStatus int
		leafStatus   int
		wantGone     bool
		wantErr      bool
	}{
		{name: "parent served and the securable is absent", schemaStatus: 200, leafStatus: 404, wantGone: true},
		{name: "parent served and the securable answers", schemaStatus: 200, leafStatus: 200, wantGone: false},
		{name: "parent not served, so the absence is unproven", schemaStatus: 404, leafStatus: 404, wantGone: false, wantErr: true},
		{name: "parent refused, so the absence is unproven", schemaStatus: 403, leafStatus: 404, wantGone: false, wantErr: true},
		{name: "parent served and the securable read fails", schemaStatus: 200, leafStatus: 500, wantGone: false, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var leafReads int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case schemaPath:
					if tc.schemaStatus != http.StatusOK {
						w.WriteHeader(tc.schemaStatus)
						_, _ = io.WriteString(w, `{"error_code":"CATALOG_DOES_NOT_EXIST","message":"no"}`)

						return
					}
					_, _ = io.WriteString(w, `{"name":"sales","full_name":"main.sales","owner":"eng"}`)
				case tablePath, volumePath:
					leafReads++
					if tc.leafStatus != http.StatusOK {
						w.WriteHeader(tc.leafStatus)
						_, _ = io.WriteString(w, `{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"no"}`)

						return
					}
					_, _ = io.WriteString(w, leafBodies[r.URL.Path])
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(srv.Close)

			client := newGoneCheckClient(t, srv.URL)
			annos := annotations.Annotations{}

			for _, probe := range []struct {
				noun string
				fn   func(context.Context, *databricks.Client, string, string, *annotations.Annotations) (bool, error)
				full string
			}{
				{"table", tableIsGone, "main.sales.orders"},
				{"volume", volumeIsGone, "main.sales.files"},
			} {
				gone, err := probe.fn(context.Background(), client, probe.full, testWorkspaceId, &annos)
				if gone != tc.wantGone {
					t.Errorf("%s: gone = %v, want %v", probe.noun, gone, tc.wantGone)
				}
				if (err != nil) != tc.wantErr {
					t.Errorf("%s: err = %v, want error %v", probe.noun, err, tc.wantErr)
				}
			}

			if tc.schemaStatus != http.StatusOK && leafReads != 0 {
				t.Errorf("read the securable %d times without the parent served, want 0", leafReads)
			}
		})
	}
}

// TestTableGoneCodeNamingTheTableNeedsNoConfirmation keeps the one unambiguous
// code cheap: it names the table, so no extra read runs.
func TestTableGoneCodeNamingTheTableNeedsNoConfirmation(t *testing.T) {
	if got := tableLevel(tablePrivileges).goneErrorCodes; len(got) != 1 || got[0] != errorCodeTableGone {
		t.Fatalf("table goneErrorCodes = %v, want only %q", got, errorCodeTableGone)
	}
	if got := volumeLevel.goneErrorCodes; len(got) != 0 {
		t.Fatalf("volume goneErrorCodes = %v, want none: Databricks never names a missing volume", got)
	}
}

func newGoneCheckClient(t *testing.T, serverURL string) *databricks.Client {
	t.Helper()
	target, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	httpClient := &http.Client{Transport: &hostRewriter{target: target}}
	client, err := databricks.NewClient(context.Background(), httpClient,
		testHostname, testAccountHost, testAccountId, "", &databricks.NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	return client
}
