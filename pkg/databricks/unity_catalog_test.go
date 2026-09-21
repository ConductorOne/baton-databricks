package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

type capturedReq struct {
	method string
	url    string
	body   string
}

type captureRT struct {
	reqs []capturedReq
	body string
}

func (rt *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	var b string
	if r.Body != nil {
		data, _ := io.ReadAll(r.Body)
		b = string(data)
	}
	rt.reqs = append(rt.reqs, capturedReq{method: r.Method, url: r.URL.String(), body: b})
	resp := rt.body
	if resp == "" {
		resp = "{}"
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(resp)),
		Request:    r,
	}, nil
}

func TestUnityCatalogRequests(t *testing.T) {
	ctx := context.Background()

	run := func(body string, fn func(c *Client)) []capturedReq {
		rt := &captureRT{body: body}
		c, err := NewClient(ctx, &http.Client{Transport: rt}, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "", &NoAuth{}, nil)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		fn(c)
		return rt.reqs
	}

	// ListCatalogs: workspace subdomain + UC path + query params, decode payload.
	reqs := run(`{"catalogs":[{"name":"prod_evaluation","owner":"SCRUM-AIFUN"}],"next_page_token":"tok2"}`, func(c *Client) {
		cats, next, _, err := c.ListCatalogs(ctx, "dbc-abc", "", 50)
		if err != nil {
			t.Fatalf("ListCatalogs: %v", err)
		}
		if len(cats) != 1 || cats[0].Name != "prod_evaluation" || cats[0].Owner != "SCRUM-AIFUN" {
			t.Fatalf("ListCatalogs decode = %+v", cats)
		}
		if next != "tok2" {
			t.Fatalf("ListCatalogs next = %q, want tok2", next)
		}
	})
	got := reqs[0]
	want := "https://dbc-abc.example.cloud.databricks.com/api/2.1/unity-catalog/catalogs?max_results=50"
	if got.method != http.MethodGet || got.url != want {
		t.Fatalf("ListCatalogs req = %s %s, want GET %s", got.method, got.url, want)
	}

	// ListSchemas: catalog_name query param.
	reqs = run(`{"schemas":[]}`, func(c *Client) {
		if _, _, _, err := c.ListSchemas(ctx, "dbc-abc", "prod_evaluation", "", 50); err != nil {
			t.Fatalf("ListSchemas: %v", err)
		}
	})
	if u := reqs[0].url; u != "https://dbc-abc.example.cloud.databricks.com/api/2.1/unity-catalog/schemas?catalog_name=prod_evaluation&max_results=50" {
		t.Fatalf("ListSchemas url = %s", u)
	}

	// ListTables: catalog_name + schema_name query params.
	reqs = run(`{"tables":[]}`, func(c *Client) {
		if _, _, _, err := c.ListTables(ctx, "dbc-abc", "prod_evaluation", "reports", "", 50); err != nil {
			t.Fatalf("ListTables: %v", err)
		}
	})
	if u := reqs[0].url; u != "https://dbc-abc.example.cloud.databricks.com/api/2.1/unity-catalog/tables?catalog_name=prod_evaluation&max_results=50&schema_name=reports" {
		t.Fatalf("ListTables url = %s", u)
	}

	// ListPermissions on a table: dotted full_name must survive as one path segment.
	reqs = run(`{"privilege_assignments":[{"principal":"alice@corp.com","privileges":["SELECT","MODIFY"]}]}`, func(c *Client) {
		pas, _, err := c.ListPermissions(ctx, "dbc-abc", SecurableTable, "prod_evaluation.reports.performance_report")
		if err != nil {
			t.Fatalf("ListPermissions: %v", err)
		}
		if len(pas) != 1 || pas[0].Principal != "alice@corp.com" || len(pas[0].Privileges) != 2 {
			t.Fatalf("ListPermissions decode = %+v", pas)
		}
	})
	if u := reqs[0].url; u != "https://dbc-abc.example.cloud.databricks.com/api/2.1/unity-catalog/permissions/table/prod_evaluation.reports.performance_report" {
		t.Fatalf("ListPermissions url = %s", u)
	}

	// UpdatePermissions: PATCH with changes body.
	reqs = run(`{"privilege_assignments":[]}`, func(c *Client) {
		if _, err := c.UpdatePermissions(ctx, "dbc-abc", SecurableSchema, "prod_evaluation.reports",
			[]PermissionsChange{{Principal: "SCRUM-AIFUN", Add: []string{"SELECT"}}}); err != nil {
			t.Fatalf("UpdatePermissions: %v", err)
		}
	})
	got = reqs[0]
	if got.method != http.MethodPatch {
		t.Fatalf("UpdatePermissions method = %s, want PATCH", got.method)
	}
	if u := got.url; u != "https://dbc-abc.example.cloud.databricks.com/api/2.1/unity-catalog/permissions/schema/prod_evaluation.reports" {
		t.Fatalf("UpdatePermissions url = %s", u)
	}
	var payload struct {
		Changes []PermissionsChange `json:"changes"`
	}
	if err := json.Unmarshal([]byte(got.body), &payload); err != nil {
		t.Fatalf("UpdatePermissions body not json: %v (%s)", err, got.body)
	}
	if len(payload.Changes) != 1 || payload.Changes[0].Principal != "SCRUM-AIFUN" || payload.Changes[0].Add[0] != "SELECT" {
		t.Fatalf("UpdatePermissions body = %s", got.body)
	}
}
