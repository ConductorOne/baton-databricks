package connector

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rolesTransport answers the assignable-roles calls Validate makes. failAccount
// makes the account-plane check (host "accounts.*") fail, which Validate treats as
// fatal.
type rolesTransport struct{ failAccount bool }

func (t rolesTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.failAccount && strings.HasPrefix(req.URL.Host, "accounts.") {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"boom"}`)),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"roles":[]}`)),
		Request:    req,
	}, nil
}

func newValidateConnector(t *testing.T, auth databricks.Auth, tr http.RoundTripper) *Databricks {
	t.Helper()
	client, err := databricks.NewClient(
		context.Background(), &http.Client{Transport: tr},
		"cloud.databricks.com", "accounts.cloud.databricks.com",
		"acct-1", "", auth, nil,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return &Databricks{client: client}
}

// Under OAuth a failed account check is a fixable misconfiguration, so Validate
// fails rather than silently dropping account-level data.
func TestValidateOAuthAccountCheckFailureReturnsError(t *testing.T) {
	d := newValidateConnector(t, &databricks.NoAuth{}, rolesTransport{failAccount: true})

	if _, err := d.Validate(context.Background()); err == nil {
		t.Fatal("Validate: want error on OAuth account check failure, got nil")
	}
}

// incrementalTransport serves the account calls Validate always makes (roles, one
// workspace) and records every path, so tests can assert whether any incremental-sync
// call (SQL warehouse lookup, statement execution) was made.
type incrementalTransport struct {
	mu    sync.Mutex
	paths []string
}

func (t *incrementalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.paths = append(t.paths, req.URL.Path)
	t.mu.Unlock()

	status, body := http.StatusOK, `{"roles":[]}`
	switch {
	case strings.HasSuffix(req.URL.Path, "/accounts/acct-1/workspaces"):
		body = `[{"workspace_id":1,"workspace_name":"ws1","deployment_name":"ws1"}]`
	case strings.Contains(req.URL.Path, "/sql/warehouses/"):
		status, body = http.StatusNotFound, `{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not found"}`
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func (t *incrementalTransport) hitIncrementalSyncAPI() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.paths {
		if strings.Contains(p, "/sql/") {
			return true
		}
	}
	return false
}

// No sql-warehouse-id means incremental sync is off: Validate succeeds without touching
// any warehouse/audit-log API.
func TestValidateWithoutWarehouseIDDisablesIncrementalSync(t *testing.T) {
	tr := &incrementalTransport{}
	d := newValidateConnector(t, &databricks.NoAuth{}, tr)

	if _, err := d.Validate(context.Background()); err != nil {
		t.Fatalf("Validate: want nil with incremental sync disabled, got %v", err)
	}
	if tr.hitIncrementalSyncAPI() {
		t.Errorf("Validate made incremental-sync calls with no sql-warehouse-id: %v", tr.paths)
	}
}

// A set sql-warehouse-id is an explicit opt-in, so a warehouse that can't be found fails
// validation instead of silently running full syncs only.
func TestValidateWithWarehouseIDFailsWhenWarehouseMissing(t *testing.T) {
	tr := &incrementalTransport{}
	d := newValidateConnector(t, &databricks.NoAuth{}, tr)
	d.sqlWarehouseID = "wh-missing"

	_, err := d.Validate(context.Background())
	if err == nil {
		t.Fatal("Validate: want error for unknown sql-warehouse-id, got nil")
	}
	if !strings.Contains(err.Error(), "sql-warehouse-id") {
		t.Errorf("Validate error = %q, want it to name sql-warehouse-id", err)
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("Validate error code = %s, want %s", got, codes.NotFound)
	}
}

// With sql-warehouse-id set, an allowlist matching no workspace leaves nothing to resolve
// audit events against, which must fail validation.
func TestValidateWithWarehouseIDFailsWhenNoWorkspaceInScope(t *testing.T) {
	tr := &incrementalTransport{}
	d := newValidateConnector(t, &databricks.NoAuth{}, tr)
	d.sqlWarehouseID = "wh-1"
	d.workspaces = []string{"not-a-workspace"}

	_, err := d.Validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no workspace is in sync scope") {
		t.Fatalf("Validate error = %v, want a no-workspace-in-scope error", err)
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("Validate error code = %s, want %s", got, codes.FailedPrecondition)
	}
}

// A whitespace-only sql-warehouse-id counts as unset, so incremental sync stays off.
func TestNewTreatsBlankWarehouseIDAsDisabled(t *testing.T) {
	d, err := New(context.Background(), "cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", &databricks.NoAuth{}, nil, nil, "   ")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.sqlWarehouseID != "" {
		t.Errorf("sqlWarehouseID = %q, want empty (incremental sync disabled)", d.sqlWarehouseID)
	}
}

// With no sql-warehouse-id the feed is a no-op: no events, no API calls.
func TestListEventsWithoutWarehouseIDIsNoop(t *testing.T) {
	tr := &incrementalTransport{}
	d := newValidateConnector(t, &databricks.NoAuth{}, tr)
	feed := newAuditEventFeed(d.client, nil, "")

	events, state, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 0 || state.HasMore {
		t.Errorf("ListEvents = %d events, HasMore=%v; want none", len(events), state.HasMore)
	}
	if len(tr.paths) != 0 {
		t.Errorf("ListEvents made API calls with incremental sync disabled: %v", tr.paths)
	}
}
