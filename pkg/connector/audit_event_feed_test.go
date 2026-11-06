package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testEarliestEvent starts a fresh cursor before lagCutoff, so ListEvents' bootstrap
// doesn't hit the guard for cursor.StartAt >= lagCutoff (see TestListEventsSkipsQueryPastLagCutoff).
func testEarliestEvent() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().Add(-2 * auditLogTrailingLag))
}

func TestBootstrapStartAt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name          string
		earliestEvent *timestamppb.Timestamp
		want          time.Time
	}{
		{"nil earliestEvent uses the lookback default", nil, now.Add(-auditLogLookback)},
		{"zero-valued earliestEvent is treated like nil", timestamppb.New(time.Unix(0, 0)), now.Add(-auditLogLookback)},
		{"recent earliestEvent is used as-is", timestamppb.New(now.Add(-time.Hour)), now.Add(-time.Hour)},
		{"earliestEvent older than retention is clamped", timestamppb.New(now.AddDate(-2, 0, 0)), now.Add(-auditLogRetention)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bootstrapStartAt(now, tc.earliestEvent); !got.Equal(tc.want) {
				t.Errorf("bootstrapStartAt() = %v, want %v", got, tc.want)
			}
		})
	}
}

// writeJSONNotFound writes a 404 with a JSON body; a plain-text one (e.g. http.NotFound)
// breaks the client's JSON decoder.
func writeJSONNotFound(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not found"}`))
}

// newProbeTestClient builds a databricks.Client whose requests are redirected to a local
// httptest.Server running handler (see redirectTransport).
func newProbeTestClient(t *testing.T, handler http.HandlerFunc) *databricks.Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	httpClient := &http.Client{Transport: &redirectTransport{target: target}}
	auth := &databricks.NoAuth{}
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func TestResolveWarehouseWorkspace(t *testing.T) {
	const warehouseId = "wh-123"

	t.Run("single workspace is still probed", func(t *testing.T) {
		probed := false
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			probed = true
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":%q}`, warehouseId)
		})
		workspaces := []databricks.Workspace{{ID: 1, DeploymentName: "dbc-only"}}

		got, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if err != nil {
			t.Fatalf("resolveWarehouseWorkspace() error = %v", err)
		}
		if got != "dbc-only" {
			t.Errorf("got %q, want %q", got, "dbc-only")
		}
		if !probed {
			t.Error("WarehouseExists was never called; a single workspace must still be probed so a bad sql-warehouse-id is caught with a clear error")
		}
	})

	t.Run("single workspace missing the warehouse gets a specific not-found error", func(t *testing.T) {
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSONNotFound(w)
		})
		workspaces := []databricks.Workspace{{ID: 1, DeploymentName: "dbc-only"}}

		_, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if err == nil {
			t.Fatal("resolveWarehouseWorkspace() error = nil, want error when the single workspace doesn't have the warehouse")
		}
		wantMsg := `sql-warehouse-id "wh-123" was not found in workspace dbc-only`
		if !strings.Contains(err.Error(), wantMsg) {
			t.Errorf("error = %q, want it to contain %q", err.Error(), wantMsg)
		}
	})

	t.Run("probes each workspace until the warehouse is found", func(t *testing.T) {
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.Header.Get("X-Test-Original-Host"), "dbc-bbb.") {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"id":%q}`, warehouseId)
				return
			}
			writeJSONNotFound(w)
		})
		workspaces := []databricks.Workspace{
			{ID: 1, DeploymentName: "dbc-aaa"},
			{ID: 2, DeploymentName: "dbc-bbb"},
		}

		got, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if err != nil {
			t.Fatalf("resolveWarehouseWorkspace() error = %v", err)
		}
		if got != "dbc-bbb" {
			t.Errorf("got %q, want %q", got, "dbc-bbb")
		}
	})

	t.Run("warehouse not found anywhere is a clear error", func(t *testing.T) {
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSONNotFound(w)
		})
		workspaces := []databricks.Workspace{
			{ID: 1, DeploymentName: "dbc-aaa"},
			{ID: 2, DeploymentName: "dbc-bbb"},
		}

		_, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if err == nil {
			t.Fatal("resolveWarehouseWorkspace() error = nil, want error when no workspace has the warehouse")
		}
	})

	t.Run("an inaccessible workspace is skipped, not fatal", func(t *testing.T) {
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.Header.Get("X-Test-Original-Host"), "dbc-aaa.") {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"not assigned to workspace"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":%q}`, warehouseId)
		})
		workspaces := []databricks.Workspace{
			{ID: 1, DeploymentName: "dbc-aaa"},
			{ID: 2, DeploymentName: "dbc-bbb"},
		}

		got, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if err != nil {
			t.Fatalf("resolveWarehouseWorkspace() error = %v", err)
		}
		if got != "dbc-bbb" {
			t.Errorf("got %q, want %q", got, "dbc-bbb")
		}
	})

	t.Run("all workspaces inaccessible is a not-found naming them", func(t *testing.T) {
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"unauthorized"}`))
		})
		workspaces := []databricks.Workspace{
			{ID: 1, DeploymentName: "dbc-aaa"},
			{ID: 2, DeploymentName: "dbc-bbb"},
		}

		_, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if got := status.Code(err); got != codes.NotFound {
			t.Fatalf("error code = %s (%v), want %s", got, err, codes.NotFound)
		}
		if !strings.Contains(err.Error(), "skipped 2 inaccessible: dbc-aaa, dbc-bbb") {
			t.Errorf("error = %q, want it to name the skipped workspaces", err.Error())
		}
	})

	t.Run("a real error from a probe is returned, not swallowed as not-found", func(t *testing.T) {
		client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"boom"}`))
		})
		workspaces := []databricks.Workspace{
			{ID: 1, DeploymentName: "dbc-aaa"},
			{ID: 2, DeploymentName: "dbc-bbb"},
		}

		_, _, err := resolveWarehouseWorkspace(context.Background(), client, workspaces, warehouseId)
		if err == nil {
			t.Fatal("resolveWarehouseWorkspace() error = nil, want a propagated probe error")
		}
	})
}

func TestEventCursorRoundTrip(t *testing.T) {
	want := eventPageCursor{
		StartAt:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		StartAfterEventID: "b",
	}

	encoded, err := encodeEventCursor(want)
	if err != nil {
		t.Fatalf("encodeEventCursor() error = %v", err)
	}

	got := decodeEventCursor(context.Background(), encoded, want.StartAt)
	if !got.StartAt.Equal(want.StartAt) || got.StartAfterEventID != want.StartAfterEventID {
		t.Errorf("decodeEventCursor() = %+v, want %+v", got, want)
	}
}

func TestDecodeEventCursorSelfHeals(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []string{"", "not-base64!!!", "aW52YWxpZC1qc29u"} // last one is base64("invalid-json")
	for _, c := range cases {
		got := decodeEventCursor(context.Background(), c, now)
		if !got.StartAt.IsZero() {
			t.Errorf("decodeEventCursor(%q) = %+v, want zero-value cursor", c, got)
		}
	}
}

// TestDecodeEventCursorResetsStaleCursor covers a valid cursor whose StartAt has aged past
// the audit log retention window, which must self-heal like a corrupt cursor.
func TestDecodeEventCursorResetsStaleCursor(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	stale := eventPageCursor{StartAt: now.Add(-auditLogRetention - time.Hour), StartAfterEventID: "x"}
	encoded, err := encodeEventCursor(stale)
	if err != nil {
		t.Fatalf("encodeEventCursor() error = %v", err)
	}

	got := decodeEventCursor(context.Background(), encoded, now)
	if !got.StartAt.IsZero() || got.StartAfterEventID != "" {
		t.Errorf("decodeEventCursor() = %+v, want zero-value cursor for a stale StartAt", got)
	}

	fresh := eventPageCursor{StartAt: now.Add(-auditLogRetention + time.Hour), StartAfterEventID: "y"}
	encoded, err = encodeEventCursor(fresh)
	if err != nil {
		t.Fatalf("encodeEventCursor() error = %v", err)
	}

	got = decodeEventCursor(context.Background(), encoded, now)
	if !got.StartAt.Equal(fresh.StartAt) || got.StartAfterEventID != fresh.StartAfterEventID {
		t.Errorf("decodeEventCursor() = %+v, want unchanged %+v (within retention)", got, fresh)
	}
}

func TestAdvanceEventCursorAdvancesToLastRawBoundary(t *testing.T) {
	cursor := eventPageCursor{StartAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	lastRawBoundary := eventPageCursor{StartAt: cursor.StartAt.Add(time.Minute), StartAfterEventID: "evt-999"}

	next, advanced := advanceEventCursor(cursor, nil, lastRawBoundary, true, cursor.StartAt.Add(auditLogTrailingLag))

	if !advanced || next != lastRawBoundary {
		t.Errorf("next = %+v, advanced = %v, want %+v, true", next, advanced, lastRawBoundary)
	}
}

// TestAdvanceEventCursorFallsBackToLastParsedRow covers a full page where only the last raw
// row fails to parse: lastRawBoundary is zero-value, but the other rows parsed fine and must
// still advance the cursor instead of stalling on a page that otherwise made progress.
func TestAdvanceEventCursorFallsBackToLastParsedRow(t *testing.T) {
	cursor := eventPageCursor{StartAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	rows := []auditLogRow{
		{EventID: "evt-1", EventTime: cursor.StartAt.Add(time.Minute)},
		{EventID: "evt-2", EventTime: cursor.StartAt.Add(2 * time.Minute)},
	}

	next, advanced := advanceEventCursor(cursor, rows, eventPageCursor{}, true, cursor.StartAt.Add(auditLogTrailingLag))

	want := eventPageCursor{StartAt: rows[1].EventTime, StartAfterEventID: rows[1].EventID}
	if !advanced || next != want {
		t.Errorf("next = %+v, advanced = %v, want %+v, true", next, advanced, want)
	}
}

// TestAdvanceEventCursorFullyMalformedPageDoesNotStall covers a full page where every row
// failed to parse: neither lastRawBoundary nor rows can advance the cursor, so this must
// signal advanced=false (letting the caller stop claiming HasMore) instead of looping forever
// on an unchanged cursor. It also must not panic indexing an empty rows slice.
func TestAdvanceEventCursorFullyMalformedPageDoesNotStall(t *testing.T) {
	cursor := eventPageCursor{StartAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), StartAfterEventID: "evt-1"}

	next, advanced := advanceEventCursor(cursor, nil, eventPageCursor{}, true, cursor.StartAt.Add(auditLogTrailingLag))

	if advanced || next != cursor {
		t.Errorf("next = %+v, advanced = %v, want unchanged cursor %+v, false", next, advanced, cursor)
	}
}

// TestAdvanceEventCursorDrainedJumpsToLagCutoff verifies a drained page advances straight to
// lagCutoff: rows are already bounded by it (see queryAuditLog), so draining proves nothing
// else exists up to that point.
func TestAdvanceEventCursorDrainedJumpsToLagCutoff(t *testing.T) {
	cursor := eventPageCursor{StartAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	lagCutoff := cursor.StartAt.Add(4 * time.Hour)

	next, advanced := advanceEventCursor(cursor, nil, eventPageCursor{}, false, lagCutoff)

	if !advanced || !next.StartAt.Equal(lagCutoff) || next.StartAfterEventID != "" {
		t.Errorf("next = %+v, advanced = %v, want StartAt=%v with no tiebreaker, true", next, advanced, lagCutoff)
	}
}

func TestMapAuditRowToResource(t *testing.T) {
	client := principalKindTestClient(t, "u-1", "Users")
	workspaceLookup := map[int64]string{123: "my-workspace"}
	accountId := "acct-1"

	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}
	workspaceParent := &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "my-workspace"}

	type wantResource struct {
		resourceType string
		resource     string
		parentType   string
		parentID     string
	}

	cases := []struct {
		name string
		row  auditLogRow
		want []wantResource
	}{
		{
			name: "account-level group create",
			row: auditLogRow{
				ActionName:    "createGroup",
				ServiceName:   "accounts",
				WorkspaceID:   0,
				RequestParams: map[string]string{"targetGroupId": "g-1"},
			},
			want: []wantResource{
				{groupResourceType.Id, groupResourceId(context.Background(), "g-1", accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name: "workspace-scoped group change stays account-parented",
			row: auditLogRow{
				ActionName:    "removeGroup",
				ServiceName:   "accounts",
				WorkspaceID:   123,
				RequestParams: map[string]string{"targetGroupId": "g-1"},
			},
			want: []wantResource{
				// Groups are only ever synced as children of the account when the Account
				// API is reachable, regardless of which workspace the change occurred in.
				{groupResourceType.Id, groupResourceId(context.Background(), "g-1", accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name: "workspace-scoped acl change also refreshes the workspace-access role",
			row: auditLogRow{
				ActionName:  "changeDatabricksWorkspaceAcl",
				ServiceName: "accounts",
				WorkspaceID: 123,
			},
			want: []wantResource{
				{workspaceResourceType.Id, "my-workspace", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(WorkspaceAccessRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
			},
		},
		{
			name: "setAdmin refreshes the user and the account-admin role",
			row: auditLogRow{
				ActionName:    "setAdmin",
				ServiceName:   "accounts",
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(AccountAdminRole, accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name: "setAccountAdmin refreshes the user and the account-admin role",
			row: auditLogRow{
				ActionName:    "setAccountAdmin",
				ServiceName:   "accounts",
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(AccountAdminRole, accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name: "removeAccountAdmin refreshes the user and the account-admin role",
			row: auditLogRow{
				ActionName:    "removeAccountAdmin",
				ServiceName:   "accounts",
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(AccountAdminRole, accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name: "deactivateUser refreshes the user",
			row: auditLogRow{
				ActionName:    "deactivateUser",
				ServiceName:   "accounts",
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
			},
		},
		{
			name: "account-level addPermissionAssignment resolves the workspace from request_params",
			row: auditLogRow{
				ActionName:    "addPermissionAssignment",
				ServiceName:   "workspace",
				WorkspaceID:   0,
				RequestParams: map[string]string{"principal_id": "u-1", "account_id": accountId, "workspace_id": "123"},
			},
			want: []wantResource{
				{workspaceResourceType.Id, "my-workspace", accountResourceType.Id, accountId},
			},
		},
		{
			name: "account-level updatePermissionAssignment resolves the workspace from request_params",
			row: auditLogRow{
				ActionName:    "updatePermissionAssignment",
				ServiceName:   "workspace",
				WorkspaceID:   0,
				RequestParams: map[string]string{"principal_id": "u-1", "workspace_id": "123"},
			},
			want: []wantResource{
				{workspaceResourceType.Id, "my-workspace", accountResourceType.Id, accountId},
			},
		},
		{
			name: "updatePermissionAssignment uses the row's workspace_id",
			row: auditLogRow{
				ActionName:    "updatePermissionAssignment",
				ServiceName:   "workspace",
				WorkspaceID:   123,
				RequestParams: map[string]string{"principal_id": "u-1"},
			},
			want: []wantResource{
				{workspaceResourceType.Id, "my-workspace", accountResourceType.Id, accountId},
			},
		},
		{
			name: "deletePermissionAssignment for a workspace outside sync scope is skipped",
			row: auditLogRow{
				ActionName:    "deletePermissionAssignment",
				ServiceName:   "workspace",
				RequestParams: map[string]string{"principal_id": "u-1", "workspace_id": "999"},
			},
		},
		{
			name: "permission assignment with an unparseable workspace_id is skipped",
			row: auditLogRow{
				ActionName:    "deletePermissionAssignment",
				ServiceName:   "workspace",
				RequestParams: map[string]string{"principal_id": "u-1", "workspace_id": "not-a-number"},
			},
		},
		{
			name: "updateUser stays account-parented, but workspace roles still refresh",
			row: auditLogRow{
				ActionName:    "updateUser",
				ServiceName:   "accounts",
				WorkspaceID:   123,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(WorkspaceAccessRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(SQLAccessRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(ClusterCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(InstancePoolCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
			},
		},
		{
			name: "updateGroup in a workspace refreshes the group and every workspace entitlement role",
			row: auditLogRow{
				ActionName:    "updateGroup",
				ServiceName:   "accounts",
				WorkspaceID:   123,
				RequestParams: map[string]string{"targetGroupId": "g-1"},
			},
			want: []wantResource{
				{groupResourceType.Id, groupResourceId(context.Background(), "g-1", accountParent), accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(WorkspaceAccessRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(SQLAccessRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(ClusterCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(InstancePoolCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
			},
		},
		{
			name: "account-level updateUser refreshes only the user",
			row: auditLogRow{
				ActionName:    "updateUser",
				ServiceName:   "accounts",
				WorkspaceID:   0,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
			},
		},
		{
			name: "account-level updateGroup refreshes only the group",
			row: auditLogRow{
				ActionName:    "updateGroup",
				ServiceName:   "accounts",
				WorkspaceID:   0,
				RequestParams: map[string]string{"targetGroupId": "g-1"},
			},
			want: []wantResource{
				{groupResourceType.Id, groupResourceId(context.Background(), "g-1", accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name: "unknown action is skipped",
			row:  auditLogRow{ActionName: "someUnityCatalogAction"},
		},
		{
			name: "known action_name under an unmapped service_name is skipped",
			row:  auditLogRow{ActionName: "delete", ServiceName: "clusters"},
		},
		{
			name: "principal delete is left to full sync",
			row:  auditLogRow{ActionName: "delete", ServiceName: "accounts", RequestParams: map[string]string{"targetUserId": "u-1"}},
		},
		{
			name: "unresolvable workspace is skipped",
			row: auditLogRow{
				ActionName:    "add",
				ServiceName:   "accounts",
				WorkspaceID:   999,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
		},
		{
			name: "missing id param is skipped",
			row:  auditLogRow{ActionName: "add", ServiceName: "accounts", WorkspaceID: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), tc.row, accountId, workspaceLookup)
			if err != nil {
				t.Fatalf("mapAuditRowToResource() error = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d affected resources, want %d: %+v", len(got), len(tc.want), got)
			}
			for i, w := range tc.want {
				if got[i].resourceId.ResourceType != w.resourceType || got[i].resourceId.Resource != w.resource {
					t.Errorf("[%d] resourceId = %+v, want type=%s id=%s", i, got[i].resourceId, w.resourceType, w.resource)
				}
				if got[i].parentResourceId.ResourceType != w.parentType || got[i].parentResourceId.Resource != w.parentID {
					t.Errorf("[%d] parentResourceId = %+v, want type=%s id=%s", i, got[i].parentResourceId, w.parentType, w.parentID)
				}
			}
		})
	}
}

func assertGroupRefresh(t *testing.T, got []affectedResource, wantGroupId string) {
	t.Helper()
	if len(got) != 1 || got[0].grant != nil {
		t.Fatalf("got %+v, want exactly one group refresh", got)
	}
	if id := got[0].resourceId; id.GetResourceType() != groupResourceType.Id || id.GetResource() != wantGroupId {
		t.Errorf("resourceId = %+v, want type=%s id=%s", id, groupResourceType.Id, wantGroupId)
	}
}

// principalKindTestClient serves GetUser/GetGroup/GetServicePrincipal lookups for a single
// native id, returning 200 for kind ("Users", "Groups", or "ServicePrincipals") and 404 for
// the other two - or 500 for all three if kind is "error".
func principalKindTestClient(t *testing.T, nativeId, kind string) *databricks.Client {
	t.Helper()
	return newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/"+nativeId) {
			writeJSONNotFound(w)
			return
		}
		if kind == "error" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"boom"}`))
			return
		}
		if strings.Contains(r.URL.Path, "/"+kind+"/") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		writeJSONNotFound(w)
	})
}

// TestMapAuditRowToResourceResolvesPrincipalType covers principal-lifecycle rows whose
// targetUserId names a service principal, a group, or nothing that still exists.
func TestMapAuditRowToResourceResolvesPrincipalType(t *testing.T) {
	accountId := "acct-1"
	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}
	workspaceParent := &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "my-workspace"}
	workspaceLookup := map[int64]string{123: "my-workspace"}
	accountAdmin := roleResourceId(AccountAdminRole, accountParent)

	type want struct{ resourceType, resource string }
	cases := []struct {
		name     string
		nativeId string
		kind     string
		row      auditLogRow
		want     []want
	}{
		{
			name: "add of a service principal", nativeId: "sp-1", kind: "ServicePrincipals",
			row:  auditLogRow{ActionName: "add", ServiceName: "accounts", RequestParams: map[string]string{"targetUserId": "sp-1"}},
			want: []want{{servicePrincipalResourceType.Id, "sp-1"}},
		},
		{
			name: "setAdmin on a service principal", nativeId: "sp-1", kind: "ServicePrincipals",
			row:  auditLogRow{ActionName: "setAdmin", ServiceName: "accounts", RequestParams: map[string]string{"targetUserId": "sp-1"}},
			want: []want{{servicePrincipalResourceType.Id, "sp-1"}, {roleResourceType.Id, accountAdmin}},
		},
		{
			name: "updateUser on a service principal in a workspace", nativeId: "sp-1", kind: "ServicePrincipals",
			row: auditLogRow{ActionName: "updateUser", ServiceName: "accounts", WorkspaceID: 123, RequestParams: map[string]string{"targetUserId": "sp-1"}},
			want: []want{
				{servicePrincipalResourceType.Id, "sp-1"},
				{roleResourceType.Id, roleResourceId(WorkspaceAccessRole, workspaceParent)},
				{roleResourceType.Id, roleResourceId(SQLAccessRole, workspaceParent)},
				{roleResourceType.Id, roleResourceId(ClusterCreateRole, workspaceParent)},
				{roleResourceType.Id, roleResourceId(InstancePoolCreateRole, workspaceParent)},
			},
		},
		{
			name: "add naming a group uses the full-sync group id", nativeId: "g-1", kind: "Groups",
			row:  auditLogRow{ActionName: "add", ServiceName: "accounts", RequestParams: map[string]string{"targetUserId": "g-1"}},
			want: []want{{groupResourceType.Id, groupResourceId(context.Background(), "g-1", accountParent)}},
		},
		{
			name: "setAdmin on an unknown principal still refreshes the role", nativeId: "gone-1", kind: "none",
			row:  auditLogRow{ActionName: "setAdmin", ServiceName: "accounts", RequestParams: map[string]string{"targetUserId": "gone-1"}},
			want: []want{{userResourceType.Id, "gone-1"}, {roleResourceType.Id, accountAdmin}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := principalKindTestClient(t, tc.nativeId, tc.kind)
			got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), tc.row, accountId, workspaceLookup)
			if err != nil {
				t.Fatalf("mapAuditRowToResource() error = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d affected resources, want %d: %+v", len(got), len(tc.want), got)
			}
			for i, w := range tc.want {
				if got[i].resourceId.GetResourceType() != w.resourceType || got[i].resourceId.GetResource() != w.resource {
					t.Errorf("[%d] resourceId = %+v, want type=%s id=%s", i, got[i].resourceId, w.resourceType, w.resource)
				}
			}
			if p := got[0].parentResourceId; p.GetResourceType() != accountResourceType.Id || p.GetResource() != accountId {
				t.Errorf("parentResourceId = %+v, want the account", p)
			}
		})
	}

	t.Run("a lookup error is returned", func(t *testing.T) {
		client := principalKindTestClient(t, "sp-1", "error")
		row := auditLogRow{ActionName: "add", ServiceName: "accounts", RequestParams: map[string]string{"targetUserId": "sp-1"}}
		if _, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), row, accountId, workspaceLookup); err == nil {
			t.Fatal("mapAuditRowToResource() error = nil, want the lookup failure")
		}
	})
}

// TestResolvePrincipalTypeServicePrincipalFirst verifies a UUID targetUserName sends the
// service principal lookup first, and a username keeps the user-first order.
func TestResolvePrincipalTypeServicePrincipalFirst(t *testing.T) {
	cases := []struct {
		name       string
		targetName string
		wantPaths  []string
	}{
		{
			name:       "application id tries service principals first",
			targetName: "8f2e6c1a-3b4d-4e5f-9a0b-1c2d3e4f5a6b",
			wantPaths:  []string{"/ServicePrincipals/sp-1"},
		},
		{
			name:       "username keeps the user-first order",
			targetName: "someone@example.com",
			wantPaths:  []string{"/Users/sp-1", "/Groups/sp-1", "/ServicePrincipals/sp-1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path[strings.Index(r.URL.Path, "/scim/v2")+len("/scim/v2"):])
				mu.Unlock()
				if strings.HasSuffix(r.URL.Path, "/ServicePrincipals/sp-1") {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{}`)
					return
				}
				writeJSONNotFound(w)
			})
			row := auditLogRow{
				ActionName:    "add",
				ServiceName:   "accounts",
				RequestParams: map[string]string{"targetUserId": "sp-1", "targetUserName": tc.targetName},
			}
			got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), row, "acct-1", nil)
			if err != nil {
				t.Fatalf("mapAuditRowToResource() error = %v", err)
			}
			if len(got) != 1 || got[0].resourceId.GetResourceType() != servicePrincipalResourceType.Id {
				t.Fatalf("got %+v, want one service principal", got)
			}
			if strings.Join(paths, ",") != strings.Join(tc.wantPaths, ",") {
				t.Errorf("lookups = %v, want %v", paths, tc.wantPaths)
			}
		})
	}
}

// TestMapAuditRowToResourceGrantMapping covers the group-membership actions that map to an
// atomic CREATE_GRANT/CREATE_REVOKE instead of a RESOURCE_CHANGE.
func TestMapAuditRowToResourceGrantMapping(t *testing.T) {
	accountId := "acct-1"
	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}
	wantGroupId := groupResourceId(context.Background(), "g-1", accountParent)
	wantEntitlementId := ent.NewEntitlementID(&v2.Resource{Id: &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: wantGroupId}}, groupMemberEntitlement)

	cases := []struct {
		name       string
		actionName string
		wantRevoke bool
	}{
		{"addPrincipalToGroup grants", "addPrincipalToGroup", false},
		{"removePrincipalFromGroup revokes", "removePrincipalFromGroup", true},
		{"addPrincipalsToGroup grants", "addPrincipalsToGroup", false},
		{"removePrincipalsFromGroup revokes", "removePrincipalsFromGroup", true},
	}

	client := principalKindTestClient(t, "u-1", "Users")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := auditLogRow{
				ActionName:    tc.actionName,
				ServiceName:   auditServiceAccounts,
				RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "u-1"},
			}

			got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), row, accountId, nil)
			if err != nil {
				t.Fatalf("mapAuditRowToResource() error = %v", err)
			}
			if len(got) != 1 || got[0].grant == nil {
				t.Fatalf("got %+v, want exactly one grant-mapped affected resource", got)
			}

			g := got[0].grant
			if g.revoke != tc.wantRevoke {
				t.Errorf("revoke = %v, want %v", g.revoke, tc.wantRevoke)
			}
			if g.entitlement.GetId() != wantEntitlementId {
				t.Errorf("entitlement id = %q, want %q", g.entitlement.GetId(), wantEntitlementId)
			}
			if g.principal.GetId().GetResourceType() != userResourceType.Id || g.principal.GetId().GetResource() != "u-1" {
				t.Errorf("principal = %+v, want type=%s id=u-1", g.principal.GetId(), userResourceType.Id)
			}
		})
	}

	t.Run("missing principal id still refreshes the group", func(t *testing.T) {
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), row, accountId, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		assertGroupRefresh(t, got, wantGroupId)
	})

	t.Run("principal resolves to a nested group", func(t *testing.T) {
		groupClient := principalKindTestClient(t, "g-2", "Groups")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "g-2"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), groupClient, newPrincipalLookups(nil), row, accountId, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		if len(got) != 1 || got[0].grant == nil {
			t.Fatalf("got %+v, want exactly one grant-mapped affected resource", got)
		}
		wantId := groupResourceId(context.Background(), "g-2", accountParent)
		if p := got[0].grant.principal.GetId(); p.GetResourceType() != groupResourceType.Id || p.GetResource() != wantId {
			t.Errorf("principal = %+v, want type=%s id=%s", p, groupResourceType.Id, wantId)
		}
	})

	t.Run("principal resolves to a service principal", func(t *testing.T) {
		spClient := principalKindTestClient(t, "sp-1", "ServicePrincipals")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "sp-1"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), spClient, newPrincipalLookups(nil), row, accountId, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		if len(got) != 1 || got[0].grant == nil {
			t.Fatalf("got %+v, want exactly one grant-mapped affected resource", got)
		}
		if p := got[0].grant.principal.GetId(); p.GetResourceType() != servicePrincipalResourceType.Id || p.GetResource() != "sp-1" {
			t.Errorf("principal = %+v, want type=%s id=sp-1", p, servicePrincipalResourceType.Id)
		}
	})

	t.Run("principal not found anywhere still refreshes the group", func(t *testing.T) {
		notFoundClient := principalKindTestClient(t, "ghost", "none")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "ghost"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), notFoundClient, newPrincipalLookups(nil), row, accountId, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		assertGroupRefresh(t, got, wantGroupId)
	})

	t.Run("a real lookup error propagates instead of being swallowed", func(t *testing.T) {
		errClient := principalKindTestClient(t, "u-1", "error")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "u-1"},
		}
		if _, _, err := mapAuditRowToResource(context.Background(), errClient, newPrincipalLookups(nil), row, accountId, nil); err == nil {
			t.Error("mapAuditRowToResource() error = nil, want a propagated lookup error")
		}
	})
}

// multiPrincipalTestClient returns a client whose account SCIM lookups are served by scimKindsHandler.
func multiPrincipalTestClient(t *testing.T, kinds map[string]string) (*databricks.Client, func() []string) {
	t.Helper()
	handler, lookups := scimKindsHandler(kinds)
	return newProbeTestClient(t, handler), lookups
}

// scimKindsHandler serves account SCIM lookups from kinds (native id to "Users", "Groups",
// "ServicePrincipals", or an HTTP status code as a string), 404s everything else, and records
// each lookup's path below /scim/v2.
func scimKindsHandler(kinds map[string]string) (http.HandlerFunc, func() []string) {
	var mu sync.Mutex
	var paths []string
	handler := func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path[strings.Index(r.URL.Path, "/scim/v2")+len("/scim/v2"):]
		mu.Lock()
		paths = append(paths, path)
		mu.Unlock()

		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		if len(parts) != 2 {
			writeJSONNotFound(w)
			return
		}
		kind := kinds[parts[1]]
		if code, err := strconv.Atoi(kind); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			fmt.Fprint(w, `{"detail":"lookup failed"}`)
			return
		}
		if kind == parts[0] {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{}`)
			return
		}
		writeJSONNotFound(w)
	}
	return handler, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(paths)
	}
}

// TestMapAuditRowToResourceMultiMember covers membership rows whose targetUserId lists several
// comma-separated members.
func TestMapAuditRowToResourceMultiMember(t *testing.T) {
	accountId := "acct-1"
	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}
	wantGroupId := groupResourceId(context.Background(), "g-1", accountParent)
	nestedGroupId := groupResourceId(context.Background(), "g-2", accountParent)

	type wantGrant struct{ resourceType, resource string }
	cases := []struct {
		name        string
		action      string
		params      map[string]string
		kinds       map[string]string
		wantGrants  []wantGrant
		wantRevoke  bool
		wantRefresh bool
		wantLookups int
	}{
		{
			name:   "three members are each granted",
			action: "addPrincipalsToGroup",
			params: map[string]string{"targetUserId": "u-1,u-2,u-3"},
			kinds:  map[string]string{"u-1": "Users", "u-2": "Users", "u-3": "Users"},
			wantGrants: []wantGrant{
				{userResourceType.Id, "u-1"}, {userResourceType.Id, "u-2"}, {userResourceType.Id, "u-3"},
			},
			wantLookups: 3,
		},
		{
			name:        "two members are each revoked",
			action:      "removePrincipalsFromGroup",
			params:      map[string]string{"targetUserId": "u-1,u-2"},
			kinds:       map[string]string{"u-1": "Users", "u-2": "Users"},
			wantGrants:  []wantGrant{{userResourceType.Id, "u-1"}, {userResourceType.Id, "u-2"}},
			wantRevoke:  true,
			wantLookups: 2,
		},
		{
			name:   "mixed principal kinds resolve separately",
			action: "addPrincipalsToGroup",
			params: map[string]string{"targetUserId": "u-1,sp-1,g-2"},
			kinds:  map[string]string{"u-1": "Users", "sp-1": "ServicePrincipals", "g-2": "Groups"},
			wantGrants: []wantGrant{
				{userResourceType.Id, "u-1"}, {servicePrincipalResourceType.Id, "sp-1"}, {groupResourceType.Id, nestedGroupId},
			},
			wantLookups: 1 + 3 + 2,
		},
		{
			name:        "single id on a plural action matches the singular output",
			action:      "addPrincipalsToGroup",
			params:      map[string]string{"targetUserId": "u-1"},
			kinds:       map[string]string{"u-1": "Users"},
			wantGrants:  []wantGrant{{userResourceType.Id, "u-1"}},
			wantLookups: 1,
		},
		{
			name:        "an unknown member refreshes the group once",
			action:      "addPrincipalsToGroup",
			params:      map[string]string{"targetUserId": "u-1,gone-1,gone-2,u-2"},
			kinds:       map[string]string{"u-1": "Users", "u-2": "Users"},
			wantGrants:  []wantGrant{{userResourceType.Id, "u-1"}, {userResourceType.Id, "u-2"}},
			wantRefresh: true,
			wantLookups: 1 + 3 + 3 + 1,
		},
		{
			name:        "spaces, blanks and repeats are cleaned up",
			action:      "addPrincipalsToGroup",
			params:      map[string]string{"targetUserId": " u-1 , u-2,u-1,, "},
			kinds:       map[string]string{"u-1": "Users", "u-2": "Users"},
			wantGrants:  []wantGrant{{userResourceType.Id, "u-1"}, {userResourceType.Id, "u-2"}},
			wantLookups: 2,
		},
		{
			name:   "aligned application ids look up service principals first",
			action: "addPrincipalsToGroup",
			params: map[string]string{
				"targetUserId":   "sp-1,u-1",
				"targetUserName": "8f2e6c1a-3b4d-4e5f-9a0b-1c2d3e4f5a6b,someone@example.com",
			},
			kinds:       map[string]string{"sp-1": "ServicePrincipals", "u-1": "Users"},
			wantGrants:  []wantGrant{{servicePrincipalResourceType.Id, "sp-1"}, {userResourceType.Id, "u-1"}},
			wantLookups: 2,
		},
		{
			name:        "too many members refresh the group without lookups",
			action:      "addPrincipalsToGroup",
			params:      map[string]string{"targetUserId": manyIDs(maxAuditMembershipLookups + 1)},
			wantRefresh: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, lookups := multiPrincipalTestClient(t, tc.kinds)
			params := map[string]string{"targetGroupId": "g-1"}
			for k, v := range tc.params {
				params[k] = v
			}
			row := auditLogRow{EventID: "evt-1", ActionName: tc.action, ServiceName: auditServiceAccounts, RequestParams: params}

			got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), row, accountId, nil)
			if err != nil {
				t.Fatalf("mapAuditRowToResource() error = %v", err)
			}

			wantLen := len(tc.wantGrants)
			if tc.wantRefresh {
				wantLen++
			}
			if len(got) != wantLen {
				t.Fatalf("got %d affected resources, want %d: %+v", len(got), wantLen, got)
			}
			for i, w := range tc.wantGrants {
				g := got[i].grant
				if g == nil {
					t.Fatalf("[%d] = %+v, want a grant", i, got[i])
				}
				if g.revoke != tc.wantRevoke {
					t.Errorf("[%d] revoke = %v, want %v", i, g.revoke, tc.wantRevoke)
				}
				if g.entitlement.GetResource().GetId().GetResource() != wantGroupId {
					t.Errorf("[%d] entitlement resource = %q, want %q", i, g.entitlement.GetResource().GetId().GetResource(), wantGroupId)
				}
				if p := g.principal.GetId(); p.GetResourceType() != w.resourceType || p.GetResource() != w.resource {
					t.Errorf("[%d] principal = %+v, want type=%s id=%s", i, p, w.resourceType, w.resource)
				}
			}
			if tc.wantRefresh {
				assertGroupRefresh(t, got[len(got)-1:], wantGroupId)
			}
			if n := len(lookups()); n != tc.wantLookups {
				t.Errorf("lookups = %d (%v), want %d", n, lookups(), tc.wantLookups)
			}
		})
	}

	t.Run("a retryable error on any member fails the row", func(t *testing.T) {
		client, _ := multiPrincipalTestClient(t, map[string]string{"u-1": "Users", "u-2": "503"})
		row := auditLogRow{
			ActionName: "addPrincipalsToGroup", ServiceName: auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "u-1,u-2,u-3"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), client, newPrincipalLookups(nil), row, accountId, nil)
		if err == nil || !isRetryableLookupError(err) {
			t.Fatalf("mapAuditRowToResource() error = %v, want a retryable error", err)
		}
		if len(got) != 0 {
			t.Errorf("got %+v, want nothing", got)
		}
	})

	t.Run("a non-retryable error keeps the other members and refreshes the group", func(t *testing.T) {
		client, _ := multiPrincipalTestClient(t, map[string]string{"u-1": "Users", "u-2": "405", "u-3": "Users"})
		row := auditLogRow{
			ActionName: "addPrincipalsToGroup", ServiceName: auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "u-1,u-2,u-3"},
		}
		lookups := newPrincipalLookups(nil)
		got, _, err := mapAuditRowToResource(context.Background(), client, lookups, row, accountId, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v, want the failure absorbed", err)
		}
		if len(got) != 3 || got[0].grant.principal.GetId().GetResource() != "u-1" || got[1].grant.principal.GetId().GetResource() != "u-3" {
			t.Fatalf("got %+v, want grants for u-1 and u-3 plus a group refresh", got)
		}
		assertGroupRefresh(t, got[2:], wantGroupId)
		if lookups.memberFailures != 1 || lookups.failures.Load() != 1 {
			t.Errorf("memberFailures = %d, failures = %d, want 1 and 1", lookups.memberFailures, lookups.failures.Load())
		}
	})

	t.Run("members past the poll budget refresh the group without lookups", func(t *testing.T) {
		client, calls := multiPrincipalTestClient(t, map[string]string{"u-1": "Users", "u-2": "Users", "u-3": "Users"})
		lookups := newPrincipalLookups(nil)
		lookups.types["u-3"] = userResourceType
		lookups.remainingMemberLookups = 1
		row := auditLogRow{
			ActionName: "addPrincipalsToGroup", ServiceName: auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "u-1,u-2,u-3"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), client, lookups, row, accountId, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		if len(got) != 3 || got[0].grant.principal.GetId().GetResource() != "u-1" || got[1].grant.principal.GetId().GetResource() != "u-3" {
			t.Fatalf("got %+v, want grants for u-1 and the cached u-3 plus a group refresh", got)
		}
		assertGroupRefresh(t, got[2:], wantGroupId)
		if n := len(calls()); n != 1 || lookups.membersOverBudget != 1 {
			t.Errorf("lookups = %v, membersOverBudget = %d, want one lookup and one member over budget", calls(), lookups.membersOverBudget)
		}
	})
}

func manyIDs(n int) string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("u-%d", i)
	}
	return strings.Join(ids, ",")
}

func TestParseAuditLogRowsDedupesNothingAndParsesFields(t *testing.T) {
	result := &databricks.StatementResult{
		Columns: []string{"event_id", "event_time", "workspace_id", "action_name", "service_name", "request_params"},
		Rows: [][]string{
			{"evt-1", "2026-01-01 00:00:00.000", "123", "createGroup", "accounts", `{"targetGroupId":"g-1"}`},
			{"evt-2", "2026-01-01T00:01:00Z", "0", "add", "accounts", `{"targetUserId":"u-1"}`},
		},
	}

	rows, _, err := parseAuditLogRows(context.Background(), result)
	if err != nil {
		t.Fatalf("parseAuditLogRows() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0].WorkspaceID != 123 || rows[0].ServiceName != "accounts" || rows[0].RequestParams["targetGroupId"] != "g-1" {
		t.Errorf("row[0] = %+v", rows[0])
	}
	if rows[1].WorkspaceID != 0 || rows[1].RequestParams["targetUserId"] != "u-1" {
		t.Errorf("row[1] = %+v", rows[1])
	}
}

func TestParseAuditLogRowsMissingColumnErrors(t *testing.T) {
	result := &databricks.StatementResult{
		Columns: []string{"event_id", "event_time"},
		Rows:    [][]string{{"evt-1", "2026-01-01 00:00:00.000"}},
	}

	if _, _, err := parseAuditLogRows(context.Background(), result); err == nil {
		t.Error("parseAuditLogRows() error = nil, want error for missing required column")
	}
}

// TestParseAuditLogRowsSkipsMalformedRow verifies a single poisoned row (unparseable
// event_time here) is skipped rather than failing the whole page.
func TestParseAuditLogRowsSkipsMalformedRow(t *testing.T) {
	result := &databricks.StatementResult{
		Columns: []string{"event_id", "event_time", "workspace_id", "action_name", "service_name", "request_params"},
		Rows: [][]string{
			{"evt-1", "not-a-timestamp", "123", "createGroup", "accounts", `{"targetGroupId":"g-1"}`},
			{"evt-2", "2026-01-01T00:01:00Z", "0", "add", "accounts", `{"targetUserId":"u-1"}`},
		},
	}

	rows, _, err := parseAuditLogRows(context.Background(), result)
	if err != nil {
		t.Fatalf("parseAuditLogRows() error = %v, want the malformed row skipped instead", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1 (malformed row skipped)", len(rows))
	}
	if rows[0].EventID != "evt-2" {
		t.Errorf("rows[0].EventID = %q, want %q", rows[0].EventID, "evt-2")
	}
}

// redirectTransport rewrites every outgoing request to target, since workspaceUrl always
// builds a "<workspace>.<hostname>" subdomain a local httptest.Server can't listen on directly.
type redirectTransport struct {
	target *url.URL
}

func (t *redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	originalHost := req.URL.Host
	req.URL.Scheme = t.target.Scheme
	req.URL.Host = t.target.Host
	req.Host = t.target.Host
	// Preserve the workspace-specific host the client intended, since it's otherwise lost
	// once every request is rewritten to the same local test server.
	req.Header.Set("X-Test-Original-Host", originalHost)
	return http.DefaultTransport.RoundTrip(req)
}

// TestListEventsSkipsQueryPastLagCutoff covers the bootstrap default landing exactly at
// lagCutoff (auditLogLookback == auditLogTrailingLag): ListEvents must not query an
// inverted (start > end) range.
func TestListEventsSkipsQueryPastLagCutoff(t *testing.T) {
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request %s %s: should have been skipped by the lagCutoff guard", r.Method, r.URL.Path)
	})

	feed := newAuditEventFeed(client, nil, "wh-1")
	events, streamState, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 0 {
		t.Errorf("len(events) = %d, want 0", len(events))
	}
	if streamState.HasMore {
		t.Error("HasMore = true, want false")
	}
	if streamState.Cursor == "" {
		t.Error("Cursor is empty, want the bootstrap cursor to still be encoded")
	}
}

// TestListEventsEndToEnd exercises ListEvents against a mocked Statement Execution API,
// verifying the service_name query filter, resource mapping, and rate-limit propagation.
func TestListEventsEndToEnd(t *testing.T) {
	const wantLimit = 100
	const wantRemaining = 42

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ListEvents always lists workspaces via the Account API before querying the audit log.
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"},
			}); err != nil {
				t.Fatalf("failed to encode mock workspaces response: %v", err)
			}
			return
		}

		// resolveWarehouseWorkspace probes every candidate workspace (including a lone
		// one) via WarehouseExists before running the audit-log query.
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"wh-1"}`)
			return
		}

		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/scim/v2/Users/u-1") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{}`)
			return
		}

		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}

		var body struct {
			Statement string `json:"statement"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode statement request body: %v", err)
		}
		if !strings.Contains(body.Statement, "service_name") {
			t.Errorf("statement does not select/filter service_name: %s", body.Statement)
		}
		if !strings.Contains(body.Statement, "'accounts'") {
			t.Errorf("statement does not filter service_name IN ('accounts', ...): %s", body.Statement)
		}

		w.Header().Set("X-Ratelimit-Limit", strconv.Itoa(wantLimit))
		w.Header().Set("X-Ratelimit-Remaining", strconv.Itoa(wantRemaining))
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]any{
			"statement_id": "stmt-1",
			"status":       map[string]any{"state": "SUCCEEDED"},
			"manifest": map[string]any{
				"schema": map[string]any{
					"columns": []map[string]any{
						{"name": "event_id"},
						{"name": "event_time"},
						{"name": "workspace_id"},
						{"name": "action_name"},
						{"name": "service_name"},
						{"name": "request_params"},
					},
				},
			},
			"result": map[string]any{
				"data_array": [][]string{
					{"evt-1", "2026-01-01T00:00:00Z", "0", "add", "accounts", `{"targetUserId":"u-1"}`},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("failed to encode mock statement response: %v", err)
		}
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	httpClient := &http.Client{Transport: &redirectTransport{target: target}}
	auth := &databricks.NoAuth{}
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	// Mirrors what Validate() sets before any sync/event-feed call runs in production.

	feed := newAuditEventFeed(client, nil, "wh-1")

	events, streamState, annos, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1: %+v", len(events), events)
	}

	rc := events[0].GetResourceChangeEvent()
	if rc.GetResourceId().GetResourceType() != userResourceType.Id || rc.GetResourceId().GetResource() != "u-1" {
		t.Errorf("event resource = %+v, want type=%s id=u-1", rc.GetResourceId(), userResourceType.Id)
	}
	if streamState.Cursor == "" {
		t.Error("StreamState.Cursor is empty, want an encoded cursor")
	}

	rld := &v2.RateLimitDescription{}
	ok, err := annos.Pick(rld)
	if err != nil {
		t.Fatalf("annos.Pick() error = %v", err)
	}
	if !ok {
		t.Fatal("annotations do not carry a RateLimitDescription, want the rate-limit headers propagated")
	}
	if rld.GetLimit() != wantLimit || rld.GetRemaining() != wantRemaining {
		t.Errorf("RateLimitDescription = %+v, want limit=%d remaining=%d", rld, wantLimit, wantRemaining)
	}
}

// TestListEventsEmitsCreateGrantEvent verifies ListEvents wires a grant-mapped audit row all
// the way to a CreateGrantEvent, not a ResourceChangeEvent.
func TestListEventsEmitsCreateGrantEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"}})
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"wh-1"}`)
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/scim/v2/Users/u-1") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{}`)
			return
		}
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"statement_id": "stmt-1",
			"status":       map[string]any{"state": "SUCCEEDED"},
			"manifest": map[string]any{
				"schema": map[string]any{
					"columns": []map[string]any{
						{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
						{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
					},
				},
			},
			"result": map[string]any{
				"data_array": [][]string{
					{"evt-1", "2026-01-01T00:00:00Z", "0", "addPrincipalToGroup", "accounts", `{"targetGroupId":"g-1","targetUserId":"u-1"}`},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("failed to encode mock statement response: %v", err)
		}
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	httpClient := &http.Client{Transport: &redirectTransport{target: target}}
	auth := &databricks.NoAuth{}
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	feed := newAuditEventFeed(client, nil, "wh-1")
	events, _, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1: %+v", len(events), events)
	}

	cg := events[0].GetCreateGrantEvent()
	if cg == nil {
		t.Fatalf("event = %+v, want a CreateGrantEvent", events[0])
	}
	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acct-1"}
	wantGroupId := groupResourceId(context.Background(), "g-1", accountParent)
	if cg.GetEntitlement().GetResource().GetId().GetResource() != wantGroupId {
		t.Errorf("entitlement resource = %q, want %q", cg.GetEntitlement().GetResource().GetId().GetResource(), wantGroupId)
	}
	if cg.GetPrincipal().GetId().GetResourceType() != userResourceType.Id || cg.GetPrincipal().GetId().GetResource() != "u-1" {
		t.Errorf("principal = %+v, want type=%s id=u-1", cg.GetPrincipal().GetId(), userResourceType.Id)
	}
}

// newLookupFailureFeed returns a feed whose single grant-mapped audit row's principal lookup
// fails with lookupStatus.
func newLookupFailureFeed(t *testing.T, lookupStatus int) *auditEventFeed {
	t.Helper()
	row := []string{"evt-1", "2026-01-01T00:00:00Z", "0", "addPrincipalToGroup", "accounts", `{"targetGroupId":"g-1","targetUserId":"u-1"}`}
	return newSingleRowFeed(t, row, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/Users/u-1") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(lookupStatus)
			fmt.Fprint(w, `{"detail":"lookup failed"}`)
			return
		}
		writeJSONNotFound(w)
	})
}

// newSingleRowFeed returns a feed whose audit query yields row, with account SCIM lookups
// served by scim.
func newSingleRowFeed(t *testing.T, row []string, scim http.HandlerFunc) *auditEventFeed {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"}})
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"wh-1"}`)
			return
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/2.0/accounts/acct-1/scim/v2/") {
			scim(w, r)
			return
		}
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"statement_id": "stmt-1",
			"status":       map[string]any{"state": "SUCCEEDED"},
			"manifest": map[string]any{
				"schema": map[string]any{
					"columns": []map[string]any{
						{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
						{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
					},
				},
			},
			"result": map[string]any{
				"data_array": [][]string{row},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("failed to encode mock statement response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	httpClient := &http.Client{Transport: &redirectTransport{target: target}}
	auth := &databricks.NoAuth{}
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	return newAuditEventFeed(client, nil, "wh-1")
}

// TestListEventsReturnsErrorOnRetryableLookupFailure verifies that a transient principal lookup
// failure fails the poll, so the cursor isn't advanced past the row.
func TestListEventsReturnsErrorOnRetryableLookupFailure(t *testing.T) {
	feed := newLookupFailureFeed(t, http.StatusServiceUnavailable)
	events, streamState, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err == nil {
		t.Fatalf("ListEvents() error = nil, want the principal lookup failure; events=%+v", events)
	}
	if events != nil || streamState != nil {
		t.Errorf("ListEvents() = (%+v, %+v), want no events and no stream state so the cursor is not advanced", events, streamState)
	}
}

// TestListEventsRefreshesGroupOnPermanentLookupFailure verifies that a non-retryable principal
// lookup failure on a membership row refreshes the group instead of blocking the cursor.
func TestListEventsRefreshesGroupOnPermanentLookupFailure(t *testing.T) {
	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acct-1"}
	wantGroupId := groupResourceId(context.Background(), "g-1", accountParent)

	for _, lookupStatus := range []int{http.StatusForbidden, http.StatusMethodNotAllowed} {
		t.Run(strconv.Itoa(lookupStatus), func(t *testing.T) {
			feed := newLookupFailureFeed(t, lookupStatus)
			events, streamState, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
			if err != nil {
				t.Fatalf("ListEvents() error = %v, want a group refresh", err)
			}
			if len(events) != 1 || events[0].GetResourceChangeEvent() == nil {
				t.Fatalf("events = %+v, want one resource change", events)
			}
			if id := events[0].GetResourceChangeEvent().GetResourceId(); id.GetResourceType() != groupResourceType.Id || id.GetResource() != wantGroupId {
				t.Errorf("resourceId = %+v, want group %s", id, wantGroupId)
			}
			if streamState == nil || streamState.Cursor == "" {
				t.Errorf("streamState = %+v, want an advanced cursor", streamState)
			}
		})
	}
}

// TestListEventsSkipsNonGrantRowOnPermanentLookupFailure verifies that a non-retryable lookup
// failure on a row with no group to fall back to is still skipped.
func TestListEventsSkipsNonGrantRowOnPermanentLookupFailure(t *testing.T) {
	row := []string{"evt-1", "2026-01-01T00:00:00Z", "0", "add", "accounts", `{"targetUserId":"u-1"}`}
	feed := newSingleRowFeed(t, row, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"detail":"lookup failed"}`)
	})
	events, streamState, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v, want the row skipped", err)
	}
	if len(events) != 0 {
		t.Errorf("len(events) = %d, want 0", len(events))
	}
	if streamState == nil || streamState.Cursor == "" {
		t.Errorf("streamState = %+v, want an advanced cursor", streamState)
	}
}

// TestListEventsEmitsEventPerGroupMember verifies a batched membership row yields one event per
// member with unique ids.
func TestListEventsEmitsEventPerGroupMember(t *testing.T) {
	row := []string{"evt-1", "2026-01-01T00:00:00Z", "0", "addPrincipalsToGroup", "accounts", `{"targetGroupId":"g-1","targetUserId":"u-1,u-2,u-3"}`}
	feed := newSingleRowFeed(t, row, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/Users/u-") && !strings.Contains(r.URL.Path, ",") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{}`)
			return
		}
		writeJSONNotFound(w)
	})
	events, _, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3: %+v", len(events), events)
	}
	for i, e := range events {
		if e.GetId() != fmt.Sprintf("evt-1/%d", i) {
			t.Errorf("[%d] id = %q, want evt-1/%d", i, e.GetId(), i)
		}
		cg := e.GetCreateGrantEvent()
		if cg == nil {
			t.Fatalf("[%d] event = %+v, want a CreateGrantEvent", i, e)
		}
		if want := fmt.Sprintf("u-%d", i+1); cg.GetPrincipal().GetId().GetResource() != want {
			t.Errorf("[%d] principal = %+v, want %s", i, cg.GetPrincipal().GetId(), want)
		}
	}
}

// TestListEventsLiveMultiMemberRows replays the request_params Databricks logged on a live Azure
// account for batched SCIM PATCHes, where names pair with ids in order.
func TestListEventsLiveMultiMemberRows(t *testing.T) {
	const (
		spId    = "143679241589540"
		userB   = "143797019507719"
		userA   = "147934750343750"
		groupId = "153270043448790"
	)
	kinds := map[string]string{spId: "ServicePrincipals", userB: "Users", userA: "Users"}

	type wantEvent struct{ resourceType, resource string }
	cases := []struct {
		name        string
		action      string
		params      string
		wantRevoke  bool
		wantEvents  []wantEvent
		wantLookups []string
	}{
		{
			name:   "add of a service principal and two users",
			action: "addPrincipalsToGroup",
			params: `{"targetGroupId":"153270043448790","targetGroupName":"batch-group",` +
				`"targetUserName":"8933382e-9e91-4caf-b0ad-b277dd60956d,member-b@example.com,member-a@example.com",` +
				`"endpoint":"scim","targetUserId":"143679241589540,143797019507719,147934750343750"}`,
			wantEvents: []wantEvent{
				{servicePrincipalResourceType.Id, spId}, {userResourceType.Id, userB}, {userResourceType.Id, userA},
			},
			wantLookups: []string{"/ServicePrincipals/" + spId, "/Users/" + userB, "/Users/" + userA},
		},
		{
			name:        "remove of two members without names",
			action:      "removePrincipalsFromGroup",
			params:      `{"targetGroupId":"153270043448790","targetGroupName":"batch-group","endpoint":"scim","targetUserId":"143679241589540,147934750343750"}`,
			wantRevoke:  true,
			wantEvents:  []wantEvent{{servicePrincipalResourceType.Id, spId}, {userResourceType.Id, userA}},
			wantLookups: []string{"/Users/" + spId, "/Groups/" + spId, "/ServicePrincipals/" + spId, "/Users/" + userA},
		},
	}

	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acct-1"}
	wantGroupId := groupResourceId(context.Background(), groupId, accountParent)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler, lookups := scimKindsHandler(kinds)
			row := []string{"627da5dc-47b1-38b3-a4c1-fac2ccfdace5", "2026-01-01T00:00:00Z", "0", tc.action, "accounts", tc.params}
			feed := newSingleRowFeed(t, row, handler)

			events, _, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
			if err != nil {
				t.Fatalf("ListEvents() error = %v", err)
			}
			if len(events) != len(tc.wantEvents) {
				t.Fatalf("len(events) = %d, want %d: %+v", len(events), len(tc.wantEvents), events)
			}
			for i, w := range tc.wantEvents {
				var entitlement *v2.Entitlement
				var principal *v2.Resource
				if tc.wantRevoke {
					entitlement, principal = events[i].GetCreateRevokeEvent().GetEntitlement(), events[i].GetCreateRevokeEvent().GetPrincipal()
				} else {
					entitlement, principal = events[i].GetCreateGrantEvent().GetEntitlement(), events[i].GetCreateGrantEvent().GetPrincipal()
				}
				if entitlement.GetResource().GetId().GetResource() != wantGroupId {
					t.Errorf("[%d] entitlement resource = %q, want %q", i, entitlement.GetResource().GetId().GetResource(), wantGroupId)
				}
				if p := principal.GetId(); p.GetResourceType() != w.resourceType || p.GetResource() != w.resource {
					t.Errorf("[%d] principal = %+v, want type=%s id=%s", i, p, w.resourceType, w.resource)
				}
			}
			if got := lookups(); strings.Join(got, ",") != strings.Join(tc.wantLookups, ",") {
				t.Errorf("lookups = %v, want %v", got, tc.wantLookups)
			}
		})
	}
}

// TestListEventsFullPageCursorAdvance verifies the HasMore wiring on a full page
// (auditLogPageLimit raw rows): it pages forward from the last raw row's boundary, but when
// no boundary can be parsed it reports HasMore=false with the cursor unchanged rather than
// claiming more data behind a cursor that would repeat the same query.
func TestListEventsFullPageCursorAdvance(t *testing.T) {
	start := eventPageCursor{StartAt: time.Now().Add(-2 * auditLogTrailingLag).UTC().Truncate(time.Second), StartAfterEventID: "evt-0"}
	lastTime := start.StartAt.Add(time.Minute)

	cases := []struct {
		name        string
		lastRowTime string
		wantHasMore bool
		wantCursor  eventPageCursor
	}{
		{
			name:        "parseable last row pages forward",
			lastRowTime: lastTime.Format(time.RFC3339),
			wantHasMore: true,
			wantCursor:  eventPageCursor{StartAt: lastTime, StartAfterEventID: fmt.Sprintf("evt-%d", auditLogPageLimit)},
		},
		{
			name:        "no parseable boundary stops without advancing",
			lastRowTime: "not-a-time",
			wantHasMore: false,
			wantCursor:  start,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces"):
					_ = json.NewEncoder(w).Encode([]map[string]any{{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1"):
					fmt.Fprint(w, `{"id":"wh-1"}`)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements"):
					// Every row but the last has an unparseable event_time, so no row parses
					// and only the last raw row can supply the boundary. The action is
					// unmapped, so no principal lookups are made either way.
					data := make([][]string, auditLogPageLimit)
					for i := range data {
						data[i] = []string{fmt.Sprintf("evt-%d", i+1), "not-a-time", "0", "someUnmappedAction", "accounts", `{}`}
					}
					data[len(data)-1][1] = tc.lastRowTime
					_ = json.NewEncoder(w).Encode(map[string]any{
						"statement_id": "stmt-1",
						"status":       map[string]any{"state": "SUCCEEDED"},
						"manifest": map[string]any{"schema": map[string]any{"columns": []map[string]any{
							{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
							{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
						}}},
						"result": map[string]any{"data_array": data},
					})
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			})

			encoded, err := encodeEventCursor(start)
			if err != nil {
				t.Fatalf("encodeEventCursor() error = %v", err)
			}

			feed := newAuditEventFeed(client, nil, "wh-1")
			events, streamState, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{Cursor: encoded})
			if err != nil {
				t.Fatalf("ListEvents() error = %v", err)
			}
			if len(events) != 0 {
				t.Errorf("len(events) = %d, want 0", len(events))
			}
			if streamState.HasMore != tc.wantHasMore {
				t.Errorf("HasMore = %v, want %v", streamState.HasMore, tc.wantHasMore)
			}
			got := decodeEventCursor(context.Background(), streamState.Cursor, time.Now())
			if !got.StartAt.Equal(tc.wantCursor.StartAt) || got.StartAfterEventID != tc.wantCursor.StartAfterEventID {
				t.Errorf("cursor = %+v, want %+v", got, tc.wantCursor)
			}
		})
	}
}

// TestListEventsFindsWarehouseOutsideWorkspacesAllowlist verifies that resolving the SQL
// warehouse's workspace is NOT scoped by --workspaces: the warehouse can live in any
// workspace in the account, so the allowlist must only narrow which workspaces' audit
// rows get resolved to resources, not which workspaces are searched for the warehouse.
func TestListEventsFindsWarehouseOutsideWorkspacesAllowlist(t *testing.T) {
	queriedWorkspace := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode([]map[string]any{
				{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"},
				{"workspace_id": 2, "workspace_name": "ws2", "deployment_name": "ws2"},
			}); err != nil {
				t.Fatalf("failed to encode mock workspaces response: %v", err)
			}
			return
		}

		// The warehouse only exists in ws1, which is NOT in the --workspaces allowlist
		// below (only ws2 is configured). Resolution must still find it in ws1.
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1") {
			isWs1 := strings.HasPrefix(r.Header.Get("X-Test-Original-Host"), "ws1.")
			w.Header().Set("Content-Type", "application/json")
			if isWs1 {
				fmt.Fprintf(w, `{"id":"wh-1"}`)
				return
			}
			writeJSONNotFound(w)
			return
		}

		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements") {
			queriedWorkspace = r.Header.Get("X-Test-Original-Host")
			w.Header().Set("Content-Type", "application/json")
			resp := map[string]any{
				"statement_id": "stmt-1",
				"status":       map[string]any{"state": "SUCCEEDED"},
				"manifest": map[string]any{
					"schema": map[string]any{
						"columns": []map[string]any{
							{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
							{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
						},
					},
				},
				"result": map[string]any{"data_array": [][]string{}},
			}
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				t.Fatalf("failed to encode mock statement response: %v", err)
			}
			return
		}

		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		http.NotFound(w, r)
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	httpClient := &http.Client{Transport: &redirectTransport{target: target}}
	auth := &databricks.NoAuth{}
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	// --workspaces is scoped to ws2 only; the warehouse lives in ws1.
	feed := newAuditEventFeed(client, []string{"ws2"}, "wh-1")

	_, _, _, err = feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v, want warehouse resolution to succeed despite living outside --workspaces", err)
	}
	if !strings.HasPrefix(queriedWorkspace, "ws1.") {
		t.Errorf("audit query ran against %q, want it to run against ws1 (where the warehouse actually lives)", queriedWorkspace)
	}
}

func TestFilterConfiguredWorkspaces(t *testing.T) {
	all := []databricks.Workspace{
		{ID: 1, Name: "Prod", DeploymentName: "dbc-prod"},
		{ID: 2, Name: "Dev", DeploymentName: "dbc-dev"},
	}

	tests := []struct {
		name       string
		configured []string
		want       []int
	}{
		{"empty allowlist keeps all", nil, []int{1, 2}},
		{"deployment name", []string{"dbc-dev"}, []int{2}},
		{"workspace name, case-insensitive", []string{"PROD"}, []int{1}},
		{"numeric ID", []string{"2"}, []int{2}},
		{"no match", []string{"other"}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterConfiguredWorkspaces(all, tt.configured)
			var ids []int
			for _, w := range got {
				ids = append(ids, w.ID)
			}
			if fmt.Sprint(ids) != fmt.Sprint(tt.want) {
				t.Errorf("filterConfiguredWorkspaces(%v) = %v, want %v", tt.configured, ids, tt.want)
			}
		})
	}
}

// TestExecuteStatementPollsPastCache guards against the uhttp GET cache freezing
// pollStatement: the poll URL never changes, so a cached RUNNING response would be
// returned until statementPollMaxWait.
func TestExecuteStatementPollsPastCache(t *testing.T) {
	var polls atomic.Int32
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		state := "PENDING"
		if r.Method == http.MethodGet {
			state = "RUNNING"
			if polls.Add(1) >= 2 {
				state = "SUCCEEDED"
			}
		}
		fmt.Fprintf(w, `{"statement_id":"stmt-1","status":{"state":%q},"manifest":{"schema":{"columns":[{"name":"x"}]}},"result":{"data_array":[]}}`, state)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, _, err := client.ExecuteStatement(ctx, "dbc-aaa", "wh-1", "SELECT 1"); err != nil {
		t.Fatalf("ExecuteStatement() error = %v", err)
	}
	if got := polls.Load(); got < 2 {
		t.Errorf("server received %d poll GETs, want >= 2 (poll response was served from cache)", got)
	}
}

// TestGetBypassesCacheButSyncCallersDont verifies incremental-sync refreshes (builder Get)
// always hit the API, while the same client call from the regular sync path stays cached.
func TestGetBypassesCacheButSyncCallersDont(t *testing.T) {
	var hits atomic.Int32
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"u-1","userName":"alice@example.com","active":true}`)
	})
	ctx := context.Background()
	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acct-1"}
	userId := &v2.ResourceId{ResourceType: userResourceType.Id, Resource: "u-1"}

	for range 2 {
		if _, _, err := client.GetUser(ctx, "", "u-1"); err != nil {
			t.Fatalf("GetUser() error = %v", err)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("regular GetUser calls made %d API calls, want 1 (second must be cached)", got)
	}

	builder := newUserBuilder(client)
	for range 2 {
		if _, _, err := builder.Get(ctx, userId, accountParent); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
	}
	if got := hits.Load() - 1; got != 2 {
		t.Errorf("builder Get made %d API calls, want 2 (must bypass the cache)", got)
	}
}

// TestListEventsCachesPrincipalTypes verifies a principal named on several rows of one page is
// looked up once, including an id that matches no principal at all.
func TestListEventsCachesPrincipalTypes(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"}})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1"):
			fmt.Fprint(w, `{"id":"wh-1"}`)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/scim/v2/"):
			mu.Lock()
			hits[r.URL.Path]++
			mu.Unlock()
			if strings.HasSuffix(r.URL.Path, "/ServicePrincipals/sp-1") {
				fmt.Fprint(w, `{}`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"detail":"not found"}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements"):
			row := func(id, principal string) []string {
				return []string{id, "2026-01-01T00:00:00Z", "0", "addPrincipalToGroup", "accounts", fmt.Sprintf(`{"targetGroupId":"g-1","targetUserId":%q}`, principal)}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"statement_id": "stmt-1",
				"status":       map[string]any{"state": "SUCCEEDED"},
				"manifest": map[string]any{"schema": map[string]any{"columns": []map[string]any{
					{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
					{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
				}}},
				"result": map[string]any{"data_array": [][]string{
					row("evt-1", "sp-1"), row("evt-2", "sp-1"), row("evt-3", "sp-1"),
					row("evt-4", "gone-1"), row("evt-5", "gone-1"),
				}},
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	})

	feed := newAuditEventFeed(client, nil, "wh-1")
	events, _, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	// Three sp-1 grants, plus one g-1 change: both gone-1 rows fall back to the same group
	// change, and only the last is kept.
	if len(events) != 4 {
		t.Fatalf("len(events) = %d, want 4", len(events))
	}
	for _, id := range []string{"sp-1", "gone-1"} {
		for _, kind := range []string{"Users", "Groups", "ServicePrincipals"} {
			path := fmt.Sprintf("/api/2.0/accounts/acct-1/scim/v2/%s/%s", kind, id)
			if got := hits[path]; got != 1 {
				t.Errorf("%s hit %d times, want 1", path, got)
			}
		}
	}
}

// TestResolvePrincipalTypeDoesNotCacheErrors verifies a failed lookup is retried on the next
// row instead of being remembered.
func TestResolvePrincipalTypeDoesNotCacheErrors(t *testing.T) {
	var calls atomic.Int32
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"detail":"denied"}`)
			return
		}
		fmt.Fprint(w, `{}`)
	})

	principalTypes := newPrincipalLookups(nil)
	if _, _, err := resolvePrincipalType(context.Background(), client, principalTypes, "u-1", false); err == nil {
		t.Fatal("first resolvePrincipalType() error = nil, want the lookup failure")
	}
	got, _, err := resolvePrincipalType(context.Background(), client, principalTypes, "u-1", false)
	if err != nil {
		t.Fatalf("second resolvePrincipalType() error = %v", err)
	}
	if got != userResourceType {
		t.Errorf("second resolvePrincipalType() = %v, want %s", got, userResourceType.Id)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("lookup made %d calls, want 2 (the error must not be cached)", n)
	}
}

// TestListEventsEmitsServicePrincipalChange verifies an accounts/add row naming a service
// principal reaches C1 as a service_principal change, resolved once across rows. The two rows'
// changes to sp-1 collapse into one.
func TestListEventsEmitsServicePrincipalChange(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"}})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1"):
			fmt.Fprint(w, `{"id":"wh-1"}`)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/scim/v2/"):
			mu.Lock()
			hits[r.URL.Path]++
			mu.Unlock()
			if strings.HasSuffix(r.URL.Path, "/ServicePrincipals/sp-1") {
				fmt.Fprint(w, `{}`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"detail":"not found"}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"statement_id": "stmt-1",
				"status":       map[string]any{"state": "SUCCEEDED"},
				"manifest": map[string]any{"schema": map[string]any{"columns": []map[string]any{
					{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
					{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
				}}},
				"result": map[string]any{"data_array": [][]string{
					{"evt-1", "2026-01-01T00:00:00Z", "0", "add", "accounts", `{"targetUserId":"sp-1"}`},
					{"evt-2", "2026-01-01T00:00:01Z", "0", "setAdmin", "accounts", `{"targetUserId":"sp-1"}`},
				}},
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	})

	feed := newAuditEventFeed(client, nil, "wh-1")
	events, _, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}

	var spEvents int
	for _, e := range events {
		id := e.GetResourceChangeEvent().GetResourceId()
		if id.GetResourceType() == userResourceType.Id {
			t.Errorf("event %+v names a user, want the service principal", id)
		}
		if id.GetResourceType() == servicePrincipalResourceType.Id && id.GetResource() == "sp-1" {
			spEvents++
		}
	}
	if spEvents != 1 {
		t.Errorf("got %d service_principal/sp-1 events, want 1: %+v", spEvents, events)
	}
	if got := hits["/api/2.0/accounts/acct-1/scim/v2/ServicePrincipals/sp-1"]; got != 1 {
		t.Errorf("service principal looked up %d times, want 1", got)
	}
}

// TestListEventsDropsDuplicateResourceChanges verifies that when a page names the same resource
// more than once (the same user, or the same workspace role refreshed by several updateUser rows),
// only the last change is kept, with its own row's ID, and grant events are never collapsed.
func TestListEventsDropsDuplicateResourceChanges(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/workspaces") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{{"workspace_id": 1, "workspace_name": "ws1", "deployment_name": "ws1"}})
			return
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/2.0/sql/warehouses/wh-1") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"wh-1"}`)
			return
		}
		if r.Method == http.MethodGet && (strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/scim/v2/Users/u-1") || strings.HasSuffix(r.URL.Path, "/api/2.0/accounts/acct-1/scim/v2/Users/u-2")) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{}`)
			return
		}
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/api/2.0/sql/statements") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"statement_id": "stmt-1",
			"status":       map[string]any{"state": "SUCCEEDED"},
			"manifest": map[string]any{
				"schema": map[string]any{
					"columns": []map[string]any{
						{"name": "event_id"}, {"name": "event_time"}, {"name": "workspace_id"},
						{"name": "action_name"}, {"name": "service_name"}, {"name": "request_params"},
					},
				},
			},
			"result": map[string]any{
				"data_array": [][]string{
					{"evt-1", "2026-01-01T00:00:00Z", "1", "updateUser", "accounts", `{"targetUserId":"u-1"}`},
					{"evt-2", "2026-01-01T00:01:00Z", "1", "updateUser", "accounts", `{"targetUserId":"u-2"}`},
					{"evt-3", "2026-01-01T00:02:00Z", "1", "updateUser", "accounts", `{"targetUserId":"u-1"}`},
					{"evt-4", "2026-01-01T00:03:00Z", "0", "addPrincipalToGroup", "accounts", `{"targetGroupId":"g-1","targetUserId":"u-1"}`},
					{"evt-5", "2026-01-01T00:04:00Z", "0", "addPrincipalToGroup", "accounts", `{"targetGroupId":"g-1","targetUserId":"u-1"}`},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("failed to encode mock statement response: %v", err)
		}
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	httpClient := &http.Client{Transport: &redirectTransport{target: target}}
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", &databricks.NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	feed := newAuditEventFeed(client, nil, "wh-1")
	events, _, _, err := feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}

	// evt-1 is fully superseded: evt-3 names u-1 and the same four roles again. evt-2 keeps only
	// u-2, its roles are superseded by evt-3. evt-4 and evt-5: one grant each.
	want := []string{"evt-2/0", "evt-3/0", "evt-3/1", "evt-3/2", "evt-3/3", "evt-3/4", "evt-4/0", "evt-5/0"}
	got := make([]string, 0, len(events))
	for _, e := range events {
		got = append(got, e.GetId())
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event IDs = %v, want %v", got, want)
	}
	for _, e := range events[6:] {
		if e.GetCreateGrantEvent() == nil {
			t.Errorf("event %s = %+v, want a CreateGrantEvent", e.GetId(), e)
		}
	}
}
