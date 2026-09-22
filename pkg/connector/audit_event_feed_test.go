package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
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
	auth := databricks.NewTokenAuth(nil, nil)
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
// system.access.audit's retention window, which must self-heal like a corrupt cursor.
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

	next := advanceEventCursor(cursor, lastRawBoundary, true, cursor.StartAt.Add(auditLogTrailingLag))

	if next != lastRawBoundary {
		t.Errorf("next = %+v, want %+v", next, lastRawBoundary)
	}
}

// TestAdvanceEventCursorFullyMalformedPageKeepsCursor covers a full page where every row
// failed to parse: lastRawBoundary is zero-value, so this must not panic indexing an empty
// rows slice, and must not skip ahead past unparsed rows either.
func TestAdvanceEventCursorFullyMalformedPageKeepsCursor(t *testing.T) {
	cursor := eventPageCursor{StartAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), StartAfterEventID: "evt-1"}

	next := advanceEventCursor(cursor, eventPageCursor{}, true, cursor.StartAt.Add(auditLogTrailingLag))

	if next != cursor {
		t.Errorf("next = %+v, want unchanged cursor %+v", next, cursor)
	}
}

// TestAdvanceEventCursorDrainedJumpsToLagCutoff verifies a drained page advances straight to
// lagCutoff: rows are already bounded by it (see queryAuditLog), so draining proves nothing
// else exists up to that point.
func TestAdvanceEventCursorDrainedJumpsToLagCutoff(t *testing.T) {
	cursor := eventPageCursor{StartAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	lagCutoff := cursor.StartAt.Add(4 * time.Hour)

	next := advanceEventCursor(cursor, eventPageCursor{}, false, lagCutoff)

	if !next.StartAt.Equal(lagCutoff) || next.StartAfterEventID != "" {
		t.Errorf("next = %+v, want StartAt=%v with no tiebreaker", next, lagCutoff)
	}
}

func TestMapAuditRowToResource(t *testing.T) {
	// None of these cases are grant-mapped, so mapAuditRowToResource should never reach the
	// principal-type lookup; a client that fails any request proves that.
	client := newProbeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request %s %s: no case here is grant-mapped", r.Method, r.URL.Path)
	})
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
		name                string
		row                 auditLogRow
		accountAPIAvailable bool
		want                []wantResource
	}{
		{
			name:                "account-level group create",
			accountAPIAvailable: true,
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
			name:                "workspace-scoped group change stays account-parented when the Account API is available",
			accountAPIAvailable: true,
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
			name:                "workspace-scoped group change is workspace-parented under token auth",
			accountAPIAvailable: false,
			row: auditLogRow{
				ActionName:    "removeGroup",
				ServiceName:   "accounts",
				WorkspaceID:   123,
				RequestParams: map[string]string{"targetGroupId": "g-1"},
			},
			want: []wantResource{
				{groupResourceType.Id, groupResourceId(context.Background(), "g-1", workspaceParent), workspaceResourceType.Id, "my-workspace"},
			},
		},
		{
			name:                "workspace-scoped acl change also refreshes the workspace-access role",
			accountAPIAvailable: true,
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
			name:                "setAdmin refreshes the user and the account-admin role",
			accountAPIAvailable: true,
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
			name: "setAccountAdmin is not a real action_name, so it's skipped",
			row:  auditLogRow{ActionName: "setAccountAdmin", ServiceName: "accounts"},
		},
		{
			name:                "updateUser stays account-parented when the Account API is available, but workspace roles still refresh",
			accountAPIAvailable: true,
			row: auditLogRow{
				ActionName:    "updateUser",
				ServiceName:   "accounts",
				WorkspaceID:   123,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(ClusterCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(InstancePoolCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
			},
		},
		{
			name:                "updateUser is workspace-parented under token auth",
			accountAPIAvailable: false,
			row: auditLogRow{
				ActionName:    "updateUser",
				ServiceName:   "accounts",
				WorkspaceID:   123,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(ClusterCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
				{roleResourceType.Id, roleResourceId(InstancePoolCreateRole, workspaceParent), workspaceResourceType.Id, "my-workspace"},
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
			name:                "unresolvable workspace is skipped",
			accountAPIAvailable: true,
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
			got, _, err := mapAuditRowToResource(context.Background(), client, tc.row, accountId, tc.accountAPIAvailable, workspaceLookup)
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

			got, _, err := mapAuditRowToResource(context.Background(), client, row, accountId, true, nil)
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

	t.Run("missing principal id is skipped", func(t *testing.T) {
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), client, row, accountId, true, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		if got != nil {
			t.Errorf("got %+v, want nil when targetUserId is missing", got)
		}
	})

	t.Run("principal resolves to a nested group", func(t *testing.T) {
		groupClient := principalKindTestClient(t, "g-2", "Groups")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "g-2"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), groupClient, row, accountId, true, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		if len(got) != 1 || got[0].grant == nil {
			t.Fatalf("got %+v, want exactly one grant-mapped affected resource", got)
		}
		if p := got[0].grant.principal.GetId(); p.GetResourceType() != groupResourceType.Id || p.GetResource() != "g-2" {
			t.Errorf("principal = %+v, want type=%s id=g-2", p, groupResourceType.Id)
		}
	})

	t.Run("principal resolves to a service principal", func(t *testing.T) {
		spClient := principalKindTestClient(t, "sp-1", "ServicePrincipals")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "sp-1"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), spClient, row, accountId, true, nil)
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

	t.Run("principal not found anywhere is skipped", func(t *testing.T) {
		notFoundClient := principalKindTestClient(t, "ghost", "none")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "ghost"},
		}
		got, _, err := mapAuditRowToResource(context.Background(), notFoundClient, row, accountId, true, nil)
		if err != nil {
			t.Fatalf("mapAuditRowToResource() error = %v", err)
		}
		if got != nil {
			t.Errorf("got %+v, want nil when the principal id matches no resource type", got)
		}
	})

	t.Run("a real lookup error propagates instead of being swallowed", func(t *testing.T) {
		errClient := principalKindTestClient(t, "u-1", "error")
		row := auditLogRow{
			ActionName:    "addPrincipalToGroup",
			ServiceName:   auditServiceAccounts,
			RequestParams: map[string]string{"targetGroupId": "g-1", "targetUserId": "u-1"},
		}
		if _, _, err := mapAuditRowToResource(context.Background(), errClient, row, accountId, true, nil); err == nil {
			t.Error("mapAuditRowToResource() error = nil, want a propagated lookup error")
		}
	})
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

	feed := newAuditEventFeed(client, nil, true, "wh-1")
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
		// ListEvents (OAuth2-only, now that token auth can't reach it) always lists
		// workspaces via the Account API before querying the audit log.
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
	auth := databricks.NewTokenAuth([]string{"ws1"}, []string{"token-1"})
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	// Mirrors what Validate() sets before any sync/event-feed call runs in production.
	client.UpdateAvailability(true, true)

	feed := newAuditEventFeed(client, []string{"ws1"}, true, "wh-1")

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
	auth := databricks.NewTokenAuth([]string{"ws1"}, []string{"token-1"})
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.UpdateAvailability(true, true)

	feed := newAuditEventFeed(client, []string{"ws1"}, true, "wh-1")
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
	auth := databricks.NewTokenAuth([]string{"ws1", "ws2"}, []string{"token-1", "token-2"})
	client, err := databricks.NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acct-1", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.UpdateAvailability(true, true)

	// --workspaces is scoped to ws2 only; the warehouse lives in ws1.
	feed := newAuditEventFeed(client, []string{"ws2"}, true, "wh-1")

	_, _, _, err = feed.ListEvents(context.Background(), testEarliestEvent(), &pagination.StreamToken{Cursor: ""})
	if err != nil {
		t.Fatalf("ListEvents() error = %v, want warehouse resolution to succeed despite living outside --workspaces", err)
	}
	if !strings.HasPrefix(queriedWorkspace, "ws1.") {
		t.Errorf("audit query ran against %q, want it to run against ws1 (where the warehouse actually lives)", queriedWorkspace)
	}
}
