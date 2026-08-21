package connector

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TestResolveSQLWorkspacesTokenAuth ensures the audit-log workspace lookup never calls the
// Account API under workspace-token auth (unreachable in that mode), building minimal
// workspaces from the configured deployment names instead.
func TestResolveSQLWorkspacesTokenAuth(t *testing.T) {
	auth := databricks.NewTokenAuth([]string{"dbc-1", "dbc-2"}, []string{"token-1", "token-2"})
	client, err := databricks.NewClient(context.Background(), &http.Client{}, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "", "", auth, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	got, err := resolveSQLWorkspaces(context.Background(), client, []string{"dbc-1", "dbc-2"})
	if err != nil {
		t.Fatalf("resolveSQLWorkspaces() error = %v", err)
	}

	want := []databricks.Workspace{{DeploymentName: "dbc-1"}, {DeploymentName: "dbc-2"}}
	if len(got) != len(want) {
		t.Fatalf("got %d workspaces, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].DeploymentName != want[i].DeploymentName || got[i].ID != 0 {
			t.Errorf("[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestEventCursorRoundTrip(t *testing.T) {
	want := eventPageCursor{
		StartAt:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LatestEventSeen:   time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		StartAfterEventID: "b",
	}

	encoded, err := encodeEventCursor(want)
	if err != nil {
		t.Fatalf("encodeEventCursor() error = %v", err)
	}

	got := decodeEventCursor(encoded)
	if !got.StartAt.Equal(want.StartAt) || !got.LatestEventSeen.Equal(want.LatestEventSeen) || got.StartAfterEventID != want.StartAfterEventID {
		t.Errorf("decodeEventCursor() = %+v, want %+v", got, want)
	}
}

func TestDecodeEventCursorSelfHeals(t *testing.T) {
	cases := []string{"", "not-base64!!!", "aW52YWxpZC1qc29u"} // last one is base64("invalid-json")
	for _, c := range cases {
		got := decodeEventCursor(c)
		if !got.StartAt.IsZero() {
			t.Errorf("decodeEventCursor(%q) = %+v, want zero-value cursor", c, got)
		}
	}
}

func TestAdvanceEventCursorFullPageAdvancesWithoutTrailingLag(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}

	rows := []auditLogRow{
		{EventID: "1", EventTime: startAt.Add(1 * time.Minute)},
		{EventID: "2", EventTime: startAt.Add(2 * time.Minute)},
	}

	next := advanceEventCursor(cursor, rows, true, startAt.Add(10*time.Minute))

	wantStart := startAt.Add(2 * time.Minute)
	if !next.StartAt.Equal(wantStart) {
		t.Errorf("StartAt = %v, want %v (no trailing lag while more pages remain)", next.StartAt, wantStart)
	}
	if next.StartAfterEventID != "2" {
		t.Errorf("StartAfterEventID = %q, want %q", next.StartAfterEventID, "2")
	}
}

func TestAdvanceEventCursorDrainedPageAppliesTrailingLag(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}
	latest := startAt.Add(5 * time.Hour)

	rows := []auditLogRow{
		{EventID: "1", EventTime: latest},
	}

	next := advanceEventCursor(cursor, rows, false, latest)

	wantStart := latest.Add(-auditLogTrailingLag)
	if !next.StartAt.Equal(wantStart) {
		t.Errorf("StartAt = %v, want %v", next.StartAt, wantStart)
	}
	// The trailing lag pushes the boundary well before the only row seen, so nothing ties.
	if next.StartAfterEventID != "" {
		t.Errorf("StartAfterEventID = %q, want empty", next.StartAfterEventID)
	}
}

// TestAdvanceEventCursorTieAtFlooredBoundaryIsRemembered covers a row landing exactly on
// the floored StartAt boundary, which would otherwise be re-fetched forever.
func TestAdvanceEventCursorTieAtFlooredBoundaryIsRemembered(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}

	rows := []auditLogRow{
		{EventID: "1", EventTime: startAt},
	}

	next := advanceEventCursor(cursor, rows, false, startAt)

	if !next.StartAt.Equal(startAt) {
		t.Errorf("StartAt = %v, want unchanged %v", next.StartAt, startAt)
	}
	if next.StartAfterEventID != "1" {
		t.Errorf("StartAfterEventID = %q, want %q", next.StartAfterEventID, "1")
	}
}

func TestAdvanceEventCursorNeverRegresses(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}

	// "now" is barely past startAt, so subtracting the trailing lag would regress.
	now := startAt.Add(1 * time.Minute)

	next := advanceEventCursor(cursor, nil, false, now)

	if next.StartAt.Before(cursor.StartAt) {
		t.Errorf("StartAt regressed: got %v, was %v", next.StartAt, cursor.StartAt)
	}
	if !next.StartAt.Equal(cursor.StartAt) {
		t.Errorf("StartAt = %v, want unchanged %v", next.StartAt, cursor.StartAt)
	}
}

func TestAdvanceEventCursorEmptyWindowTrailsWallClock(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}
	now := startAt.Add(10 * time.Hour)

	next := advanceEventCursor(cursor, nil, false, now)

	wantStart := now.Add(-auditLogTrailingLag)
	if !next.StartAt.Equal(wantStart) {
		t.Errorf("StartAt = %v, want %v", next.StartAt, wantStart)
	}
}

func TestMapAuditRowToResource(t *testing.T) {
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
				ActionName:    "addPrincipalToGroup",
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
				ActionName:    "addPrincipalToGroup",
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
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			want: []wantResource{
				{userResourceType.Id, "u-1", accountResourceType.Id, accountId},
				{roleResourceType.Id, roleResourceId(AccountAdminRole, accountParent), accountResourceType.Id, accountId},
			},
		},
		{
			name:                "updateUser stays account-parented when the Account API is available, but workspace roles still refresh",
			accountAPIAvailable: true,
			row: auditLogRow{
				ActionName:    "updateUser",
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
			name:                "unresolvable workspace is skipped",
			accountAPIAvailable: true,
			row: auditLogRow{
				ActionName:    "createUser",
				WorkspaceID:   999,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
		},
		{
			name: "missing id param is skipped",
			row:  auditLogRow{ActionName: "createUser", WorkspaceID: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapAuditRowToResource(context.Background(), tc.row, accountId, tc.accountAPIAvailable, workspaceLookup)
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

func TestParseAuditLogRowsDedupesNothingAndParsesFields(t *testing.T) {
	result := &databricks.StatementResult{
		Columns: []string{"event_id", "event_time", "workspace_id", "action_name", "request_params"},
		Rows: [][]string{
			{"evt-1", "2026-01-01 00:00:00.000", "123", "createGroup", `{"targetGroupId":"g-1"}`},
			{"evt-2", "2026-01-01T00:01:00Z", "0", "createUser", `{"targetUserId":"u-1"}`},
		},
	}

	rows, err := parseAuditLogRows(result)
	if err != nil {
		t.Fatalf("parseAuditLogRows() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0].WorkspaceID != 123 || rows[0].RequestParams["targetGroupId"] != "g-1" {
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

	if _, err := parseAuditLogRows(result); err == nil {
		t.Error("parseAuditLogRows() error = nil, want error for missing required column")
	}
}
