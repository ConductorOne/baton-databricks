package connector

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestEventCursorRoundTrip(t *testing.T) {
	want := eventPageCursor{
		StartAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LatestEventSeen: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		LastEventIDs:    []string{"a", "b"},
	}

	encoded, err := encodeEventCursor(want)
	if err != nil {
		t.Fatalf("encodeEventCursor() error = %v", err)
	}

	got := decodeEventCursor(encoded)
	if !got.StartAt.Equal(want.StartAt) || !got.LatestEventSeen.Equal(want.LatestEventSeen) || len(got.LastEventIDs) != 2 {
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
	if len(next.LastEventIDs) != 1 || next.LastEventIDs[0] != "2" {
		t.Errorf("LastEventIDs = %v, want [2]", next.LastEventIDs)
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
	if len(next.LastEventIDs) != 0 {
		t.Errorf("LastEventIDs = %v, want empty", next.LastEventIDs)
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
	if len(next.LastEventIDs) != 1 || next.LastEventIDs[0] != "1" {
		t.Errorf("LastEventIDs = %v, want [1]", next.LastEventIDs)
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

	cases := []struct {
		name             string
		row              auditLogRow
		wantOK           bool
		wantResourceType string
		wantResource     string
		wantParentType   string
		wantParentID     string
	}{
		{
			name: "account-level group create",
			row: auditLogRow{
				ActionName:    "createGroup",
				WorkspaceID:   0,
				RequestParams: map[string]string{"targetGroupId": "g-1"},
			},
			wantOK:           true,
			wantResourceType: groupResourceType.Id,
			wantResource:     groupResourceId(context.Background(), "g-1", &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}),
			wantParentType:   accountResourceType.Id,
			wantParentID:     accountId,
		},
		{
			name: "workspace-scoped acl change",
			row: auditLogRow{
				ActionName:  "changeDatabricksWorkspaceAcl",
				WorkspaceID: 123,
			},
			wantOK:           true,
			wantResourceType: workspaceResourceType.Id,
			wantResource:     "my-workspace",
			wantParentType:   accountResourceType.Id,
			wantParentID:     accountId,
		},
		{
			name: "unknown action is skipped",
			row: auditLogRow{
				ActionName: "someUnityCatalogAction",
			},
			wantOK: false,
		},
		{
			name: "unresolvable workspace is skipped",
			row: auditLogRow{
				ActionName:    "createUser",
				WorkspaceID:   999,
				RequestParams: map[string]string{"targetUserId": "u-1"},
			},
			wantOK: false,
		},
		{
			name: "missing id param is skipped",
			row: auditLogRow{
				ActionName:  "createUser",
				WorkspaceID: 0,
			},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resourceId, parentResourceId, ok := mapAuditRowToResource(tc.row, accountId, workspaceLookup)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if resourceId.ResourceType != tc.wantResourceType || resourceId.Resource != tc.wantResource {
				t.Errorf("resourceId = %+v, want type=%s id=%s", resourceId, tc.wantResourceType, tc.wantResource)
			}
			if parentResourceId.ResourceType != tc.wantParentType || parentResourceId.Resource != tc.wantParentID {
				t.Errorf("parentResourceId = %+v, want type=%s id=%s", parentResourceId, tc.wantParentType, tc.wantParentID)
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
