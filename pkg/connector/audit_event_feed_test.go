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

func TestResolveQueryWorkspace(t *testing.T) {
	workspaces := []databricks.Workspace{
		{ID: 1, DeploymentName: "dbc-zzz"},
		{ID: 2, DeploymentName: "dbc-aaa"},
	}

	t.Run("pinned workspace is used regardless of alphabetical order", func(t *testing.T) {
		got, lookup, err := resolveQueryWorkspace(context.Background(), workspaces, "dbc-zzz")
		if err != nil {
			t.Fatalf("resolveQueryWorkspace() error = %v", err)
		}
		if got != "dbc-zzz" {
			t.Errorf("queryWorkspaceId = %q, want %q", got, "dbc-zzz")
		}
		if lookup[1] != "dbc-zzz" || lookup[2] != "dbc-aaa" {
			t.Errorf("lookup = %+v, want ids 1 and 2 mapped to their deployment names", lookup)
		}
	})

	t.Run("pinned workspace match is case-insensitive", func(t *testing.T) {
		got, _, err := resolveQueryWorkspace(context.Background(), workspaces, "DBC-ZZZ")
		if err != nil {
			t.Fatalf("resolveQueryWorkspace() error = %v", err)
		}
		if got != "dbc-zzz" {
			t.Errorf("queryWorkspaceId = %q, want %q", got, "dbc-zzz")
		}
	})

	t.Run("unknown pinned workspace is a clear config error", func(t *testing.T) {
		_, _, err := resolveQueryWorkspace(context.Background(), workspaces, "dbc-does-not-exist")
		if err == nil {
			t.Fatal("resolveQueryWorkspace() error = nil, want error for unresolvable sql-warehouse-workspace")
		}
	})

	t.Run("no pin falls back to the arbitrary alphabetical pick", func(t *testing.T) {
		got, _, err := resolveQueryWorkspace(context.Background(), workspaces, "")
		if err != nil {
			t.Fatalf("resolveQueryWorkspace() error = %v", err)
		}
		if got != "dbc-aaa" {
			t.Errorf("queryWorkspaceId = %q, want %q (sqlQueryWorkspace's default)", got, "dbc-aaa")
		}
	})

	t.Run("single workspace needs no pin", func(t *testing.T) {
		got, _, err := resolveQueryWorkspace(context.Background(), workspaces[:1], "")
		if err != nil {
			t.Fatalf("resolveQueryWorkspace() error = %v", err)
		}
		if got != "dbc-zzz" {
			t.Errorf("queryWorkspaceId = %q, want %q", got, "dbc-zzz")
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

// TestAdvanceEventCursorFullPageAdvancesPastLagWindow verifies that a full page of rows
// older than the trailing-lag boundary still advances StartAt to the last row processed.
func TestAdvanceEventCursorFullPageAdvancesPastLagWindow(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}

	rows := []auditLogRow{
		{EventID: "1", EventTime: startAt.Add(1 * time.Minute)},
		{EventID: "2", EventTime: startAt.Add(2 * time.Minute)},
	}

	// now is far enough past the rows that startAt+2min is still older than
	// now-auditLogTrailingLag, so the lag clamp shouldn't kick in.
	now := startAt.Add(2*time.Minute + auditLogTrailingLag + time.Hour)

	next := advanceEventCursor(cursor, rows, true, now)

	wantStart := startAt.Add(2 * time.Minute)
	if !next.StartAt.Equal(wantStart) {
		t.Errorf("StartAt = %v, want %v (rows are older than the lag window, so no clamp)", next.StartAt, wantStart)
	}
	if next.StartAfterEventID != "2" {
		t.Errorf("StartAfterEventID = %q, want %q", next.StartAfterEventID, "2")
	}
}

// TestAdvanceEventCursorFullPageClampsToLagWindow verifies intra-page paging never
// advances StartAt past now-auditLogTrailingLag when the rows are within the lag window.
func TestAdvanceEventCursorFullPageClampsToLagWindow(t *testing.T) {
	startAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: startAt}

	rows := []auditLogRow{
		{EventID: "1", EventTime: startAt.Add(1 * time.Minute)},
		{EventID: "2", EventTime: startAt.Add(2 * time.Minute)},
	}

	// now is close to the rows' timestamps, so startAt+2min falls inside the lag window.
	now := startAt.Add(10 * time.Minute)

	next := advanceEventCursor(cursor, rows, true, now)

	wantStart := now.Add(-auditLogTrailingLag)
	if !next.StartAt.Equal(wantStart) {
		t.Errorf("StartAt = %v, want %v (clamped to the trailing-lag boundary)", next.StartAt, wantStart)
	}
	if next.StartAfterEventID != "" {
		t.Errorf("StartAfterEventID = %q, want empty (clamped boundary doesn't tie to a real row)", next.StartAfterEventID)
	}

	// Once the burst drains, the trailing lag must still apply going forward.
	drainedLatest := startAt.Add(3 * time.Hour)
	drainedRows := []auditLogRow{{EventID: "3", EventTime: drainedLatest}}
	drainedNow := drainedLatest.Add(5 * time.Minute)
	drained := advanceEventCursor(next, drainedRows, false, drainedNow)

	wantDrainedStart := drainedLatest.Add(-auditLogTrailingLag)
	if !drained.StartAt.Equal(wantDrainedStart) {
		t.Errorf("drained StartAt = %v, want %v (trailing lag re-applied after drain)", drained.StartAt, wantDrainedStart)
	}
	if !drained.StartAt.After(next.StartAt) {
		t.Errorf("drained StartAt = %v did not advance past the clamped intra-page StartAt %v", drained.StartAt, next.StartAt)
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
				ActionName:    "addPrincipalToGroup",
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
				ActionName:    "addPrincipalToGroup",
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
		Columns: []string{"event_id", "event_time", "workspace_id", "action_name", "service_name", "request_params"},
		Rows: [][]string{
			{"evt-1", "2026-01-01 00:00:00.000", "123", "createGroup", "accounts", `{"targetGroupId":"g-1"}`},
			{"evt-2", "2026-01-01T00:01:00Z", "0", "add", "accounts", `{"targetUserId":"u-1"}`},
		},
	}

	rows, err := parseAuditLogRows(context.Background(), result)
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

	if _, err := parseAuditLogRows(context.Background(), result); err == nil {
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

	rows, err := parseAuditLogRows(context.Background(), result)
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

// TestAdvanceEventCursorLargeTiedBurstMakesProgress verifies more than auditLogPageLimit rows
// sharing one event_time still make progress via the (event_time, event_id) cursor.
func TestAdvanceEventCursorLargeTiedBurstMakesProgress(t *testing.T) {
	tied := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cursor := eventPageCursor{StartAt: tied.Add(-time.Minute)}

	rows := make([]auditLogRow, auditLogPageLimit)
	for i := range rows {
		rows[i] = auditLogRow{EventID: fmt.Sprintf("evt-%04d", i), EventTime: tied}
	}

	// now is far past the lag window so the intra-page clamp doesn't interfere.
	now := tied.Add(auditLogTrailingLag + time.Hour)

	next := advanceEventCursor(cursor, rows, true, now)
	if !next.StartAt.Equal(tied) {
		t.Fatalf("StartAt = %v, want %v (advances to the tied timestamp, not stuck before it)", next.StartAt, tied)
	}
	lastID := rows[len(rows)-1].EventID
	if next.StartAfterEventID != lastID {
		t.Fatalf("StartAfterEventID = %q, want %q (last row in the tied burst)", next.StartAfterEventID, lastID)
	}

	// A later, non-tied page must advance the cursor past the tied burst.
	followUpLatest := tied.Add(5 * time.Hour)
	followUp := []auditLogRow{{EventID: "evt-1000", EventTime: followUpLatest}}
	drained := advanceEventCursor(next, followUp, false, followUpLatest.Add(5*time.Minute))
	if !drained.StartAt.After(tied) {
		t.Errorf("drained StartAt = %v did not advance past the tied burst's timestamp %v", drained.StartAt, tied)
	}
}

// redirectTransport rewrites every outgoing request to target, since workspaceUrl always
// builds a "<workspace>.<hostname>" subdomain a local httptest.Server can't listen on directly.
type redirectTransport struct {
	target *url.URL
}

func (t *redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = t.target.Scheme
	req.URL.Host = t.target.Host
	req.Host = t.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestListEventsEndToEnd exercises ListEvents against a mocked Statement Execution API,
// verifying the service_name query filter, resource mapping, and rate-limit propagation.
func TestListEventsEndToEnd(t *testing.T) {
	const wantLimit = 100
	const wantRemaining = 42

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	feed := newAuditEventFeed(client, []string{"ws1"}, true, "wh-1", "ws1")

	events, streamState, annos, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{Cursor: ""})
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
