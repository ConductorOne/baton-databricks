package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	auditEventFeedId = "databricks_audit_log"

	// The first poll looks back this far since there's no prior watermark yet.
	auditLogLookback = 1 * time.Hour

	// Trail the watermark by this much instead of the newest event seen, since slower-indexing
	// areas of the audited system could otherwise have events skipped permanently.
	auditLogTrailingLag = 4 * time.Hour

	auditLogPageLimit = 1000

	// auditLogRetention mirrors system.access.audit's documented 365-day retention; a cursor
	// older than this can no longer be satisfied by the table and is treated as stale.
	auditLogRetention = 365 * 24 * time.Hour

	auditServiceAccounts = "accounts"
)

// auditActionMapping describes what an audit log action_name affects: an optional primary
// resource (resourceType + the request_params key holding its native ID), an optional
// account-scoped role, and/or optional workspace-scoped roles/entitlements.
type auditActionMapping struct {
	resourceType *v2.ResourceType
	idParam      string
	accountRole  string
	roleNames    []string
}

// auditActionKey identifies an audit log action by (service_name, action_name), since
// action_name alone is ambiguous across services (e.g. "delete" also means "cluster terminated").
type auditActionKey struct {
	Service string
	Action  string
}

// auditLogActions maps (service_name, action_name) pairs to the resources they affect, per
// https://docs.databricks.com/aws/en/admin/account-settings/audit-logs.
var auditLogActions = map[auditActionKey]auditActionMapping{
	{auditServiceAccounts, "createGroup"}:               {resourceType: groupResourceType, idParam: "targetGroupId"},
	{auditServiceAccounts, "addPrincipalToGroup"}:       {resourceType: groupResourceType, idParam: "targetGroupId"},
	{auditServiceAccounts, "removePrincipalFromGroup"}:  {resourceType: groupResourceType, idParam: "targetGroupId"},
	{auditServiceAccounts, "addPrincipalsToGroup"}:      {resourceType: groupResourceType, idParam: "targetGroupId"},
	{auditServiceAccounts, "removePrincipalsFromGroup"}: {resourceType: groupResourceType, idParam: "targetGroupId"},
	{auditServiceAccounts, "removeGroup"}:               {resourceType: groupResourceType, idParam: "targetGroupId"},
	{auditServiceAccounts, "updateGroup"}: {
		resourceType: groupResourceType, idParam: "targetGroupId",
		roleNames: []string{ClusterCreateRole, InstancePoolCreateRole},
	},
	// "add"/"delete" are the real user-lifecycle events; deleteUser is a parameterless PII purge.
	{auditServiceAccounts, "add"}: {resourceType: userResourceType, idParam: "targetUserId"},
	{auditServiceAccounts, "updateUser"}: {
		resourceType: userResourceType, idParam: "targetUserId",
		roleNames: []string{ClusterCreateRole, InstancePoolCreateRole},
	},
	{auditServiceAccounts, "delete"}:                 {resourceType: userResourceType, idParam: "targetUserId"},
	{auditServiceAccounts, "createServicePrincipal"}: {resourceType: servicePrincipalResourceType, idParam: "targetServicePrincipalId"},
	{auditServiceAccounts, "updateServicePrincipal"}: {
		resourceType: servicePrincipalResourceType, idParam: "targetServicePrincipalId",
		roleNames: []string{ClusterCreateRole, InstancePoolCreateRole},
	},
	{auditServiceAccounts, "deleteServicePrincipal"}:       {resourceType: servicePrincipalResourceType, idParam: "targetServicePrincipalId"},
	{auditServiceAccounts, "changeDatabricksWorkspaceAcl"}: {resourceType: workspaceResourceType, roleNames: []string{WorkspaceAccessRole}},
	{auditServiceAccounts, "changeDatabricksSqlAcl"}:       {roleNames: []string{SQLAccessRole}},
	{auditServiceAccounts, "setAdmin"}:                     {resourceType: userResourceType, idParam: "targetUserId", accountRole: AccountAdminRole},
	// removeAdmin revokes *workspace* admin, not account admin, so only the user is refreshed.
	{auditServiceAccounts, "removeAdmin"}: {resourceType: userResourceType, idParam: "targetUserId"},
}

func auditLogActionNames() []string {
	seen := make(map[string]struct{}, len(auditLogActions))
	for key := range auditLogActions {
		seen[key.Action] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func auditLogServiceNames() []string {
	seen := make(map[string]struct{}, len(auditLogActions))
	for key := range auditLogActions {
		seen[key.Service] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// eventPageCursor is the opaque state persisted between ListEvents calls. (StartAt,
// StartAfterEventID) form a composite boundary: unprocessed rows are those with
// event_time > StartAt, or event_time == StartAt AND event_id > StartAfterEventID. This
// keeps the boundary well-ordered even when many rows share the same event_time.
type eventPageCursor struct {
	StartAt           time.Time `json:"start_at"`
	StartAfterEventID string    `json:"start_after_event_id"`
}

func encodeEventCursor(c eventPageCursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to marshal event cursor: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// decodeEventCursor returns a zero-value cursor (self-healing to the lookback default) when
// missing, corrupt, or stale. Corrupt/missing is routine and logged at Debug; a stale-but-valid
// cursor indicates a real data gap and is logged at Warn.
func decodeEventCursor(ctx context.Context, s string, now time.Time) eventPageCursor {
	l := ctxzap.Extract(ctx)

	if s == "" {
		return eventPageCursor{}
	}

	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		l.Debug("databricks-connector: corrupt event cursor, resetting to lookback default", zap.Error(err))
		return eventPageCursor{}
	}

	var c eventPageCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		l.Debug("databricks-connector: corrupt event cursor, resetting to lookback default", zap.Error(err))
		return eventPageCursor{}
	}

	if !c.StartAt.IsZero() && now.Sub(c.StartAt) > auditLogRetention {
		l.Warn("databricks-connector: event cursor is older than system.access.audit's retention window, resetting to lookback default",
			zap.Time("cursor_start_at", c.StartAt),
		)
		return eventPageCursor{}
	}

	return c
}

type auditLogRow struct {
	EventID       string
	EventTime     time.Time
	WorkspaceID   int64
	ActionName    string
	ServiceName   string
	RequestParams map[string]string
}

type auditEventFeed struct {
	client                *databricks.Client
	workspaces            []string
	enableIncrementalSync bool
	sqlWarehouseID        string

	// queryWorkspaceID caches which workspace hosts sqlWarehouseID across polls.
	queryWorkspaceMu sync.Mutex
	queryWorkspaceID string
}

func newAuditEventFeed(
	client *databricks.Client,
	workspaces []string,
	enableIncrementalSync bool,
	sqlWarehouseID string,
) *auditEventFeed {
	return &auditEventFeed{
		client:                client,
		workspaces:            workspaces,
		enableIncrementalSync: enableIncrementalSync,
		sqlWarehouseID:        sqlWarehouseID,
	}
}

// resolveQueryWorkspaceID returns which workspace hosts f.sqlWarehouseID, resolving it
// once via resolveWarehouseWorkspace rather than probing on every poll.
func (f *auditEventFeed) resolveQueryWorkspaceID(ctx context.Context, workspaces []databricks.Workspace) (string, *v2.RateLimitDescription, error) {
	f.queryWorkspaceMu.Lock()
	defer f.queryWorkspaceMu.Unlock()

	if f.queryWorkspaceID != "" {
		return f.queryWorkspaceID, nil, nil
	}

	id, rateLimit, err := resolveWarehouseWorkspace(ctx, f.client, workspaces, f.sqlWarehouseID)
	if err != nil {
		return "", rateLimit, err
	}

	f.queryWorkspaceID = id
	return id, rateLimit, nil
}

// EventFeedMetadata is registered unconditionally; enable-incremental-sync gates behavior
// inside ListEvents instead, to avoid confusing "feed not found" errors when it's off.
func (f *auditEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id:                  auditEventFeedId,
		SupportedEventTypes: []v2.EventType{v2.EventType_EVENT_TYPE_RESOURCE_CHANGE},
	}
}

func (f *auditEventFeed) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	annos := annotations.Annotations{}

	if !f.enableIncrementalSync {
		return nil, &pagination.StreamState{}, nil, nil
	}

	now := time.Now()
	cursor := decodeEventCursor(ctx, pToken.Cursor, now)

	if cursor.StartAt.IsZero() {
		start := now.Add(-auditLogLookback)
		if earliestEvent != nil {
			start = earliestEvent.AsTime()
		}
		cursor = eventPageCursor{StartAt: start}
	}

	workspaces, err := resolveSQLWorkspaces(ctx, f.client, f.workspaces)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}
	if len(workspaces) == 0 {
		return nil, nil, nil, fmt.Errorf("databricks-connector: no workspace available to query system.access.audit")
	}

	workspaceLookup := make(map[int64]string, len(workspaces))
	for _, w := range workspaces {
		workspaceLookup[int64(w.ID)] = w.DeploymentName
	}

	queryWorkspaceId, rateLimit, err := f.resolveQueryWorkspaceID(ctx, workspaces)
	if rateLimit != nil {
		annos.WithRateLimiting(rateLimit)
	}
	if err != nil {
		return nil, nil, annos, err
	}

	rows, rateLimit, err := f.queryAuditLog(ctx, queryWorkspaceId, cursor)
	if err != nil {
		if rateLimit != nil {
			annos.WithRateLimiting(rateLimit)
		}
		return nil, nil, annos, fmt.Errorf("databricks-connector: failed to query audit log: %w", err)
	}

	if rateLimit != nil {
		annos.WithRateLimiting(rateLimit)
	}

	var events []*v2.Event
	for _, row := range rows {
		affected := mapAuditRowToResource(ctx, row, f.client.GetAccountId(), f.client.IsAccountAPIAvailable(), workspaceLookup)
		if len(affected) == 0 {
			l.Debug("databricks-connector: skipping audit row with no resource mapping",
				zap.String("action_name", row.ActionName),
				zap.String("event_id", row.EventID),
			)
			continue
		}

		for i, a := range affected {
			events = append(events, &v2.Event{
				Id:         fmt.Sprintf("%s/%d", row.EventID, i),
				OccurredAt: timestamppb.New(row.EventTime),
				Event: &v2.Event_ResourceChangeEvent{
					ResourceChangeEvent: &v2.ResourceChangeEvent{
						ResourceId:       a.resourceId,
						ParentResourceId: a.parentResourceId,
					},
				},
			})
		}
	}

	hasMore := len(rows) >= auditLogPageLimit
	nextCursor := advanceEventCursor(cursor, rows, hasMore, now)

	encoded, err := encodeEventCursor(nextCursor)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to encode event cursor: %w", err)
	}

	return events, &pagination.StreamState{Cursor: encoded, HasMore: hasMore}, annos, nil
}

// advanceEventCursor advances only to the last row processed (by the well-ordered
// (event_time, event_id) boundary) while a page is full, and once drained, trails the
// newest event seen (or wall-clock time if empty) by auditLogTrailingLag.
func advanceEventCursor(cursor eventPageCursor, rows []auditLogRow, hasMore bool, now time.Time) eventPageCursor {
	latest := cursor.StartAt
	lastEventID := cursor.StartAfterEventID
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		latest = last.EventTime
		lastEventID = last.EventID
	}

	if hasMore {
		startAt := latest
		startAfterEventID := lastEventID
		// Clamp intra-page advances to the trailing-lag boundary; otherwise the never-regress
		// floor below would permanently defeat auditLogTrailingLag for this burst.
		if laggedFloor := now.Add(-auditLogTrailingLag); startAt.After(laggedFloor) {
			startAt = laggedFloor
			startAfterEventID = ""
		}
		return eventPageCursor{StartAt: startAt, StartAfterEventID: startAfterEventID}
	}

	target := latest.Add(-auditLogTrailingLag)
	if len(rows) == 0 {
		target = now.Add(-auditLogTrailingLag)
	}
	if target.Before(cursor.StartAt) {
		target = cursor.StartAt
	}

	startAfterEventID := ""
	if target.Equal(latest) {
		startAfterEventID = lastEventID
	}

	return eventPageCursor{StartAt: target, StartAfterEventID: startAfterEventID}
}

// affectedResource is one resource a mapped audit row's action changed.
type affectedResource struct {
	resourceId       *v2.ResourceId
	parentResourceId *v2.ResourceId
}

// mapAuditRowToResource maps an audit row to every Baton resource its action affects, skipping
// anything unresolvable. The principal's parent mirrors how it's actually synced (see
// groupGrantParent in helpers.go), not the scope the audit row occurred in.
func mapAuditRowToResource(ctx context.Context, row auditLogRow, accountId string, accountAPIAvailable bool, workspaceLookup map[int64]string) []affectedResource {
	mapping, ok := auditLogActions[auditActionKey{Service: row.ServiceName, Action: row.ActionName}]
	if !ok {
		return nil
	}

	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}

	var workspaceParent *v2.ResourceId
	if row.WorkspaceID != 0 {
		deploymentName, found := workspaceLookup[row.WorkspaceID]
		if !found {
			return nil
		}
		workspaceParent = &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: deploymentName}
	}

	var affected []affectedResource

	switch {
	case mapping.resourceType == workspaceResourceType:
		if workspaceParent == nil {
			return nil
		}
		affected = append(affected, affectedResource{resourceId: workspaceParent, parentResourceId: accountParent})
	case mapping.resourceType != nil:
		parent := accountParent
		if !accountAPIAvailable {
			if workspaceParent == nil {
				return nil
			}
			parent = workspaceParent
		}

		nativeId, ok := row.RequestParams[mapping.idParam]
		if !ok || nativeId == "" {
			return nil
		}

		resourceId := &v2.ResourceId{ResourceType: mapping.resourceType.Id, Resource: nativeId}
		if mapping.resourceType == groupResourceType {
			resourceId.Resource = groupResourceId(ctx, nativeId, parent)
		}

		affected = append(affected, affectedResource{resourceId: resourceId, parentResourceId: parent})
	}

	if mapping.accountRole != "" {
		affected = append(affected, affectedResource{
			resourceId:       &v2.ResourceId{ResourceType: roleResourceType.Id, Resource: roleResourceId(mapping.accountRole, accountParent)},
			parentResourceId: accountParent,
		})
	}

	if workspaceParent != nil {
		for _, roleName := range mapping.roleNames {
			affected = append(affected, affectedResource{
				resourceId:       &v2.ResourceId{ResourceType: roleResourceType.Id, Resource: roleResourceId(roleName, workspaceParent)},
				parentResourceId: workspaceParent,
			})
		}
	}

	return affected
}

// resolveSQLWorkspaces returns the workspaces available to run the audit-log SQL query
// against, without calling the Account API under token auth (unreachable there).
func resolveSQLWorkspaces(ctx context.Context, client *databricks.Client, configuredWorkspaces []string) ([]databricks.Workspace, error) {
	if client.IsTokenAuth() {
		workspaces := make([]databricks.Workspace, 0, len(configuredWorkspaces))
		for _, name := range configuredWorkspaces {
			workspaces = append(workspaces, databricks.Workspace{DeploymentName: name})
		}
		return workspaces, nil
	}

	workspaces, _, err := client.ListWorkspaces(ctx)
	if err != nil {
		return nil, err
	}

	if len(configuredWorkspaces) == 0 {
		return workspaces, nil
	}

	configured := make(map[string]struct{}, len(configuredWorkspaces))
	for _, name := range configuredWorkspaces {
		configured[name] = struct{}{}
	}

	filtered := make([]databricks.Workspace, 0, len(workspaces))
	for _, w := range workspaces {
		if _, ok := matchConfiguredWorkspace(configured, w.DeploymentName, w.Name, strconv.Itoa(w.ID)); ok {
			filtered = append(filtered, w)
		}
	}

	return filtered, nil
}

// resolveWarehouseWorkspace finds which workspace hosts warehouseId by probing each
// candidate workspace, since Databricks has no account-level lookup for this.
func resolveWarehouseWorkspace(ctx context.Context, client *databricks.Client, workspaces []databricks.Workspace, warehouseId string) (string, *v2.RateLimitDescription, error) {
	if len(workspaces) == 1 {
		return workspaces[0].DeploymentName, nil, nil
	}

	var rateLimit *v2.RateLimitDescription
	for _, w := range workspaces {
		found, rl, err := client.WarehouseExists(ctx, w.DeploymentName, warehouseId)
		if rl != nil {
			rateLimit = rl
		}
		if err != nil {
			return "", rateLimit, fmt.Errorf(
				"databricks-connector: failed to check workspace %s for sql-warehouse-id %s: %w",
				w.DeploymentName, warehouseId, err,
			)
		}
		if found {
			return w.DeploymentName, rateLimit, nil
		}
	}

	return "", rateLimit, fmt.Errorf(
		"databricks-connector: sql-warehouse-id %q was not found in any of the %d available workspaces",
		warehouseId, len(workspaces),
	)
}

func (f *auditEventFeed) queryAuditLog(ctx context.Context, workspaceId string, cursor eventPageCursor) ([]auditLogRow, *v2.RateLimitDescription, error) {
	// The (event_time, event_id) tiebreaker keeps ordering deterministic and lets us page
	// with a composite > predicate, so progress never stalls even if many rows share one
	// event_time (see advanceEventCursor); this holds as long as event_id compares consistently
	// under Databricks SQL's ">"/ORDER BY, which is true for this table's opaque IDs.
	statement := fmt.Sprintf(`
		SELECT event_id, event_time, workspace_id, action_name, service_name, request_params
		FROM system.access.audit
		WHERE event_date >= :start_date
		  AND (event_time > :start_time OR (event_time = :start_time AND event_id > :start_after_event_id))
		  AND service_name IN (%s)
		  AND action_name IN (%s)
		ORDER BY event_time ASC, event_id ASC
		LIMIT %d
	`, quotedInClause(auditLogServiceNames()), quotedInClause(auditLogActionNames()), auditLogPageLimit)

	result, rateLimit, err := f.client.ExecuteStatement(
		ctx,
		workspaceId,
		f.sqlWarehouseID,
		statement,
		databricks.StatementParameter{Name: "start_date", Value: cursor.StartAt.UTC().Format("2006-01-02"), Type: "DATE"},
		// RFC3339Nano, not RFC3339: the (event_time, event_id) tiebreaker needs start_time
		// to round-trip at the same sub-second precision parseAuditLogRows parses.
		databricks.StatementParameter{Name: "start_time", Value: cursor.StartAt.UTC().Format(time.RFC3339Nano), Type: "TIMESTAMP"},
		databricks.StatementParameter{Name: "start_after_event_id", Value: cursor.StartAfterEventID, Type: "STRING"},
	)
	if err != nil {
		return nil, rateLimit, err
	}

	rows, err := parseAuditLogRows(ctx, result)
	return rows, rateLimit, err
}

func quotedInClause(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + v + "'"
	}

	return strings.Join(quoted, ", ")
}

const (
	colEventID       = "event_id"
	colEventTime     = "event_time"
	colWorkspaceID   = "workspace_id"
	colActionName    = "action_name"
	colServiceName   = "service_name"
	colRequestParams = "request_params"
)

// parseAuditLogRows skips (and logs) any individual row that fails to parse instead of
// failing the whole page; a missing expected column is a schema problem, so that still fails.
func parseAuditLogRows(ctx context.Context, result *databricks.StatementResult) ([]auditLogRow, error) {
	colIndex := make(map[string]int, len(result.Columns))
	for i, name := range result.Columns {
		colIndex[name] = i
	}

	maxColIndex := 0
	for _, name := range []string{colEventID, colEventTime, colWorkspaceID, colActionName, colServiceName, colRequestParams} {
		idx, ok := colIndex[name]
		if !ok {
			return nil, fmt.Errorf("audit log query result missing column %q", name)
		}
		if idx > maxColIndex {
			maxColIndex = idx
		}
	}

	l := ctxzap.Extract(ctx)

	rows := make([]auditLogRow, 0, len(result.Rows))
	for _, r := range result.Rows {
		row, err := parseAuditLogRow(r, colIndex, maxColIndex)
		if err != nil {
			l.Warn("databricks-connector: skipping malformed audit log row", zap.Error(err))
			continue
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func parseAuditLogRow(r []string, colIndex map[string]int, maxColIndex int) (auditLogRow, error) {
	if len(r) <= maxColIndex {
		return auditLogRow{}, fmt.Errorf("audit log query result row has %d columns, expected at least %d", len(r), maxColIndex+1)
	}

	eventTime, err := time.Parse("2006-01-02 15:04:05.999", r[colIndex[colEventTime]])
	if err != nil {
		eventTime, err = time.Parse(time.RFC3339, r[colIndex[colEventTime]])
		if err != nil {
			return auditLogRow{}, fmt.Errorf("failed to parse event_time %q: %w", r[colIndex[colEventTime]], err)
		}
	}

	var workspaceId int64
	if v := r[colIndex[colWorkspaceID]]; v != "" {
		workspaceId, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return auditLogRow{}, fmt.Errorf("failed to parse workspace_id %q: %w", v, err)
		}
	}

	requestParams := map[string]string{}
	if v := r[colIndex[colRequestParams]]; v != "" {
		if err := json.Unmarshal([]byte(v), &requestParams); err != nil {
			return auditLogRow{}, fmt.Errorf("failed to parse request_params %q: %w", v, err)
		}
	}

	return auditLogRow{
		EventID:       r[colIndex[colEventID]],
		EventTime:     eventTime,
		WorkspaceID:   workspaceId,
		ActionName:    r[colIndex[colActionName]],
		ServiceName:   r[colIndex[colServiceName]],
		RequestParams: requestParams,
	}, nil
}
