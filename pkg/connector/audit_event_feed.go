package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

// auditLogActions maps audit log action_name values to the resources they affect.
var auditLogActions = map[string]auditActionMapping{
	"createGroup":              {resourceType: groupResourceType, idParam: "targetGroupId"},
	"addPrincipalToGroup":      {resourceType: groupResourceType, idParam: "targetGroupId"},
	"removePrincipalFromGroup": {resourceType: groupResourceType, idParam: "targetGroupId"},
	"deleteGroup":              {resourceType: groupResourceType, idParam: "targetGroupId"},
	"updateGroup": {
		resourceType: groupResourceType, idParam: "targetGroupId",
		roleNames: []string{ClusterCreateRole, InstancePoolCreateRole},
	},
	"createUser": {resourceType: userResourceType, idParam: "targetUserId"},
	"updateUser": {
		resourceType: userResourceType, idParam: "targetUserId",
		roleNames: []string{ClusterCreateRole, InstancePoolCreateRole},
	},
	"deleteUser":             {resourceType: userResourceType, idParam: "targetUserId"},
	"createServicePrincipal": {resourceType: servicePrincipalResourceType, idParam: "targetServicePrincipalId"},
	"updateServicePrincipal": {
		resourceType: servicePrincipalResourceType, idParam: "targetServicePrincipalId",
		roleNames: []string{ClusterCreateRole, InstancePoolCreateRole},
	},
	"deleteServicePrincipal":       {resourceType: servicePrincipalResourceType, idParam: "targetServicePrincipalId"},
	"changeDatabricksWorkspaceAcl": {resourceType: workspaceResourceType, roleNames: []string{WorkspaceAccessRole}},
	"changeDatabricksSqlAcl":       {roleNames: []string{SQLAccessRole}},
	"setAdmin":                     {resourceType: userResourceType, idParam: "targetUserId", accountRole: AccountAdminRole},
	"removeAdmin":                  {resourceType: userResourceType, idParam: "targetUserId", accountRole: AccountAdminRole},
}

func auditLogActionNames() []string {
	names := make([]string, 0, len(auditLogActions))
	for name := range auditLogActions {
		names = append(names, name)
	}
	return names
}

// eventPageCursor is the opaque state persisted between ListEvents calls. (StartAt,
// StartAfterEventID) form a composite boundary: unprocessed rows are those with
// event_time > StartAt, or event_time == StartAt AND event_id > StartAfterEventID. This
// keeps the boundary well-ordered even when many rows share the same event_time.
type eventPageCursor struct {
	StartAt           time.Time `json:"start_at"`
	StartAfterEventID string    `json:"start_after_event_id"`
	LatestEventSeen   time.Time `json:"latest_event_seen"`
}

func encodeEventCursor(c eventPageCursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to marshal event cursor: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// decodeEventCursor returns a zero-value cursor when missing or corrupt, so callers
// self-heal by resetting to the lookback default.
func decodeEventCursor(s string) eventPageCursor {
	if s == "" {
		return eventPageCursor{}
	}

	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return eventPageCursor{}
	}

	var c eventPageCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return eventPageCursor{}
	}

	return c
}

type auditLogRow struct {
	EventID       string
	EventTime     time.Time
	WorkspaceID   int64
	ActionName    string
	RequestParams map[string]string
}

type auditEventFeed struct {
	client                *databricks.Client
	workspaces            []string
	enableIncrementalSync bool
	sqlWarehouseID        string
	sqlWarehouseWorkspace string
}

func newAuditEventFeed(
	client *databricks.Client,
	workspaces []string,
	enableIncrementalSync bool,
	sqlWarehouseID string,
	sqlWarehouseWorkspace string,
) *auditEventFeed {
	return &auditEventFeed{
		client:                client,
		workspaces:            workspaces,
		enableIncrementalSync: enableIncrementalSync,
		sqlWarehouseID:        sqlWarehouseID,
		sqlWarehouseWorkspace: sqlWarehouseWorkspace,
	}
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

	if !f.enableIncrementalSync {
		return nil, &pagination.StreamState{}, nil, nil
	}

	cursor := decodeEventCursor(pToken.Cursor)
	now := time.Now()

	if cursor.StartAt.IsZero() {
		start := now.Add(-auditLogLookback)
		if earliestEvent != nil && earliestEvent.AsTime().After(start) {
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

	queryWorkspaceId, workspaceLookup, err := resolveQueryWorkspace(ctx, workspaces, f.sqlWarehouseWorkspace)
	if err != nil {
		return nil, nil, nil, err
	}

	rows, err := f.queryAuditLog(ctx, queryWorkspaceId, cursor)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to query audit log: %w", err)
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

	return events, &pagination.StreamState{Cursor: encoded, HasMore: hasMore}, nil, nil
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
		return eventPageCursor{StartAt: latest, StartAfterEventID: lastEventID, LatestEventSeen: latest}
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

	return eventPageCursor{StartAt: target, StartAfterEventID: startAfterEventID, LatestEventSeen: latest}
}

// affectedResource is one resource a mapped audit row's action changed.
type affectedResource struct {
	resourceId       *v2.ResourceId
	parentResourceId *v2.ResourceId
}

// mapAuditRowToResource maps an audit row to every Baton resource its action affects
// (a principal, an account role, and/or workspace roles), skipping anything unresolvable.
// The principal's parent mirrors how it's actually synced (see groupGrantParent in
// helpers.go): account when the Account API is reachable, the specific workspace
// otherwise — not whichever scope the audit row happened to occur in. Getting this wrong
// produces a resource ID that was never synced, so the real resource never gets refreshed.
func mapAuditRowToResource(ctx context.Context, row auditLogRow, accountId string, accountAPIAvailable bool, workspaceLookup map[int64]string) []affectedResource {
	mapping, ok := auditLogActions[row.ActionName]
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
// against. The Account API (ListWorkspaces) is unreachable under workspace-token auth, so
// this builds minimal workspaces from the configured deployment names instead of calling
// it, mirroring workspaceBuilder.List's token-auth branch. Those minimal workspaces have no
// numeric ID (token auth never learns one), so workspace-scoped audit rows can't be
// resolved back to a deployment name via sqlQueryWorkspace's lookup and are skipped by
// mapAuditRowToResource — a known limitation of token auth, not a regression, since
// incremental sync couldn't run under token auth at all before this.
func resolveSQLWorkspaces(ctx context.Context, client *databricks.Client, configuredWorkspaces []string) ([]databricks.Workspace, error) {
	if client.IsTokenAuth() {
		workspaces := make([]databricks.Workspace, 0, len(configuredWorkspaces))
		for _, name := range configuredWorkspaces {
			workspaces = append(workspaces, databricks.Workspace{DeploymentName: name})
		}
		return workspaces, nil
	}

	workspaces, _, err := client.ListWorkspaces(ctx)
	return workspaces, err
}

// resolveQueryWorkspace picks the workspace whose SQL warehouse runs the audit-log query,
// and builds the workspace-ID-to-deployment-name lookup used to resolve audit rows. SQL
// warehouses only exist in one workspace, so sqlWarehouseWorkspace should be set to pin the
// workspace that actually hosts sql-warehouse-id; querying the wrong workspace's endpoint
// with that ID 404s. When unset, sqlQueryWorkspace's arbitrary (alphabetically-first) pick
// is used instead, which only happens to be correct when there's a single workspace.
func resolveQueryWorkspace(ctx context.Context, workspaces []databricks.Workspace, sqlWarehouseWorkspace string) (string, map[int64]string, error) {
	queryWorkspaceId, lookup := sqlQueryWorkspace(workspaces)

	if sqlWarehouseWorkspace != "" {
		found := false
		for _, w := range workspaces {
			if strings.EqualFold(w.DeploymentName, sqlWarehouseWorkspace) {
				queryWorkspaceId = w.DeploymentName
				found = true
				break
			}
		}
		if !found {
			return "", nil, fmt.Errorf(
				"databricks-connector: sql-warehouse-workspace %q is not one of the available workspaces",
				sqlWarehouseWorkspace,
			)
		}
		return queryWorkspaceId, lookup, nil
	}

	if len(workspaces) > 1 {
		ctxzap.Extract(ctx).Debug(
			"databricks-connector: sql-warehouse-workspace is not set and more than one workspace is available, so "+
				"the workspace used to query system.access.audit was picked arbitrarily; this will fail if "+
				"sql-warehouse-id does not live in the picked workspace — set sql-warehouse-workspace to the "+
				"deployment name of the workspace that actually hosts the SQL warehouse to fix this deterministically",
			zap.String("picked_workspace", queryWorkspaceId),
			zap.Int("available_workspace_count", len(workspaces)),
		)
	}

	return queryWorkspaceId, lookup, nil
}

// sqlQueryWorkspace deterministically picks the workspace used to run the audit log query
// and builds the workspace-ID-to-deployment-name lookup used to resolve audit rows.
func sqlQueryWorkspace(workspaces []databricks.Workspace) (string, map[int64]string) {
	best := workspaces[0]
	lookup := make(map[int64]string, len(workspaces))
	for _, w := range workspaces {
		lookup[int64(w.ID)] = w.DeploymentName
		if w.DeploymentName < best.DeploymentName {
			best = w
		}
	}

	return best.DeploymentName, lookup
}

func (f *auditEventFeed) queryAuditLog(ctx context.Context, workspaceId string, cursor eventPageCursor) ([]auditLogRow, error) {
	// The (event_time, event_id) tiebreaker keeps ordering deterministic and lets us page
	// with a composite > predicate, so progress never stalls even if many rows share one
	// event_time (see advanceEventCursor).
	statement := fmt.Sprintf(`
		SELECT event_id, event_time, workspace_id, action_name, request_params
		FROM system.access.audit
		WHERE event_date >= :start_date
		  AND (event_time > :start_time OR (event_time = :start_time AND event_id > :start_after_event_id))
		  AND action_name IN (%s)
		ORDER BY event_time ASC, event_id ASC
		LIMIT %d
	`, quotedInClause(auditLogActionNames()), auditLogPageLimit)

	result, err := f.client.ExecuteStatement(
		ctx,
		workspaceId,
		f.sqlWarehouseID,
		statement,
		databricks.StatementParameter{Name: "start_date", Value: cursor.StartAt.UTC().Format("2006-01-02"), Type: "DATE"},
		databricks.StatementParameter{Name: "start_time", Value: cursor.StartAt.UTC().Format(time.RFC3339), Type: "TIMESTAMP"},
		databricks.StatementParameter{Name: "start_after_event_id", Value: cursor.StartAfterEventID, Type: "STRING"},
	)
	if err != nil {
		return nil, err
	}

	return parseAuditLogRows(result)
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
	colRequestParams = "request_params"
)

func parseAuditLogRows(result *databricks.StatementResult) ([]auditLogRow, error) {
	colIndex := make(map[string]int, len(result.Columns))
	for i, name := range result.Columns {
		colIndex[name] = i
	}

	for _, name := range []string{colEventID, colEventTime, colWorkspaceID, colActionName, colRequestParams} {
		if _, ok := colIndex[name]; !ok {
			return nil, fmt.Errorf("audit log query result missing column %q", name)
		}
	}

	rows := make([]auditLogRow, 0, len(result.Rows))
	for _, r := range result.Rows {
		eventTime, err := time.Parse("2006-01-02 15:04:05.999", r[colIndex[colEventTime]])
		if err != nil {
			eventTime, err = time.Parse(time.RFC3339, r[colIndex[colEventTime]])
			if err != nil {
				return nil, fmt.Errorf("failed to parse event_time %q: %w", r[colIndex[colEventTime]], err)
			}
		}

		var workspaceId int64
		if v := r[colIndex[colWorkspaceID]]; v != "" {
			workspaceId, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse workspace_id %q: %w", v, err)
			}
		}

		requestParams := map[string]string{}
		if v := r[colIndex[colRequestParams]]; v != "" {
			if err := json.Unmarshal([]byte(v), &requestParams); err != nil {
				return nil, fmt.Errorf("failed to parse request_params %q: %w", v, err)
			}
		}

		rows = append(rows, auditLogRow{
			EventID:       r[colIndex[colEventID]],
			EventTime:     eventTime,
			WorkspaceID:   workspaceId,
			ActionName:    r[colIndex[colActionName]],
			RequestParams: requestParams,
		})
	}

	return rows, nil
}
