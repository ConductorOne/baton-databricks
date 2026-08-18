package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
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

	// Databricks audit log delivery can lag up to 24h, so the first poll looks back that far.
	auditLogLookback = 24 * time.Hour

	// Trail the watermark by this much instead of the newest event seen, since slower-indexing
	// areas of the audited system could otherwise have events skipped permanently.
	auditLogTrailingLag = 4 * time.Hour

	auditLogPageLimit = 1000
)

// auditLogActions maps audit log action_name values to the resource type they affect and
// the request_params key holding the native resource ID. Not yet verified against a live workspace.
var auditLogActions = map[string]struct {
	resourceType *v2.ResourceType
	idParam      string
}{
	"createGroup":                  {groupResourceType, "targetGroupId"},
	"addPrincipalToGroup":          {groupResourceType, "targetGroupId"},
	"removePrincipalFromGroup":     {groupResourceType, "targetGroupId"},
	"deleteGroup":                  {groupResourceType, "targetGroupId"},
	"createUser":                   {userResourceType, "targetUserId"},
	"updateUser":                   {userResourceType, "targetUserId"},
	"deleteUser":                   {userResourceType, "targetUserId"},
	"createServicePrincipal":       {servicePrincipalResourceType, "targetServicePrincipalId"},
	"updateServicePrincipal":       {servicePrincipalResourceType, "targetServicePrincipalId"},
	"deleteServicePrincipal":       {servicePrincipalResourceType, "targetServicePrincipalId"},
	"changeDatabricksWorkspaceAcl": {workspaceResourceType, ""},
}

func auditLogActionNames() []string {
	names := make([]string, 0, len(auditLogActions))
	for name := range auditLogActions {
		names = append(names, name)
	}
	return names
}

// eventPageCursor is the opaque state persisted between ListEvents calls. StartAt only
// ever advances forward, and LastEventIDs dedupes rows tied exactly on that boundary.
type eventPageCursor struct {
	StartAt         time.Time `json:"start_at"`
	LatestEventSeen time.Time `json:"latest_event_seen"`
	LastEventIDs    []string  `json:"last_event_ids"`
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
	enableIncrementalSync bool
	sqlWarehouseID        string
}

func newAuditEventFeed(client *databricks.Client, enableIncrementalSync bool, sqlWarehouseID string) *auditEventFeed {
	return &auditEventFeed{
		client:                client,
		enableIncrementalSync: enableIncrementalSync,
		sqlWarehouseID:        sqlWarehouseID,
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

	workspaces, _, err := f.client.ListWorkspaces(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}
	if len(workspaces) == 0 {
		return nil, nil, nil, fmt.Errorf("databricks-connector: no workspace available to query system.access.audit")
	}

	queryWorkspaceId, workspaceLookup := sqlQueryWorkspace(workspaces)

	rows, err := f.queryAuditLog(ctx, queryWorkspaceId, cursor)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to query audit log: %w", err)
	}

	seen := make(map[string]struct{}, len(cursor.LastEventIDs))
	for _, id := range cursor.LastEventIDs {
		seen[id] = struct{}{}
	}

	var events []*v2.Event
	for _, row := range rows {
		if _, ok := seen[row.EventID]; ok {
			continue
		}

		resourceId, parentResourceId, ok := mapAuditRowToResource(row, f.client.GetAccountId(), workspaceLookup)
		if !ok {
			l.Debug("databricks-connector: skipping audit row with no resource mapping",
				zap.String("action_name", row.ActionName),
				zap.String("event_id", row.EventID),
			)
			continue
		}

		events = append(events, &v2.Event{
			Id:         row.EventID,
			OccurredAt: timestamppb.New(row.EventTime),
			Event: &v2.Event_ResourceChangeEvent{
				ResourceChangeEvent: &v2.ResourceChangeEvent{
					ResourceId:       resourceId,
					ParentResourceId: parentResourceId,
				},
			},
		})
	}

	hasMore := len(rows) >= auditLogPageLimit
	nextCursor := advanceEventCursor(cursor, rows, hasMore, now)

	encoded, err := encodeEventCursor(nextCursor)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to encode event cursor: %w", err)
	}

	return events, &pagination.StreamState{Cursor: encoded, HasMore: hasMore}, nil, nil
}

// advanceEventCursor advances only to the last row processed while a page is full, and
// once drained, trails the newest event seen (or wall-clock time if empty) by auditLogTrailingLag.
func advanceEventCursor(cursor eventPageCursor, rows []auditLogRow, hasMore bool, now time.Time) eventPageCursor {
	latest := cursor.StartAt
	var latestIDs []string
	for _, row := range rows {
		switch {
		case row.EventTime.After(latest):
			latest = row.EventTime
			latestIDs = []string{row.EventID}
		case row.EventTime.Equal(latest):
			latestIDs = append(latestIDs, row.EventID)
		}
	}

	if hasMore {
		return eventPageCursor{StartAt: latest, LatestEventSeen: latest, LastEventIDs: latestIDs}
	}

	target := latest.Add(-auditLogTrailingLag)
	if len(rows) == 0 {
		target = now.Add(-auditLogTrailingLag)
	}
	if target.Before(cursor.StartAt) {
		target = cursor.StartAt
	}

	var idsAtTarget []string
	if target.Equal(latest) {
		idsAtTarget = latestIDs
	}

	return eventPageCursor{StartAt: target, LatestEventSeen: latest, LastEventIDs: idsAtTarget}
}

// mapAuditRowToResource maps an audit row to the Baton resource it affects, returning
// ok=false if the action isn't tracked or the ID/workspace can't be resolved.
func mapAuditRowToResource(row auditLogRow, accountId string, workspaceLookup map[int64]string) (*v2.ResourceId, *v2.ResourceId, bool) {
	mapping, ok := auditLogActions[row.ActionName]
	if !ok {
		return nil, nil, false
	}

	accountParent := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: accountId}

	if mapping.resourceType == workspaceResourceType {
		deploymentName, found := workspaceLookup[row.WorkspaceID]
		if !found {
			return nil, nil, false
		}
		return &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: deploymentName}, accountParent, true
	}

	parentResourceId := accountParent
	if row.WorkspaceID != 0 {
		deploymentName, found := workspaceLookup[row.WorkspaceID]
		if !found {
			return nil, nil, false
		}
		parentResourceId = &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: deploymentName}
	}

	nativeId, ok := row.RequestParams[mapping.idParam]
	if !ok || nativeId == "" {
		return nil, nil, false
	}

	if mapping.resourceType == groupResourceType {
		return &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: groupResourceId(context.Background(), nativeId, parentResourceId)}, parentResourceId, true
	}

	return &v2.ResourceId{ResourceType: mapping.resourceType.Id, Resource: nativeId}, parentResourceId, true
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
	statement := fmt.Sprintf(`
		SELECT event_id, event_time, workspace_id, action_name, request_params
		FROM system.access.audit
		WHERE event_date >= :start_date
		  AND event_time >= :start_time
		  AND action_name IN (%s)
		ORDER BY event_time ASC
		LIMIT %d
	`, quotedInClause(auditLogActionNames()), auditLogPageLimit)

	result, err := f.client.ExecuteStatement(
		ctx,
		workspaceId,
		f.sqlWarehouseID,
		statement,
		databricks.StatementParameter{Name: "start_date", Value: cursor.StartAt.Format("2006-01-02"), Type: "DATE"},
		databricks.StatementParameter{Name: "start_time", Value: cursor.StartAt.Format(time.RFC3339), Type: "TIMESTAMP"},
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

	out := ""
	for i, v := range quoted {
		if i > 0 {
			out += ", "
		}
		out += v
	}
	return out
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
