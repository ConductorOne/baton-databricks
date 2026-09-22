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
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
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
// account-scoped role, and/or optional workspace-scoped roles/entitlements. grant, when set,
// means the row is an atomic entitlement assignment/revocation rather than a generic change.
type auditActionMapping struct {
	resourceType *v2.ResourceType
	idParam      string
	accountRole  string
	roleNames    []string
	grant        *auditGrantMapping
}

// auditGrantMapping names the principal and direction of a grant-mapped action. The
// entitlement's resource is resourceType/idParam on the enclosing auditActionMapping.
type auditGrantMapping struct {
	entitlement    string
	principalType  *v2.ResourceType
	principalParam string
	revoke         bool
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
	{auditServiceAccounts, "createGroup"}: {resourceType: groupResourceType, idParam: "targetGroupId"},
	// Databricks documents targetUserId as the added/removed member's id on all four of these
	// actions, singular and plural alike: https://docs.databricks.com/aws/en/admin/account-settings/audit-logs.
	{auditServiceAccounts, "addPrincipalToGroup"}: {
		resourceType: groupResourceType, idParam: "targetGroupId",
		grant: &auditGrantMapping{entitlement: groupMemberEntitlement, principalType: userResourceType, principalParam: "targetUserId"},
	},
	{auditServiceAccounts, "removePrincipalFromGroup"}: {
		resourceType: groupResourceType, idParam: "targetGroupId",
		grant: &auditGrantMapping{entitlement: groupMemberEntitlement, principalType: userResourceType, principalParam: "targetUserId", revoke: true},
	},
	{auditServiceAccounts, "addPrincipalsToGroup"}: {
		resourceType: groupResourceType, idParam: "targetGroupId",
		grant: &auditGrantMapping{entitlement: groupMemberEntitlement, principalType: userResourceType, principalParam: "targetUserId"},
	},
	{auditServiceAccounts, "removePrincipalsFromGroup"}: {
		resourceType: groupResourceType, idParam: "targetGroupId",
		grant: &auditGrantMapping{entitlement: groupMemberEntitlement, principalType: userResourceType, principalParam: "targetUserId", revoke: true},
	},
	{auditServiceAccounts, "removeGroup"}: {resourceType: groupResourceType, idParam: "targetGroupId"},
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

	// queryWorkspaceID caches which workspace hosts sqlWarehouseID across polls. It's a discovered value, not provided by configs.
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
// once via resolveWarehouseWorkspace rather than probing on every poll. allWorkspaces
// must be every workspace in the account, not filtered by the --workspaces allowlist:
// the warehouse can live in a workspace outside that scope.
func (f *auditEventFeed) resolveQueryWorkspaceID(ctx context.Context, allWorkspaces []databricks.Workspace) (string, *v2.RateLimitDescription, error) {
	f.queryWorkspaceMu.Lock()
	defer f.queryWorkspaceMu.Unlock()

	if f.queryWorkspaceID != "" {
		return f.queryWorkspaceID, nil, nil
	}

	id, rateLimit, err := resolveWarehouseWorkspace(ctx, f.client, allWorkspaces, f.sqlWarehouseID)
	if err != nil {
		return "", rateLimit, err
	}

	f.queryWorkspaceID = id
	return id, rateLimit, nil
}

// EventFeedMetadata is registered unconditionally; enable-incremental-sync gates behavior
// inside ListEvents instead, to avoid confusing "feed not found" errors when it's off.
func (f *auditEventFeed) EventFeedMetadata(_ context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: auditEventFeedId,
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_RESOURCE_CHANGE,
			v2.EventType_EVENT_TYPE_CREATE_GRANT,
			v2.EventType_EVENT_TYPE_CREATE_REVOKE,
		},
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

	// Rows are only ever fetched up to lagCutoff (see queryAuditLog), so slow-indexing rows
	// have auditLogTrailingLag to appear before we consider that window done. If the cursor
	// is already past it (e.g. right after bootstrap, where the lookback is narrower than the
	// lag), there's nothing to query yet.
	lagCutoff := now.Add(-auditLogTrailingLag)
	if !cursor.StartAt.Before(lagCutoff) {
		encoded, err := encodeEventCursor(cursor)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("databricks-connector: failed to encode event cursor: %w", err)
		}
		return nil, &pagination.StreamState{Cursor: encoded, HasMore: false}, annos, nil
	}

	// queries all the available workspaces to locate the one that contains the query warehouse.
	allWorkspaces, _, err := f.client.ListWorkspaces(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}
	scopedWorkspaces := filterConfiguredWorkspaces(allWorkspaces, f.workspaces)
	if len(scopedWorkspaces) == 0 {
		return nil, nil, nil, fmt.Errorf("databricks-connector: no workspace available to query system.access.audit")
	}

	workspaceLookup := make(map[int64]string, len(scopedWorkspaces))
	for _, w := range scopedWorkspaces {
		workspaceLookup[int64(w.ID)] = w.DeploymentName
	}

	queryWorkspaceId, rateLimit, err := f.resolveQueryWorkspaceID(ctx, allWorkspaces)
	if rateLimit != nil {
		annos.WithRateLimiting(rateLimit)
	}
	if err != nil {
		return nil, nil, annos, err
	}

	rows, rawRowCount, rateLimit, err := f.queryAuditLog(ctx, queryWorkspaceId, cursor, lagCutoff)
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
			event := &v2.Event{
				Id:         fmt.Sprintf("%s/%d", row.EventID, i),
				OccurredAt: timestamppb.New(row.EventTime),
			}
			switch {
			case a.grant != nil && a.grant.revoke:
				event.Event = &v2.Event_CreateRevokeEvent{
					CreateRevokeEvent: &v2.CreateRevokeEvent{Entitlement: a.grant.entitlement, Principal: a.grant.principal},
				}
			case a.grant != nil:
				event.Event = &v2.Event_CreateGrantEvent{
					CreateGrantEvent: &v2.CreateGrantEvent{Entitlement: a.grant.entitlement, Principal: a.grant.principal},
				}
			default:
				event.Event = &v2.Event_ResourceChangeEvent{
					ResourceChangeEvent: &v2.ResourceChangeEvent{ResourceId: a.resourceId, ParentResourceId: a.parentResourceId},
				}
			}
			events = append(events, event)
		}
	}

	hasMore := rawRowCount >= auditLogPageLimit
	nextCursor := advanceEventCursor(rows, hasMore, lagCutoff)

	encoded, err := encodeEventCursor(nextCursor)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: failed to encode event cursor: %w", err)
	}

	return events, &pagination.StreamState{Cursor: encoded, HasMore: hasMore}, annos, nil
}

// advanceEventCursor advances to the last row processed while more of the page remains, or
// to lagCutoff once drained: rows are already bounded by lagCutoff (see queryAuditLog), so a
// drained page proves nothing else exists up to that point.
func advanceEventCursor(rows []auditLogRow, hasMore bool, lagCutoff time.Time) eventPageCursor {
	if hasMore {
		last := rows[len(rows)-1]
		return eventPageCursor{StartAt: last.EventTime, StartAfterEventID: last.EventID}
	}
	return eventPageCursor{StartAt: lagCutoff}
}

// affectedResource is either one resource a mapped audit row's action changed (RESOURCE_CHANGE),
// or, when grant is set, the entitlement/principal it assigned or revoked (CREATE_GRANT/CREATE_REVOKE).
type affectedResource struct {
	resourceId       *v2.ResourceId
	parentResourceId *v2.ResourceId
	grant            *affectedGrant
}

type affectedGrant struct {
	entitlement *v2.Entitlement
	principal   *v2.Resource
	revoke      bool
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

		if mapping.grant != nil {
			g, ok := buildAffectedGrant(row, mapping.grant, resourceId, parent)
			if !ok {
				return nil
			}
			return []affectedResource{{grant: g}}
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

// buildAffectedGrant builds the entitlement (on entitlementResourceId/parent) and principal a
// grant-mapped row names, skipping rows missing the principal's native id.
func buildAffectedGrant(row auditLogRow, gm *auditGrantMapping, entitlementResourceId, parent *v2.ResourceId) (*affectedGrant, bool) {
	principalNativeId, ok := row.RequestParams[gm.principalParam]
	if !ok || principalNativeId == "" {
		return nil, false
	}

	entitlementResource := &v2.Resource{Id: entitlementResourceId, ParentResourceId: parent}
	return &affectedGrant{
		entitlement: ent.NewAssignmentEntitlement(entitlementResource, gm.entitlement),
		principal:   &v2.Resource{Id: &v2.ResourceId{ResourceType: gm.principalType.Id, Resource: principalNativeId}},
		revoke:      gm.revoke,
	}, true
}

// filterConfiguredWorkspaces narrows workspaces to configuredWorkspaces (the --workspaces
// allowlist), or returns workspaces unchanged when the allowlist is empty. This scopes
// which workspaces' audit rows get resolved to resources — it must NOT be applied before
// locating the query warehouse (resolveWarehouseWorkspace), which can live in any workspace
// in the account regardless of this allowlist.
func filterConfiguredWorkspaces(workspaces []databricks.Workspace, configuredWorkspaces []string) []databricks.Workspace {
	if len(configuredWorkspaces) == 0 {
		return workspaces
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

	return filtered
}

// resolveWarehouseWorkspace finds which workspace hosts warehouseId by probing each
// candidate workspace, since Databricks has no account-level lookup for this.
func resolveWarehouseWorkspace(ctx context.Context, client *databricks.Client, workspaces []databricks.Workspace, warehouseId string) (string, *v2.RateLimitDescription, error) {
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

	if len(workspaces) == 1 {
		return "", rateLimit, fmt.Errorf(
			"databricks-connector: sql-warehouse-id %q was not found in workspace %s",
			warehouseId, workspaces[0].DeploymentName,
		)
	}
	return "", rateLimit, fmt.Errorf(
		"databricks-connector: sql-warehouse-id %q was not found in any of the %d available workspaces",
		warehouseId, len(workspaces),
	)
}

// queryAuditLog returns rows in (cursor, lagCutoff], plus the raw row count (before
// parseAuditLogRows may drop malformed ones) so callers can tell if the page was full.
func (f *auditEventFeed) queryAuditLog(ctx context.Context, workspaceId string, cursor eventPageCursor, lagCutoff time.Time) ([]auditLogRow, int, *v2.RateLimitDescription, error) {
	// The (event_time, event_id) tiebreaker keeps ordering deterministic and lets us page
	// with a composite > predicate, so progress never stalls even if many rows share one
	// event_time (see advanceEventCursor); this holds as long as event_id compares consistently
	// under Databricks SQL's ">"/ORDER BY, which is true for this table's opaque IDs.
	statement := fmt.Sprintf(`
		SELECT event_id, event_time, workspace_id, action_name, service_name, request_params
		FROM system.access.audit
		WHERE event_date >= :start_date
		  AND (event_time > :start_time OR (event_time = :start_time AND event_id > :start_after_event_id))
		  AND event_time <= :lag_cutoff
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
		databricks.StatementParameter{Name: "lag_cutoff", Value: lagCutoff.UTC().Format(time.RFC3339Nano), Type: "TIMESTAMP"},
	)
	if err != nil {
		return nil, 0, rateLimit, err
	}

	rows, err := parseAuditLogRows(ctx, result)
	return rows, len(result.Rows), rateLimit, err
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
