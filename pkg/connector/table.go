package connector

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.ResourceSyncerV2 = (*tableBuilder)(nil)

const (
	securableTypeTable = "TABLE"

	// The canonical UPPER_SNAKE form the API answers with.
	privilegeAllPrivileges = "ALL_PRIVILEGES"
	privilegeRefresh       = "REFRESH"

	// A view and a materialized view are both securable_type "table", so
	// table_type is the only field that says which privileges apply.
	tableTypeManaged              = "MANAGED"
	tableTypeExternal             = "EXTERNAL"
	tableTypeManagedShallowClone  = "MANAGED_SHALLOW_CLONE"
	tableTypeExternalShallowClone = "EXTERNAL_SHALLOW_CLONE"
	tableTypeForeign              = "FOREIGN"
	tableTypeView                 = "VIEW"
	tableTypeMaterializedView     = "MATERIALIZED_VIEW"
	tableTypeStreamingTable       = "STREAMING_TABLE"
	tableTypeMetricView           = "METRIC_VIEW"

	profileKeyTableName = "table_name"
	profileKeyTableType = "table_type"

	// tableWriteRemedy is what a 403 on the PATCH tells the operator.
	tableWriteRemedy = "the connector must own the metastore, the parent catalog, the parent schema or the table, " +
		"or hold MANAGE on the table, the parent schema or the parent catalog"

	// tableTrustRemedy is what a refusal to report a partial privilege list
	// tells the operator.
	tableTrustRemedy = "the connector's service principal must own the metastore, the parent catalog, the parent schema or the table, " +
		"hold MANAGE on the table, the parent schema or the parent catalog, or be assigned Admin directly on the workspace whose " +
		"admins group owns the parent catalog"
)

// tablePrivileges is the set a real table takes. BROWSE, and everything that
// creates a securable, stop higher up.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
var tablePrivileges = []string{
	privilegeAllPrivileges,
	"APPLY_TAG",
	"DELETE",
	"INSERT",
	privilegeManage,
	"MODIFY",
	"READ_METADATA",
	"SELECT",
	"UPDATE",
}

// viewPrivileges matches the privileges reference, ALL_PRIVILEGES included. The
// REST grants API still answers an added ALL_PRIVILEGES on a view with a 400, so
// Grant rejects that write while Grants still reports one SQL already granted.
var viewPrivileges = []string{
	privilegeAllPrivileges,
	"APPLY_TAG",
	privilegeManage,
	"READ_METADATA",
	"SELECT",
}

// A materialized view accepts ALL_PRIVILEGES, and a permissions read answers
// with only it, not the SELECT and REFRESH it implies.
var materializedViewPrivileges = append(slices.Clone(viewPrivileges), privilegeRefresh)

// A streaming table takes REFRESH and MODIFY, not the table DML trio.
var streamingTablePrivileges = []string{
	privilegeAllPrivileges,
	"APPLY_TAG",
	privilegeManage,
	"MODIFY",
	"READ_METADATA",
	privilegeRefresh,
	"SELECT",
}

// A foreign table is read-only: its rows live in the federated source, so the
// DML trio and MODIFY cannot be granted on one.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference
var foreignTablePrivileges = slices.DeleteFunc(slices.Clone(tablePrivileges), func(privilege string) bool {
	return slices.Contains([]string{"DELETE", "INSERT", "MODIFY", "UPDATE"}, privilege)
})

// anyTablePrivileges is for a table whose variant is not knowable. REFRESH is in
// it so a held one is not dropped, and Grant still refuses to ask for it: only a
// materialized view or a streaming table accepts that write.
var anyTablePrivileges = append(slices.Clone(tablePrivileges), privilegeRefresh)

type tableBuilder struct {
	client *databricks.Client
	// clientID is the applicationId of the service principal the connector
	// authenticates as, which is what grant visibility is decided against.
	clientID string
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
	sync       syncPrincipals
}

func newTableBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *tableBuilder {
	return &tableBuilder{client: client, clientID: clientID, workspaces: workspaces, sync: sync}
}

func (b *tableBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return tableResourceType
}

// tablePermissionsName: the permissions endpoint addresses a table by its dotted
// full name. The metastore is carried by the host.
func tablePermissionsName(resourceID string) (string, error) {
	_, catalogName, schemaName, name, err := parseSchemaChildID(resourceID, "table")
	if err != nil {
		return "", err
	}

	return strings.Join([]string{catalogName, schemaName, name}, schemaNameSeparator), nil
}

func tableGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	return schemaChildGrantsAreTrustworthy(ctx, client, trust, databricks.SecurableTable, annos)
}

// tableLevel is built per table rather than declared once: table_type decides
// which privileges this one reports and accepts. See privilegesForTableType.
func tableLevel(privileges []string) securableLevel {
	return securableLevel{
		securableType:   databricks.SecurableTable,
		noun:            "table",
		privileges:      privileges,
		writeRemedy:     tableWriteRemedy,
		trustRemedy:     tableTrustRemedy,
		permissionsName: tablePermissionsName,
		trusts:          tableGrantsAreTrustworthy,
	}
}

func tableResource(
	table *databricks.Table,
	metastoreID, catalogName, schemaName string,
	parent *v2.ResourceId,
	route schemaChildRoute,
) (*v2.Resource, error) {
	fullName := strings.Join([]string{catalogName, schemaName, table.Name}, schemaNameSeparator)
	profile := schemaChildProfile(metastoreID, catalogName, schemaName, fullName, table.Owner, securableTypeTable, route)
	profile[profileKeyTableName] = table.Name
	profile[profileKeyTableType] = table.TableType

	resourceID := schemaChildResourceID(metastoreID, catalogName, schemaName, table.Name)

	// The display name is the full name rather than the leaf: main.sales.orders
	// and main.finance.orders would otherwise both read "orders" in C1.
	return rs.NewAppResource(
		fullName,
		tableResourceType,
		resourceID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: resourceID}),
	)
}

// List pages the tables, views and materialized views of one schema.
func (b *tableBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	return listSchemaChildren(ctx, b.client, b.clientID, b.workspaces, tableResourceType, "table", parentResourceID, attr,
		func(ctx context.Context, route schemaChildRoute, metastoreID, catalogName, schemaName, pageToken string) (
			[]*v2.Resource, string, *v2.RateLimitDescription, error) {
			tables, next, rateLimit, err := b.client.ListTables(ctx, route.Workspace, catalogName, schemaName, pageToken, ResourcesPageSize)
			if err != nil {
				return nil, "", rateLimit, err
			}

			var rv []*v2.Resource
			for _, table := range tables {
				if table.Name == "" {
					continue
				}

				built, err := tableResource(&table, metastoreID, catalogName, schemaName, parentResourceID, route)
				if err != nil {
					return nil, "", rateLimit, fmt.Errorf(
						"databricks-connector: failed to build the resource for table %s.%s.%s: %w", catalogName, schemaName, table.Name, err)
				}
				rv = append(rv, built)
			}

			return rv, next, rateLimit, nil
		})
}

// isViewLike reports a view or a metric view. Both reject the table DML set, and
// the grants API rejects adding ALL_PRIVILEGES on either.
func isViewLike(tableType string) bool {
	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case tableTypeView, tableTypeMetricView:
		return true
	default:
		return false
	}
}

// refreshIsRequestable reports the table types whose grants API accepts REFRESH.
// The privileges reference lists it on a materialized view, and a streaming table
// takes it too. On every other type, including one this connector does not name,
// a held REFRESH is still reported and the grant is not offered.
func refreshIsRequestable(tableType string) bool {
	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case tableTypeMaterializedView, tableTypeStreamingTable:
		return true
	default:
		return false
	}
}

// privilegesForTableType returns the set for a known table_type. MANAGED,
// EXTERNAL and both shallow clones stay on the table set. A foreign table is
// read-only. Anything else, including a type Databricks adds later, gets the
// union, so a privilege already held is reported rather than dropped.
func privilegesForTableType(tableType string) []string {
	if isViewLike(tableType) {
		return viewPrivileges
	}

	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case tableTypeMaterializedView:
		return materializedViewPrivileges
	case tableTypeStreamingTable:
		return streamingTablePrivileges
	case tableTypeManaged, tableTypeExternal, tableTypeManagedShallowClone, tableTypeExternalShallowClone:
		return tablePrivileges
	case tableTypeForeign:
		return foreignTablePrivileges
	default:
		return anyTablePrivileges
	}
}

// privilegesForTable is the set this table reports. A missing table_type falls
// back to the union rather than to one variant.
func privilegesForTable(ctx context.Context, resource *v2.Resource) []string {
	variant, _ := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyTableType)
	if variant == "" {
		ctxzap.Extract(ctx).Debug("databricks-connector: table has no table_type; reporting every variant's privileges",
			zap.String("table", resource.GetId().GetResource()))

		return anyTablePrivileges
	}

	return privilegesForTableType(variant)
}

// Entitlements are per table, not static: the privilege set follows table_type,
// and a privilege the grants API refuses must not be offered as requestable.
// ConductorOne would otherwise advertise requests that always fail.
func (b *tableBuilder) Entitlements(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	variant, _ := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyTableType)
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)

	privileges := privilegesForTable(ctx, resource)
	rv := make([]*v2.Entitlement, 0, len(privileges)+1)
	for _, privilege := range privileges {
		options := []ent.EntitlementOption{
			ent.WithDisplayName(fmt.Sprintf("%s %s", resource.GetDisplayName(), privilege)),
			ent.WithDescription(fmt.Sprintf("%s privilege on table %s in Databricks", privilege, resource.GetDisplayName())),
		}
		// ALL_PRIVILEGES on a view, and REFRESH on anything other than a
		// materialized view or a streaming table, are reported when a principal
		// already holds them. The grants API rejects adding either, so neither is
		// requestable.
		reportOnly := (privilege == privilegeAllPrivileges && isViewLike(variant)) ||
			(privilege == privilegeRefresh && !refreshIsRequestable(variant))
		if !reportOnly {
			options = append([]ent.EntitlementOption{grantable}, options...)
		}
		rv = append(rv, ent.NewPermissionEntitlement(resource, privilege, options...))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(resource, ownerEntitlement,
		ent.WithDisplayName(fmt.Sprintf("%s owner", resource.GetDisplayName())),
		ent.WithDescription(fmt.Sprintf("Owns table %s in Databricks", resource.GetDisplayName())),
	))

	return rv, nil, nil
}

func (b *tableBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return syncSecurableGrants(ctx, b.client, b.clientID, b.sync, tableLevel(privilegesForTable(ctx, resource)), resource, attr)
}

func (b *tableBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	resource := entitlement.GetResource()
	if err := refuseUngrantableTablePrivilege(resource, entitlement); err != nil {
		return nil, err
	}

	return grantSecurable(ctx, b.client, b.clientID, tableLevel(privilegesForTable(ctx, resource)), principal, entitlement)
}

// refuseUngrantableTablePrivilege rejects the two privileges a table reports but
// the grants API refuses to add, before any PATCH goes out. Revoke is not held to
// this: a privilege SQL granted still has to be removable.
func refuseUngrantableTablePrivilege(resource *v2.Resource, entitlement *v2.Entitlement) error {
	privilege, err := parseEntitlementPrivilege(entitlement.GetId())
	if err != nil {
		return err
	}

	variant, _ := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyTableType)
	normalized := strings.ToUpper(strings.TrimSpace(variant))
	switch {
	case privilege == privilegeAllPrivileges && isViewLike(variant):
		return status.Errorf(codes.InvalidArgument,
			"databricks-connector: the grants API rejects ALL_PRIVILEGES on a %s; grant SELECT or MANAGE instead", normalized)
	case privilege == privilegeRefresh && !refreshIsRequestable(variant):
		return status.Errorf(codes.InvalidArgument,
			"databricks-connector: REFRESH can be granted on a materialized view or a streaming table, not on table type %q", normalized)
	}

	return nil
}

func (b *tableBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	resource := revoked.GetEntitlement().GetResource()

	return revokeSecurable(ctx, b.client, b.clientID, tableLevel(privilegesForTable(ctx, resource)), revoked)
}
