package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// securableIDSep separates the workspace deployment name from the Unity Catalog
// dotted full name inside a resource ID. UC full names use dots, so a distinct
// separator keeps the two parts unambiguous. Child List() calls only receive the
// parent's ResourceId, so everything needed to enumerate children (workspace +
// full name) must be encoded here.
const securableIDSep = "::"

// ownerEntitlement is a read-only entitlement marking the Unity Catalog owner of
// a securable. Ownership carries implicit full control that the permissions API
// does not report, so it is surfaced explicitly for access reviews. It is not
// provisionable through the permissions API.
const ownerEntitlement = "owner"

// Unity Catalog privileges per securable level. Supersets of what the
// permissions API may report so every observed grant maps to an entitlement.
var (
	catalogPrivileges = []string{
		"ALL_PRIVILEGES", "USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE",
		"CREATE_FUNCTION", "CREATE_MODEL", "CREATE_VOLUME", "CREATE_MATERIALIZED_VIEW",
		"MODIFY", "SELECT", "EXECUTE", "READ_VOLUME", "WRITE_VOLUME", "REFRESH",
		"APPLY_TAG", "BROWSE", "MANAGE",
	}
	schemaPrivileges = []string{
		"ALL_PRIVILEGES", "USE_SCHEMA", "CREATE_TABLE", "CREATE_FUNCTION", "CREATE_MODEL",
		"CREATE_VOLUME", "CREATE_MATERIALIZED_VIEW", "MODIFY", "SELECT", "EXECUTE",
		"READ_VOLUME", "WRITE_VOLUME", "REFRESH", "APPLY_TAG", "BROWSE", "MANAGE",
	}
	tablePrivileges = []string{
		"ALL_PRIVILEGES", "SELECT", "MODIFY", "APPLY_TAG", "BROWSE", "MANAGE",
	}
)

func makeSecurableID(workspaceId, fullName string) string {
	return workspaceId + securableIDSep + fullName
}

// splitSecurableID reverses makeSecurableID into (workspaceId, fullName).
func splitSecurableID(id string) (string, string) {
	workspaceId, fullName, found := strings.Cut(id, securableIDSep)
	if !found {
		return "", id
	}
	return workspaceId, fullName
}

func securableProfile(workspaceId, fullName, owner string) map[string]interface{} {
	return map[string]interface{}{
		"workspace_id": workspaceId,
		"full_name":    fullName,
		"owner":        owner,
	}
}

// securablePrivilegeEntitlements builds one permission entitlement per privilege
// plus the read-only owner entitlement.
func securablePrivilegeEntitlements(resource *v2.Resource, privileges []string) []*v2.Entitlement {
	rv := make([]*v2.Entitlement, 0, len(privileges)+1)
	for _, priv := range privileges {
		rv = append(rv, ent.NewPermissionEntitlement(
			resource,
			priv,
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
			ent.WithDisplayName(fmt.Sprintf("%s %s", resource.DisplayName, priv)),
			ent.WithDescription(fmt.Sprintf("%s privilege on %s", priv, resource.DisplayName)),
		))
	}
	rv = append(rv, ent.NewPermissionEntitlement(
		resource,
		ownerEntitlement,
		ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s owner", resource.DisplayName)),
		ent.WithDescription(fmt.Sprintf("Owner of %s", resource.DisplayName)),
	))
	return rv
}

// resolvePrincipalResourceID resolves a Unity Catalog principal string (a user
// name/email, a group display name, or a service principal application ID) to a
// connector resource ID. Returns nil when the principal cannot be matched to any
// known identity so the caller can skip it.
func resolvePrincipalResourceID(ctx context.Context, c *databricks.Client, workspaceId, principal string) (*v2.ResourceId, error) {
	if userID, _, err := c.FindUserID(ctx, workspaceId, principal); err == nil && userID != "" {
		return &v2.ResourceId{ResourceType: userResourceType.Id, Resource: userID}, nil
	} else if err != nil {
		return nil, err
	}

	if groupID, _, err := c.FindGroupID(ctx, workspaceId, principal); err == nil && groupID != "" {
		return &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: groupID}, nil
	} else if err != nil {
		return nil, err
	}

	if spID, _, err := c.FindServicePrincipalID(ctx, workspaceId, principal); err == nil && spID != "" {
		return &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: spID}, nil
	} else if err != nil {
		return nil, err
	}

	return nil, nil
}

// resolvePrincipalName converts a connector principal resource ID back into the
// Unity Catalog principal string expected by the permissions API.
func resolvePrincipalName(ctx context.Context, c *databricks.Client, workspaceId string, principal *v2.ResourceId) (string, error) {
	switch principal.ResourceType {
	case userResourceType.Id:
		name, _, err := c.FindUsername(ctx, workspaceId, principal.Resource)
		return name, err
	case groupResourceType.Id:
		name, _, err := c.FindGroupDisplayName(ctx, workspaceId, principal.Resource)
		return name, err
	case servicePrincipalResourceType.Id:
		name, _, err := c.FindServicePrincipalAppID(ctx, workspaceId, principal.Resource)
		return name, err
	default:
		return "", fmt.Errorf("databricks-connector: unsupported principal type: %s", principal.ResourceType)
	}
}

// securableGrants lists the direct privilege assignments on a securable and
// converts them into grants, expanding group principals and adding the owner.
func securableGrants(ctx context.Context, c *databricks.Client, resource *v2.Resource, securableType string) ([]*v2.Grant, error) {
	l := ctxzap.Extract(ctx)
	workspaceId, fullName := splitSecurableID(resource.Id.Resource)

	assignments, _, err := c.ListPermissions(ctx, workspaceId, securableType, fullName)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list %s permissions for %q: %w", securableType, fullName, err)
	}

	var rv []*v2.Grant
	for _, a := range assignments {
		principalID, err := resolvePrincipalResourceID(ctx, c, workspaceId, a.Principal)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to resolve principal %q: %w", a.Principal, err)
		}
		if principalID == nil {
			l.Warn("databricks-connector: skipping unresolved unity catalog principal",
				zap.String("principal", a.Principal),
				zap.String("securable", fullName),
			)
			continue
		}

		for _, priv := range a.Privileges {
			g, err := securableGrant(ctx, c, resource, workspaceId, priv, principalID)
			if err != nil {
				return nil, err
			}
			rv = append(rv, g)
		}
	}

	// Owners hold implicit full control the permissions API omits.
	if owner, ok := rs.GetProfileStringValue(rs.GetProfile(resource), "owner"); ok && owner != "" {
		principalID, err := resolvePrincipalResourceID(ctx, c, workspaceId, owner)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to resolve owner %q: %w", owner, err)
		}
		if principalID == nil {
			l.Warn("databricks-connector: skipping unresolved unity catalog owner",
				zap.String("owner", owner),
				zap.String("securable", fullName),
			)
			return rv, nil
		}

		g, err := securableGrant(ctx, c, resource, workspaceId, ownerEntitlement, principalID)
		if err != nil {
			return nil, err
		}
		rv = append(rv, g)
	}

	return rv, nil
}

// securableGrant builds a single grant, adding group-membership expansion when
// the principal is a group so reviews see the effective members.
func securableGrant(ctx context.Context, c *databricks.Client, resource *v2.Resource, workspaceId, entitlement string, principalID *v2.ResourceId) (*v2.Grant, error) {
	if principalID.ResourceType != groupResourceType.Id {
		return grant.NewGrant(resource, entitlement, principalID), nil
	}

	groupParent, err := groupGrantParent(c.IsAccountAPIAvailable(), c.GetAccountId(), workspaceId)
	if err != nil {
		return nil, err
	}
	expandedID, expandAnnotation, err := groupGrantExpansion(ctx, principalID.Resource, groupParent)
	if err != nil {
		return nil, err
	}
	return grant.NewGrant(resource, entitlement, expandedID, grant.WithAnnotation(expandAnnotation)), nil
}

// securableGrantChange applies a single add/remove privilege change on a securable.
func securableGrantChange(ctx context.Context, c *databricks.Client, entitlement *v2.Entitlement, principal *v2.Resource, add bool) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if !isValidPrincipal(principal.Id) {
		l.Warn("databricks-connector: only users, groups and service principals can hold unity catalog privileges",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)
		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can hold unity catalog privileges")
	}

	privilege := entitlement.Slug
	if privilege == ownerEntitlement {
		return nil, fmt.Errorf("databricks-connector: unity catalog ownership cannot be provisioned via the permissions API")
	}

	workspaceId, fullName := splitSecurableID(entitlement.Resource.Id.Resource)
	securableType, ok := rs.GetProfileStringValue(rs.GetProfile(entitlement.Resource), "securable_type")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: securable type not found on entitlement resource")
	}

	principalName, err := resolvePrincipalName(ctx, c, workspaceId, principal.Id)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to resolve principal name: %w", err)
	}
	if principalName == "" {
		return nil, fmt.Errorf("databricks-connector: could not resolve principal %s to a unity catalog principal", principal.Id.String())
	}

	change := databricks.PermissionsChange{Principal: principalName}
	if add {
		change.Add = []string{privilege}
	} else {
		change.Remove = []string{privilege}
	}

	if _, err := c.UpdatePermissions(ctx, workspaceId, securableType, fullName, []databricks.PermissionsChange{change}); err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update %s permissions for %q: %w", securableType, fullName, err)
	}

	return nil, nil
}

// ---- Catalog builder ----

type catalogBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (b *catalogBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return catalogResourceType
}

func catalogResource(workspaceId string, catalog *databricks.Catalog, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := securableProfile(workspaceId, catalog.Name, catalog.Owner)
	profile["securable_type"] = databricks.SecurableCatalog

	return rs.NewResource(
		catalog.Name,
		catalogResourceType,
		makeSecurableID(workspaceId, catalog.Name),
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: schemaResourceType.Id}),
	)
}

func (b *catalogBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil || parentResourceID.ResourceType != workspaceResourceType.Id {
		return nil, nil, nil
	}
	workspaceId := parentResourceID.Resource

	bag, pageToken, err := parseCursorToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: catalogResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	catalogs, nextToken, _, err := b.client.ListCatalogs(ctx, workspaceId, pageToken, ResourcesPageSize)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list catalogs: %w", err)
	}

	var rv []*v2.Resource
	for _, catalog := range catalogs {
		cCopy := catalog
		cr, err := catalogResource(workspaceId, &cCopy, parentResourceID)
		if err != nil {
			return nil, nil, err
		}
		rv = append(rv, cr)
	}

	nextPage, err := bag.NextToken(nextToken)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func (b *catalogBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return securablePrivilegeEntitlements(resource, catalogPrivileges), nil, nil
}

func (b *catalogBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	grants, err := securableGrants(ctx, b.client, resource, databricks.SecurableCatalog)
	if err != nil {
		return nil, nil, err
	}
	return grants, nil, nil
}

func (b *catalogBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.client, entitlement, principal, true)
}

func (b *catalogBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.client, grant.Entitlement, grant.Principal, false)
}

func newCatalogBuilder(client *databricks.Client) *catalogBuilder {
	return &catalogBuilder{client: client, resourceType: catalogResourceType}
}

// ---- Schema builder ----

type schemaBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (b *schemaBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return schemaResourceType
}

func schemaResource(workspaceId string, schema *databricks.Schema, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := securableProfile(workspaceId, schema.FullName, schema.Owner)
	profile["securable_type"] = databricks.SecurableSchema

	return rs.NewResource(
		schema.Name,
		schemaResourceType,
		makeSecurableID(workspaceId, schema.FullName),
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: tableResourceType.Id}),
	)
}

func (b *schemaBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil || parentResourceID.ResourceType != catalogResourceType.Id {
		return nil, nil, nil
	}
	workspaceId, catalogName := splitSecurableID(parentResourceID.Resource)

	bag, pageToken, err := parseCursorToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: schemaResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	schemas, nextToken, _, err := b.client.ListSchemas(ctx, workspaceId, catalogName, pageToken, ResourcesPageSize)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list schemas: %w", err)
	}

	var rv []*v2.Resource
	for _, schema := range schemas {
		sCopy := schema
		sr, err := schemaResource(workspaceId, &sCopy, parentResourceID)
		if err != nil {
			return nil, nil, err
		}
		rv = append(rv, sr)
	}

	nextPage, err := bag.NextToken(nextToken)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func (b *schemaBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return securablePrivilegeEntitlements(resource, schemaPrivileges), nil, nil
}

func (b *schemaBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	grants, err := securableGrants(ctx, b.client, resource, databricks.SecurableSchema)
	if err != nil {
		return nil, nil, err
	}
	return grants, nil, nil
}

func (b *schemaBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.client, entitlement, principal, true)
}

func (b *schemaBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.client, grant.Entitlement, grant.Principal, false)
}

func newSchemaBuilder(client *databricks.Client) *schemaBuilder {
	return &schemaBuilder{client: client, resourceType: schemaResourceType}
}

// ---- Table builder ----

type tableBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (b *tableBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return tableResourceType
}

func tableResource(workspaceId string, table *databricks.Table, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := securableProfile(workspaceId, table.FullName, table.Owner)
	profile["securable_type"] = databricks.SecurableTable
	profile["table_type"] = table.TableType

	return rs.NewResource(
		table.Name,
		tableResourceType,
		makeSecurableID(workspaceId, table.FullName),
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
	)
}

func (b *tableBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil || parentResourceID.ResourceType != schemaResourceType.Id {
		return nil, nil, nil
	}
	workspaceId, schemaFullName := splitSecurableID(parentResourceID.Resource)
	catalogName, schemaName, found := strings.Cut(schemaFullName, ".")
	if !found {
		return nil, nil, fmt.Errorf("databricks-connector: malformed schema id: %q", parentResourceID.Resource)
	}

	bag, pageToken, err := parseCursorToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: tableResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	tables, nextToken, _, err := b.client.ListTables(ctx, workspaceId, catalogName, schemaName, pageToken, ResourcesPageSize)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list tables: %w", err)
	}

	var rv []*v2.Resource
	for _, table := range tables {
		tCopy := table
		tr, err := tableResource(workspaceId, &tCopy, parentResourceID)
		if err != nil {
			return nil, nil, err
		}
		rv = append(rv, tr)
	}

	nextPage, err := bag.NextToken(nextToken)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func (b *tableBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return securablePrivilegeEntitlements(resource, tablePrivileges), nil, nil
}

func (b *tableBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	grants, err := securableGrants(ctx, b.client, resource, databricks.SecurableTable)
	if err != nil {
		return nil, nil, err
	}
	return grants, nil, nil
}

func (b *tableBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.client, entitlement, principal, true)
}

func (b *tableBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.client, grant.Entitlement, grant.Principal, false)
}

func newTableBuilder(client *databricks.Client) *tableBuilder {
	return &tableBuilder{client: client, resourceType: tableResourceType}
}
