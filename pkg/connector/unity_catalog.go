package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"

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

// Unity Catalog privileges per securable level. These are the entitlements
// exposed for each level; a grant referencing a privilege outside its level's
// set is skipped (with a warning) so no grant points at an entitlement that
// Entitlements() never produced.
var (
	catalogPrivileges = []string{
		"ALL_PRIVILEGES", "USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE",
		"CREATE_FUNCTION", "CREATE_MODEL", "CREATE_VOLUME", "CREATE_MATERIALIZED_VIEW",
		"MODIFY", "SELECT", "EXECUTE", "READ_VOLUME", "WRITE_VOLUME", "REFRESH",
		"APPLY_TAG", "BROWSE", "EXTERNAL_USE_SCHEMA", "MANAGE",
	}
	schemaPrivileges = []string{
		"ALL_PRIVILEGES", "USE_SCHEMA", "CREATE_TABLE", "CREATE_FUNCTION", "CREATE_MODEL",
		"CREATE_VOLUME", "CREATE_MATERIALIZED_VIEW", "MODIFY", "SELECT", "EXECUTE",
		"READ_VOLUME", "WRITE_VOLUME", "REFRESH", "APPLY_TAG", "BROWSE",
		"EXTERNAL_USE_SCHEMA", "MANAGE",
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

func securableProfile(workspaceId, fullName, owner, securableType string) map[string]interface{} {
	return map[string]interface{}{
		"workspace_id":   workspaceId,
		"full_name":      fullName,
		"owner":          owner,
		"securable_type": securableType,
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

// ucResolver resolves Unity Catalog principal strings to/from connector resource
// IDs, caching results per (scope, principal) to avoid a SCIM lookup storm:
// Grants() runs for every catalog, schema and table, and each grant would
// otherwise cost up to three uncached SCIM list calls per principal.
type ucResolver struct {
	client *databricks.Client
	mu     sync.Mutex
	cache  map[string]*v2.ResourceId
}

func newUCResolver(client *databricks.Client) *ucResolver {
	return &ucResolver{client: client, cache: make(map[string]*v2.ResourceId)}
}

// scimScopes returns the SCIM scopes to search, in order. Unity Catalog grants
// are typically held by account-level identities, which are synced from account
// SCIM (scope "") whenever the account API is available; the workspace scope is
// a fallback for workspace-local identities (and the only scope under token
// auth, where identities are synced per workspace).
func (r *ucResolver) scimScopes(workspaceId string) []string {
	if r.client.IsAccountAPIAvailable() {
		return []string{"", workspaceId}
	}
	return []string{workspaceId}
}

// resolve maps a Unity Catalog principal string (user name/email, group display
// name, or service principal application ID) to a connector resource ID. Returns
// nil when no identity matches. Results (including negatives) are cached.
func (r *ucResolver) resolve(ctx context.Context, workspaceId, principal string) (*v2.ResourceId, error) {
	key := workspaceId + "\x00" + principal

	r.mu.Lock()
	if id, ok := r.cache[key]; ok {
		r.mu.Unlock()
		return id, nil
	}
	r.mu.Unlock()

	var result *v2.ResourceId
	for _, scope := range r.scimScopes(workspaceId) {
		id, err := resolvePrincipalResourceID(ctx, r.client, scope, principal)
		if err != nil {
			return nil, err
		}
		if id != nil {
			result = id
			break
		}
	}

	r.mu.Lock()
	r.cache[key] = result
	r.mu.Unlock()
	return result, nil
}

// resolveName converts a connector principal resource ID back into the Unity
// Catalog principal string expected by the permissions API, searching the same
// scopes as resolve.
func (r *ucResolver) resolveName(ctx context.Context, workspaceId string, principal *v2.ResourceId) (string, error) {
	for _, scope := range r.scimScopes(workspaceId) {
		name, err := resolvePrincipalName(ctx, r.client, scope, principal)
		if err != nil {
			return "", err
		}
		if name != "" {
			return name, nil
		}
	}
	return "", nil
}

// resolvePrincipalResourceID resolves a Unity Catalog principal string against a
// single SCIM scope (workspaceId "" targets the account API). Returns nil when
// the principal cannot be matched to any known identity in that scope.
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

// resolvePrincipalName converts a connector principal resource ID into the Unity
// Catalog principal string within a single SCIM scope. Returns "" when not found.
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
// Failures degrade to an empty result (logged) rather than aborting the sync;
// context cancellation still propagates.
func securableGrants(ctx context.Context, r *ucResolver, resource *v2.Resource, securableType string, privileges []string) ([]*v2.Grant, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	workspaceId, fullName := splitSecurableID(resource.Id.Resource)

	assignments, ratelimit, err := r.client.ListPermissions(ctx, workspaceId, securableType, fullName)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		l.Warn("databricks-connector: unable to list unity catalog permissions, skipping securable grants",
			zap.String("securable_type", securableType),
			zap.String("securable", fullName),
			zap.Error(err),
		)
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	if ratelimit != nil {
		annos.WithRateLimiting(ratelimit)
	}

	known := make(map[string]struct{}, len(privileges))
	for _, p := range privileges {
		known[p] = struct{}{}
	}

	var rv []*v2.Grant
	for _, a := range assignments {
		principalID, err := r.resolve(ctx, workspaceId, a.Principal)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			l.Warn("databricks-connector: failed to resolve unity catalog principal, skipping",
				zap.String("principal", a.Principal),
				zap.String("securable", fullName),
				zap.Error(err),
			)
			continue
		}
		if principalID == nil {
			l.Warn("databricks-connector: skipping unresolved unity catalog principal",
				zap.String("principal", a.Principal),
				zap.String("securable", fullName),
			)
			continue
		}

		for _, priv := range a.Privileges {
			if _, ok := known[priv]; !ok {
				l.Warn("databricks-connector: skipping unmodeled unity catalog privilege",
					zap.String("privilege", priv),
					zap.String("securable_type", securableType),
					zap.String("securable", fullName),
				)
				continue
			}
			g, err := securableGrant(ctx, r.client, resource, workspaceId, priv, principalID)
			if err != nil {
				return nil, nil, err
			}
			rv = append(rv, g)
		}
	}

	// Owners hold implicit full control the permissions API omits.
	if owner, ok := rs.GetProfileStringValue(rs.GetProfile(resource), "owner"); ok && owner != "" {
		principalID, err := r.resolve(ctx, workspaceId, owner)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			l.Warn("databricks-connector: failed to resolve unity catalog owner, skipping owner grant",
				zap.String("owner", owner),
				zap.String("securable", fullName),
				zap.Error(err),
			)
			return rv, annos, nil
		}
		if principalID == nil {
			l.Warn("databricks-connector: skipping unresolved unity catalog owner",
				zap.String("owner", owner),
				zap.String("securable", fullName),
			)
			return rv, annos, nil
		}

		g, err := securableGrant(ctx, r.client, resource, workspaceId, ownerEntitlement, principalID)
		if err != nil {
			return nil, nil, err
		}
		rv = append(rv, g)
	}

	return rv, annos, nil
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
func securableGrantChange(ctx context.Context, r *ucResolver, entitlement *v2.Entitlement, principal *v2.Resource, add bool) (annotations.Annotations, error) {
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

	principalName, err := r.resolveName(ctx, workspaceId, principal.Id)
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

	if _, err := r.client.UpdatePermissions(ctx, workspaceId, securableType, fullName, []databricks.PermissionsChange{change}); err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update %s permissions for %q: %w", securableType, fullName, err)
	}

	return nil, nil
}

// syncOpResults builds a SyncOpResults carrying the next page token and any
// rate-limit feedback so the SDK can back off on the highest-volume calls.
func syncOpResults(nextPage string, ratelimit *v2.RateLimitDescription) *rs.SyncOpResults {
	res := &rs.SyncOpResults{NextPageToken: nextPage}
	if ratelimit != nil {
		res.Annotations = annotations.Annotations{}
		res.Annotations.WithRateLimiting(ratelimit)
	}
	return res
}

// ---- Catalog builder ----

type catalogBuilder struct {
	resolver     *ucResolver
	resourceType *v2.ResourceType
}

func (b *catalogBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return catalogResourceType
}

func catalogResource(workspaceId string, catalog *databricks.Catalog, parent *v2.ResourceId) (*v2.Resource, error) {
	return rs.NewResource(
		catalog.Name,
		catalogResourceType,
		makeSecurableID(workspaceId, catalog.Name),
		rs.WithResourceProfile(securableProfile(workspaceId, catalog.Name, catalog.Owner, databricks.SecurableCatalog)),
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

	catalogs, nextToken, ratelimit, err := b.resolver.client.ListCatalogs(ctx, workspaceId, pageToken, ResourcesPageSize)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		// Degrade gracefully: workspaces without Unity Catalog (or where the
		// service principal lacks UC access) must not fail the sync.
		ctxzap.Extract(ctx).Warn("databricks-connector: unable to list catalogs, skipping unity catalog for workspace",
			zap.String("workspace", workspaceId),
			zap.Error(err),
		)
		return nil, nil, nil
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

	return rv, syncOpResults(nextPage, ratelimit), nil
}

func (b *catalogBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return securablePrivilegeEntitlements(resource, catalogPrivileges), nil, nil
}

func (b *catalogBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	grants, annos, err := securableGrants(ctx, b.resolver, resource, databricks.SecurableCatalog, catalogPrivileges)
	if err != nil {
		return nil, nil, err
	}
	return grants, &rs.SyncOpResults{Annotations: annos}, nil
}

func (b *catalogBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.resolver, entitlement, principal, true)
}

func (b *catalogBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.resolver, grant.Entitlement, grant.Principal, false)
}

func newCatalogBuilder(resolver *ucResolver) *catalogBuilder {
	return &catalogBuilder{resolver: resolver, resourceType: catalogResourceType}
}

// ---- Schema builder ----

type schemaBuilder struct {
	resolver     *ucResolver
	resourceType *v2.ResourceType
	syncTables   bool
}

func (b *schemaBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return schemaResourceType
}

func schemaResource(workspaceId string, schema *databricks.Schema, parent *v2.ResourceId, syncTables bool) (*v2.Resource, error) {
	opts := []rs.ResourceOption{
		rs.WithResourceProfile(securableProfile(workspaceId, schema.FullName, schema.Owner, databricks.SecurableSchema)),
		rs.WithParentResourceID(parent),
	}
	if syncTables {
		opts = append(opts, rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: tableResourceType.Id}))
	}

	return rs.NewResource(
		schema.Name,
		schemaResourceType,
		makeSecurableID(workspaceId, schema.FullName),
		opts...,
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

	schemas, nextToken, ratelimit, err := b.resolver.client.ListSchemas(ctx, workspaceId, catalogName, pageToken, ResourcesPageSize)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		ctxzap.Extract(ctx).Warn("databricks-connector: unable to list schemas, skipping catalog",
			zap.String("workspace", workspaceId),
			zap.String("catalog", catalogName),
			zap.Error(err),
		)
		return nil, nil, nil
	}

	var rv []*v2.Resource
	for _, schema := range schemas {
		sCopy := schema
		sr, err := schemaResource(workspaceId, &sCopy, parentResourceID, b.syncTables)
		if err != nil {
			return nil, nil, err
		}
		rv = append(rv, sr)
	}

	nextPage, err := bag.NextToken(nextToken)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, syncOpResults(nextPage, ratelimit), nil
}

func (b *schemaBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return securablePrivilegeEntitlements(resource, schemaPrivileges), nil, nil
}

func (b *schemaBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	grants, annos, err := securableGrants(ctx, b.resolver, resource, databricks.SecurableSchema, schemaPrivileges)
	if err != nil {
		return nil, nil, err
	}
	return grants, &rs.SyncOpResults{Annotations: annos}, nil
}

func (b *schemaBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.resolver, entitlement, principal, true)
}

func (b *schemaBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.resolver, grant.Entitlement, grant.Principal, false)
}

func newSchemaBuilder(resolver *ucResolver, syncTables bool) *schemaBuilder {
	return &schemaBuilder{resolver: resolver, resourceType: schemaResourceType, syncTables: syncTables}
}

// ---- Table builder ----

type tableBuilder struct {
	resolver     *ucResolver
	resourceType *v2.ResourceType
}

func (b *tableBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return tableResourceType
}

func tableResource(workspaceId string, table *databricks.Table, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := securableProfile(workspaceId, table.FullName, table.Owner, databricks.SecurableTable)
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

	tables, nextToken, ratelimit, err := b.resolver.client.ListTables(ctx, workspaceId, catalogName, schemaName, pageToken, ResourcesPageSize)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		ctxzap.Extract(ctx).Warn("databricks-connector: unable to list tables, skipping schema",
			zap.String("workspace", workspaceId),
			zap.String("catalog", catalogName),
			zap.String("schema", schemaName),
			zap.Error(err),
		)
		return nil, nil, nil
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

	return rv, syncOpResults(nextPage, ratelimit), nil
}

func (b *tableBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return securablePrivilegeEntitlements(resource, tablePrivileges), nil, nil
}

func (b *tableBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	grants, annos, err := securableGrants(ctx, b.resolver, resource, databricks.SecurableTable, tablePrivileges)
	if err != nil {
		return nil, nil, err
	}
	return grants, &rs.SyncOpResults{Annotations: annos}, nil
}

func (b *tableBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.resolver, entitlement, principal, true)
}

func (b *tableBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return securableGrantChange(ctx, b.resolver, grant.Entitlement, grant.Principal, false)
}

func newTableBuilder(resolver *ucResolver) *tableBuilder {
	return &tableBuilder{resolver: resolver, resourceType: tableResourceType}
}
