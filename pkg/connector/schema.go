package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*schemaBuilder)(nil)

const (
	securableTypeSchema = "SCHEMA"

	// informationSchema is the read-only schema Databricks puts in every catalog.
	// Its privileges are not grantable, so it is left out of the sync.
	informationSchema = "information_schema"

	// schemaNameSeparator joins the catalog and the schema into the dotted full
	// name the permissions endpoint addresses a schema by. Databricks rejects a
	// name holding a period, so splitting on it is unambiguous.
	schemaNameSeparator = "."

	profileKeySchemaName = "schema_name"

	// profileKeyCatalogOwner carries the parent catalog's owner, which is one of
	// the things that makes a schema's privilege read complete. Grants is handed
	// only the schema, so List writes it down.
	profileKeyCatalogOwner = "catalog_owner"

	schemaWriteRemedy = "the connector must own the metastore, the parent catalog or the schema, or hold MANAGE on the schema or the parent catalog"

	schemaTrustRemedy = "the connector's service principal must own the metastore, the parent catalog or the schema, hold MANAGE on the schema or " +
		"on the parent catalog, or be assigned Admin directly on the workspace whose admins group owns the parent catalog"

	schemaListRemedy = "the connector's service principal must own the metastore or the catalog, hold MANAGE, USE_SCHEMA or ALL_PRIVILEGES on the catalog, " +
		"or be assigned Admin directly on the workspace whose admins group owns the catalog"

	errorCodeSchemaGone = "SCHEMA_DOES_NOT_EXIST"
)

// schemaPrivileges is the catalog set minus the three that stop at the catalog.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
var schemaPrivileges = slices.DeleteFunc(slices.Clone(catalogPrivileges), func(privilege string) bool {
	return slices.Contains([]string{"BROWSE", "CREATE_SCHEMA", "USE_CATALOG"}, privilege)
})

// schemaListingPrivileges each make ListSchemas return every schema in the
// catalog. A complete listing does not need MANAGE: USE_SCHEMA and ALL_PRIVILEGES
// carry it too, and neither of those makes a privilege read complete.
var schemaListingPrivileges = []string{privilegeManage, "USE_SCHEMA", "ALL_PRIVILEGES"}

var schemaLevel = securableLevel{
	securableType:   databricks.SecurableSchema,
	noun:            "schema",
	privileges:      schemaPrivileges,
	writeRemedy:     schemaWriteRemedy,
	trustRemedy:     schemaTrustRemedy,
	permissionsName: schemaPermissionsName,
	trusts:          schemaGrantsAreTrustworthy,
	// A permissions read names the leaf; the GET and the listings name the parent.
	goneErrorCodes: []string{errorCodeSchemaGone, errorCodeCatalogGone},
}

type schemaBuilder struct {
	securableOps
	workspaces map[string]struct{}
}

func newSchemaBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *schemaBuilder {
	return &schemaBuilder{
		securableOps: securableOps{client: client, clientID: clientID, sync: sync, level: schemaLevel},
		workspaces:   workspaces,
	}
}

func (b *schemaBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return schemaResourceType
}

func schemaResourceID(metastoreID, catalogName, name string) string {
	return metastoreID + securableKeySeparator + catalogName + schemaNameSeparator + name
}

func parseSchemaID(resourceID string) (string, string, string, error) {
	metastoreID, fullName, ok := strings.Cut(resourceID, securableKeySeparator)
	if !ok {
		return "", "", "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema resource id %q", resourceID)
	}

	catalogName, name, ok := strings.Cut(fullName, schemaNameSeparator)
	if !ok || metastoreID == "" || catalogName == "" || name == "" {
		return "", "", "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema resource id %q", resourceID)
	}

	return metastoreID, catalogName, name, nil
}

// schemaPermissionsName: the permissions endpoint addresses a schema by its
// dotted full name. The metastore is carried by the host.
func schemaPermissionsName(resourceID string) (string, error) {
	_, catalogName, name, err := parseSchemaID(resourceID)
	if err != nil {
		return "", err
	}

	return catalogName + schemaNameSeparator + name, nil
}

func schemaResource(
	schema *databricks.Schema,
	metastoreID, catalogName string,
	parent *v2.ResourceId,
	route schemaRoute,
) (*v2.Resource, error) {
	fullName := catalogName + schemaNameSeparator + schema.Name
	profile := map[string]any{
		profileKeyMetastoreID:    metastoreID,
		profileKeyMetastoreOwner: route.MetastoreOwner,
		profileKeyFullName:       fullName,
		profileKeyCatalogName:    catalogName,
		profileKeyCatalogOwner:   route.CatalogOwner,
		profileKeySchemaName:     schema.Name,
		profileKeyOwner:          schema.Owner,
		profileKeySecurableType:  securableTypeSchema,
		profileKeyWorkspace:      route.Workspace,
		profileKeyWorkspaceID:    route.WorkspaceID,
		profileKeyWorkspaceName:  route.WorkspaceName,
	}

	resourceID := schemaResourceID(metastoreID, catalogName, schema.Name)

	// The full name is the display name: main.sales and finance.sales would
	// otherwise be two rows both reading "sales" in C1.
	return rs.NewAppResource(
		fullName,
		schemaResourceType,
		resourceID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: tableResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: volumeResourceType.Id},
			&v2.RawId{Id: resourceID},
		),
	)
}

// schemaRoute is the workspace a catalog's schemas are listed through, carried
// in the page token so later pages neither re-resolve it nor prove the listing
// complete again. It is the catalog's route plus the catalog's owner, which a
// schema's trust check needs and a catalog's does not.
type schemaRoute struct {
	catalogRoute
	CatalogOwner string `json:"c"`
}

func schemaRouteOn(route catalogRoute, catalog *databricks.Catalog) schemaRoute {
	return schemaRoute{catalogRoute: route, CatalogOwner: catalog.Owner}
}

// List pages a catalog's schemas through a workspace that can see every schema.
// Databricks answers a caller without catalog-wide visibility with a short list
// at 200, so a listing that cannot be proved complete stops the sync.
func (b *schemaBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	ctx = context.WithValue(ctx, syncSessionKey{}, attr.Session)
	if parentResourceID.GetResourceType() != catalogResourceType.Id {
		return nil, nil, nil
	}

	metastoreID, catalogName, err := parseCatalogID(parentResourceID.GetResource())
	if err != nil {
		return nil, nil, err
	}

	annos := annotations.Annotations{}
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(attr.PageToken.Token); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema page token: %v", err)
	}

	if bag.Current() == nil {
		if err := b.pushRoute(ctx, metastoreID, catalogName, bag, &annos); err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if bag.Current() == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: no workspace in sync scope lists the catalog; listing no schemas",
				zap.String("metastore", metastoreID),
				zap.String("catalog", catalogName))

			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}
	}

	var route schemaRoute
	if err := json.Unmarshal([]byte(bag.ResourceID()), &route); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "databricks-connector: invalid schema page token: %v", err)
	}

	schemas, next, rateLimit, err := b.client.ListSchemas(ctx, route.Workspace, catalogName, bag.PageToken(), ResourcesPageSize)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, schemaListingError(catalogName, route.Workspace, err)
	}

	var rv []*v2.Resource
	for _, schema := range schemas {
		if schema.Name == "" || schema.Name == informationSchema {
			continue
		}

		built, err := schemaResource(&schema, metastoreID, catalogName, parentResourceID, route)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build the resource for schema %s.%s: %w", catalogName, schema.Name, err)
		}
		rv = append(rv, built)
	}

	if next != "" && next == bag.PageToken() {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.Internal,
			"databricks-connector: listing the schemas of catalog %s repeated page token %q", catalogName, next)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
}

// pushRoute records the first running in-scope workspace that both lists the
// catalog and can read every schema, and refuses when none can: returning no
// schemas would make C1 delete them.
func (b *schemaBuilder) pushRoute(
	ctx context.Context,
	metastoreID, catalogName string,
	bag *pagination.Bag,
	annos *annotations.Annotations,
) error {
	scope, err := scopedWorkspaces(ctx, b.client, b.workspaces, annos)
	if err != nil {
		return err
	}

	usable, inScope, err := scope.attached(ctx, b.client, metastoreID, annos)
	if err != nil {
		return assignmentListError(metastoreID, err)
	}
	if len(usable) == 0 {
		if inScope {
			return status.Errorf(codes.FailedPrecondition,
				"databricks-connector: metastore %s has no running workspace in sync scope", metastoreID)
		}

		return nil
	}

	owner, err := metastoreOwner(ctx, b.client, metastoreID, annos)
	// The metastore syncer removes the parent in the same sync.
	if errors.Is(err, errMetastoreGone) {
		return nil
	}
	if err != nil {
		return err
	}

	accept := func(ctx context.Context, workspace databricks.Workspace, catalog *databricks.Catalog) (bool, error) {
		return schemaListingIsComplete(ctx, b.client, securableTrust{
			ownPrincipal:   b.clientID,
			metastoreOwner: owner,
			parentOwner:    catalog.Owner,
			fullName:       catalogName,
			workspace:      workspace.DeploymentName,
			workspaceID:    strconv.Itoa(workspace.ID),
		}, annos)
	}

	workspace, catalog, err := workspaceForCatalog(ctx, b.client, metastoreID, catalogName, usable, catalogProbe{}, accept, annos)
	if err != nil {
		switch {
		// A catalog proved gone holds no schemas, and the catalog syncer drops the
		// parent in the same sync.
		case errors.Is(err, errCatalogGone):
			return nil
		case errors.Is(err, errCatalogNotAccepted):
			return status.Errorf(codes.PermissionDenied,
				"databricks-connector: cannot list every schema in catalog %s; %s",
				catalogName, schemaListRemedy)
		default:
			return err
		}
	}

	encoded, err := json.Marshal(schemaRouteOn(catalogRoute{
		Workspace:      workspace.DeploymentName,
		WorkspaceID:    strconv.Itoa(workspace.ID),
		WorkspaceName:  workspace.Name,
		MetastoreOwner: owner,
	}, catalog))
	if err != nil {
		return err
	}
	bag.Push(pagination.PageState{ResourceTypeID: schemaResourceType.Id, ResourceID: string(encoded)})

	return nil
}

// errSchemaGone marks a schema Databricks named as missing.
var errSchemaGone = errors.New("schema no longer exists")

// schemaThrough reads one schema through workspace. It reports errSchemaGone
// when Databricks names the schema itself missing, which proves nothing on its
// own, so the caller decides what that absence means. Every other refusal leaves
// the schema unknown: read as empty, C1 would delete its tables and volumes.
func schemaThrough(
	ctx context.Context,
	client *databricks.Client,
	workspace, catalogName, schemaName string,
	annos *annotations.Annotations,
) (*databricks.Schema, error) {
	fullName := catalogName + schemaNameSeparator + schemaName
	schema, rateLimit, err := client.GetSchema(ctx, workspace, fullName)
	noteRateLimit(annos, rateLimit)
	switch {
	case err == nil:
	case databricks.IsSecurableGone(err, errorCodeSchemaGone):
		return nil, errSchemaGone
	case status.Code(err) == codes.InvalidArgument:
		// The answer does not describe the schema asked for.
		return nil, err
	default:
		return nil, refuseUnreadable(fmt.Sprintf(
			"databricks-connector: schema %s cannot be read through workspace %s, "+
				"so its tables and volumes are unknown", fullName, workspace), err)
	}

	return schema, nil
}

// schemaListingError returns NotFound only when Databricks names the missing
// catalog. Any other refusal leaves the listing unknown rather than empty: a
// short list would read as deletions in C1.
func schemaListingError(catalogName, workspace string, err error) error {
	if databricks.IsSecurableGone(err, errorCodeCatalogGone) {
		return uhttp.WrapErrors(codes.NotFound,
			fmt.Sprintf("databricks-connector: catalog %s no longer exists", catalogName), err)
	}

	switch apiStatus(err) {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: the schemas of catalog %s cannot be listed through workspace %s, "+
				"so which schemas are in scope is unknown; %s, or leave that workspace out with workspaces or databricks-exclude-workspaces",
				catalogName, workspace, schemaListRemedy),
			err)
	}

	return fmt.Errorf("databricks-connector: failed to list the schemas of catalog %s through workspace %s: %w",
		catalogName, workspace, err)
}

// schemaListingIsComplete is true when ListSchemas returns every schema of the
// catalog: ownership, a direct Admin of the owning admins group's workspace,
// MANAGE, USE_SCHEMA or ALL_PRIVILEGES each settle it. It reports errCatalogGone
// when Databricks names the catalog missing.
func schemaListingIsComplete(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner sees everything below it, and the metastore's owner is its admin.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) || ownsPrincipal(trust.ownPrincipal, trust.parentOwner) {
		return true, nil
	}

	// The workspace admins group acts as the owner for a direct Admin of its workspace.
	if ownerWorkspaceID, ok := catalogAdminWorkspaceID(trust.parentOwner); ok {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, ownerWorkspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	complete, err := holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, trust.fullName,
		schemaListingPrivileges, []string{errorCodeCatalogGone}, annos)
	if errors.Is(err, errSecurableGone) {
		return false, errCatalogGone
	}

	return complete, err
}

// schemaGrantsAreTrustworthy is true for an owner of the schema, catalog, or
// metastore, a holder of MANAGE on the schema or catalog, or a direct Admin
// when the workspace admins group owns the catalog.
func schemaGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is never reported as a grant.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.parentOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.owner) {
		return true, nil
	}

	if ownerWorkspaceID, ok := catalogAdminWorkspaceID(trust.parentOwner); ok {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, ownerWorkspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	// One effective read reports MANAGE on the schema and on the catalog above it.
	return holdsEffectivePrivilegeOn(ctx, client, trust, databricks.SecurableSchema, trust.fullName, managePrivilege, annos)
}

func (b *schemaBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(schemaPrivileges)+1)
	for _, privilege := range schemaPrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a schema in Databricks", privilege)),
		))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a schema in Databricks"),
	))

	return rv, nil, nil
}

func (b *schemaBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// Grants carries the sync session so the workspace-Admin check behind the trust
// rule is read once per workspace per sync rather than once per schema.
func (b *schemaBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ctx = context.WithValue(ctx, syncSessionKey{}, attr.Session)

	return b.syncGrants(ctx, resource, attr)
}

func (b *schemaBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	ctx = databricks.WithoutCache(ctx)
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, entitlement, &annos)
	if err != nil {
		if isSecurableGoneSentinel(err) {
			return annos, status.Errorf(codes.NotFound, "databricks-connector: schema %s no longer exists",
				entitlement.GetResource().GetId().GetResource())
		}

		return annos, err
	}

	return b.grant(ctx, principal.GetId(), routed)
}

func (b *schemaBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	ctx = databricks.WithoutCache(ctx)
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, revoked.GetEntitlement(), &annos)
	if err != nil {
		if isSecurableGoneSentinel(err) {
			annos.Update(&v2.GrantAlreadyRevoked{})

			return annos, nil
		}

		return annos, err
	}

	return b.revoke(ctx, revoked.GetPrincipal().GetId(), routed)
}

// entitlementOnLiveRoute rebuilds the entitlement's schema from what Databricks
// reports now: the three owners the trust check short-circuits on ride on the
// profile, where they can be hours old, so a transfer away from the connector
// would otherwise let a truncated privilege list decide the write.
func (b *schemaBuilder) entitlementOnLiveRoute(
	ctx context.Context,
	entitlement *v2.Entitlement,
	annos *annotations.Annotations,
) (*v2.Entitlement, error) {
	resource := entitlement.GetResource()
	metastoreID, catalogName, schemaName, err := parseSchemaID(resource.GetId().GetResource())
	if err != nil {
		return nil, err
	}

	owner, err := metastoreOwner(ctx, b.client, metastoreID, annos)
	if err != nil {
		return nil, err
	}

	route, catalog, err := liveCatalogRoute(ctx, b.client, b.clientID, b.workspaces, resource, metastoreID, catalogName, owner, annos)
	if err != nil {
		return nil, err
	}

	fullName := catalogName + schemaNameSeparator + schemaName
	schema, rateLimit, err := b.client.GetSchema(ctx, route.Workspace, fullName)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		// Only Databricks naming the securable proves it gone; an unbound workspace
		// refuses with 403, and reading that as an absence would report a false revoke.
		if databricks.IsSecurableGone(err, errorCodeSchemaGone, errorCodeCatalogGone) {
			return nil, errSecurableGone
		}

		return nil, refuseUnreadable(fmt.Sprintf(
			"databricks-connector: failed to read schema %s through workspace %s", fullName, route.Workspace), err)
	}

	route.CatalogOwner = catalog.Owner
	routed, err := schemaResource(schema, metastoreID, catalogName, resource.GetParentResourceId(), route)
	if err != nil {
		return nil, err
	}

	return entitlementOnResource(entitlement, routed)
}

// liveCatalogRoute picks the workspace the write goes through: the recorded one
// when this sync still admits it and it still serves the catalog, otherwise the
// first running in-scope workspace that does. A catalog no workspace serves is
// not proof of deletion, so that case is settled against the metastore's
// unbound listing.
func liveCatalogRoute(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	workspaces map[string]struct{},
	resource *v2.Resource,
	metastoreID, catalogName, metastoreOwner string,
	annos *annotations.Annotations,
) (schemaRoute, *databricks.Catalog, error) {
	var probe catalogProbe
	if route, ok := profileCatalogRoute(client, workspaces, resource, metastoreOwner); ok {
		catalog, err := catalogThrough(ctx, client, route.Workspace, metastoreID, catalogName, annos)
		switch {
		case err == nil:
		case errors.Is(err, errCatalogGone):
			probe = catalogProbe{workspace: route.Workspace, gone: true}
		case deniesTrust(err):
			probe = catalogProbe{workspace: route.Workspace, deny: err}
		}
		switch {
		case catalog != nil && catalog.MetastoreID == metastoreID:
			return schemaRouteOn(route, catalog), catalog, nil
		case catalog != nil:
		case err == nil:
			probe = catalogProbe{workspace: route.Workspace}
		}
	}

	scope, err := scopedWorkspaces(ctx, client, workspaces, annos)
	if err != nil {
		return schemaRoute{}, nil, err
	}
	usable, _, err := scope.attached(ctx, client, metastoreID, annos)
	if err != nil {
		return schemaRoute{}, nil, assignmentListError(metastoreID, err)
	}
	if len(usable) == 0 {
		return schemaRoute{}, nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: metastore %s has no running workspace in sync scope to write catalog %s through",
			metastoreID, catalogName)
	}

	workspace, catalog, err := workspaceForCatalog(ctx, client, metastoreID, catalogName, usable, probe, nil, annos)
	if err != nil {
		if errors.Is(err, errCatalogGone) {
			return schemaRoute{}, nil, confirmCatalogGone(
				ctx, client, clientID, metastoreID, catalogName, usable[0].DeploymentName, metastoreOwner, annos)
		}

		return schemaRoute{}, nil, err
	}

	return schemaRouteOn(catalogRoute{
		Workspace:      workspace.DeploymentName,
		WorkspaceID:    strconv.Itoa(workspace.ID),
		WorkspaceName:  workspace.Name,
		MetastoreOwner: metastoreOwner,
	}, catalog), catalog, nil
}
