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
	"google.golang.org/protobuf/proto"
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

	// profileKeyCatalogOwner carries the owner of the parent catalog. Owning the
	// catalog is one of the things that makes a schema's privilege read complete,
	// and Grants is handed only the schema, so List writes the owner down instead
	// of listing the catalogs again on every call.
	profileKeyCatalogOwner = "catalog_owner"

	// schemaWriteRemedy is what a 403 on the PATCH tells the operator.
	schemaWriteRemedy = "the connector must own the metastore, the parent catalog or the schema, or hold MANAGE on the schema or the parent catalog"

	// schemaTrustRemedy is what a refusal to report a partial privilege list
	// tells the operator.
	schemaTrustRemedy = "the connector's service principal must own the metastore, the parent catalog or the schema, hold MANAGE on the schema or " +
		"on the parent catalog, or be assigned Admin directly on the workspace whose admins group owns the parent catalog"

	// schemaListRemedy is what a refusal to sync a catalog whose schema listing
	// would be partial tells the operator.
	schemaListRemedy = "the connector's service principal must own the metastore or the catalog, hold MANAGE, USE_SCHEMA or ALL_PRIVILEGES on the catalog, " +
		"or be assigned Admin directly on the workspace whose admins group owns the catalog"

	// errorCodeSchemaGone is what Databricks names in error_code when the schema
	// a request addresses does not exist.
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
	// A permissions read names the leaf, so a dropped schema and a dropped catalog
	// both answer SCHEMA_DOES_NOT_EXIST there. The GET and the listings name the
	// parent instead, which is where CATALOG_DOES_NOT_EXIST reaches this level.
	goneErrorCodes: []string{errorCodeSchemaGone, errorCodeCatalogGone},
}

type schemaBuilder struct {
	securableOps
	// workspaces is the configured allowlist, empty when none was given.
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

// schemaResourceID keys a schema by its metastore and its dotted full name, which
// extends the catalog's "{metastore_id}::{catalog_name}".
func schemaResourceID(metastoreID, catalogName, name string) string {
	return metastoreID + securableKeySeparator + catalogName + schemaNameSeparator + name
}

// parseSchemaID reads a schema resource id, "{metastore_id}::{catalog}.{schema}".
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

	// The display name is the full name rather than the leaf: main.sales and
	// finance.sales would otherwise be two rows both reading "sales" in C1.
	return rs.NewAppResource(
		fullName,
		schemaResourceType,
		resourceID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: resourceID}),
	)
}

// schemaRoute is the workspace a catalog's schemas are listed through, carried in
// the page token so later pages neither re-resolve the route nor prove again that
// the listing is complete.
type schemaRoute struct {
	Workspace     string `json:"w"`
	WorkspaceID   string `json:"i"`
	WorkspaceName string `json:"n"`
	// MetastoreOwner and CatalogOwner ride along so each schema's profile can
	// carry them. See profileKeyCatalogOwner.
	MetastoreOwner string `json:"o"`
	CatalogOwner   string `json:"c"`
}

// List pages a catalog's schemas through a workspace that can see every schema.
// Databricks answers a caller without catalog-wide visibility with only the
// schemas it owns or can use, at 200, so a listing that cannot be proved complete
// stops the sync and C1 keeps the schemas it already has.
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

// pushRoute records a workspace that can list every schema of the catalog. It
// walks the running in-scope workspaces until one both lists the catalog and can
// read every schema. An empty stack means no workspace this sync can use serves
// the metastore. A catalog none of them can list completely fails the sync:
// returning no schemas would make C1 delete them. system and samples are not
// special here, because the catalog sync leaves them out, when it leaves them
// out, before this List runs.
//
// A gone answer is treated as unproven on its own. An unbound catalog was
// measured to refuse with 403, which lands in lastDeny, but what a bound caller
// without USE_CATALOG is told was not measured, so the listing is emptied only
// when every workspace said gone and none refused.
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
	// The metastore syncer removes the parent in the same sync, so a metastore
	// gone empties what C1 is already removing instead of failing with the bare
	// sentinel, which reaches C1 as Unknown and is retried.
	if errors.Is(err, errMetastoreGone) {
		return nil
	}
	if err != nil {
		return err
	}

	var sawCatalog, sawGone, provenGone bool
	var lastDeny error
	for _, workspace := range usable {
		catalog, err := catalogThrough(ctx, b.client, workspace.DeploymentName, metastoreID, catalogName, annos)
		if err != nil {
			if errors.Is(err, errCatalogGone) {
				sawGone = true
				continue
			}
			if deniesTrust(err) {
				lastDeny = err
				continue
			}
			return err
		}
		if catalog == nil {
			continue
		}
		sawCatalog = true
		// This workspace served the catalog just now, which supersedes a gone
		// answer an earlier one gave: the reads run in order, and the catalog
		// cannot be absent before it is present.
		provenGone = false

		workspaceID := strconv.Itoa(workspace.ID)
		route := schemaRoute{
			Workspace:      workspace.DeploymentName,
			WorkspaceID:    workspaceID,
			WorkspaceName:  workspace.Name,
			MetastoreOwner: owner,
			CatalogOwner:   catalog.Owner,
		}
		complete, err := schemaListingIsComplete(ctx, b.client, securableTrust{
			ownPrincipal:   b.clientID,
			metastoreOwner: owner,
			parentOwner:    catalog.Owner,
			fullName:       catalogName,
			workspace:      route.Workspace,
			workspaceID:    workspaceID,
		}, annos)
		if err != nil {
			// The permissions read this check ends on names the catalog missing
			// only when it is gone: a workspace the catalog is not bound to
			// refuses with 403 instead, which deniesTrust catches above. So a
			// catalog dropped between the listing and this read is an absence,
			// not a failed sync.
			if errors.Is(err, errCatalogGone) {
				provenGone = true

				continue
			}

			return err
		}
		if !complete {
			continue
		}

		encoded, err := json.Marshal(route)
		if err != nil {
			return err
		}
		bag.Push(pagination.PageState{ResourceTypeID: schemaResourceType.Id, ResourceID: string(encoded)})

		return nil
	}
	// A catalog proved gone holds no schemas, whether or not a workspace listed
	// it a moment earlier. The catalog syncer drops the parent in the same sync,
	// so this empties what C1 is already removing. A refusal anywhere
	// in the walk leaves the picture mixed, and nothing is emptied on it.
	if provenGone && lastDeny == nil {
		return nil
	}
	if !sawCatalog {
		// A workspace that does not serve the catalog answers the same way as
		// one whose catalog is gone, so this is an absence only when no
		// workspace refused.
		if sawGone && lastDeny == nil {
			return nil
		}
		msg := fmt.Sprintf("databricks-connector: no workspace in sync scope lists catalog %s of metastore %s", catalogName, metastoreID)
		if lastDeny != nil {
			msg = fmt.Sprintf("%s: %s", msg, lastDeny.Error())
		}
		return status.Errorf(codes.FailedPrecondition, "%s", msg)
	}

	return status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot list every schema in catalog %s; %s",
		catalogName, schemaListRemedy)
}

// schemaListingError returns NotFound only when Databricks names the missing
// catalog; any other refusal leaves the schema listing unknown.
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
// catalog, which ownership, a direct Admin of the owning admins group's workspace,
// MANAGE, USE_SCHEMA or ALL_PRIVILEGES each settle. It reports errCatalogGone when
// Databricks names the catalog missing, which is what tells an absent catalog apart
// from a privilege this caller may not read.
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
	// An owner holds every privilege implicitly, which is not reported as a grant.
	// On the live tenant the connector owns the metastore, so this settles the
	// whole rule with no extra call at all.
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

	// MANAGE on the schema or on the catalog above it makes the privilege list
	// complete, and one effective read reports both.
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

// Grants carries the sync session into the shared path so the workspace-Admin
// check behind the trust rule is read once per workspace per sync rather than
// once per schema.
func (b *schemaBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ctx = context.WithValue(ctx, syncSessionKey{}, attr.Session)

	return b.syncGrants(ctx, resource, attr)
}

func (b *schemaBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	ctx = databricks.WithoutCache(ctx)
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, entitlement, &annos)
	if err != nil {
		if isSecurableGoneSentinel(err) || errors.Is(err, errSecurableGone) {
			return annos, status.Errorf(codes.NotFound, "databricks-connector: schema %s no longer exists",
				entitlement.GetResource().GetId().GetResource())
		}

		return annos, err
	}

	writeAnnos, err := b.grant(ctx, principal.GetId(), routed)

	return append(annos, writeAnnos...), err
}

func (b *schemaBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	ctx = databricks.WithoutCache(ctx)
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, revoked.GetEntitlement(), &annos)
	if err != nil {
		if isSecurableGoneSentinel(err) || errors.Is(err, errSecurableGone) {
			annos.Update(&v2.GrantAlreadyRevoked{})

			return annos, nil
		}

		return annos, err
	}

	writeAnnos, err := b.revoke(ctx, revoked.GetPrincipal().GetId(), routed)

	return append(annos, writeAnnos...), err
}

// entitlementOnLiveRoute rebuilds the entitlement's schema from what Databricks
// reports right now: the metastore owner, the catalog owner, the schema owner and
// a workspace this sync still admits. The three owners are what the trust check
// short-circuits on, and the last sync's copy of them can be hours old, so a
// transfer away from the connector would otherwise let a partial privilege list
// decide a write. The profile is trusted for the route to try first, never for
// standing.
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

	route, catalog, err := b.liveCatalogRoute(ctx, resource, metastoreID, catalogName, owner, annos)
	if err != nil {
		return nil, err
	}

	fullName := catalogName + schemaNameSeparator + schemaName
	schema, rateLimit, err := b.client.GetSchema(ctx, route.Workspace, fullName)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		// Only Databricks naming the securable proves it is gone. A workspace the
		// catalog is not bound to refuses with 403 instead, and reading that as an
		// absence would report a revoke that never ran.
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

	copyEntitlement, ok := proto.Clone(entitlement).(*v2.Entitlement)
	if !ok {
		return nil, status.Error(codes.Internal, "databricks-connector: cannot copy the schema entitlement")
	}
	copyEntitlement.Resource = routed

	return copyEntitlement, nil
}

// liveCatalogRoute picks the workspace the write goes through: the one the last
// sync recorded when this sync still admits it and it still serves the catalog,
// otherwise the first running in-scope workspace that does. A catalog no
// workspace serves is not proof of deletion, because an unbound workspace answers
// the same 404 as a deleted one, so that case refuses rather than reporting the
// grant gone.
func (b *schemaBuilder) liveCatalogRoute(
	ctx context.Context,
	resource *v2.Resource,
	metastoreID, catalogName, metastoreOwner string,
	annos *annotations.Annotations,
) (schemaRoute, *databricks.Catalog, error) {
	if route, ok := b.profileRoute(resource, metastoreOwner); ok {
		catalog, err := catalogThrough(ctx, b.client, route.Workspace, metastoreID, catalogName, annos)
		// The recorded route is an optimization: any answer short of the catalog
		// falls through to the walk, which reads the account listings and settles
		// it there.
		if err == nil && catalog != nil && catalog.MetastoreID == metastoreID {
			return route, catalog, nil
		}
	}

	scope, err := scopedWorkspaces(ctx, b.client, b.workspaces, annos)
	if err != nil {
		return schemaRoute{}, nil, err
	}
	usable, _, err := scope.attached(ctx, b.client, metastoreID, annos)
	if err != nil {
		return schemaRoute{}, nil, assignmentListError(metastoreID, err)
	}
	if len(usable) == 0 {
		return schemaRoute{}, nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: metastore %s has no running workspace in sync scope to write catalog %s through",
			metastoreID, catalogName)
	}

	var lastDeny error
	for _, workspace := range usable {
		catalog, err := catalogThrough(ctx, b.client, workspace.DeploymentName, metastoreID, catalogName, annos)
		switch {
		case err == nil && catalog != nil:
			return schemaRoute{
				Workspace:      workspace.DeploymentName,
				WorkspaceID:    strconv.Itoa(workspace.ID),
				WorkspaceName:  workspace.Name,
				MetastoreOwner: metastoreOwner,
				CatalogOwner:   catalog.Owner,
			}, catalog, nil
		case err == nil, errors.Is(err, errCatalogGone):
		case deniesTrust(err):
			lastDeny = err
		default:
			return schemaRoute{}, nil, err
		}
	}

	msg := fmt.Sprintf("databricks-connector: no workspace in sync scope serves catalog %s of metastore %s, so whether schema %s still exists cannot be proved",
		catalogName, metastoreID, resource.GetId().GetResource())
	if lastDeny != nil {
		return schemaRoute{}, nil, uhttp.WrapErrors(codes.FailedPrecondition, msg, lastDeny)
	}

	return schemaRoute{}, nil, status.Errorf(codes.FailedPrecondition, "%s", msg)
}

// profileRoute is the route an earlier sync wrote, when this sync still admits
// that workspace. The profile carries the three identifiers the configuration
// matches on, so the allowlist and the exclude set are both answered without
// reading the account's workspaces.
func (b *schemaBuilder) profileRoute(resource *v2.Resource, metastoreOwner string) (schemaRoute, bool) {
	profile := rs.GetProfile(resource)
	route := schemaRoute{MetastoreOwner: metastoreOwner}
	route.Workspace, _ = rs.GetProfileStringValue(profile, profileKeyWorkspace)
	route.WorkspaceID, _ = rs.GetProfileStringValue(profile, profileKeyWorkspaceID)
	route.WorkspaceName, _ = rs.GetProfileStringValue(profile, profileKeyWorkspaceName)
	if route.Workspace == "" {
		return schemaRoute{}, false
	}
	if b.client.IsWorkspaceExcluded(route.WorkspaceName, route.Workspace, route.WorkspaceID) {
		return schemaRoute{}, false
	}
	if len(b.workspaces) > 0 {
		if _, ok := matchConfiguredWorkspace(b.workspaces, route.Workspace, route.WorkspaceName, route.WorkspaceID); !ok {
			return schemaRoute{}, false
		}
	}

	return route, true
}
