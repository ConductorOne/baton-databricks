package connector

import (
	"context"
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
	"github.com/conductorone/baton-sdk/pkg/session"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*catalogBuilder)(nil)

const (
	securableTypeCatalog = "CATALOG"

	// privilegeManage is the only privilege that lets its holder read every
	// privilege assignment on a catalog without owning it.
	privilegeManage = "MANAGE"

	// errorCodeCatalogGone is what Databricks names in error_code when the
	// catalog a request addresses does not exist.
	errorCodeCatalogGone   = "CATALOG_DOES_NOT_EXIST"
	errorCodeMetastoreGone = "METASTORE_DOES_NOT_EXIST"

	// systemCatalog is the built-in catalog Databricks puts in every metastore.
	// samples is the other one; both are SYSTEM_CATALOG. Both are excluded
	// independently of the connector's ownership.
	systemCatalog = "system"

	// workspaceAdminsGroupPrefix starts the name of the system group Databricks
	// creates for a workspace, _workspace_admins_<workspace>_<workspace id>.
	workspaceAdminsGroupPrefix = "_workspace_admins_"

	profileKeyFullName      = "full_name"
	profileKeyCatalogName   = "catalog_name"
	profileKeyCatalogType   = "catalog_type"
	profileKeyIsolationMode = "isolation_mode"

	// profileKeyMetastoreOwner carries the owner of the metastore the catalog
	// belongs to. Owning the metastore is what makes a privilege read complete,
	// and the catalog syncer is only handed the metastore's id, so List writes
	// the owner down instead of resolving it again on every Grants call.
	profileKeyMetastoreOwner = "metastore_owner"

	// catalogWriteRemedy is what a 403 on the PATCH tells the operator.
	catalogWriteRemedy = "the connector must own the metastore or the catalog, or hold MANAGE on the catalog"

	// catalogTrustRemedy is what a refusal to report a partial privilege list
	// tells the operator.
	catalogTrustRemedy = "the connector's service principal must own the metastore or the catalog, hold MANAGE on the catalog, " +
		"or be assigned Admin directly on the workspace whose admins group owns it"
)

// catalogPrivileges is the widest privilege set in Unity Catalog: BROWSE,
// CREATE_SCHEMA and USE_CATALOG stop here. Canonical UPPER_SNAKE names, the form
// the permissions API answers with. The PATCH accepts and echoes every entry,
// including ones missing from the databricks-sdk-go Privilege enum.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
var catalogPrivileges = []string{
	"ALL_PRIVILEGES",
	"APPLY_TAG",
	"BROWSE",
	"CREATE_SCHEMA",
	"USE_CATALOG",
	"USE_SCHEMA",
	"CREATE_FUNCTION",
	"CREATE_TABLE",
	"CREATE_MATERIALIZED_VIEW",
	"CREATE_MODEL",
	"CREATE_VOLUME",
	"CREATE_FEATURE",
	"CREATE_SECRET",
	"CREATE_SERVICE",
	"SELECT",
	"MODIFY",
	"DELETE",
	"INSERT",
	"UPDATE",
	"EXECUTE",
	"REFRESH",
	"READ_VOLUME",
	"WRITE_VOLUME",
	"READ_METADATA",
	"READ_FEATURE",
	"READ_SECRET",
	"REFERENCE_SECRET",
	"WRITE_SECRET",
	"EXTERNAL_USE_SCHEMA",
	"MANAGE",
}

var catalogLevel = securableLevel{
	securableType:   databricks.SecurableCatalog,
	noun:            "catalog",
	privileges:      catalogPrivileges,
	writeRemedy:     catalogWriteRemedy,
	trustRemedy:     catalogTrustRemedy,
	permissionsName: catalogPermissionsName,
	trusts:          catalogGrantsAreTrustworthy,
	listProvesTrust: true,
	goneErrorCodes:  []string{errorCodeCatalogGone},
}

var errCatalogGone = errors.New("catalog no longer exists")

type catalogBuilder struct {
	securableOps
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
}

func newCatalogBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *catalogBuilder {
	return &catalogBuilder{securableOps: securableOps{client: client, clientID: clientID, sync: sync, level: catalogLevel}, workspaces: workspaces}
}

func (b *catalogBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return catalogResourceType
}

// catalogResourceID keys a catalog by its metastore. Catalog names repeat across
// metastores, so the name alone would merge two catalogs into one resource.
func catalogResourceID(metastoreID, name string) string {
	return metastoreID + securableKeySeparator + name
}

// parseCatalogID reads a catalog resource id, "{metastore_id}::{catalog_name}".
func parseCatalogID(resourceID string) (string, string, error) {
	metastoreID, name, ok := strings.Cut(resourceID, securableKeySeparator)
	if !ok || metastoreID == "" || name == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid catalog resource id %q", resourceID)
	}

	return metastoreID, name, nil
}

// catalogPermissionsName: the permissions endpoint addresses a catalog by its
// bare name, which is also its full name. The metastore is carried by the host.
func catalogPermissionsName(resourceID string) (string, error) {
	_, name, err := parseCatalogID(resourceID)

	return name, err
}

func catalogResource(
	catalog *databricks.Catalog,
	metastoreID string,
	parent *v2.ResourceId,
	route catalogRoute,
) (*v2.Resource, error) {
	profile := map[string]any{
		profileKeyMetastoreID:    metastoreID,
		profileKeyMetastoreOwner: route.MetastoreOwner,
		profileKeyFullName:       catalog.Name,
		profileKeyCatalogName:    catalog.Name,
		profileKeyCatalogType:    catalog.CatalogType,
		profileKeyIsolationMode:  catalog.IsolationMode,
		profileKeyOwner:          catalog.Owner,
		profileKeySecurableType:  securableTypeCatalog,
		profileKeyWorkspace:      route.Workspace,
		profileKeyWorkspaceID:    route.WorkspaceID,
		profileKeyWorkspaceName:  route.WorkspaceName,
	}

	resourceID := catalogResourceID(metastoreID, catalog.Name)

	return rs.NewAppResource(
		catalog.Name,
		catalogResourceType,
		resourceID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: schemaResourceType.Id},
			&v2.RawId{Id: resourceID},
		),
	)
}

// catalogRoute identifies one running in-scope workspace serving a metastore.
type catalogRoute struct {
	Workspace     string `json:"w"`
	WorkspaceID   string `json:"i"`
	WorkspaceName string `json:"n"`
	// MetastoreOwner rides along so each catalog's profile can carry it. See
	// profileKeyMetastoreOwner.
	MetastoreOwner string `json:"o"`
}

// List reads a metastore's catalogs through every running in-scope workspace.
// A catalog bound to only one of them is absent from the others.
func (b *catalogBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != metastoreResourceType.Id {
		return nil, nil, nil
	}

	metastoreID := parentResourceID.GetResource()
	annos := annotations.Annotations{}
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(attr.PageToken.Token); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid catalog page token: %v", err)
	}

	ctx = context.WithValue(ctx, syncSessionKey{}, attr.Session)
	routes, err := b.catalogRoutes(ctx, metastoreID, attr.Session, &annos)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}
	if bag.Current() == nil {
		// The bag is a stack, so the routes go in backward to come out in order.
		// The last one walked is the copy that survives the upsert, which is what
		// puts its workspace on the profile Grants and provisioning read back.
		for _, route := range slices.Backward(routes) {
			bag.Push(pagination.PageState{ResourceTypeID: catalogResourceType.Id, ResourceID: route.Workspace})
		}
		if bag.Current() == nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}
	}
	routeIndex := slices.IndexFunc(routes, func(route catalogRoute) bool { return route.Workspace == bag.ResourceID() })
	if routeIndex < 0 {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: catalog page token names an unknown workspace %s", bag.ResourceID())
	}
	route := routes[routeIndex]

	catalogs, next, rateLimit, err := b.client.ListCatalogs(ctx, route.Workspace, bag.PageToken(), ResourcesPageSize, false)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, catalogListingError(metastoreID, route.Workspace, err)
	}

	var rv []*v2.Resource
	for _, catalog := range catalogs {
		if catalog.Name == "" || (catalog.MetastoreID != "" && catalog.MetastoreID != metastoreID) {
			continue
		}

		if catalog.Name == systemCatalog || catalog.CatalogType == "SYSTEM_CATALOG" {
			continue
		}

		// A partial privilege list is indistinguishable from an empty one, and
		// leaving the catalog out is not the safe half: ConductorOne deletes every
		// resource a sync stops emitting, so the catalog, its grants, and its
		// schemas, tables and volumes all go. The sync stops instead and keeps them.
		trustworthy, err := catalogGrantsAreTrustworthy(ctx, b.client, securableTrust{
			ownPrincipal:   b.clientID,
			owner:          catalog.Owner,
			metastoreOwner: route.MetastoreOwner,
			fullName:       catalog.Name,
			workspace:      route.Workspace,
			workspaceID:    route.WorkspaceID,
		}, &annos)
		if err != nil {
			if errors.Is(err, errCatalogGone) {
				continue
			}
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if !trustworthy {
			return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.PermissionDenied,
				"databricks-connector: cannot read every privilege on catalog %s through workspace %s; %s",
				catalog.Name, route.Workspace, catalogTrustRemedy)
		}

		built, err := catalogResource(&catalog, metastoreID, parentResourceID, route)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build the resource for catalog %s: %w", catalog.Name, err)
		}
		rv = append(rv, built)
	}

	if next != "" && next == bag.PageToken() {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.Internal,
			"databricks-connector: listing catalogs of metastore %s repeated page token %q", metastoreID, next)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
}

// catalogRoutes reuses the metastore's routes from this sync. A targeted sync
// without a metastore List resolves them live and remembers the result.
func (b *catalogBuilder) catalogRoutes(ctx context.Context, metastoreID string, store sessions.SessionStore, annos *annotations.Annotations) ([]catalogRoute, error) {
	key := catalogRoutesKey(metastoreID)
	routes, found, err := session.GetJSON[[]catalogRoute](ctx, store, key)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to read catalog routes for metastore %s: %w", metastoreID, err)
	}
	if found {
		return routes, nil
	}
	routes, err = b.resolveCatalogRoutes(ctx, metastoreID, annos)
	if err != nil {
		return nil, err
	}
	if err := session.SetJSON(ctx, store, key, routes); err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to store catalog routes for metastore %s: %w", metastoreID, err)
	}
	return routes, nil
}

func (b *catalogBuilder) resolveCatalogRoutes(ctx context.Context, metastoreID string, annos *annotations.Annotations) ([]catalogRoute, error) {
	scope, err := scopedWorkspaces(ctx, b.client, b.workspaces, annos)
	if err != nil {
		return nil, err
	}
	usable, inScope, err := scope.attached(ctx, b.client, metastoreID, annos)
	if errors.Is(err, errMetastoreGone) {
		return nil, nil
	}
	if err != nil {
		return nil, assignmentListError(metastoreID, err)
	}
	if len(usable) == 0 {
		if inScope {
			return nil, status.Errorf(codes.FailedPrecondition, "databricks-connector: metastore %s has no running workspace in sync scope", metastoreID)
		}
		return nil, nil
	}
	owner, err := metastoreOwner(ctx, b.client, metastoreID, annos)
	if errors.Is(err, errMetastoreGone) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return catalogRoutesForWorkspaces(usable, owner), nil
}

// metastoreOwner reads the owner of the metastore the catalogs hang from. It is
// resolved once per metastore, with the route, because owning the metastore is
// what makes a catalog's privilege read complete. A metastore the account no
// longer lists is reported as errMetastoreGone: the account listing is what
// proves the metastore is gone, and provisioning answers for a deleted parent
// instead of failing every task against it.
func metastoreOwner(
	ctx context.Context,
	client *databricks.Client,
	metastoreID string,
	annos *annotations.Annotations,
) (string, error) {
	metastores, rateLimit, err := client.ListMetastores(ctx)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return "", refuseUnreadable("databricks-connector: failed to list metastores", err)
	}

	for _, metastore := range metastores {
		if metastore.MetastoreID == metastoreID {
			return metastore.Owner, nil
		}
	}

	ctxzap.Extract(ctx).Debug("databricks-connector: the account no longer lists the metastore",
		zap.String("metastore", metastoreID))

	return "", errMetastoreGone
}

// catalogThrough reads one catalog through workspace, nil when that workspace
// does not serve it. A single GET replaces draining the listing, which cost one
// page walk per catalog per workspace, and it separates the two answers the
// listing ran together: a refusal naming the catalog as nonexistent is an
// absence, and anything else is this workspace not seeing a catalog that may
// well exist.
func catalogThrough(
	ctx context.Context,
	client *databricks.Client,
	workspace, metastoreID, catalogName string,
	annos *annotations.Annotations,
) (*databricks.Catalog, error) {
	catalog, rateLimit, err := client.GetCatalog(ctx, workspace, catalogName)
	noteRateLimit(annos, rateLimit)
	switch {
	case err == nil:
	case databricks.IsSecurableGone(err, errorCodeCatalogGone):
		return nil, errCatalogGone
	case deniesTrust(err):
		return nil, err
	case status.Code(err) == codes.InvalidArgument:
		// The name does not survive URL assembly, or Databricks answered with a
		// different catalog. Either way the answer does not describe the catalog
		// asked for, and a provisioning write must not be addressed from it.
		return nil, err
	default:
		return nil, catalogListingError(metastoreID, workspace, err)
	}

	// A catalog served by this workspace but belonging to another metastore is
	// not this metastore's catalog.
	if catalog.MetastoreID != "" && catalog.MetastoreID != metastoreID {
		return nil, nil
	}

	return catalog, nil
}

// catalogListingError reads a workspace that refuses the catalog listing as
// unknown, not as listing nothing: a short list would read as deletions in C1.
func catalogListingError(metastoreID, workspace string, err error) error {
	switch apiStatus(err) {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: the catalogs of metastore %s cannot be listed through attached workspace %s, "+
				"so which catalogs are in scope is unknown; make the connector principal an Admin of that workspace, "+
				"or leave the workspace out with workspaces or databricks-exclude-workspaces", metastoreID, workspace),
			err)
	}

	return fmt.Errorf("databricks-connector: failed to list the catalogs of metastore %s through workspace %s: %w",
		metastoreID, workspace, err)
}

func (b *catalogBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(catalogPrivileges)+1)
	for _, privilege := range catalogPrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a catalog in Databricks", privilege)),
		))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a catalog in Databricks"),
	))

	return rv, nil, nil
}

func (b *catalogBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// catalogGrantsAreTrustworthy is true for the metastore owner, the catalog
// owner, a holder of MANAGE, or a direct Admin when the workspace admins group
// owns the catalog.
func catalogGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	// The metastore's owner is its admin, and reads every privilege below it.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) || ownsPrincipal(trust.ownPrincipal, trust.owner) {
		return true, nil
	}

	// The workspace admins group acts as the owner for a direct Admin of its workspace.
	if ownerWorkspaceID, ok := catalogAdminWorkspaceID(trust.owner); ok {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, ownerWorkspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	manages, err := holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, trust.fullName,
		managePrivilege, []string{errorCodeCatalogGone}, annos)
	if errors.Is(err, errSecurableGone) {
		return false, errCatalogGone
	}

	return manages, err
}

func catalogAdminWorkspaceID(owner string) (string, bool) {
	if !strings.HasPrefix(owner, workspaceAdminsGroupPrefix) {
		return "", false
	}
	separator := strings.LastIndex(owner, "_")
	if separator <= len(workspaceAdminsGroupPrefix) || separator == len(owner)-1 {
		return "", false
	}
	id := owner[separator+1:]
	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		return "", false
	}

	return id, true
}

func (b *catalogBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return b.syncGrants(ctx, resource, attr)
}

func (b *catalogBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	ctx = databricks.WithoutCache(ctx)
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, entitlement, &annos)
	if err != nil {
		if errors.Is(err, errMetastoreGone) || errors.Is(err, errCatalogGone) {
			return annos, status.Errorf(codes.NotFound, "databricks-connector: catalog %s is no longer on metastore %s",
				entitlement.GetResource().GetId().GetResource(), entitlement.GetResource().GetParentResourceId().GetResource())
		}
		return annos, err
	}

	return b.grant(ctx, principal.GetId(), routed)
}

func (b *catalogBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	ctx = databricks.WithoutCache(ctx)
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, revoked.GetEntitlement(), &annos)
	if err != nil {
		if errors.Is(err, errMetastoreGone) || errors.Is(err, errCatalogGone) {
			annos.Update(&v2.GrantAlreadyRevoked{})
			return annos, nil
		}
		return annos, err
	}

	return b.revoke(ctx, revoked.GetPrincipal().GetId(), routed)
}

// entitlementOnLiveRoute returns the entitlement addressed at a workspace. The
// workspace an earlier sync wrote on the profile is tried first, with one catalog
// read and no account listing: that read answers what the two listings were asked
// for, since a workspace that returns this metastore's catalog is by construction
// running and still serving it. The listings run only when the profile names no
// workspace, this sync no longer admits it, or it no longer serves the catalog.
func (b *catalogBuilder) entitlementOnLiveRoute(
	ctx context.Context,
	entitlement *v2.Entitlement,
	annos *annotations.Annotations,
) (*v2.Entitlement, error) {
	ctx = databricks.WithoutCache(ctx)
	resource := entitlement.GetResource()
	metastoreID, catalogName, err := parseCatalogID(resource.GetId().GetResource())
	if err != nil {
		return nil, err
	}
	// The owner decides trust and whether a deletion can be proved, so it is read
	// live on both paths. The profile is trusted for the route, never for standing.
	owner, err := metastoreOwner(ctx, b.client, metastoreID, annos)
	if err != nil {
		return nil, err
	}

	var probe catalogProbe
	if route, ok := b.profileRoute(resource, owner); ok {
		catalog, err := catalogThrough(ctx, b.client, route.Workspace, metastoreID, catalogName, annos)
		// This read is an optimization, so no answer it gives is fatal: a catalog
		// absent from this one workspace is not absent from the metastore, a refusal
		// here is not a refusal everywhere, and a workspace that has stopped serving
		// is what the walk exists to route around. Anything short of the catalog
		// falls through, and only the walk's own errors reach the caller.
		switch {
		case err == nil:
		case errors.Is(err, errCatalogGone):
			probe = catalogProbe{workspace: route.Workspace, gone: true}
		case deniesTrust(err):
			probe = catalogProbe{workspace: route.Workspace, deny: err}
		}
		switch {
		// Skipping the attachment listing means the catalog has to name this
		// metastore itself: a workspace that serves it is attached to the metastore
		// that owns it. An answer that omits the id proves nothing, so it falls
		// through to the walk and is read again there, where the listings ran.
		case catalog != nil && catalog.MetastoreID == metastoreID:
			return routedEntitlement(entitlement, catalog, metastoreID, route)
		case catalog != nil:
		case err == nil:
			probe = catalogProbe{workspace: route.Workspace}
		}
	}

	scope, err := scopedWorkspaces(ctx, b.client, b.workspaces, annos)
	if err != nil {
		return nil, err
	}
	usable, _, err := scope.attached(ctx, b.client, metastoreID, annos)
	if err != nil {
		return nil, assignmentListError(metastoreID, err)
	}
	if len(usable) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: metastore %s has no running workspace in sync scope to prove whether catalog %s was deleted",
			metastoreID, catalogName)
	}

	workspace, catalog, err := workspaceForCatalog(ctx, b.client, metastoreID, catalogName, usable, probe, annos)
	if err != nil {
		if errors.Is(err, errCatalogGone) {
			return nil, b.confirmCatalogGone(ctx, metastoreID, catalogName, usable[0].DeploymentName, owner, annos)
		}
		return nil, err
	}

	return routedEntitlement(entitlement, catalog, metastoreID, catalogRoute{
		Workspace:      workspace.DeploymentName,
		WorkspaceID:    strconv.Itoa(workspace.ID),
		WorkspaceName:  workspace.Name,
		MetastoreOwner: owner,
	})
}

// catalogProbe is the outcome of the one read entitlementOnLiveRoute makes
// through the profile workspace. The walk carries it so that workspace is not
// read a second time and its answer still counts toward "gone everywhere".
// A zero value means nothing was read and the walk reads every workspace.
type catalogProbe struct {
	workspace string
	gone      bool
	deny      error
}

// profileRoute is the route an earlier sync wrote, when this sync still admits
// that workspace. The profile carries the three identifiers the configuration
// matches on, so the allowlist and the exclude set are both answered here without
// reading the account's workspaces or the metastore's assignments.
func (b *catalogBuilder) profileRoute(resource *v2.Resource, owner string) (catalogRoute, bool) {
	profile := rs.GetProfile(resource)
	route := catalogRoute{MetastoreOwner: owner}
	route.Workspace, _ = rs.GetProfileStringValue(profile, profileKeyWorkspace)
	route.WorkspaceID, _ = rs.GetProfileStringValue(profile, profileKeyWorkspaceID)
	route.WorkspaceName, _ = rs.GetProfileStringValue(profile, profileKeyWorkspaceName)
	if route.Workspace == "" {
		return catalogRoute{}, false
	}
	if b.client.IsWorkspaceExcluded(route.WorkspaceName, route.Workspace, route.WorkspaceID) {
		return catalogRoute{}, false
	}
	if len(b.workspaces) > 0 {
		if _, ok := matchConfiguredWorkspace(b.workspaces, route.Workspace, route.WorkspaceName, route.WorkspaceID); !ok {
			return catalogRoute{}, false
		}
	}

	return route, true
}

// routedEntitlement copies the entitlement onto the catalog as read through route.
func routedEntitlement(
	entitlement *v2.Entitlement,
	catalog *databricks.Catalog,
	metastoreID string,
	route catalogRoute,
) (*v2.Entitlement, error) {
	routed, err := catalogResource(catalog, metastoreID, entitlement.GetResource().GetParentResourceId(), route)
	if err != nil {
		return nil, err
	}

	copyEntitlement := proto.Clone(entitlement).(*v2.Entitlement)
	copyEntitlement.Resource = routed

	return copyEntitlement, nil
}

func (b *catalogBuilder) confirmCatalogGone(
	ctx context.Context,
	metastoreID, catalogName, workspace, metastoreOwner string,
	annos *annotations.Annotations,
) error {
	ctx = databricks.WithoutCache(ctx)
	if !ownsPrincipal(b.clientID, metastoreOwner) || workspace == "" {
		return status.Errorf(codes.FailedPrecondition,
			"databricks-connector: cannot prove whether catalog %s of metastore %s was deleted; the metastore owner must list it with include_unbound=true",
			catalogName, metastoreID)
	}

	pageToken := ""
	for {
		catalogs, next, rateLimit, err := b.client.ListCatalogs(ctx, workspace, pageToken, ResourcesPageSize, true)
		noteRateLimit(annos, rateLimit)
		if err != nil {
			msg := fmt.Sprintf("databricks-connector: cannot prove whether catalog %s of metastore %s was deleted with include_unbound=true",
				catalogName, metastoreID)
			if deniesTrust(err) {
				return uhttp.WrapErrors(codes.FailedPrecondition, msg, err)
			}
			return fmt.Errorf("%s: %w", msg, err)
		}
		for _, catalog := range catalogs {
			if catalog.Name == catalogName && (catalog.MetastoreID == "" || catalog.MetastoreID == metastoreID) {
				return status.Errorf(codes.FailedPrecondition,
					"databricks-connector: catalog %s still exists but no workspace in sync scope can list it",
					catalogName)
			}
		}
		if next == "" {
			return errCatalogGone
		}
		if next == pageToken {
			return status.Errorf(codes.Internal, "databricks-connector: listing unbound catalogs repeated page token %q", next)
		}
		pageToken = next
	}
}
