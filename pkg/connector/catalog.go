package connector

import (
	"context"
	"encoding/json"
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

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*catalogBuilder)(nil)

const (
	securableTypeCatalog = "CATALOG"

	// privilegeManage is the only privilege that lets its holder read every
	// privilege assignment on a catalog without owning it.
	privilegeManage = "MANAGE"

	// errorCodeCatalogGone is what Databricks names in error_code when the
	// catalog a request addresses does not exist.
	errorCodeCatalogGone = "CATALOG_DOES_NOT_EXIST"

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
	goneErrorCodes:  []string{errorCodeCatalogGone},
}

type catalogBuilder struct {
	client   *databricks.Client
	clientID string
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
	sync       syncPrincipals
	// seen drops a catalog an earlier workspace in this sync already emitted.
	seen map[string]struct{}
}

func newCatalogBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *catalogBuilder {
	return &catalogBuilder{client: client, clientID: clientID, workspaces: workspaces, sync: sync}
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
		rs.WithAnnotation(&v2.RawId{Id: resourceID}),
	)
}

// catalogRoute is one workspace the metastore's catalogs are listed through,
// carried in the page token so later pages neither re-resolve the route nor
// re-read the workspaces already walked.
type catalogRoute struct {
	Workspace     string `json:"w"`
	WorkspaceID   string `json:"i"`
	WorkspaceName string `json:"n"`
	// MetastoreOwner rides along so each catalog's profile can carry it. See
	// profileKeyMetastoreOwner.
	MetastoreOwner string `json:"o"`
}

// List reads a metastore's catalogs through the one workspace the route picked.
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

	if bag.Current() == nil {
		if err := b.pushRoute(ctx, metastoreID, bag, &annos); err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if bag.Current() == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: metastore has no workspace in sync scope; listing no catalogs",
				zap.String("metastore", metastoreID))

			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}
	}

	var route catalogRoute
	if err := json.Unmarshal([]byte(bag.ResourceID()), &route); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid catalog page token: %v", err)
	}

	catalogs, next, rateLimit, err := b.client.ListCatalogs(ctx, route.Workspace, bag.PageToken(), ResourcesPageSize)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, catalogListingError(metastoreID, route.Workspace, err)
	}

	var rv []*v2.Resource
	for _, catalog := range catalogs {
		if catalog.Name == "" || (catalog.MetastoreID != "" && catalog.MetastoreID != metastoreID) {
			continue
		}

		key := catalogResourceID(metastoreID, catalog.Name)
		if _, ok := b.seen[key]; ok {
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
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if !trustworthy {
			return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.PermissionDenied,
				"databricks-connector: cannot read every privilege on catalog %s through workspace %s; %s",
				catalog.Name, route.Workspace, catalogTrustRemedy)
		}

		if b.seen == nil {
			b.seen = map[string]struct{}{}
		}
		b.seen[key] = struct{}{}

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

// pushRoute records every running in-scope workspace the catalogs are read
// through. A catalog bound to only one of them is absent from the others, so
// listing a single workspace would make ConductorOne delete it. An empty stack
// means no workspace this sync can use serves the metastore.
func (b *catalogBuilder) pushRoute(ctx context.Context, metastoreID string, bag *pagination.Bag, annos *annotations.Annotations) error {
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
	if err != nil {
		return err
	}

	// The bag pops the last state first, so push the later workspaces first.
	// A catalog listed by more than one workspace is then kept from the
	// earliest one in assignment order.
	for i := len(usable) - 1; i >= 0; i-- {
		workspace := usable[i]
		encoded, err := json.Marshal(catalogRoute{
			Workspace:      workspace.DeploymentName,
			WorkspaceID:    strconv.Itoa(workspace.ID),
			WorkspaceName:  workspace.Name,
			MetastoreOwner: owner,
		})
		if err != nil {
			return err
		}
		bag.Push(pagination.PageState{ResourceTypeID: catalogResourceType.Id, ResourceID: string(encoded)})
	}

	return nil
}

// metastoreOwner reads the owner of the metastore the catalogs hang from. It is
// resolved once per metastore, with the route, because owning the metastore is
// what makes a catalog's privilege read complete. A metastore the account no
// longer lists has no owner to compare, which leaves the other rules to settle it.
func metastoreOwner(
	ctx context.Context,
	client *databricks.Client,
	metastoreID string,
	annos *annotations.Annotations,
) (string, error) {
	metastores, rateLimit, err := client.ListMetastores(ctx)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return "", fmt.Errorf("databricks-connector: failed to list metastores: %w", err)
	}

	for _, metastore := range metastores {
		if metastore.MetastoreID == metastoreID {
			return metastore.Owner, nil
		}
	}

	ctxzap.Extract(ctx).Debug("databricks-connector: the account no longer lists the metastore, so its owner is unknown",
		zap.String("metastore", metastoreID))

	return "", nil
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
	case databricks.IsSecurableGone(err, errorCodeCatalogGone), deniesTrust(err):
		return nil, nil
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

	// The workspace admins group acts as the owner for a direct Admin of that workspace.
	if workspaceAdminsGroupOwns(trust.owner, trust.workspaceID) {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, trust.workspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	return holdsCatalogManage(ctx, client, trust, annos)
}

// workspaceAdminsGroupOwns reports whether owner is the system group Databricks
// creates for a workspace, named _workspace_admins_<workspace>_<workspace id>.
// That group is the default owner of the workspace catalog, and an object's owner
// sees every grant on it. A direct Admin of the workspace is treated as a member
// of the group; the group's membership itself is not listed.
func workspaceAdminsGroupOwns(owner, workspaceID string) bool {
	if owner == "" || workspaceID == "" {
		return false
	}

	return strings.HasPrefix(owner, workspaceAdminsGroupPrefix) && strings.HasSuffix(owner, "_"+workspaceID)
}

// holdsCatalogManage asks the permissions endpoint for the connector's own
// effective privileges on the catalog. The principal filter is effective, not
// literal: a row can be the MANAGE a group holds and this principal inherits, so
// only the privileges are read and never the principal field. The read bypasses
// the cache, because a remembered page would decide this call with the access of
// an earlier one.
//
// ALL_PRIVILEGES is not enough. Databricks leaves MANAGE out of it, so its holder
// still reads a partial list.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
func holdsCatalogManage(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	if trust.ownPrincipal == "" || trust.workspace == "" {
		return false, nil
	}

	var manages bool
	rateLimit, err := client.ForEachUncachedPermissionsPage(ctx, trust.workspace, databricks.SecurableCatalog, trust.fullName, trust.ownPrincipal,
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			for _, assignment := range page {
				if slices.Contains(assignment.Privileges, privilegeManage) {
					manages = true

					return true, nil
				}
			}

			return false, nil
		})
	noteRateLimit(annos, rateLimit)
	if err != nil {
		if !deniesTrust(err) {
			return false, fmt.Errorf("databricks-connector: failed to read the connector's own privileges on catalog %s: %w", trust.fullName, err)
		}
		ctxzap.Extract(ctx).Debug("databricks-connector: the connector's own privileges on the catalog cannot be read, so MANAGE stays unproven",
			zap.String("catalog", trust.fullName),
			zap.Error(err))

		return false, nil
	}

	return manages, nil
}

func (b *catalogBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return syncSecurableGrants(ctx, b.client, b.clientID, b.sync, catalogLevel, resource, attr)
}

func (b *catalogBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	routed, err := b.entitlementOnLiveRoute(ctx, entitlement, &annos)
	if err != nil {
		return annos, err
	}

	return grantSecurable(ctx, b.client, b.clientID, catalogLevel, principal, routed)
}

func (b *catalogBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	// A catalog no workspace lists is not a catalog proven gone, so this does not
	// answer GrantAlreadyRevoked: the privilege would stay in Databricks while
	// ConductorOne recorded it as revoked. A failed revoke is retried, and a
	// catalog that really was deleted leaves C1 through the next sync.
	routed, err := b.entitlementOnLiveRoute(ctx, revoked.GetEntitlement(), &annos)
	if err != nil {
		return annos, err
	}

	grantCopy := proto.Clone(revoked).(*v2.Grant)
	grantCopy.Entitlement = routed

	return revokeSecurable(ctx, b.client, b.clientID, catalogLevel, grantCopy)
}

// entitlementOnLiveRoute copies the entitlement with the workspace resolved from
// the catalog id. A provisioning call carries ids, not the profile List wrote,
// and that profile can name a workspace that no longer lists the catalog.
func (b *catalogBuilder) entitlementOnLiveRoute(
	ctx context.Context,
	entitlement *v2.Entitlement,
	annos *annotations.Annotations,
) (*v2.Entitlement, error) {
	resource := entitlement.GetResource()
	metastoreID, catalogName, err := parseCatalogID(resource.GetId().GetResource())
	if err != nil {
		return nil, err
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
			"databricks-connector: metastore %s has no running workspace in sync scope", metastoreID)
	}

	workspace, catalog, err := workspaceForCatalog(ctx, b.client, metastoreID, catalogName, usable, annos)
	if err != nil {
		return nil, err
	}

	owner, err := metastoreOwner(ctx, b.client, metastoreID, annos)
	if err != nil {
		return nil, err
	}
	routed, err := catalogResource(catalog, metastoreID, resource.GetParentResourceId(), catalogRoute{
		Workspace:      workspace.DeploymentName,
		WorkspaceID:    strconv.Itoa(workspace.ID),
		WorkspaceName:  workspace.Name,
		MetastoreOwner: owner,
	})
	if err != nil {
		return nil, err
	}

	copyEntitlement := proto.Clone(entitlement).(*v2.Entitlement)
	copyEntitlement.Resource = routed

	return copyEntitlement, nil
}
