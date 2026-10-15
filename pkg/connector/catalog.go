package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*catalogBuilder)(nil)

type catalogBuilder struct {
	securableDeps
}

func newCatalogBuilder(client *databricks.Client, uc *unityCatalog, sync syncPrincipals) *catalogBuilder {
	return &catalogBuilder{securableDeps{client: client, uc: uc, sync: sync}}
}

func (b *catalogBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return catalogResourceType
}

var catalogKind = securableKind{
	name:        databricks.SecurableCatalog,
	parse:       parseSecurableRef,
	privileges:  catalogPrivileges,
	writeRemedy: catalogTrustRemedy,
	access: func(ctx context.Context, d *securableDeps, resource *v2.Resource, ref securableRef) (string, accessState, *v2.RateLimitDescription, error) {
		return d.catalogAccess(ctx, resource, ref, true)
	},
}

func (b *catalogBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != metastoreResourceType.Id {
		return nil, nil, nil
	}

	parent := securableRef{metastoreID: parentResourceID.GetResource()}
	annos := annotations.Annotations{}
	items, next, rateLimit, err := pageCatalogs(ctx, b.uc, parent, attr.PageToken.Token)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list catalogs under %s: %w", parent.resourceKey(), err)
	}

	var rv []*v2.Resource
	for _, item := range items {
		childRef := parent.child(item.name)
		built, err := newSecurableResource(catalogResourceType, securableTypeCatalog, nil, item, childRef, parentResourceID)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build %s resource %s: %w", catalogResourceType.Id, childRef.resourceKey(), err)
		}

		rv = append(rv, built)
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: next}, nil
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

	// Owner is not grantable. Ownership is single-valued and has no revoke.
	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a catalog in Databricks"),
	))

	return rv, nil, nil
}

func (b *catalogBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (b *catalogBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ref, err := parseSecurableRef(resource.GetId().GetResource())
	if err != nil {
		return nil, nil, err
	}

	annos := annotations.Annotations{}
	workspace, state, rateLimit, err := b.catalogAccess(ctx, resource, ref, false)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if code := status.Code(err); code == codes.PermissionDenied || code == codes.FailedPrecondition {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if state != securableInScope {
		ctxzap.Extract(ctx).Debug("databricks-connector: catalog is not reachable through a workspace this sync can use; skipping its grants",
			zap.String("catalog", ref.resourceKey()),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	}

	l := ctxzap.Extract(ctx)
	pageToken := attr.PageToken.Token
	assignments, nextPageToken, rateLimit, err := b.client.ListPermissions(
		ctx, workspace, databricks.SecurableCatalog, ref.permissionsName(), "", pageToken)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list permissions on %s %s: %w", databricks.SecurableCatalog, ref.permissionsName(), err)
	}

	owner := ""
	if pageToken == "" {
		owner = profileString(resource, profileKeyOwner)
	}

	principals, principalRateLimit, err := b.uc.principalsForGrants(ctx, assignments, owner)
	noteRateLimit(&annos, principalRateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve account principals: %w", err)
	}

	var rv []*v2.Grant
	for _, assignment := range assignments {
		principalId, ok := principals.lookup(assignment)
		if !ok {
			l.Debug("databricks-connector: skipping a grant whose principal does not resolve",
				zap.String("securable", ref.resourceKey()),
				zap.String("principal", assignment.Principal),
				zap.String("principal_id", assignment.PrincipalID.String()),
			)

			continue
		}

		if !emitPrincipal(b.sync, principalId.GetResourceType()) {
			continue
		}

		options := groupSecurableGrantOptions(principalId)

		for _, assigned := range assignment.Privileges {
			if assigned == "" {
				continue
			}
			if !slices.Contains(catalogPrivileges, assigned) {
				l.Debug("databricks-connector: skipping a privilege the resource does not offer",
					zap.String("securable", ref.resourceKey()),
					zap.String("privilege", assigned),
				)

				continue
			}

			rv = append(rv, grant.NewGrant(resource, assigned, principalId, options...))
		}
	}

	if owner != "" {
		principalId, found := principals.lookupName(owner)
		if !found {
			l.Debug("databricks-connector: securable owner does not resolve to a synced principal",
				zap.String("securable", ref.resourceKey()),
				zap.String("owner", owner),
			)
		} else if emitPrincipal(b.sync, principalId.GetResourceType()) {
			options := groupSecurableGrantOptions(principalId)
			options = append(options, grant.WithAnnotation(&v2.GrantImmutable{}))
			rv = append(rv, grant.NewGrant(resource, ownerEntitlement, principalId, options...))
		}
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextPageToken}, nil
}

func (b *catalogBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return b.grantSecurable(ctx, principal, entitlement, catalogKind)
}

func (b *catalogBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return b.revokeSecurable(ctx, revoked, catalogKind)
}

// catalogPageState is one usable workspace of the metastore, carried in the page
// token so later pages neither rebuild the routing nor re-read other workspaces.
type catalogPageState struct {
	Workspace      string `json:"w"`
	WorkspaceID    string `json:"i"`
	WorkspaceName  string `json:"n"`
	MetastoreOwner string `json:"o"`
}

// pageCatalogs pages a metastore's catalogs one workspace page at a time. A catalog
// several workspaces list is emitted once per workspace; the SDK keeps one row
// per resource id.
func pageCatalogs(
	ctx context.Context,
	uc *unityCatalog,
	parent securableRef,
	pageToken string,
) ([]securable, string, *v2.RateLimitDescription, error) {
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(pageToken); err != nil {
		return nil, "", nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid catalog page token: %v", err)
	}

	var rateLimit *v2.RateLimitDescription
	if bag.Current() == nil {
		snap, routingRateLimit, err := uc.buildRouting(ctx)
		rateLimit = routingRateLimit
		if err != nil {
			return nil, "", rateLimit, err
		}
		workspaces, state, err := usableWorkspaces(snap, parent.metastoreID)
		if err != nil || state != securableInScope {
			return nil, "", rateLimit, err
		}

		// Pushed in reverse so the first workspace is read first.
		for _, workspace := range slices.Backward(workspaces) {
			encoded, err := json.Marshal(catalogPageState{
				Workspace:      workspace,
				WorkspaceID:    snap.workspaceIDs[workspace],
				WorkspaceName:  snap.workspaceNames[workspace],
				MetastoreOwner: snap.metastores[parent.metastoreID].Owner,
			})
			if err != nil {
				return nil, "", rateLimit, err
			}
			bag.Push(pagination.PageState{ResourceTypeID: catalogResourceType.Id, ResourceID: string(encoded)})
		}
	}

	var current catalogPageState
	if err := json.Unmarshal([]byte(bag.ResourceID()), &current); err != nil {
		return nil, "", rateLimit, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid catalog page token: %v", err)
	}

	catalogs, next, listRateLimit, err := uc.client.ListCatalogs(ctx, current.Workspace, bag.PageToken(), ResourcesPageSize)
	if listRateLimit != nil {
		rateLimit = listRateLimit
	}
	if err != nil {
		return nil, "", rateLimit, catalogListingError(parent.metastoreID, current.Workspace, err)
	}

	snap := routingSnapshot{workspaceIDs: map[string]string{current.Workspace: current.WorkspaceID}}
	items := make([]securable, 0, len(catalogs))
	for _, catalog := range catalogs {
		if catalog.Name == "" || (catalog.MetastoreID != "" && catalog.MetastoreID != parent.metastoreID) {
			continue
		}

		ref := parent.child(catalog.Name)
		trusted, trustRateLimit, err := uc.catalogGrantsAreTrustworthy(ctx, snap, current.MetastoreOwner, catalog.Owner, ref, current.Workspace)
		if trustRateLimit != nil {
			rateLimit = trustRateLimit
		}
		if err != nil {
			// CATALOG_DOES_NOT_EXIST is this catalog disappearing between the
			// listing and the probe. Any other 404 must not leave List as
			// NotFound: the SDK would skip the page and drop the catalogs on it.
			if databricks.IsSecurableGone(err, databricks.ErrorCodeCatalogDoesNotExist) {
				ctxzap.Extract(ctx).Debug("databricks-connector: catalog is gone; skipping it",
					zap.String("catalog", catalog.Name),
				)

				continue
			}
			if isNotFoundError(err) {
				return nil, "", rateLimit, uhttp.WrapErrors(codes.FailedPrecondition,
					fmt.Sprintf("databricks-connector: permissions on catalog %s returned 404 without %s, so this is not an absence",
						catalog.Name, databricks.ErrorCodeCatalogDoesNotExist),
					err)
			}

			return nil, "", rateLimit, fmt.Errorf(
				"databricks-connector: failed to confirm grant visibility for catalog %s: %w", catalog.Name, err)
		}
		if !trusted {
			return nil, "", rateLimit, status.Errorf(codes.PermissionDenied,
				"databricks-connector: cannot read every grant on catalog %s: %s", catalog.Name, catalogTrustRemedy)
		}

		items = append(items, securable{
			name:  catalog.Name,
			owner: catalog.Owner,
			profile: map[string]any{
				profileKeyCatalogName:    catalog.Name,
				profileKeyCatalogType:    catalog.CatalogType,
				profileKeyIsolationMode:  catalog.IsolationMode,
				profileKeyMetastoreOwner: current.MetastoreOwner,
				profileKeyWorkspace:      current.Workspace,
				profileKeyWorkspaceID:    current.WorkspaceID,
				profileKeyWorkspaceName:  current.WorkspaceName,
			},
		})
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, "", rateLimit, err
	}

	return items, nextToken, rateLimit, nil
}
