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

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*schemaBuilder)(nil)

type schemaBuilder struct {
	securableDeps
}

func newSchemaBuilder(client *databricks.Client, uc *unityCatalog, sync syncPrincipals) *schemaBuilder {
	return &schemaBuilder{securableDeps{client: client, uc: uc, sync: sync}}
}

func (b *schemaBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return schemaResourceType
}

var schemaKind = securableKind{
	name:        databricks.SecurableSchema,
	parse:       parseSecurableRef,
	privileges:  schemaPrivileges,
	writeRemedy: catalogTrustRemedy,
	goneCodes:   []string{databricks.ErrorCodeSchemaDoesNotExist},
	access: func(ctx context.Context, d *securableDeps, resource *v2.Resource, ref securableRef) (string, accessState, *v2.RateLimitDescription, error) {
		return d.catalogAccess(ctx, resource, ref, true)
	},
}

// schemaPageState is the workspace a catalog's schemas are read through, carried
// in the page token so later pages do not rebuild the routing.
type schemaPageState struct {
	Workspace      string `json:"w"`
	WorkspaceID    string `json:"i"`
	WorkspaceName  string `json:"n"`
	MetastoreOwner string `json:"o"`
	CatalogOwner   string `json:"c"`
}

func (b *schemaBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != catalogResourceType.Id {
		return nil, nil, nil
	}

	parent, err := parseSecurableRef(parentResourceID.GetResource())
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse schema parent %q: %w", parentResourceID.GetResource(), err)
	}

	annos := annotations.Annotations{}
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(attr.PageToken.Token); err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema page token: %v", err)
	}

	var rateLimit *v2.RateLimitDescription
	if bag.Current() == nil {
		current, openRateLimit, err := b.openSchemaListing(ctx, parent)
		rateLimit = openRateLimit
		noteRateLimit(&annos, rateLimit)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if current == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: parent catalog is no longer listed; skipping its schemas",
				zap.String("catalog", parent.catalog()),
			)

			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}

		encoded, err := json.Marshal(current)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		bag.Push(pagination.PageState{ResourceTypeID: schemaResourceType.Id, ResourceID: string(encoded)})
	}

	var current schemaPageState
	if err := json.Unmarshal([]byte(bag.ResourceID()), &current); err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema page token: %v", err)
	}

	schemas, next, listRateLimit, err := b.client.ListSchemas(ctx, current.Workspace, parent.catalog(), bag.PageToken(), ResourcesPageSize)
	if listRateLimit != nil {
		rateLimit = listRateLimit
	}
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		// access already confirmed the catalog. A NotFound here is not "this
		// catalog has no schemas": the SDK would skip it as a warning and C1
		// would drop the schemas it already has.
		if isNotFoundError(err) {
			return nil, &rs.SyncOpResults{Annotations: annos}, uhttp.WrapErrors(codes.Unavailable,
				fmt.Sprintf("databricks-connector: catalog %s is listed, but its schemas could not be read through workspace %s",
					parent.catalog(), current.Workspace),
				err)
		}

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list schemas under %s: %w", parent.resourceKey(), err)
	}

	var rv []*v2.Resource
	for _, schema := range schemas {
		if schema.Name == "" {
			continue
		}
		childRef := parent.child(schema.Name)
		if isUnderInformationSchema(childRef) {
			continue
		}

		built, err := newSecurableResource(schemaResourceType, securableTypeSchema, []*v2.ResourceType{tableResourceType, volumeResourceType}, securable{
			name:  schema.Name,
			owner: schema.Owner,
			profile: map[string]any{
				profileKeyCatalogName:    parent.catalog(),
				profileKeySchemaName:     schema.Name,
				profileKeyMetastoreOwner: current.MetastoreOwner,
				profileKeyWorkspace:      current.Workspace,
				profileKeyWorkspaceID:    current.WorkspaceID,
				profileKeyWorkspaceName:  current.WorkspaceName,
			},
		}, childRef, parentResourceID)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build %s resource %s: %w", schemaResourceType.Id, childRef.resourceKey(), err)
		}
		rv = append(rv, built)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
}

// openSchemaListing picks a workspace whose schema listing is complete. nil
// means the parent catalog is not listed by this sync.
func (b *schemaBuilder) openSchemaListing(ctx context.Context, parent securableRef) (*schemaPageState, *v2.RateLimitDescription, error) {
	return b.openListedCatalog(ctx, parent, "schema")
}

// openListedCatalog picks a workspace whose schema listing is complete. Tables
// and volumes are listed through that same workspace: a short schema listing is
// already a failed sync, and a later page must not rebuild the routing.
// nil means the parent catalog is not listed by this sync.
func (d *securableDeps) openListedCatalog(ctx context.Context, parent securableRef, noun string) (*schemaPageState, *v2.RateLimitDescription, error) {
	snap, rateLimit, err := d.uc.buildRouting(ctx)
	if err != nil {
		return nil, rateLimit, fmt.Errorf("databricks-connector: failed to resolve the workspace that lists %ss under %s: %w", noun, parent.resourceKey(), err)
	}

	catalogs, state, catalogRateLimit, err := d.uc.catalogsOf(ctx, snap, parent.metastoreID)
	if catalogRateLimit != nil {
		rateLimit = catalogRateLimit
	}
	if err != nil {
		return nil, rateLimit, fmt.Errorf("databricks-connector: failed to resolve the workspace that lists %ss under %s: %w", noun, parent.resourceKey(), err)
	}
	if state != securableInScope {
		return nil, rateLimit, nil
	}

	facts, ok := catalogs[parent.catalog()]
	if !ok {
		return nil, rateLimit, nil
	}

	metastoreOwner := snap.metastores[parent.metastoreID].Owner
	for _, workspace := range facts.workspaces {
		complete, probeRateLimit, err := d.uc.schemaListingIsComplete(ctx, snap, metastoreOwner, facts.owner, parent.catalog(), workspace)
		if probeRateLimit != nil {
			rateLimit = probeRateLimit
		}
		if err != nil {
			return nil, rateLimit, fmt.Errorf("databricks-connector: failed to confirm schema listing visibility for catalog %s: %w", parent.catalog(), err)
		}
		if !complete {
			continue
		}

		return &schemaPageState{
			Workspace:      workspace,
			WorkspaceID:    snap.workspaceIDs[workspace],
			WorkspaceName:  snap.workspaceNames[workspace],
			MetastoreOwner: metastoreOwner,
			CatalogOwner:   facts.owner,
		}, rateLimit, nil
	}

	return nil, rateLimit, status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot list every %s in catalog %s: %s", noun, parent.catalog(), schemaListRemedy)
}

// listSchemaChildren pages one kind of securable under a schema. The workspace
// is chosen once and stored on the page token.
func (d *securableDeps) listSchemaChildren(
	ctx context.Context,
	parentResourceID *v2.ResourceId,
	attr rs.SyncOpAttrs,
	resourceType *v2.ResourceType,
	securableType, noun string,
	fetch func(ctx context.Context, workspace, pageToken string) ([]securable, string, *v2.RateLimitDescription, error),
) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != schemaResourceType.Id {
		return nil, nil, nil
	}

	parent, err := parseSecurableRef(parentResourceID.GetResource())
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse %s parent %q: %w", noun, parentResourceID.GetResource(), err)
	}
	if isUnderInformationSchema(parent) {
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(attr.PageToken.Token); err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid %s page token: %v", noun, err)
	}

	var rateLimit *v2.RateLimitDescription
	if bag.Current() == nil {
		current, openRateLimit, err := d.openListedCatalog(ctx, parent, noun)
		rateLimit = openRateLimit
		noteRateLimit(&annos, rateLimit)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if current == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: parent catalog is no longer listed",
				zap.String("resource_type", resourceType.Id),
				zap.String("parent", parent.resourceKey()),
			)

			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}

		encoded, err := json.Marshal(current)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		bag.Push(pagination.PageState{ResourceTypeID: resourceType.Id, ResourceID: string(encoded)})
	}

	var current schemaPageState
	if err := json.Unmarshal([]byte(bag.ResourceID()), &current); err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid %s page token: %v", noun, err)
	}

	items, next, listRateLimit, err := fetch(ctx, current.Workspace, bag.PageToken())
	if listRateLimit != nil {
		rateLimit = listRateLimit
	}
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if isNotFoundError(err) {
			return nil, &rs.SyncOpResults{Annotations: annos}, uhttp.WrapErrors(codes.Unavailable,
				fmt.Sprintf("databricks-connector: schema %s is listed, but its %ss could not be read through workspace %s",
					parent.fullName(), noun, current.Workspace),
				err)
		}

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list %ss under %s: %w", noun, parent.resourceKey(), err)
	}

	var rv []*v2.Resource
	for _, item := range items {
		if item.profile == nil {
			item.profile = map[string]any{}
		}
		item.profile[profileKeyMetastoreOwner] = current.MetastoreOwner
		item.profile[profileKeyWorkspace] = current.Workspace
		item.profile[profileKeyWorkspaceID] = current.WorkspaceID
		item.profile[profileKeyWorkspaceName] = current.WorkspaceName

		childRef := parent.child(item.name)
		built, err := newSecurableResource(resourceType, securableType, nil, item, childRef, parentResourceID)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build %s resource %s: %w", resourceType.Id, childRef.resourceKey(), err)
		}
		rv = append(rv, built)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
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

func (b *schemaBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return b.grantsOn(ctx, resource, attr, databricks.SecurableSchema, schemaPrivileges)
}

// grantsOn reads one page of direct grants. offered is the set this resource
// reports; a privilege outside it is not a grant ConductorOne can represent.
func (d *securableDeps) grantsOn(
	ctx context.Context,
	resource *v2.Resource,
	attr rs.SyncOpAttrs,
	securableType string,
	offered []string,
) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ref, err := parseSecurableRef(resource.GetId().GetResource())
	if err != nil {
		return nil, nil, err
	}

	annos := annotations.Annotations{}
	workspace, state, rateLimit, err := d.catalogAccess(ctx, resource, ref, false)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if code := status.Code(err); code == codes.PermissionDenied || code == codes.FailedPrecondition {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if state != securableInScope {
		ctxzap.Extract(ctx).Debug("databricks-connector: parent catalog is no longer listed; skipping its grants",
			zap.String("securable", ref.resourceKey()),
			zap.String("catalog", ref.catalog()),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	}

	l := ctxzap.Extract(ctx)
	pageToken := attr.PageToken.Token
	assignments, nextPageToken, rateLimit, err := d.client.ListPermissions(
		ctx, workspace, securableType, ref.permissionsName(), "", pageToken)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list permissions on %s %s: %w", securableType, ref.permissionsName(), err)
	}

	owner := ""
	if pageToken == "" {
		owner = profileString(resource, profileKeyOwner)
	}

	principals, principalRateLimit, err := d.uc.principalsForGrants(ctx, assignments, owner)
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
		if !emitPrincipal(d.sync, principalId.GetResourceType()) {
			continue
		}

		options := groupSecurableGrantOptions(principalId)
		for _, assigned := range assignment.Privileges {
			if assigned == "" || !slices.Contains(offered, assigned) {
				continue
			}
			rv = append(rv, grant.NewGrant(resource, assigned, principalId, options...))
		}
	}

	if owner != "" {
		principalId, found := principals.lookupName(owner)
		if found && emitPrincipal(d.sync, principalId.GetResourceType()) {
			options := groupSecurableGrantOptions(principalId)
			options = append(options, grant.WithAnnotation(&v2.GrantImmutable{}))
			rv = append(rv, grant.NewGrant(resource, ownerEntitlement, principalId, options...))
		}
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextPageToken}, nil
}

func (b *schemaBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return b.grantSecurable(ctx, principal, entitlement, schemaKind)
}

func (b *schemaBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return b.revokeSecurable(ctx, revoked, schemaKind)
}
