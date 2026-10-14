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
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*metastoreBuilder)(nil)

type metastoreBuilder struct {
	securableDeps
}

func newMetastoreBuilder(client *databricks.Client, uc *unityCatalog, sync syncPrincipals) *metastoreBuilder {
	return &metastoreBuilder{securableDeps{client: client, uc: uc, sync: sync}}
}

func (m *metastoreBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return metastoreResourceType
}

var metastoreKind = securableKind{
	name:       databricks.SecurableMetastore,
	parse:      parseMetastoreID,
	privileges: metastorePrivileges,
	// MANAGE is not assignable on a metastore, so naming it here would send the
	// operator after a privilege Databricks rejects. The authority is ownership.
	writeRemedy: "the connector principal must own the metastore or be assigned Admin directly on a workspace attached to it",
	access: func(ctx context.Context, d *securableDeps, resource *v2.Resource, ref securableRef) (string, accessState, *v2.RateLimitDescription, error) {
		return d.accessPath(ctx, resource, ref, true)
	},
}

func metastoreResource(_ context.Context, metastore *databricks.Metastore, parent *v2.ResourceId, workspace, workspaceID, workspaceName string) (*v2.Resource, error) {
	profile := map[string]any{
		profileKeyMetastoreID:   metastore.MetastoreID,
		profileKeyRegion:        metastore.Region,
		profileKeyOwner:         metastore.Owner,
		profileKeySecurableType: securableTypeMetastore,
		profileKeyWorkspace:     workspace,
		profileKeyWorkspaceID:   workspaceID,
		profileKeyWorkspaceName: workspaceName,
	}

	name := metastore.Name
	if name == "" {
		name = metastore.MetastoreID
	}

	// The metastore id is the resource id and its RawId: it survives a rename, and
	// the permissions endpoint addresses a metastore by UUID and rejects its name.
	return rs.NewAppResource(
		name,
		metastoreResourceType,
		metastore.MetastoreID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: catalogResourceType.Id},
			&v2.RawId{Id: metastore.MetastoreID},
		),
	)
}

// List returns the account's metastores, which arrive in a single unpaged response.
func (m *metastoreBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != accountResourceType.Id {
		return nil, nil, nil
	}

	// Read through the routing snapshot rather than the raw listing: with a
	// workspace allowlist the two differ, and emitting a metastore no in-scope
	// workspace can reach would advertise a resource whose grants cannot be read.
	snap, rateLimit, err := m.uc.buildRouting(ctx)
	annos := annotations.Annotations{}
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: failed to list metastores: %w", err)
	}

	var rv []*v2.Resource
	for _, metastore := range snap.metastores {
		if metastore.MetastoreID == "" {
			continue
		}

		workspace := ""
		if routed := snap.workspaces[metastore.MetastoreID]; len(routed) > 0 {
			workspace = routed[0]
		}
		mr, err := metastoreResource(ctx, &metastore, parentResourceID, workspace, snap.workspaceIDs[workspace], snap.workspaceNames[workspace])
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build the resource for metastore %s: %w", metastore.MetastoreID, err)
		}

		rv = append(rv, mr)
	}

	return rv, &rs.SyncOpResults{Annotations: annos}, nil
}

func (m *metastoreBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(metastorePrivileges)+1)
	for _, privilege := range metastorePrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a metastore in Databricks", privilege)),
		))
	}

	// Owner is not grantable. Ownership is single-valued and has no revoke.
	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a metastore in Databricks"),
	))

	return rv, nil, nil
}

func (m *metastoreBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// parseMetastoreID reads a metastore reference out of a resource id. A metastore
// is addressed by a bare id; the separator only appears in the composite ids of
// the securables below it, so finding one here means the wrong resource arrived.
func parseMetastoreID(resourceID string) (securableRef, error) {
	if resourceID == "" || strings.Contains(resourceID, securableKeySeparator) {
		return securableRef{}, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid metastore resource id %q", resourceID)
	}

	return securableRef{metastoreID: resourceID}, nil
}

func (m *metastoreBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ref, err := parseMetastoreID(resource.GetId().GetResource())
	if err != nil {
		return nil, nil, err
	}

	annos := annotations.Annotations{}
	workspace, state, rateLimit, err := m.accessPath(ctx, resource, ref, false)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if status.Code(err) == codes.PermissionDenied {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	switch state {
	case securableGone:
		ctxzap.Extract(ctx).Debug("databricks-connector: metastore is no longer listed",
			zap.String("metastore", ref.metastoreID),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	case securableOutOfScope:
		ctxzap.Extract(ctx).Debug("databricks-connector: no workspace this sync can use is attached to the metastore; skipping its grants",
			zap.String("metastore", ref.metastoreID),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	case securableInScope:
	}

	l := ctxzap.Extract(ctx)
	pageToken := attr.PageToken.Token
	assignments, nextPageToken, rateLimit, err := m.client.ListPermissions(
		ctx, workspace, databricks.SecurableMetastore, ref.permissionsName(), "", pageToken)
	noteRateLimit(&annos, rateLimit)
	// A 404 is returned, not read as an absence: the permissions endpoint also answers
	// 404 for a workspace that does not serve this metastore, and only the error_code
	// the APIError keeps tells that apart from a securable that is gone.
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list permissions on %s %s: %w", databricks.SecurableMetastore, ref.permissionsName(), err)
	}

	owner := ""
	if pageToken == "" {
		if v, ok := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyOwner); ok {
			owner = v
		}
	}

	principals, principalRateLimit, err := m.uc.principalsForGrants(ctx, assignments, owner)
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

		if !emitPrincipal(m.sync, principalId.GetResourceType()) {
			continue
		}

		options := groupSecurableGrantOptions(principalId)

		for _, assigned := range assignment.Privileges {
			if assigned == "" {
				continue
			}
			if !slices.Contains(metastorePrivileges, assigned) {
				l.Debug("databricks-connector: skipping a privilege the resource does not offer",
					zap.String("securable", ref.resourceKey()),
					zap.String("privilege", assigned),
				)

				continue
			}

			rv = append(rv, grant.NewGrant(resource, assigned, principalId, options...))
		}
	}

	if pageToken == "" {
		owner, ok := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyOwner)
		if ok && owner != "" {
			principalId, found := principals.lookupName(owner)
			if !found {
				l.Debug("databricks-connector: securable owner does not resolve to a synced principal",
					zap.String("securable", ref.resourceKey()),
					zap.String("owner", owner),
				)
			} else if emitPrincipal(m.sync, principalId.GetResourceType()) {
				options := groupSecurableGrantOptions(principalId)
				options = append(options, grant.WithAnnotation(&v2.GrantImmutable{}))
				rv = append(rv, grant.NewGrant(resource, ownerEntitlement, principalId, options...))
			}
		}
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextPageToken}, nil
}

func (m *metastoreBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return m.grantSecurable(ctx, principal, entitlement, metastoreKind)
}

func (m *metastoreBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return m.revokeSecurable(ctx, revoked, metastoreKind)
}
