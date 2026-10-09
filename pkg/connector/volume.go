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
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*volumeBuilder)(nil)

type volumeBuilder struct {
	securableDeps
}

func newVolumeBuilder(client *databricks.Client, uc *unityCatalog, willSync func(string) bool) *volumeBuilder {
	return &volumeBuilder{securableDeps{client: client, uc: uc, willSync: willSync}}
}

func (b *volumeBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return volumeResourceType
}

func (b *volumeBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != schemaResourceType.Id {
		return nil, nil, nil
	}

	parent, err := parseSecurableRef(parentResourceID.GetResource())
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse volume parent %q: %w", parentResourceID.GetResource(), err)
	}

	if isUnderInformationSchema(parent) {
		return nil, nil, nil
	}

	l := ctxzap.Extract(ctx)
	annos := annotations.Annotations{}
	snap, rateLimit, err := b.uc.buildRouting(ctx)
	if err != nil {
		noteRateLimit(&annos, rateLimit)

		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the workspace that lists %ss under %s: %w",
			volumeResourceType.Id, parent.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := accessCatalog(snap, parent, databricks.SecurableVolume, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the workspace that lists %ss under %s: %w",
			volumeResourceType.Id, parent.resourceKey(), err)
	}
	if !exists {
		l.Debug("databricks-connector: securable parent is no longer listed",
			zap.String("resource_type", volumeResourceType.Id),
			zap.String("parent", parent.resourceKey()),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	}

	volumes, next, rateLimit, err := b.client.ListVolumes(ctx, workspace, parent.catalog(), parent.schema(), attr.PageToken.Token, ResourcesPageSize)
	noteRateLimit(&annos, rateLimit)
	items := make([]securable, 0, len(volumes))
	for _, volume := range volumes {
		if volume.Name == "" {
			continue
		}

		items = append(items, securable{
			name:  volume.Name,
			owner: volume.Owner,
			profile: map[string]any{
				profileKeyCatalogName: parent.catalog(),
				profileKeySchemaName:  parent.schema(),
				profileKeyVolumeName:  volume.Name,
				profileKeyVolumeType:  volume.VolumeType,
			},
		})
	}

	if err != nil {
		if !isNotFoundError(err) {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to list %ss under %s: %w", volumeResourceType.Id, parent.resourceKey(), err)
		}

		l.Debug("databricks-connector: securable parent no longer exists",
			zap.String("resource_type", volumeResourceType.Id),
			zap.String("parent", parent.resourceKey()),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	}

	var rv []*v2.Resource
	for _, item := range items {
		childRef := parent.child(item.name)
		if isUnderInformationSchema(childRef) {
			continue
		}

		built, err := newSecurableResource(volumeResourceType, securableTypeVolume, nil, item, childRef, parentResourceID)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build %s resource %s: %w", volumeResourceType.Id, childRef.resourceKey(), err)
		}

		rv = append(rv, built)
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: next}, nil
}

func (b *volumeBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(volumePrivileges)+1)
	for _, privilege := range volumePrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a volume in Databricks", privilege)),
		))
	}

	// Owner is not grantable. Ownership is single-valued and has no revoke.
	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a volume in Databricks"),
	))

	return rv, nil, nil
}

func (b *volumeBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (b *volumeBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ref, err := parseSecurableRef(resource.GetId().GetResource())
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse volume resource id: %w", err)
	}

	annos := annotations.Annotations{}
	snap, rateLimit, err := b.uc.buildRouting(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := accessCatalog(snap, ref, databricks.SecurableVolume, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if !exists {
		ctxzap.Extract(ctx).Debug("databricks-connector: volume is no longer listed",
			zap.String("volume", ref.resourceKey()),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	}

	trustworthy, rateLimit, err := b.uc.grantsAreTrustworthy(ctx, snap, ref, workspace)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to probe grant visibility on volume %s: %w", ref.permissionsName(), err)
	}
	if !trustworthy {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant under catalog %s: the connector principal needs MANAGE on it",
			ref.catalog())
	}

	l := ctxzap.Extract(ctx)
	pageToken := attr.PageToken.Token
	assignments, nextPageToken, rateLimit, err := b.client.ListPermissions(
		ctx, workspace, databricks.SecurableVolume, ref.permissionsName(), "", pageToken)
	noteRateLimit(&annos, rateLimit)
	// The permissions endpoint answers 404 for a securable that is gone, for a principal
	// it does not know, and for a workspace that does not serve this metastore, and the
	// three are separable only by message text.
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list permissions on %s %s: %w", databricks.SecurableVolume, ref.permissionsName(), err)
	}

	needPrincipals := len(assignments) > 0
	if pageToken == "" {
		if owner, ok := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyOwner); ok && owner != "" {
			needPrincipals = true
		}
	}

	principals := newPrincipalIndex()
	if needPrincipals {
		var principalRateLimit *v2.RateLimitDescription
		principals, principalRateLimit, err = b.uc.listPrincipals(ctx)
		noteRateLimit(&annos, principalRateLimit)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to list account principals: %w", err)
		}
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

		if principalId.GetResourceType() != userResourceType.Id && !b.willSync(principalId.GetResourceType()) {
			continue
		}

		var options []grant.GrantOption
		if principalId.GetResourceType() == groupResourceType.Id {
			// Shallow stays unset: setting it would suppress propagation through a nested group.
			options = []grant.GrantOption{grant.WithAnnotation(&v2.GrantExpandable{
				EntitlementIds:  []string{fmt.Sprintf("group:%s:%s", principalId.GetResource(), groupMemberEntitlement)},
				ResourceTypeIds: []string{userResourceType.Id, groupResourceType.Id, servicePrincipalResourceType.Id},
			})}
		}

		for _, assigned := range assignment.Privileges {
			if assigned == "" {
				continue
			}
			if !slices.Contains(volumePrivileges, assigned) {
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
			} else if principalId.GetResourceType() == userResourceType.Id || b.willSync(principalId.GetResourceType()) {
				var options []grant.GrantOption
				if principalId.GetResourceType() == groupResourceType.Id {
					// Shallow stays unset: setting it would suppress propagation through a nested group.
					options = []grant.GrantOption{grant.WithAnnotation(&v2.GrantExpandable{
						EntitlementIds:  []string{fmt.Sprintf("group:%s:%s", principalId.GetResource(), groupMemberEntitlement)},
						ResourceTypeIds: []string{userResourceType.Id, groupResourceType.Id, servicePrincipalResourceType.Id},
					})}
				}
				options = append(options, grant.WithAnnotation(&v2.GrantImmutable{}))
				rv = append(rv, grant.NewGrant(resource, ownerEntitlement, principalId, options...))
			}
		}
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextPageToken}, nil
}

func (b *volumeBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	principalID := principal.GetId()

	if !isValidPrincipal(principalID) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a Unity Catalog privilege, got %s",
			principalID.GetResourceType())
	}

	ref, err := parseSecurableRef(entitlement.GetResource().GetId().GetResource())
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to parse %s resource id: %w", databricks.SecurableVolume, err)
	}

	entitlementID := entitlement.GetId()
	separator := strings.LastIndex(entitlementID, ":")
	if separator < 0 || separator == len(entitlementID)-1 {
		return annos, fmt.Errorf("databricks-connector: failed to read the privilege from entitlement %q: %w",
			entitlementID, fmt.Errorf("invalid entitlement id %q", entitlementID))
	}
	privilege := entitlementID[separator+1:]

	if privilege == ownerEntitlement {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: ownership of %s %s is single-valued and has no revoke, so it cannot be provisioned; change the owner in Databricks",
			databricks.SecurableVolume, ref.permissionsName())
	}

	if isLegacyPrivilege(privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a Hive-era alias that Databricks rewrites server-side — CREATE on a schema becomes "+
				"CREATE_TABLE plus CREATE_FUNCTION — so it would grant more than was asked for and would never be read back "+
				"under that name; ask for the explicit privilege instead",
			privilege)
	}

	if !slices.Contains(volumePrivileges, privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable privilege on %s %s",
			privilege, databricks.SecurableVolume, ref.permissionsName())
	}

	snap, rateLimit, err := b.uc.buildRouting(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := accessCatalog(snap, ref, databricks.SecurableVolume, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if !exists {
		if !b.uc.scope.covers(ref.catalog()) {
			return annos, status.Errorf(codes.InvalidArgument,
				"databricks-connector: catalog %s is out of scope for this sync, so %s %s cannot be changed; "+
					"change %s to bring the catalog into scope",
				ref.catalog(), databricks.SecurableVolume, ref.permissionsName(), b.uc.scope.describe())
		}

		return annos, status.Errorf(codes.NotFound,
			"databricks-connector: catalog %s is no longer listed in metastore %s, so %s %s cannot be changed",
			ref.catalog(), ref.metastoreID, databricks.SecurableVolume, ref.permissionsName())
	}

	trustworthy, rateLimit, err := b.uc.grantsAreTrustworthy(ctx, snap, ref, workspace)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to probe grant visibility on %s %s: %w",
			databricks.SecurableVolume, ref.permissionsName(), err)
	}
	if !trustworthy {
		return annos, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant under catalog %s: the connector principal needs MANAGE on it",
			ref.catalog())
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read the SCIM id of principal %s: %w",
			principalID.GetResource(), err)
	}

	principalName, _, err := unityPrincipalName(ctx, b.client, principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the Unity Catalog name of principal %s: %w",
			principalID.GetResource(), err)
	}

	if principalName == "" {
		return annos, status.Errorf(codes.NotFound,
			"databricks-connector: principal %s no longer exists in Databricks, so it cannot be granted %s on %s %s",
			scimID, privilege, databricks.SecurableVolume, ref.permissionsName())
	}

	matchHeld := func(assignments []databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool) {
		var nameOnly databricks.PrivilegeAssignment
		var sawNameOnly bool
		for _, assignment := range assignments {
			if !slices.Contains(assignment.Privileges, privilege) {
				continue
			}

			assignmentID := assignment.PrincipalID.String()
			if scimID != "" && assignmentID == scimID && assignmentID != "0" {
				return assignment, true
			}
			if principalName == "" || assignment.Principal != principalName {
				continue
			}
			if scimID != "" && assignmentID != "" && assignmentID != "0" && assignmentID != scimID {
				continue
			}
			if !sawNameOnly {
				nameOnly = assignment
				sawNameOnly = true
			}
		}
		if sawNameOnly {
			return nameOnly, true
		}

		return databricks.PrivilegeAssignment{}, false
	}

	assignments, rateLimit, err := b.client.DrainPermissions(ctx, workspace, databricks.SecurableVolume, ref.permissionsName())
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w",
			databricks.SecurableVolume, ref.permissionsName(), err)
	}

	if _, held := matchHeld(assignments); held {
		annos.Update(&v2.GrantAlreadyExists{})

		return annos, nil
	}

	var echoed []databricks.PrivilegeAssignment
	rateLimit, err = b.client.UpdatePermissionsUntil(ctx, workspace, databricks.SecurableVolume, ref.permissionsName(),
		[]databricks.PermissionsChange{{Principal: principalName, Add: []string{privilege}}},
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			echoed = append(echoed, page...)
			_, held := matchHeld(echoed)
			return held, nil
		})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if isForbiddenError(err) {
			return annos, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("databricks-connector: cannot grant %s on %s %s: the connector principal needs MANAGE on it",
					privilege, databricks.SecurableVolume, ref.permissionsName()),
				err,
			)
		}

		return annos, fmt.Errorf("databricks-connector: failed to grant %s on %s %s: %w",
			privilege, databricks.SecurableVolume, ref.permissionsName(), err)
	}

	if _, held := matchHeld(echoed); !held {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: Databricks accepted granting %s to %s on %s %s but did not report the privilege on the securable afterwards, "+
				"so the grant was not applied",
			privilege, principalName, databricks.SecurableVolume, ref.permissionsName())
	}

	return annos, nil
}

func (b *volumeBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	principalID := revoked.GetPrincipal().GetId()
	entitlement := revoked.GetEntitlement()

	if !isValidPrincipal(principalID) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a Unity Catalog privilege, got %s",
			principalID.GetResourceType())
	}

	ref, err := parseSecurableRef(entitlement.GetResource().GetId().GetResource())
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to parse %s resource id: %w", databricks.SecurableVolume, err)
	}

	entitlementID := entitlement.GetId()
	separator := strings.LastIndex(entitlementID, ":")
	if separator < 0 || separator == len(entitlementID)-1 {
		return annos, fmt.Errorf("databricks-connector: failed to read the privilege from entitlement %q: %w",
			entitlementID, fmt.Errorf("invalid entitlement id %q", entitlementID))
	}
	privilege := entitlementID[separator+1:]

	if privilege == ownerEntitlement {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: ownership of %s %s is single-valued and has no revoke, so it cannot be provisioned; change the owner in Databricks",
			databricks.SecurableVolume, ref.permissionsName())
	}

	if isLegacyPrivilege(privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a Hive-era alias that Databricks rewrites server-side — CREATE on a schema becomes "+
				"CREATE_TABLE plus CREATE_FUNCTION — so it would grant more than was asked for and would never be read back "+
				"under that name; ask for the explicit privilege instead",
			privilege)
	}

	if !slices.Contains(volumePrivileges, privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable privilege on %s %s",
			privilege, databricks.SecurableVolume, ref.permissionsName())
	}

	snap, rateLimit, err := b.uc.buildRouting(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := accessCatalog(snap, ref, databricks.SecurableVolume, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if !exists {
		if !b.uc.scope.covers(ref.catalog()) {
			return annos, status.Errorf(codes.InvalidArgument,
				"databricks-connector: catalog %s is out of scope for this sync, so %s %s cannot be changed; "+
					"change %s to bring the catalog into scope",
				ref.catalog(), databricks.SecurableVolume, ref.permissionsName(), b.uc.scope.describe())
		}

		// The catalog is gone, so the volume's privilege is gone with it and the end
		// state the revoke asked for already holds. Reporting NotFound instead would
		// fail the task on every retry against a securable that can never return.
		annos.Update(&v2.GrantAlreadyRevoked{})

		return annos, nil
	}

	trustworthy, rateLimit, err := b.uc.grantsAreTrustworthy(ctx, snap, ref, workspace)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to probe grant visibility on %s %s: %w",
			databricks.SecurableVolume, ref.permissionsName(), err)
	}
	if !trustworthy {
		return annos, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant under catalog %s: the connector principal needs MANAGE on it",
			ref.catalog())
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read the SCIM id of principal %s: %w",
			principalID.GetResource(), err)
	}

	principalName, _, err := unityPrincipalName(ctx, b.client, principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the Unity Catalog name of principal %s: %w",
			principalID.GetResource(), err)
	}

	matchHeld := func(assignments []databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool) {
		var nameOnly databricks.PrivilegeAssignment
		var sawNameOnly bool
		for _, assignment := range assignments {
			if !slices.Contains(assignment.Privileges, privilege) {
				continue
			}

			assignmentID := assignment.PrincipalID.String()
			if scimID != "" && assignmentID == scimID && assignmentID != "0" {
				return assignment, true
			}
			if principalName == "" || assignment.Principal != principalName {
				continue
			}
			if scimID != "" && assignmentID != "" && assignmentID != "0" && assignmentID != scimID {
				continue
			}
			if !sawNameOnly {
				nameOnly = assignment
				sawNameOnly = true
			}
		}
		if sawNameOnly {
			return nameOnly, true
		}

		return databricks.PrivilegeAssignment{}, false
	}

	var seen []databricks.PrivilegeAssignment
	rateLimit, err = b.client.ForEachUncachedPermissionsPage(ctx, workspace, databricks.SecurableVolume, ref.permissionsName(), "",
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			seen = append(seen, page...)
			matched, ok := matchHeld(seen)
			if !ok {
				return false, nil
			}
			matchedID := matched.PrincipalID.String()
			return scimID != "" && matchedID == scimID && matchedID != "0", nil
		})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w",
			databricks.SecurableVolume, ref.permissionsName(), err)
	}

	held, ok := matchHeld(seen)
	if !ok {
		annos.Update(&v2.GrantAlreadyRevoked{})

		return annos, nil
	}

	var removal databricks.PermissionsChange
	if heldID := held.PrincipalID.String(); heldID != "" && heldID != "0" {
		removal = databricks.PermissionsChange{PrincipalID: held.PrincipalID, Remove: []string{privilege}}
	} else if principalName != "" {
		removal = databricks.PermissionsChange{Principal: principalName, Remove: []string{privilege}}
	} else {
		removal = databricks.PermissionsChange{PrincipalID: held.PrincipalID, Remove: []string{privilege}}
	}

	result, rateLimit, err := b.client.UpdatePermissions(ctx, workspace, databricks.SecurableVolume, ref.permissionsName(),
		[]databricks.PermissionsChange{removal})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if isForbiddenError(err) {
			return annos, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("databricks-connector: cannot revoke %s on %s %s: the connector principal needs MANAGE on it",
					privilege, databricks.SecurableVolume, ref.permissionsName()),
				err,
			)
		}

		return annos, fmt.Errorf("databricks-connector: failed to revoke %s on %s %s: %w",
			privilege, databricks.SecurableVolume, ref.permissionsName(), err)
	}

	if _, stillHeld := matchHeld(result); stillHeld {
		label := principalName
		if label == "" {
			label = scimID
		}

		return annos, status.Errorf(codes.Internal,
			"databricks-connector: Databricks accepted revoking %s from %s on %s %s but still reports the privilege on the securable, "+
				"so the revoke was not applied",
			privilege, label, databricks.SecurableVolume, ref.permissionsName())
	}

	return annos, nil
}
