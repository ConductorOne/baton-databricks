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

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*metastoreBuilder)(nil)

type metastoreBuilder struct {
	securableDeps
}

func newMetastoreBuilder(client *databricks.Client, uc *unityCatalog, willSync func(string) bool) *metastoreBuilder {
	return &metastoreBuilder{securableDeps{client: client, uc: uc, willSync: willSync}}
}

func (m *metastoreBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return metastoreResourceType
}

func metastoreResource(_ context.Context, metastore *databricks.Metastore, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]any{
		profileKeyMetastoreID:   metastore.MetastoreID,
		profileKeyRegion:        metastore.Region,
		profileKeyOwner:         metastore.Owner,
		profileKeySecurableType: securableTypeMetastore,
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
			&v2.RawId{Id: metastore.MetastoreID},
		),
	)
}

// List returns the account's metastores, which arrive in a single unpaged response.
func (m *metastoreBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != accountResourceType.Id {
		return nil, nil, nil
	}

	metastores, rateLimit, err := m.client.ListMetastores(ctx)
	annos := annotations.Annotations{}
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: failed to list metastores: %w", err)
	}

	var rv []*v2.Resource
	for _, metastore := range metastores {
		if metastore.MetastoreID == "" {
			continue
		}

		mr, err := metastoreResource(ctx, &metastore, parentResourceID)
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
		return securableRef{}, fmt.Errorf("databricks-connector: invalid metastore resource id %q", resourceID)
	}

	return securableRef{metastoreID: resourceID}, nil
}

func (m *metastoreBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	ref, err := parseMetastoreID(resource.GetId().GetResource())
	if err != nil {
		return nil, nil, err
	}

	annos := annotations.Annotations{}
	snap, rateLimit, err := m.uc.buildRouting(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := workspaceFromSnapshot(snap, ref.metastoreID, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if !exists {
		ctxzap.Extract(ctx).Debug("databricks-connector: metastore is no longer listed",
			zap.String("metastore", ref.metastoreID),
		)

		return nil, &rs.SyncOpResults{Annotations: annos}, nil
	}

	trustworthy, rateLimit, err := m.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to probe grant visibility on metastore %s: %w", ref.permissionsName(), err)
	}
	if !trustworthy {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant on metastore %s: the connector principal needs to be a metastore admin",
			ref.metastoreID)
	}

	l := ctxzap.Extract(ctx)
	pageToken := attr.PageToken.Token
	assignments, nextPageToken, rateLimit, err := m.client.ListPermissions(
		ctx, workspace, databricks.SecurableMetastore, ref.permissionsName(), "", pageToken)
	noteRateLimit(&annos, rateLimit)
	// The permissions endpoint answers 404 for a securable that is gone, for a principal
	// it does not know, and for a workspace that does not serve this metastore, and the
	// three are separable only by message text.
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
			"databricks-connector: failed to list permissions on %s %s: %w", databricks.SecurableMetastore, ref.permissionsName(), err)
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
		principals, principalRateLimit, err = m.uc.listPrincipals(ctx)
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

		if principalId.GetResourceType() != userResourceType.Id && !m.willSync(principalId.GetResourceType()) {
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
			} else if principalId.GetResourceType() == userResourceType.Id || m.willSync(principalId.GetResourceType()) {
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

func (m *metastoreBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	principalID := principal.GetId()

	if !isValidPrincipal(principalID) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a Unity Catalog privilege, got %s",
			principalID.GetResourceType())
	}

	ref, err := parseMetastoreID(entitlement.GetResource().GetId().GetResource())
	if err != nil {
		return annos, err
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
			"databricks-connector: ownership of metastore %s is single-valued and has no revoke, so it cannot be provisioned; change the owner in Databricks",
			ref.permissionsName())
	}

	if isLegacyPrivilege(privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a Hive-era alias that Databricks rewrites server-side — CREATE on a schema becomes "+
				"CREATE_TABLE plus CREATE_FUNCTION — so it would grant more than was asked for and would never be read back "+
				"under that name; ask for the explicit privilege instead",
			privilege)
	}

	if !slices.Contains(metastorePrivileges, privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable privilege on metastore %s",
			privilege, ref.permissionsName())
	}

	snap, rateLimit, err := m.uc.buildRouting(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := workspaceFromSnapshot(snap, ref.metastoreID, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if !exists {
		return annos, status.Errorf(codes.NotFound,
			"databricks-connector: metastore %s is no longer listed on the account, so it cannot be changed", ref.metastoreID)
	}

	trustworthy, rateLimit, err := m.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to probe grant visibility on metastore %s: %w",
			ref.permissionsName(), err)
	}
	if !trustworthy {
		return annos, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant on metastore %s: the connector principal needs to be a metastore admin",
			ref.metastoreID)
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read the SCIM id of principal %s: %w",
			principalID.GetResource(), err)
	}

	principalName, _, err := unityPrincipalName(ctx, m.client, principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the Unity Catalog name of principal %s: %w",
			principalID.GetResource(), err)
	}

	if principalName == "" {
		return annos, status.Errorf(codes.NotFound,
			"databricks-connector: principal %s no longer exists in Databricks, so it cannot be granted %s on %s %s",
			scimID, privilege, databricks.SecurableMetastore, ref.permissionsName())
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

	assignments, rateLimit, err := m.client.DrainPermissions(ctx, workspace, databricks.SecurableMetastore, ref.permissionsName())
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w",
			databricks.SecurableMetastore, ref.permissionsName(), err)
	}

	if _, held := matchHeld(assignments); held {
		annos.Update(&v2.GrantAlreadyExists{})

		return annos, nil
	}

	var echoed []databricks.PrivilegeAssignment
	rateLimit, err = m.client.UpdatePermissionsUntil(ctx, workspace, databricks.SecurableMetastore, ref.permissionsName(),
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
				// MANAGE is not assignable on a metastore, so naming it here would send the
				// operator after a privilege Databricks rejects. The authority is ownership.
				fmt.Sprintf("databricks-connector: cannot grant %s on %s %s: the connector principal needs to be a metastore admin (its owner) or an account admin",
					privilege, databricks.SecurableMetastore, ref.permissionsName()),
				err,
			)
		}

		return annos, fmt.Errorf("databricks-connector: failed to grant %s on %s %s: %w",
			privilege, databricks.SecurableMetastore, ref.permissionsName(), err)
	}

	if _, held := matchHeld(echoed); !held {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: Databricks accepted granting %s to %s on %s %s but did not report the privilege on the securable afterwards, "+
				"so the grant was not applied",
			privilege, principalName, databricks.SecurableMetastore, ref.permissionsName())
	}

	return annos, nil
}

func (m *metastoreBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	principalID := revoked.GetPrincipal().GetId()
	entitlement := revoked.GetEntitlement()

	if !isValidPrincipal(principalID) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a Unity Catalog privilege, got %s",
			principalID.GetResourceType())
	}

	ref, err := parseMetastoreID(entitlement.GetResource().GetId().GetResource())
	if err != nil {
		return annos, err
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
			"databricks-connector: ownership of metastore %s is single-valued and has no revoke, so it cannot be provisioned; change the owner in Databricks",
			ref.permissionsName())
	}

	if isLegacyPrivilege(privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a Hive-era alias that Databricks rewrites server-side — CREATE on a schema becomes "+
				"CREATE_TABLE plus CREATE_FUNCTION — so it would grant more than was asked for and would never be read back "+
				"under that name; ask for the explicit privilege instead",
			privilege)
	}

	if !slices.Contains(metastorePrivileges, privilege) {
		return annos, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable privilege on metastore %s",
			privilege, ref.permissionsName())
	}

	snap, rateLimit, err := m.uc.buildRouting(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	workspace, exists, rateLimit, err := workspaceFromSnapshot(snap, ref.metastoreID, rateLimit)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	if !exists {
		// The metastore is gone from the account, so the privilege is gone with it and
		// the end state the revoke asked for already holds. Reporting NotFound instead
		// would fail the task on every retry against a securable that can never return.
		annos.Update(&v2.GrantAlreadyRevoked{})

		return annos, nil
	}

	trustworthy, rateLimit, err := m.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to probe grant visibility on metastore %s: %w",
			ref.permissionsName(), err)
	}
	if !trustworthy {
		return annos, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant on metastore %s: the connector principal needs to be a metastore admin",
			ref.metastoreID)
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read the SCIM id of principal %s: %w",
			principalID.GetResource(), err)
	}

	// Grant rejects an empty name because it has to send one to add a privilege.
	// Revoke does not: a principal deleted after the grant was synced keeps its
	// numeric id in the assignment, and removing by id is what clears the row.
	principalName, _, err := unityPrincipalName(ctx, m.client, principalID)
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
	rateLimit, err = m.client.ForEachUncachedPermissionsPage(ctx, workspace, databricks.SecurableMetastore, ref.permissionsName(), "",
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
			databricks.SecurableMetastore, ref.permissionsName(), err)
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

	result, rateLimit, err := m.client.UpdatePermissions(ctx, workspace, databricks.SecurableMetastore, ref.permissionsName(),
		[]databricks.PermissionsChange{removal})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if isForbiddenError(err) {
			return annos, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("databricks-connector: cannot revoke %s on %s %s: the connector principal needs to be a metastore admin (its owner) or an account admin",
					privilege, databricks.SecurableMetastore, ref.permissionsName()),
				err,
			)
		}

		return annos, fmt.Errorf("databricks-connector: failed to revoke %s on %s %s: %w",
			privilege, databricks.SecurableMetastore, ref.permissionsName(), err)
	}

	if _, stillHeld := matchHeld(result); stillHeld {
		label := principalName
		if label == "" {
			label = scimID
		}

		return annos, status.Errorf(codes.Internal,
			"databricks-connector: Databricks accepted revoking %s from %s on %s %s but still reports the privilege on the securable, "+
				"so the revoke was not applied",
			privilege, label, databricks.SecurableMetastore, ref.permissionsName())
	}

	return annos, nil
}
