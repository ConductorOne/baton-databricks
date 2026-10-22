package connector

import (
	"context"
	"fmt"
	"slices"
	"strconv"
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
	client   *databricks.Client
	clientID string
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
	sync       syncPrincipals
}

func newMetastoreBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *metastoreBuilder {
	return &metastoreBuilder{client: client, clientID: clientID, workspaces: workspaces, sync: sync}
}

func (m *metastoreBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return metastoreResourceType
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

	// The permissions endpoint addresses a metastore by UUID, so that is the id.
	return rs.NewAppResource(
		name,
		metastoreResourceType,
		metastore.MetastoreID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: metastore.MetastoreID}),
	)
}

// List emits each metastore with the workspace its permissions are read through.
// A metastore with no usable workspace is left out: its grants cannot be read.
func (m *metastoreBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != accountResourceType.Id {
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	metastores, rateLimit, err := m.client.ListMetastores(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: failed to list metastores: %w", err)
	}

	scoped, running, err := m.scopedWorkspaces(ctx, &annos)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	l := ctxzap.Extract(ctx)
	var rv []*v2.Resource
	for _, metastore := range metastores {
		if metastore.MetastoreID == "" {
			continue
		}

		// An assignment list that cannot be read, including a 404, leaves which
		// workspaces serve the metastore unknown. Skipping it here would drop the
		// metastore and make ConductorOne remove its grants, so the sync fails and
		// keeps what it already holds. A metastore that is really gone stops being
		// returned by ListMetastores instead.
		workspace, inScope, err := m.routeMetastore(ctx, metastore.MetastoreID, scoped, running, &annos)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to list the workspaces assigned to metastore %s, so the sync cannot tell which serve it: %w",
				metastore.MetastoreID, err)
		}
		if workspace == nil {
			if inScope {
				return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.FailedPrecondition,
					"databricks-connector: metastore %s has no running workspace in sync scope", metastore.MetastoreID)
			}
			l.Debug("databricks-connector: metastore has no workspace in sync scope; skipping it",
				zap.String("metastore", metastore.MetastoreID))
			continue
		}

		mr, err := metastoreResource(ctx, &metastore, parentResourceID, workspace.DeploymentName, strconv.Itoa(workspace.ID), workspace.Name)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
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

	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a metastore in Databricks"),
	))

	return rv, nil, nil
}

func (m *metastoreBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (m *metastoreBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)
	metastoreID := resource.GetId().GetResource()
	profile := rs.GetProfile(resource)
	workspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	if workspace == "" {
		l.Debug("databricks-connector: metastore has no workspace on its profile; returning no grants", zap.String("metastore", metastoreID))
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	if err := m.ensureGrantsAreTrustworthy(ctx, resource, metastoreID, workspace, &annos); err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	pageToken := attr.PageToken.Token
	assignments, nextPageToken, rateLimit, err := m.client.ListPermissions(ctx, workspace, databricks.SecurableMetastore, metastoreID, "", pageToken)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		// FailedPrecondition, not NotFound: the SDK reads NotFound as "no grants".
		// Whether the metastore is gone is settled by the next List.
		return nil, &rs.SyncOpResults{Annotations: annos}, uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: failed to list permissions on metastore %s through workspace %s", metastoreID, workspace), err)
	}

	resolver := newPrincipalResolver(m.client, &annos)
	var rv []*v2.Grant
	for _, assignment := range assignments {
		principalId, err := resolver.resolve(ctx, assignment.PrincipalID.String(), assignment.Principal)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: %w", err)
		}
		if principalId == nil {
			l.Debug("databricks-connector: skipping a grant whose principal does not resolve",
				zap.String("metastore", metastoreID),
				zap.String("principal", assignment.Principal),
				zap.String("principal_id", assignment.PrincipalID.String()))
			continue
		}
		if !emitPrincipal(m.sync, principalId.GetResourceType()) {
			continue
		}

		options := groupSecurableGrantOptions(principalId)
		for _, privilege := range assignment.Privileges {
			if slices.Contains(metastorePrivileges, privilege) {
				rv = append(rv, grant.NewGrant(resource, privilege, principalId, options...))
			}
		}
	}

	// The owner is emitted once, with the first page.
	if owner, _ := rs.GetProfileStringValue(profile, profileKeyOwner); pageToken == "" && owner != "" {
		principalId, err := resolver.resolve(ctx, "", owner)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: %w", err)
		}
		if principalId != nil && emitPrincipal(m.sync, principalId.GetResourceType()) {
			options := append(groupSecurableGrantOptions(principalId), grant.WithAnnotation(&v2.GrantImmutable{}))
			rv = append(rv, grant.NewGrant(resource, ownerEntitlement, principalId, options...))
		}
	}

	if nextPageToken != "" && nextPageToken == pageToken {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.Internal,
			"databricks-connector: listing privileges on metastore %s repeated page token %q", metastoreID, nextPageToken)
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextPageToken}, nil
}

type metastoreChange struct {
	metastoreID   string
	workspace     string
	privilege     string
	scimID        string
	principalName string
}

// resolveChange validates a Grant/Revoke and resolves what the PATCH needs.
func (m *metastoreBuilder) resolveChange(
	ctx context.Context,
	principalID *v2.ResourceId,
	entitlement *v2.Entitlement,
	isGrant bool,
	annos *annotations.Annotations,
) (metastoreChange, error) {
	if !isValidPrincipal(principalID) {
		return metastoreChange{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a metastore privilege, got %s", principalID.GetResourceType())
	}

	resource := entitlement.GetResource()
	metastoreID := resource.GetId().GetResource()
	if metastoreID == "" {
		return metastoreChange{}, status.Error(codes.InvalidArgument, "databricks-connector: missing metastore resource id")
	}

	privilege, err := parseEntitlementPrivilege(entitlement.GetId())
	if err != nil {
		return metastoreChange{}, err
	}
	switch {
	case privilege == ownerEntitlement:
		return metastoreChange{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: metastore ownership cannot be provisioned; change the owner in Databricks")
	case slices.Contains(legacyPrivileges, strings.ToUpper(strings.TrimSpace(privilege))):
		return metastoreChange{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a legacy alias Databricks rewrites; use the explicit privilege", privilege)
	case !slices.Contains(metastorePrivileges, privilege):
		return metastoreChange{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable metastore privilege", privilege)
	}

	workspace, _ := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyWorkspace)
	if workspace == "" {
		return metastoreChange{}, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: metastore %s has no workspace on its profile", metastoreID)
	}

	// A privilege list the connector cannot fully read also cannot be safely
	// written to: the PATCH would be built from the rows it is allowed to see.
	if err := m.ensureGrantsAreTrustworthy(ctx, resource, metastoreID, workspace, annos); err != nil {
		return metastoreChange{}, err
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return metastoreChange{}, fmt.Errorf("databricks-connector: %w", err)
	}

	principalName, err := unityPrincipalName(ctx, m.client, principalID)
	if err != nil {
		return metastoreChange{}, fmt.Errorf("databricks-connector: failed to resolve principal %s: %w", principalID.GetResource(), err)
	}
	// Revoke can still remove a deleted principal's row by its id.
	if isGrant && principalName == "" {
		return metastoreChange{}, status.Errorf(codes.NotFound, "databricks-connector: principal %s no longer exists in Databricks", scimID)
	}

	return metastoreChange{
		metastoreID:   metastoreID,
		workspace:     workspace,
		privilege:     privilege,
		scimID:        scimID,
		principalName: principalName,
	}, nil
}

func (c metastoreChange) matchHeld(assignments []databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool) {
	return matchHeldPrivilege(assignments, c.scimID, c.principalName, c.privilege)
}

func (c metastoreChange) updateError(verb string, err error) error {
	if isForbiddenError(err) {
		return uhttp.WrapErrors(codes.PermissionDenied,
			fmt.Sprintf("databricks-connector: cannot %s %s on metastore %s: the connector must own the metastore or be Admin on an attached workspace",
				verb, c.privilege, c.metastoreID),
			err)
	}

	return fmt.Errorf("databricks-connector: failed to %s %s on metastore %s: %w", verb, c.privilege, c.metastoreID, err)
}

func (m *metastoreBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	change, err := m.resolveChange(ctx, principal.GetId(), entitlement, true, &annos)
	if err != nil {
		return annos, err
	}

	// Read past the cache: a stale page would skip a needed PATCH.
	assignments, rateLimit, err := m.client.DrainPermissions(ctx, change.workspace, databricks.SecurableMetastore, change.metastoreID)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on metastore %s: %w", change.metastoreID, err)
	}
	if _, held := change.matchHeld(assignments); held {
		annos.Update(&v2.GrantAlreadyExists{})
		return annos, nil
	}

	var echoed []databricks.PrivilegeAssignment
	rateLimit, err = m.client.UpdatePermissionsUntil(ctx, change.workspace, databricks.SecurableMetastore, change.metastoreID,
		[]databricks.PermissionsChange{{Principal: change.principalName, Add: []string{change.privilege}}},
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			echoed = append(echoed, page...)
			_, held := change.matchHeld(echoed)
			return held, nil
		})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, change.updateError("grant", err)
	}

	// Databricks no-ops a PATCH it cannot apply, so only the echo confirms it.
	if _, held := change.matchHeld(echoed); !held {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: granting %s on metastore %s was accepted but not applied", change.privilege, change.metastoreID)
	}

	return annos, nil
}

func (m *metastoreBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	change, err := m.resolveChange(ctx, revoked.GetPrincipal().GetId(), revoked.GetEntitlement(), false, &annos)
	if err != nil {
		return annos, err
	}

	var seen []databricks.PrivilegeAssignment
	rateLimit, err := m.client.ForEachUncachedPermissionsPage(ctx, change.workspace, databricks.SecurableMetastore, change.metastoreID, "",
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			seen = append(seen, page...)
			matched, ok := change.matchHeld(seen)
			// Only an id match is final; a name match may be superseded on a later page.
			return ok && change.scimID != "" && matched.PrincipalID.String() == change.scimID && change.scimID != "0", nil
		})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on metastore %s: %w", change.metastoreID, err)
	}

	held, ok := change.matchHeld(seen)
	if !ok {
		annos.Update(&v2.GrantAlreadyRevoked{})
		return annos, nil
	}

	removal := databricks.PermissionsChange{Principal: change.principalName, Remove: []string{change.privilege}}
	if heldID := held.PrincipalID.String(); heldID != "" && heldID != "0" {
		removal = databricks.PermissionsChange{PrincipalID: held.PrincipalID, Remove: []string{change.privilege}}
	}

	result, rateLimit, err := m.client.UpdatePermissions(ctx, change.workspace, databricks.SecurableMetastore, change.metastoreID,
		[]databricks.PermissionsChange{removal})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, change.updateError("revoke", err)
	}
	if _, stillHeld := change.matchHeld(result); stillHeld {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: revoking %s on metastore %s was accepted but not applied", change.privilege, change.metastoreID)
	}

	return annos, nil
}
