package connector

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	securableTypeMetastore = "METASTORE"

	profileKeyMetastoreID   = "metastore_id"
	profileKeyOwner         = "owner"
	profileKeySecurableType = "securable_type"
	profileKeyRegion        = "region"
	profileKeyWorkspace     = "workspace"
	profileKeyWorkspaceID   = "workspace_id"
	profileKeyWorkspaceName = "workspace_name"

	// Not a dot: a securable's full name is already dot-separated.
	securableKeySeparator = "::"
)

// syncPrincipals says which principal types this sync includes.
type syncPrincipals struct {
	groups            bool
	servicePrincipals bool
}

func (s syncPrincipals) enabled(resourceType string) bool {
	switch resourceType {
	case groupResourceType.Id:
		return s.groups
	case servicePrincipalResourceType.Id:
		return s.servicePrincipals
	default:
		return false
	}
}

func emitPrincipal(sync syncPrincipals, resourceType string) bool {
	return resourceType == userResourceType.Id || sync.enabled(resourceType)
}

// securableRef is a securable keyed by its metastore plus its name components,
// catalog first. Databricks rejects a name holding a space, a period or a forward
// slash, so splitting a full name on "." is unambiguous.
type securableRef struct {
	metastoreID string
	parts       []string
}

func (r securableRef) fullName() string {
	return strings.Join(r.parts, ".")
}

// permissionsName is the dotted full name, or the metastore's UUID for the metastore
// itself, whose name the endpoint rejects with 400 "Invalid UUID string".
func (r securableRef) permissionsName() string {
	if len(r.parts) == 0 {
		return r.metastoreID
	}

	return r.fullName()
}

// resourceKey is the resource id, which every level also carries as its RawId.
func (r securableRef) resourceKey() string {
	if len(r.parts) == 0 {
		return r.metastoreID
	}

	return r.metastoreID + securableKeySeparator + r.fullName()
}

// securableDeps is the client state every Unity Catalog builder shares.
type securableDeps struct {
	client *databricks.Client
	uc     *unityCatalog
	sync   syncPrincipals
}

// securableKind is what Grant/Revoke need to know about one securable level.
type securableKind struct {
	// name is the permissions endpoint path segment, also used in messages.
	name       string
	parse      func(string) (securableRef, error)
	privileges []string
}

type changeMode int

const (
	changeGrant changeMode = iota
	changeRevoke
)

func parseEntitlementPrivilege(entitlementID string) (string, error) {
	separator := strings.LastIndex(entitlementID, ":")
	if separator < 0 || separator == len(entitlementID)-1 {
		return "", status.Errorf(codes.InvalidArgument,
			"databricks-connector: failed to read the privilege from entitlement %q: invalid entitlement id", entitlementID)
	}

	return entitlementID[separator+1:], nil
}

// matchHeldPrivilege finds the assignment that carries privilege for this
// principal. Id wins; name is used only when the assignment has no id.
func matchHeldPrivilege(
	assignments []databricks.PrivilegeAssignment,
	scimID, principalName, privilege string,
) (databricks.PrivilegeAssignment, bool) {
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

type securableChange struct {
	ref           securableRef
	privilege     string
	workspace     string
	scimID        string
	principalName string
}

// resolveSecurableChange is the Grant/Revoke prelude: principal, privilege,
// routing, visibility probe, and the Unity Catalog principal name.
func (d *securableDeps) resolveSecurableChange(
	ctx context.Context,
	principalID *v2.ResourceId,
	entitlement *v2.Entitlement,
	kind securableKind,
	mode changeMode,
) (securableChange, annotations.Annotations, bool, error) {
	annos := annotations.Annotations{}
	if !isValidPrincipal(principalID) {
		return securableChange{}, annos, true, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a Unity Catalog privilege, got %s",
			principalID.GetResourceType())
	}

	ref, err := kind.parse(entitlement.GetResource().GetId().GetResource())
	if err != nil {
		return securableChange{}, annos, true, err
	}

	privilege, err := parseEntitlementPrivilege(entitlement.GetId())
	if err != nil {
		return securableChange{}, annos, true, err
	}

	if privilege == ownerEntitlement {
		return securableChange{}, annos, true, status.Errorf(codes.InvalidArgument,
			"databricks-connector: ownership of %s %s is single-valued and has no revoke, so it cannot be provisioned; change the owner in Databricks",
			kind.name, ref.permissionsName())
	}

	if isLegacyPrivilege(privilege) {
		return securableChange{}, annos, true, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a Hive-era alias that Databricks rewrites server-side — CREATE on a schema becomes "+
				"CREATE_TABLE plus CREATE_FUNCTION — so it would grant more than was asked for and would never be read back "+
				"under that name; ask for the explicit privilege instead",
			privilege)
	}

	if !slices.Contains(kind.privileges, privilege) {
		return securableChange{}, annos, true, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable privilege on %s %s",
			privilege, kind.name, ref.permissionsName())
	}

	// The profile was written by the last sync, and the metastore may have been
	// deleted since: a write must confirm it is still listed before addressing it.
	workspace, state, rateLimit, err := d.accessPath(ctx, entitlement.GetResource(), ref, true)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if status.Code(err) == codes.PermissionDenied {
			return securableChange{}, annos, true, err
		}

		return securableChange{}, annos, true, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	switch state {
	case metastoreGone:
		if mode == changeRevoke {
			annos.Update(&v2.GrantAlreadyRevoked{})
			return securableChange{}, annos, true, nil
		}

		return securableChange{}, annos, true, status.Errorf(codes.NotFound,
			"databricks-connector: %s %s no longer exists in Databricks, so it cannot be changed", kind.name, ref.permissionsName())
	case metastoreOutOfScope:
		// The metastore still holds its privileges, so a revoke cannot be reported done.
		return securableChange{}, annos, true, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: %s %s cannot be changed: no workspace this sync can use is attached to it — every attached one is "+
				"left out of workspaces or named in databricks-exclude-workspaces, or none is assigned — so it is outside the sync scope",
			kind.name, ref.permissionsName())
	case metastoreInScope:
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return securableChange{}, annos, true, fmt.Errorf("databricks-connector: failed to read the SCIM id of principal %s: %w",
			principalID.GetResource(), err)
	}

	principalName, _, err := unityPrincipalName(ctx, d.client, principalID)
	if err != nil {
		return securableChange{}, annos, true, fmt.Errorf("databricks-connector: failed to resolve the Unity Catalog name of principal %s: %w",
			principalID.GetResource(), err)
	}
	// Revoke does not need the name: a deleted principal keeps its numeric id in
	// the assignment, and removing by id is what clears the row.
	if mode == changeGrant && principalName == "" {
		return securableChange{}, annos, true, status.Errorf(codes.NotFound,
			"databricks-connector: principal %s no longer exists in Databricks, so it cannot be granted %s on %s %s",
			scimID, privilege, kind.name, ref.permissionsName())
	}

	return securableChange{
		ref:           ref,
		privilege:     privilege,
		workspace:     workspace,
		scimID:        scimID,
		principalName: principalName,
	}, annos, false, nil
}

// accessPath names the workspace a metastore's permissions are read through.
// Unless confirm is set, the workspace the last sync wrote on the profile is
// tried first, which skips listing every metastore's workspaces per Grants call.
func (d *securableDeps) accessPath(
	ctx context.Context,
	resource *v2.Resource,
	ref securableRef,
	confirm bool,
) (string, metastoreState, *v2.RateLimitDescription, error) {
	if !confirm {
		workspace, ok, rateLimit, err := d.profileWorkspace(ctx, resource, ref)
		if err != nil {
			return "", metastoreInScope, rateLimit, err
		}
		if ok {
			return workspace, metastoreInScope, rateLimit, nil
		}
	}

	snap, rateLimit, err := d.uc.buildRouting(ctx)
	if err != nil {
		return "", metastoreInScope, rateLimit, err
	}
	workspaces, state, err := usableWorkspaces(snap, ref.metastoreID)
	if err != nil || state != metastoreInScope {
		return "", state, rateLimit, err
	}

	// Admin on any one attached workspace is enough, and ListWorkspaces order says
	// nothing about which one that is, so every usable workspace is tried.
	for _, workspace := range workspaces {
		trustworthy, probeRateLimit, err := d.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
		if probeRateLimit != nil {
			rateLimit = probeRateLimit
		}
		if err != nil {
			if ctx.Err() != nil || !isUnreadableWorkspaceError(err) {
				return "", metastoreInScope, rateLimit, err
			}
			ctxzap.Extract(ctx).Debug("databricks-connector: could not read the permission assignments of an attached workspace; trying the next one",
				zap.String("metastore", ref.metastoreID),
				zap.String("workspace", workspace),
				zap.Error(err),
			)

			continue
		}
		if trustworthy {
			return workspace, metastoreInScope, rateLimit, nil
		}
	}

	return "", metastoreInScope, rateLimit, status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot read every grant on metastore %s: the connector principal must own the metastore or be assigned Admin directly on a workspace attached to it",
		ref.metastoreID)
}

// profileWorkspace reports false whenever the profile workspace cannot be used
// as is, so the caller rebuilds the routing instead. Only a rate limit or a
// cancelled context is returned: retrying elsewhere would not help either.
func (d *securableDeps) profileWorkspace(
	ctx context.Context,
	resource *v2.Resource,
	ref securableRef,
) (string, bool, *v2.RateLimitDescription, error) {
	workspace, workspaceID, workspaceName, owner := profileAccessPath(resource)
	if workspace == "" || workspaceID == "" {
		return "", false, nil, nil
	}
	if len(d.uc.workspaces) > 0 {
		if _, ok := matchConfiguredWorkspace(d.uc.workspaces, workspace, workspaceName, workspaceID); !ok {
			return "", false, nil, nil
		}
	}

	snap := profileRoutingSnapshot(ref.metastoreID, workspace, workspaceID, owner)
	trustworthy, rateLimit, err := d.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
	if err != nil {
		if ctx.Err() != nil || isRateLimitError(err) {
			return "", false, rateLimit, err
		}
		ctxzap.Extract(ctx).Debug("databricks-connector: the workspace on the metastore profile did not answer; rebuilding the routing",
			zap.String("metastore", ref.metastoreID),
			zap.String("workspace", workspace),
			zap.Error(err),
		)

		return "", false, rateLimit, nil
	}

	return workspace, trustworthy, rateLimit, nil
}

func profileAccessPath(resource *v2.Resource) (string, string, string, string) {
	if resource == nil {
		return "", "", "", ""
	}
	profile := rs.GetProfile(resource)
	workspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	workspaceID, _ := rs.GetProfileStringValue(profile, profileKeyWorkspaceID)
	workspaceName, _ := rs.GetProfileStringValue(profile, profileKeyWorkspaceName)
	owner, _ := rs.GetProfileStringValue(profile, profileKeyOwner)

	return workspace, workspaceID, workspaceName, owner
}

func profileRoutingSnapshot(metastoreID, workspace, workspaceID, owner string) routingSnapshot {
	return routingSnapshot{
		metastores: map[string]databricks.Metastore{
			metastoreID: {MetastoreID: metastoreID, Owner: owner},
		},
		workspaces:   map[string][]string{metastoreID: {workspace}},
		workspaceIDs: map[string]string{workspace: workspaceID},
	}
}
