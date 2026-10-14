package connector

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	workspacePermissionAdmin = "ADMIN"
)

// catalogTrustRemedy is what PermissionDenied on a catalog tells the operator.
const catalogTrustRemedy = "the connector principal must own the metastore or the catalog, hold MANAGE on the catalog, or be a direct Admin of the workspace whose admins group owns it"

// metastoreGrantsAreTrustworthy reports whether every direct grant on a metastore
// is visible. MANAGE and ALL_PRIVILEGES are rejected at the metastore level, so the
// authority is owning it or administering a workspace it is assigned to.
func (u *unityCatalog) metastoreGrantsAreTrustworthy(
	ctx context.Context,
	snap routingSnapshot,
	metastoreID, workspace string,
) (bool, *v2.RateLimitDescription, error) {
	if metastore, ok := snap.metastores[metastoreID]; ok && u.ownsPrincipal(metastore.Owner) {
		return true, nil, nil
	}

	return u.isWorkspaceAdmin(ctx, snap, workspace)
}

// catalogGrantsAreTrustworthy reports whether the connector sees every direct grant
// in a catalog's subtree. Databricks shows the full list only to a metastore admin,
// the owner, or a holder of MANAGE. A direct workspace Admin also sees it when the
// catalog is owned by that workspace's admins group. Anyone else reads just their
// own grants with a 200.
func (u *unityCatalog) catalogGrantsAreTrustworthy(
	ctx context.Context,
	snap routingSnapshot,
	metastoreOwner, catalogOwner string,
	ref securableRef,
	workspace string,
) (bool, *v2.RateLimitDescription, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	if u.ownsPrincipal(metastoreOwner) || u.ownsPrincipal(catalogOwner) {
		return true, nil, nil
	}

	// The workspace admins group acts as the owner for a direct Admin of that workspace.
	if workspaceAdminsGroupOwns(catalogOwner, snap.workspaceIDs[workspace]) {
		admin, rateLimit, err := u.isWorkspaceAdmin(ctx, snap, workspace)
		if err != nil || admin {
			return admin, rateLimit, err
		}
	}

	// The principal filter is effective, not literal: a row may be the group's MANAGE
	// this principal inherits, so nothing here may match on identity.
	var manageable bool
	rateLimit, err := u.client.ForEachUncachedPermissionsPage(
		ctx, workspace, databricks.SecurableCatalog, ref.catalog(), u.ownPrincipal,
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			if !holdsManage(page) {
				return false, nil
			}
			manageable = true
			return true, nil
		})
	if err != nil {
		return false, rateLimit, err
	}

	return manageable, rateLimit, nil
}

// workspaceAdminsGroupOwns reports whether owner is the system group Databricks
// creates for a workspace, named _workspace_admins_<workspace>_<workspace id>.
func workspaceAdminsGroupOwns(owner, workspaceID string) bool {
	if owner == "" || workspaceID == "" {
		return false
	}

	return strings.HasPrefix(owner, "_workspace_admins_") && strings.HasSuffix(owner, "_"+workspaceID)
}

// isWorkspaceAdmin lists the workspace's permission assignments on every call.
func (u *unityCatalog) isWorkspaceAdmin(
	ctx context.Context,
	snap routingSnapshot,
	workspace string,
) (bool, *v2.RateLimitDescription, error) {
	if workspace == "" {
		return false, nil, nil
	}

	workspaceID := snap.workspaceIDs[workspace]
	if workspaceID == "" {
		return false, nil, nil
	}

	assignments, rateLimit, err := u.client.ListWorkspaceMembers(ctx, workspaceID)
	if err != nil {
		return false, rateLimit, fmt.Errorf("failed to list the permission assignments of workspace %s: %w", workspace, err)
	}

	return u.holdsWorkspaceAdmin(ctx, assignments, rateLimit)
}

// holdsWorkspaceAdmin matches the connector's own principal against a workspace's
// ADMIN assignments. A service principal is named by its applicationId.
// Admin inherited only through a group is ignored: the connector principal must
// hold ADMIN itself. Walking every admin group's membership on every Grants call
// is the cost that would close this gap.
func (u *unityCatalog) holdsWorkspaceAdmin(
	ctx context.Context,
	assignments []databricks.WorkspaceAssignment,
	rateLimit *v2.RateLimitDescription,
) (bool, *v2.RateLimitDescription, error) {
	var adminIDs []string

	for _, assignment := range assignments {
		principal := assignment.Principal
		if principal == nil || !slices.Contains(assignment.Permissions, workspacePermissionAdmin) {
			continue
		}
		if u.ownsPrincipal(principal.ServicePrincipalAppID) {
			return true, rateLimit, nil
		}
		if principal.ID != 0 {
			adminIDs = append(adminIDs, strconv.Itoa(principal.ID))
		}
	}

	if len(adminIDs) == 0 {
		return false, rateLimit, nil
	}

	ownID, idRateLimit, err := u.ownPrincipalScimID(ctx)
	if idRateLimit != nil {
		rateLimit = idRateLimit
	}
	if err != nil {
		return false, rateLimit, err
	}

	return ownID != "" && slices.Contains(adminIDs, ownID), rateLimit, nil
}

// holdsManage reads only the privileges: the principal field may name the group the
// MANAGE is inherited through. ALL_PRIVILEGES is not enough — Databricks excludes
// MANAGE and READ METADATA from it, so its holder still reads a partial list.
func holdsManage(assignments []databricks.PrivilegeAssignment) bool {
	for _, assignment := range assignments {
		for _, privilege := range assignment.Privileges {
			if privilege == privilegeManage {
				return true
			}
		}
	}

	return false
}
