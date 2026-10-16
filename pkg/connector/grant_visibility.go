package connector

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	workspacePermissionAdmin = "ADMIN"
)

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
