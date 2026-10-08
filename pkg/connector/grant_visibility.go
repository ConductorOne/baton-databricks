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

// grantsAreTrustworthy reports whether the connector can see every direct grant in
// a catalog's subtree. The verdict is not kept.
func (u *unityCatalog) grantsAreTrustworthy(
	ctx context.Context,
	snap routingSnapshot,
	ref securableRef,
	workspace string,
) (bool, *v2.RateLimitDescription, error) {
	return u.probeCatalog(ctx, snap, ref, workspace, nil)
}

// metastoreGrantsAreTrustworthy is the same partial-read guard one level up.
// MANAGE and ALL_PRIVILEGES are rejected at the metastore level, so the authority
// is owning the metastore or administering a workspace it is assigned to.
func (u *unityCatalog) metastoreGrantsAreTrustworthy(
	ctx context.Context,
	snap routingSnapshot,
	metastoreID, workspace string,
) (bool, *v2.RateLimitDescription, error) {
	return u.metastoreProbe(ctx, snap, metastoreID, workspace, nil)
}

func (u *unityCatalog) metastoreProbe(
	ctx context.Context,
	snap routingSnapshot,
	metastoreID, workspace string,
	rateLimit *v2.RateLimitDescription,
) (bool, *v2.RateLimitDescription, error) {
	if metastore, ok := snap.metastores[metastoreID]; ok && u.ownsPrincipal(metastore.Owner) {
		return true, rateLimit, nil
	}

	admin, adminRateLimit, err := u.isWorkspaceAdmin(ctx, snap, workspace)
	if adminRateLimit != nil {
		rateLimit = adminRateLimit
	}
	if err != nil {
		return false, rateLimit, err
	}

	return admin, rateLimit, nil
}

func (u *unityCatalog) probeCatalog(
	ctx context.Context,
	snap routingSnapshot,
	ref securableRef,
	workspace string,
	rateLimit *v2.RateLimitDescription,
) (bool, *v2.RateLimitDescription, error) {
	// A metastore owner reads across the metastore.
	if metastore, ok := snap.metastores[ref.metastoreID]; ok && u.ownsPrincipal(metastore.Owner) {
		return true, rateLimit, nil
	}

	// A securable's owner holds every privilege implicitly, which is not reported as
	// a grant, so ownership has to be checked separately.
	if catalogs := snap.catalogs[ref.metastoreID]; catalogs != nil {
		if facts, ok := catalogs[ref.catalog()]; ok && u.ownsPrincipal(facts.owner) {
			return true, rateLimit, nil
		}
	}

	// The principal filter is effective, not literal: a row may be the group's MANAGE
	// this principal inherits, so nothing here may match on identity.
	var manageable bool
	permissionsRateLimit, err := u.client.ForEachPermissionsPage(
		ctx, workspace, databricks.SecurableCatalog, ref.catalog(), u.ownPrincipal,
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			if !holdsManage(page) {
				return false, nil
			}
			manageable = true
			return true, nil
		})
	if permissionsRateLimit != nil {
		rateLimit = permissionsRateLimit
	}
	if err != nil {
		// No not-found exemption: this call answers 404 both for a catalog that is gone
		// and for a principal it does not know, separable only by message text.
		return false, rateLimit, err
	}
	if manageable {
		return true, rateLimit, nil
	}

	// A workspace admin holds the metastore-level privileges of the metastore its
	// workspace is assigned to, so it reads every grant under the catalog.
	admin, adminRateLimit, err := u.isWorkspaceAdmin(ctx, snap, workspace)
	if adminRateLimit != nil {
		rateLimit = adminRateLimit
	}
	if err != nil {
		return false, rateLimit, err
	}

	return admin, rateLimit, nil
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
