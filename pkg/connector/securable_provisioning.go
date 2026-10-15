package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grantSecurable adds one privilege on a Unity Catalog securable. Databricks no-ops
// a privilege already held, so the pre-read and the echoed assignments are the
// only evidence of GrantAlreadyExists and of the change landing.
func (d *securableDeps) grantSecurable(
	ctx context.Context,
	principal *v2.Resource,
	entitlement *v2.Entitlement,
	kind securableKind,
) (annotations.Annotations, error) {
	change, annos, done, err := d.resolveSecurableChange(ctx, principal.GetId(), entitlement, kind, changeGrant)
	if done {
		return annos, err
	}

	name := change.ref.permissionsName()
	matchHeld := func(assignments []databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool) {
		return matchHeldPrivilege(assignments, change.scimID, change.principalName, change.privilege)
	}

	held, rateLimit, err := d.preReadHeld(ctx, change, kind, matchHeld)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w", kind.name, name, err)
	}

	if _, ok := held(); ok {
		annos.Update(&v2.GrantAlreadyExists{})

		return annos, nil
	}

	var echoed []databricks.PrivilegeAssignment
	rateLimit, err = d.client.UpdatePermissionsUntil(ctx, change.workspace, kind.name, name,
		[]databricks.PermissionsChange{{Principal: change.principalName, Add: []string{change.privilege}}},
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			echoed = append(echoed, page...)
			_, held := matchHeld(echoed)
			return held, nil
		})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if isForbiddenError(err) {
			return annos, uhttp.WrapErrors(codes.PermissionDenied,
				fmt.Sprintf("databricks-connector: cannot grant %s on %s %s: %s", change.privilege, kind.name, name, kind.writeRemedy),
				err)
		}

		return annos, fmt.Errorf("databricks-connector: failed to grant %s on %s %s: %w", change.privilege, kind.name, name, err)
	}

	if _, held := matchHeld(echoed); !held {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: Databricks accepted granting %s to %s on %s %s but did not report the privilege on the securable afterwards, "+
				"so the grant was not applied",
			change.privilege, change.principalName, kind.name, name)
	}

	return annos, nil
}

// revokeSecurable removes one privilege. Databricks no-ops a privilege already
// absent, so the pre-read decides GrantAlreadyRevoked and the echoed assignments
// confirm the removal.
func (d *securableDeps) revokeSecurable(
	ctx context.Context,
	revoked *v2.Grant,
	kind securableKind,
) (annotations.Annotations, error) {
	change, annos, done, err := d.resolveSecurableChange(ctx, revoked.GetPrincipal().GetId(), revoked.GetEntitlement(), kind, changeRevoke)
	if done {
		return annos, err
	}

	name := change.ref.permissionsName()
	matchHeld := func(assignments []databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool) {
		return matchHeldPrivilege(assignments, change.scimID, change.principalName, change.privilege)
	}

	readHeld, rateLimit, err := d.preReadHeld(ctx, change, kind, matchHeld)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w", kind.name, name, err)
	}

	held, ok := readHeld()
	if !ok {
		annos.Update(&v2.GrantAlreadyRevoked{})

		return annos, nil
	}

	var removal databricks.PermissionsChange
	if heldID := held.PrincipalID.String(); heldID != "" && heldID != "0" {
		removal = databricks.PermissionsChange{PrincipalID: held.PrincipalID, Remove: []string{change.privilege}}
	} else {
		removal = databricks.PermissionsChange{Principal: change.principalName, Remove: []string{change.privilege}}
	}

	result, rateLimit, err := d.client.UpdatePermissions(ctx, change.workspace, kind.name, name, []databricks.PermissionsChange{removal})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if isForbiddenError(err) {
			return annos, uhttp.WrapErrors(codes.PermissionDenied,
				fmt.Sprintf("databricks-connector: cannot revoke %s on %s %s: %s", change.privilege, kind.name, name, kind.writeRemedy),
				err)
		}

		return annos, fmt.Errorf("databricks-connector: failed to revoke %s on %s %s: %w", change.privilege, kind.name, name, err)
	}

	if _, stillHeld := matchHeld(result); stillHeld {
		label := change.principalName
		if label == "" {
			label = change.scimID
		}

		return annos, status.Errorf(codes.Internal,
			"databricks-connector: Databricks accepted revoking %s from %s on %s %s but still reports the privilege on the securable, "+
				"so the revoke was not applied",
			change.privilege, label, kind.name, name)
	}

	return annos, nil
}

// preReadHeld reads the securable's assignments past the cache, stopping at the
// page that names the principal by id: no later row can outrank that match. The
// returned func reports the match over every page read.
func (d *securableDeps) preReadHeld(
	ctx context.Context,
	change securableChange,
	kind securableKind,
	matchHeld func([]databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool),
) (func() (databricks.PrivilegeAssignment, bool), *v2.RateLimitDescription, error) {
	var seen []databricks.PrivilegeAssignment
	rateLimit, err := d.client.ForEachUncachedPermissionsPage(ctx, change.workspace, kind.name, change.ref.permissionsName(), "",
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			seen = append(seen, page...)
			matched, ok := matchHeld(seen)
			if !ok {
				return false, nil
			}
			matchedID := matched.PrincipalID.String()
			return change.scimID != "" && matchedID == change.scimID && matchedID != "0", nil
		})

	return func() (databricks.PrivilegeAssignment, bool) { return matchHeld(seen) }, rateLimit, err
}
