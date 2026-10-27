package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// securableKeySeparator keys a securable by its metastore. It is not a dot: a
// securable's full name is already dot-separated.
const securableKeySeparator = "::"

// securableLevel is the only thing the permissions endpoint needs to tell one
// Unity Catalog level from another: how a resource id addresses the securable,
// what it can be granted, and what a refusal tells the operator.
type securableLevel struct {
	// securableType is the endpoint path segment, lowercase. It is not the
	// uppercase securable_type a listed securable carries.
	securableType string

	// noun names the level in operator-facing messages.
	noun string

	privileges []string

	// writeRemedy is what a 403 on the PATCH tells the operator.
	writeRemedy string

	// trustRemedy is what a refusal to report a privilege list the connector
	// cannot fully read tells the operator.
	trustRemedy string

	// permissionsName turns a resource id into the name the endpoint addresses
	// the securable by.
	permissionsName func(resourceID string) (string, error)

	// goneErrorCodes are the error_code values Databricks returns when the
	// securable, or a parent of it, no longer exists. They are the only proof
	// of absence: an unbound or unprivileged workspace answers 404 too.
	goneErrorCodes []string

	// trusts reports whether the connector reads every privilege assignment on
	// the securable. What proves that differs per level, so the rule travels
	// with the level rather than being decided by the shared path.
	trusts func(ctx context.Context, client *databricks.Client, trust securableTrust, annos *annotations.Annotations) (bool, error)
}

// securableTrust is what the per-level rules read to settle a privilege list:
// who the connector authenticates as, who owns the securable and the metastore
// above it, and where the read is addressed.
type securableTrust struct {
	// ownPrincipal is the applicationId of the connector's service principal.
	ownPrincipal string

	owner string

	// metastoreOwner is empty on a metastore, whose own owner is in owner.
	metastoreOwner string

	// parentOwner is the owner of the catalog the securable belongs to, which a
	// schema, a table and a volume have: the levels above a catalog hang from
	// the metastore directly.
	parentOwner string

	// schemaOwner is the owner of the schema the securable belongs to, which
	// only a table and a volume have.
	schemaOwner string

	fullName    string
	workspace   string
	workspaceID string
}

// ensureGrantsAreTrustworthy refuses the call unless the privilege list is
// complete. Databricks returns HTTP 200 with a partial list, and reporting it
// would make ConductorOne revoke the rest. The check is not cached.
func ensureGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	level securableLevel,
	resource *v2.Resource,
	fullName, workspace string,
	annos *annotations.Annotations,
) error {
	profile := rs.GetProfile(resource)
	owner, _ := rs.GetProfileStringValue(profile, profileKeyOwner)
	metastoreOwner, _ := rs.GetProfileStringValue(profile, profileKeyMetastoreOwner)
	workspaceID, _ := rs.GetProfileStringValue(profile, profileKeyWorkspaceID)
	// A metastore and a catalog hang from the metastore directly, so these keys
	// are absent there and the only owner above is the metastore's.
	parentOwner, _ := rs.GetProfileStringValue(profile, profileKeyCatalogOwner)
	schemaOwner, _ := rs.GetProfileStringValue(profile, profileKeySchemaOwner)

	trustworthy, err := level.trusts(ctx, client, securableTrust{
		ownPrincipal:   clientID,
		owner:          owner,
		metastoreOwner: metastoreOwner,
		parentOwner:    parentOwner,
		schemaOwner:    schemaOwner,
		fullName:       fullName,
		workspace:      workspace,
		workspaceID:    workspaceID,
	}, annos)
	if err != nil {
		return err
	}
	if !trustworthy {
		return status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every privilege on %s %s through workspace %s, and Databricks answers a partial read with 200, "+
				"so reporting it would revoke the access ConductorOne cannot see; %s",
			level.noun, fullName, workspace, level.trustRemedy)
	}

	return nil
}

// syncSecurableGrants emits the privileges held on one securable, plus its owner.
// The workspace comes off the profile the last List wrote: every Unity Catalog
// read is addressed to a workspace host, and resolving one here would be an
// account-plane round trip per securable.
func syncSecurableGrants(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	sync syncPrincipals,
	level securableLevel,
	resource *v2.Resource,
	attr rs.SyncOpAttrs,
) ([]*v2.Grant, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)
	resourceID := resource.GetId().GetResource()
	fullName, err := level.permissionsName(resourceID)
	if err != nil {
		return nil, nil, err
	}

	profile := rs.GetProfile(resource)
	workspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	if workspace == "" {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: %s %s has no workspace on its profile", level.noun, fullName)
	}

	annos := annotations.Annotations{}
	pageToken := attr.PageToken.Token
	// A metastore's route was already chosen as a workspace the connector can fully
	// read. Other levels still have to prove it here, once per securable: the proof
	// is an uncached read, and a transient refusal on a later page would otherwise
	// reverse a verdict the earlier pages were already emitted under.
	if level.securableType != databricks.SecurableMetastore && pageToken == "" {
		if err := ensureGrantsAreTrustworthy(ctx, client, clientID, level, resource, fullName, workspace, &annos); err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
	}

	assignments, nextPageToken, rateLimit, err := client.ListPermissions(ctx, workspace, level.securableType, fullName, "", pageToken)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		// FailedPrecondition, not NotFound: the SDK reads NotFound as "no grants".
		// Whether the securable is gone is settled by the next List.
		return nil, &rs.SyncOpResults{Annotations: annos}, uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: failed to list permissions on %s %s through workspace %s", level.noun, fullName, workspace), err)
	}

	resolver := newPrincipalResolver(client, &annos)
	var rv []*v2.Grant
	for _, assignment := range assignments {
		principalId, err := resolver.resolve(ctx, assignment.PrincipalID.String(), assignment.Principal)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: %w", err)
		}
		if principalId == nil {
			l.Debug("databricks-connector: skipping a grant whose principal does not resolve",
				zap.String("securable", resourceID),
				zap.String("principal", assignment.Principal),
				zap.String("principal_id", assignment.PrincipalID.String()))

			continue
		}
		if !emitPrincipal(sync, principalId.GetResourceType()) {
			continue
		}

		options := groupSecurableGrantOptions(principalId)
		for _, privilege := range assignment.Privileges {
			if !slices.Contains(level.privileges, privilege) {
				// A privilege Databricks added after this connector shipped is not
				// an entitlement, so it cannot be emitted. Say so rather than
				// dropping it without a trace.
				l.Debug("databricks-connector: the securable reports a privilege this connector does not model",
					zap.String("securable", resourceID),
					zap.String("privilege", privilege))

				continue
			}
			rv = append(rv, grant.NewGrant(resource, privilege, principalId, options...))
		}
	}

	// The owner is emitted once, with the first page.
	if owner, _ := rs.GetProfileStringValue(profile, profileKeyOwner); pageToken == "" && owner != "" {
		principalId, err := resolver.resolve(ctx, "", owner)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: %w", err)
		}
		if principalId != nil && emitPrincipal(sync, principalId.GetResourceType()) {
			options := append(groupSecurableGrantOptions(principalId), grant.WithAnnotation(&v2.GrantImmutable{}))
			rv = append(rv, grant.NewGrant(resource, ownerEntitlement, principalId, options...))
		}
	}

	if nextPageToken != "" && nextPageToken == pageToken {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.Internal,
			"databricks-connector: listing privileges on %s %s repeated page token %q", level.noun, fullName, nextPageToken)
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextPageToken}, nil
}

// change is one validated Grant or Revoke, resolved down to what the PATCH needs.
type change struct {
	level         securableLevel
	fullName      string
	workspace     string
	privilege     string
	scimID        string
	principalName string
}

// resolveChange validates a Grant/Revoke and resolves what the PATCH needs.
func resolveChange(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	level securableLevel,
	principalID *v2.ResourceId,
	entitlement *v2.Entitlement,
	isGrant bool,
	annos *annotations.Annotations,
) (change, error) {
	if !isValidPrincipal(principalID) {
		return change{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: only users, groups and service principals can hold a %s privilege, got %s",
			level.noun, principalID.GetResourceType())
	}

	resource := entitlement.GetResource()
	fullName, err := level.permissionsName(resource.GetId().GetResource())
	if err != nil {
		return change{}, err
	}

	privilege, err := parseEntitlementPrivilege(entitlement.GetId())
	if err != nil {
		return change{}, err
	}
	switch {
	case privilege == ownerEntitlement:
		return change{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s ownership cannot be provisioned; change the owner in Databricks", level.noun)
	case slices.Contains(legacyPrivileges, strings.ToUpper(strings.TrimSpace(privilege))):
		return change{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is a legacy alias Databricks rewrites; use the explicit privilege", privilege)
	case !slices.Contains(level.privileges, privilege):
		return change{}, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s is not a grantable %s privilege", privilege, level.noun)
	}

	workspace, _ := rs.GetProfileStringValue(rs.GetProfile(resource), profileKeyWorkspace)
	if workspace == "" {
		return change{}, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: %s %s has no workspace on its profile", level.noun, fullName)
	}

	// A privilege list the connector cannot fully read also cannot be safely
	// written to: the PATCH would be built from the rows it is allowed to see.
	if err := ensureGrantsAreTrustworthy(ctx, client, clientID, level, resource, fullName, workspace, annos); err != nil {
		return change{}, err
	}

	scimID, err := unityPrincipalScimID(principalID)
	if err != nil {
		return change{}, fmt.Errorf("databricks-connector: %w", err)
	}

	principalName, err := unityPrincipalName(ctx, client, principalID)
	if err != nil {
		if !errors.Is(err, errPrincipalNotFound) {
			return change{}, fmt.Errorf("databricks-connector: failed to resolve principal %s: %w", principalID.GetResource(), err)
		}
		// Revoke addresses a vanished principal's rows by id.
		if isGrant {
			return change{}, status.Errorf(codes.NotFound, "databricks-connector: principal %s no longer exists in Databricks", scimID)
		}
	}

	return change{
		level:         level,
		fullName:      fullName,
		workspace:     workspace,
		privilege:     privilege,
		scimID:        scimID,
		principalName: principalName,
	}, nil
}

func (c change) matchHeld(assignments []databricks.PrivilegeAssignment) (databricks.PrivilegeAssignment, bool) {
	return matchHeldPrivilege(assignments, c.scimID, c.principalName, c.privilege)
}

func (c change) updateError(verb string, err error) error {
	if isForbiddenError(err) {
		return uhttp.WrapErrors(codes.PermissionDenied,
			fmt.Sprintf("databricks-connector: cannot %s %s on %s %s: %s",
				verb, c.privilege, c.level.noun, c.fullName, c.level.writeRemedy),
			err)
	}

	return fmt.Errorf("databricks-connector: failed to %s %s on %s %s: %w", verb, c.privilege, c.level.noun, c.fullName, err)
}

// grantSecurable adds one privilege. Databricks no-ops a PATCH it cannot apply,
// so the pre-read decides GrantAlreadyExists and the echo is the only proof the
// change landed.
func grantSecurable(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	level securableLevel,
	principal *v2.Resource,
	entitlement *v2.Entitlement,
) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	change, err := resolveChange(ctx, client, clientID, level, principal.GetId(), entitlement, true, &annos)
	if err != nil {
		return annos, err
	}

	// Read past the cache: a stale page would skip a needed PATCH.
	assignments, rateLimit, err := client.DrainPermissions(ctx, change.workspace, level.securableType, change.fullName)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w", level.noun, change.fullName, err)
	}
	if _, held := change.matchHeld(assignments); held {
		annos.Update(&v2.GrantAlreadyExists{})
		return annos, nil
	}

	var echoed []databricks.PrivilegeAssignment
	rateLimit, err = client.UpdatePermissionsUntil(ctx, change.workspace, level.securableType, change.fullName,
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

	if _, held := change.matchHeld(echoed); !held {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: granting %s on %s %s was accepted but not applied", change.privilege, level.noun, change.fullName)
	}

	return annos, nil
}

// revokeSecurable removes one privilege. Databricks no-ops a privilege already
// absent, so the pre-read decides GrantAlreadyRevoked and the echo confirms the
// removal.
func revokeSecurable(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	level securableLevel,
	revoked *v2.Grant,
) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	change, err := resolveChange(ctx, client, clientID, level, revoked.GetPrincipal().GetId(), revoked.GetEntitlement(), false, &annos)
	if err != nil {
		return annos, err
	}

	var seen []databricks.PrivilegeAssignment
	rateLimit, err := client.ForEachUncachedPermissionsPage(ctx, change.workspace, level.securableType, change.fullName, "",
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			seen = append(seen, page...)
			matched, ok := change.matchHeld(seen)
			// Only an id match is final; a name match may be superseded on a later page.
			return ok && change.scimID != "" && matched.PrincipalID.String() == change.scimID && change.scimID != "0", nil
		})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		// Databricks names the missing securable in error_code. That is the only
		// answer that proves it is gone, so there is nothing left to revoke: a
		// bare 404 also comes back from a workspace the securable is not bound to.
		if databricks.IsSecurableGone(err, level.goneErrorCodes...) {
			annos.Update(&v2.GrantAlreadyRevoked{})

			return annos, nil
		}

		return annos, fmt.Errorf("databricks-connector: failed to read privileges on %s %s: %w", level.noun, change.fullName, err)
	}

	// A missed row can be a partial read, and removing a privilege nobody holds is a no-op.
	held, ok := change.matchHeld(seen)
	removal := databricks.PermissionsChange{Principal: change.principalName, Remove: []string{change.privilege}}
	switch heldID := held.PrincipalID.String(); {
	case ok && heldID != "" && heldID != "0":
		removal = databricks.PermissionsChange{PrincipalID: held.PrincipalID, Remove: []string{change.privilege}}
	case change.principalName == "":
		removal = databricks.PermissionsChange{PrincipalID: json.Number(change.scimID), Remove: []string{change.privilege}}
	}

	result, rateLimit, err := client.UpdatePermissions(ctx, change.workspace, level.securableType, change.fullName,
		[]databricks.PermissionsChange{removal})
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return annos, change.updateError("revoke", err)
	}
	if _, stillHeld := change.matchHeld(result); stillHeld {
		return annos, status.Errorf(codes.Internal,
			"databricks-connector: revoking %s on %s %s was accepted but not applied", change.privilege, level.noun, change.fullName)
	}
	if !ok {
		annos.Update(&v2.GrantAlreadyRevoked{})
	}

	return annos, nil
}
