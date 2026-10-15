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
	securableTypeCatalog   = "CATALOG"
	securableTypeSchema    = "SCHEMA"

	profileKeyMetastoreID    = "metastore_id"
	profileKeyMetastoreOwner = "metastore_owner"
	profileKeyFullName       = "full_name"
	profileKeyOwner          = "owner"
	profileKeySecurableType  = "securable_type"
	profileKeyCatalogName    = "catalog_name"
	profileKeySchemaName     = "schema_name"
	profileKeyCatalogType    = "catalog_type"
	profileKeyIsolationMode  = "isolation_mode"
	profileKeyRegion         = "region"
	profileKeyWorkspace      = "workspace"
	profileKeyWorkspaceID    = "workspace_id"
	profileKeyWorkspaceName  = "workspace_name"

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

// parseSecurableRef reads a securable resource id, "{metastore_id}::{full_name}".
func parseSecurableRef(resourceID string) (securableRef, error) {
	metastoreID, fullName, ok := strings.Cut(resourceID, securableKeySeparator)
	if !ok || metastoreID == "" || fullName == "" {
		return securableRef{}, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid securable resource id %q", resourceID)
	}

	return securableRef{metastoreID: metastoreID, parts: strings.Split(fullName, ".")}, nil
}

func (r securableRef) catalog() string {
	return r.part(0)
}

func (r securableRef) part(i int) string {
	if i < len(r.parts) {
		return r.parts[i]
	}

	return ""
}

func (r securableRef) child(name string) securableRef {
	return securableRef{metastoreID: r.metastoreID, parts: append(slices.Clone(r.parts), name)}
}

// isUnderInformationSchema reports a securable inside Databricks' information_schema,
// which is a virtual schema the connector does not sync.
func isUnderInformationSchema(ref securableRef) bool {
	return slices.Contains(ref.parts, "information_schema")
}

// securable is one row of a child listing, before it becomes a resource.
type securable struct {
	name    string
	owner   string
	profile map[string]any
}

// RawId equals the resource id: C1 matches preloaded resources on that annotation.
func newSecurableResource(
	resourceType *v2.ResourceType,
	securableTypeEnum string,
	childTypes []*v2.ResourceType,
	item securable,
	ref securableRef,
	parent *v2.ResourceId,
) (*v2.Resource, error) {
	profile := map[string]any{
		profileKeyMetastoreID:   ref.metastoreID,
		profileKeyFullName:      ref.fullName(),
		profileKeyOwner:         item.owner,
		profileKeySecurableType: securableTypeEnum,
	}
	for key, value := range item.profile {
		profile[key] = value
	}

	resourceKey := ref.resourceKey()
	options := []rs.ResourceOption{
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: resourceKey}),
	}

	for _, childType := range childTypes {
		// Without this annotation the child's List is never invoked.
		options = append(options, rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: childType.Id}))
	}

	// The display name is the full name rather than the leaf: main.sales.orders and
	// main.finance.orders would otherwise be two rows reading "orders" in C1.
	return rs.NewAppResource(ref.fullName(), resourceType, resourceKey, nil, options...)
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
	// writeRemedy is what a 403 on the PATCH tells the operator.
	writeRemedy string
	// goneCodes are the Databricks error_code values that mean this securable
	// itself was deleted. A 404 with any other code is a routing miss.
	goneCodes []string
	// access resolves the workspace a write goes through, confirmed against
	// Databricks rather than the profile.
	access func(ctx context.Context, d *securableDeps, resource *v2.Resource, ref securableRef) (string, accessState, *v2.RateLimitDescription, error)
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

	// The profile was written by the last sync, and the securable may have been
	// deleted since: a write must confirm it is still listed before addressing it.
	workspace, state, rateLimit, err := kind.access(ctx, d, entitlement.GetResource(), ref)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		if status.Code(err) == codes.PermissionDenied {
			return securableChange{}, annos, true, err
		}

		return securableChange{}, annos, true, fmt.Errorf("databricks-connector: failed to resolve the access path for %s: %w", ref.resourceKey(), err)
	}
	switch state {
	case securableGone:
		if mode == changeRevoke {
			annos.Update(&v2.GrantAlreadyRevoked{})
			return securableChange{}, annos, true, nil
		}

		return securableChange{}, annos, true, status.Errorf(codes.NotFound,
			"databricks-connector: %s %s no longer exists in Databricks, so it cannot be changed", kind.name, ref.permissionsName())
	case securableOutOfScope:
		// The securable still holds its privileges, so a revoke cannot be reported done.
		return securableChange{}, annos, true, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: %s %s cannot be changed: no workspace this sync can use reaches it — every one that could is "+
				"left out of workspaces or named in databricks-exclude-workspaces, or none is attached — so it is outside the sync scope",
			kind.name, ref.permissionsName())
	case securableInScope:
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
) (string, accessState, *v2.RateLimitDescription, error) {
	if !confirm {
		route := readProfileRoute(resource)
		snap := profileRoutingSnapshot(ref.metastoreID, route.workspace, route.workspaceID, route.owner)
		workspace, ok, rateLimit, err := d.profileWorkspace(ctx, ref, route, func(ctx context.Context, workspace string) (bool, *v2.RateLimitDescription, error) {
			return d.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
		})
		if err != nil {
			return "", securableInScope, rateLimit, err
		}
		if ok {
			return workspace, securableInScope, rateLimit, nil
		}
	}

	snap, rateLimit, err := d.uc.buildRouting(ctx)
	if err != nil {
		return "", securableInScope, rateLimit, err
	}
	workspaces, state, err := usableWorkspaces(snap, ref.metastoreID)
	if err != nil || state != securableInScope {
		return "", state, rateLimit, err
	}

	workspace, probeRateLimit, err := firstTrustedWorkspace(ctx, ref, workspaces, func(ctx context.Context, workspace string) (bool, *v2.RateLimitDescription, error) {
		return d.uc.metastoreGrantsAreTrustworthy(ctx, snap, ref.metastoreID, workspace)
	})
	if probeRateLimit != nil {
		rateLimit = probeRateLimit
	}
	if err != nil {
		return "", securableInScope, rateLimit, err
	}
	if workspace == "" {
		return "", securableInScope, rateLimit, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant on metastore %s: the connector principal must own the metastore or be assigned Admin directly on a workspace attached to it",
			ref.metastoreID)
	}

	return workspace, securableInScope, rateLimit, nil
}

// workspaceProbe reports whether every grant on a securable is visible through workspace.
type workspaceProbe func(ctx context.Context, workspace string) (bool, *v2.RateLimitDescription, error)

// firstTrustedWorkspace returns the first workspace whose probe passes, or "" when
// none does. Visibility through any one workspace is enough, and the listing order
// says nothing about which one that is. A workspace that refuses the probe is skipped.
func firstTrustedWorkspace(
	ctx context.Context,
	ref securableRef,
	workspaces []string,
	probe workspaceProbe,
) (string, *v2.RateLimitDescription, error) {
	var rateLimit *v2.RateLimitDescription
	for _, workspace := range workspaces {
		trustworthy, probeRateLimit, err := probe(ctx, workspace)
		if probeRateLimit != nil {
			rateLimit = probeRateLimit
		}
		if err != nil {
			if ctx.Err() != nil || !isUnreadableWorkspaceError(err) {
				return "", rateLimit, err
			}
			ctxzap.Extract(ctx).Debug("databricks-connector: the visibility probe was refused through this workspace; trying the next one",
				zap.String("securable", ref.resourceKey()),
				zap.String("workspace", workspace),
				zap.Error(err),
			)

			continue
		}
		if trustworthy {
			return workspace, rateLimit, nil
		}
	}

	return "", rateLimit, nil
}

// profileRoute is the access path the last sync wrote on a securable's profile.
type profileRoute struct {
	workspace      string
	workspaceID    string
	workspaceName  string
	owner          string
	metastoreOwner string
}

func readProfileRoute(resource *v2.Resource) profileRoute {
	return profileRoute{
		workspace:      profileString(resource, profileKeyWorkspace),
		workspaceID:    profileString(resource, profileKeyWorkspaceID),
		workspaceName:  profileString(resource, profileKeyWorkspaceName),
		owner:          profileString(resource, profileKeyOwner),
		metastoreOwner: profileString(resource, profileKeyMetastoreOwner),
	}
}

// profileRouteAllowed reports whether the profile workspace is still one this
// sync is allowed to read through.
func profileRouteAllowed(uc *unityCatalog, route profileRoute) bool {
	if route.workspace == "" || route.workspaceID == "" {
		return false
	}
	if len(uc.workspaces) == 0 {
		return true
	}
	_, ok := matchConfiguredWorkspace(uc.workspaces, route.workspace, route.workspaceName, route.workspaceID)

	return ok
}

// profileWorkspace reports false whenever the profile workspace cannot be used
// as is, so the caller rebuilds the routing instead. Only a rate limit or a
// cancelled context is returned: retrying elsewhere would not help either.
func (d *securableDeps) profileWorkspace(
	ctx context.Context,
	ref securableRef,
	route profileRoute,
	probe workspaceProbe,
) (string, bool, *v2.RateLimitDescription, error) {
	if !profileRouteAllowed(d.uc, route) {
		return "", false, nil, nil
	}

	trustworthy, rateLimit, err := probe(ctx, route.workspace)
	if err != nil {
		if ctx.Err() != nil || isRateLimitError(err) {
			return "", false, rateLimit, err
		}
		ctxzap.Extract(ctx).Debug("databricks-connector: the workspace on the profile did not answer; rebuilding the routing",
			zap.String("securable", ref.resourceKey()),
			zap.String("workspace", route.workspace),
			zap.Error(err),
		)

		return "", false, rateLimit, nil
	}

	return route.workspace, trustworthy, rateLimit, nil
}

func profileString(resource *v2.Resource, key string) string {
	value, _ := rs.GetProfileStringValue(rs.GetProfile(resource), key)
	return value
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
