package connector

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

const (
	RoleMemberEntitlement = "member"
	RoleType              = "role"
	EntitlementType       = "entitlement"
)

var roles = []string{
	AccountAdminRole,
}

var entitlements = []string{
	WorkspaceAccessRole,
	SQLAccessRole,
	ClusterCreateRole,
	InstancePoolCreateRole,
}

type roleBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
	// unverifiableRoleWarns is the builder's process-wide count, which samples the
	// stripped-projection warning (same logarithmic pattern as the audit feed's
	// lookup-failure warning).
	unverifiableRoleWarns *atomic.Int64
	pageProgress          *paginationProgress
}

func (r *roleBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return roleResourceType
}

// roleResourceId builds a role's resource ID, namespaced by workspace for workspace roles.
func roleResourceId(role string, parent *v2.ResourceId) string {
	if parent.GetResourceType() == workspaceResourceType.Id {
		return fmt.Sprintf("%s:%s", parent.Resource, role)
	}
	return role
}

func roleResource(ctx context.Context, role string, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"role_name":   role,
		"parent_type": parent.GetResourceType(),
		"parent_id":   parent.GetResource(),
	}

	roleID := roleResourceId(role, parent)

	resource, err := rs.NewRoleResource(
		role,
		roleResourceType,
		roleID,
		nil,
		rs.WithParentResourceID(parent),
		rs.WithResourceProfile(profile),
		rs.WithAnnotation(&v2.RawId{Id: roleID}),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the roles from the database as resource objects.
// Roles include a RoleTrait because they are the 'shape' of a standard role.
func (r *roleBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil {
		return nil, nil, nil
	}

	var rv []*v2.Resource
	if parentResourceID.ResourceType == accountResourceType.Id {
		for _, role := range roles {
			rr, err := roleResource(ctx, role, parentResourceID)
			if err != nil {
				return nil, nil, err
			}

			rv = append(rv, rr)
		}
	}

	if parentResourceID.ResourceType == workspaceResourceType.Id {
		for _, ent := range entitlements {
			er, err := roleResource(ctx, ent, parentResourceID)
			if err != nil {
				return nil, nil, err
			}

			rv = append(rv, er)
		}
	}

	return rv, nil, nil
}

// Entitlements returns membership entitlements for a given role.
func (r *roleBuilder) Entitlements(
	_ context.Context,
	resource *v2.Resource,
	_ rs.SyncOpAttrs,
) (
	[]*v2.Entitlement,
	*rs.SyncOpResults,
	error,
) {
	return []*v2.Entitlement{
		ent.NewAssignmentEntitlement(
			resource,
			RoleMemberEntitlement,
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
			ent.WithDisplayName(fmt.Sprintf("%s role", resource.DisplayName)),
			ent.WithDescription(fmt.Sprintf("%s Databricks role", resource.DisplayName)),
		),
	}, nil, nil
}

// Grants returns all the grants for a given role.
// Since Databricks API does not support listing grants for a role, so that it aligns with the sdk API,
// we have to go through all the users, groups and servicePrincipals to check if they have the role.
func (r *roleBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	var rv []*v2.Grant

	profile := rs.GetProfile(resource)

	parentType, parentID, err := getParentInfoFromProfile(profile)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to get parent info from role profile: %w", err)
	}

	isWorkspaceRole := parentType == workspaceResourceType.Id

	var workspaceId string
	// If the role is a workspace role, we need to update the client config to use the workspace API.
	if isWorkspaceRole {
		workspaceId = parentID
	}

	roleName, ok := rs.GetProfileStringValue(profile, "role_name")
	if !ok {
		return nil, nil, fmt.Errorf("databricks-connector: failed to get role type from role profile")
	}

	bag, page, err := parsePageToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: roleResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	switch bag.ResourceTypeID() {
	case roleResourceType.Id:
		bag.Pop()
		bag.Push(pagination.PageState{
			ResourceTypeID: userResourceType.Id,
		})
		bag.Push(pagination.PageState{
			ResourceTypeID: groupResourceType.Id,
		})
		bag.Push(pagination.PageState{
			ResourceTypeID: servicePrincipalResourceType.Id,
		})

	case userResourceType.Id:
		users, total, _, err := r.client.ListUsers(
			ctx,
			workspaceId,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewUserRolesAttrVars(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to list users: %w", err)
		}

		// check if user has the role
		for _, u := range users {
			if !isWorkspaceRole && u.HaveRole(roleName) {
				uID, err := rs.NewResourceID(userResourceType, u.ID)
				if err != nil {
					return nil, nil, fmt.Errorf("databricks-connector: failed to create user resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, uID))
			} else if isWorkspaceRole && u.HaveEntitlement(roleName) {
				uID, err := rs.NewResourceID(userResourceType, u.ID)
				if err != nil {
					return nil, nil, fmt.Errorf("databricks-connector: failed to create user resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, uID))
			}
		}

		token, tokErr := prepareNextToken(page, len(users), total, pageProgressCheck{r.pageProgress, "roles/users/" + roleName, func() string { return users[0].ID }})
		if tokErr != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to prepare next page token: %w", tokErr)
		}
		err = bag.Next(token)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	case groupResourceType.Id:
		groups, total, _, err := r.client.ListGroups(
			ctx,
			workspaceId,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewGroupRolesAttrVars(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to list groups: %w", err)
		}

		// check if group has the role
		l := ctxzap.Extract(ctx)
		for _, g := range groups {
			fromFallback := false
			// Classify the group. Groups without meta in the list object get a fallback
			// read chain. Vendor evidence (CI tenant, runs 45715149358 / 45719189560 /
			// 45720792196): the account-plane SCIM API strips an explicit
			// attributes=roles,entitlements,meta projection from multi-group lists,
			// filtered lists AND single-group GETs; a filtered read with NO attributes
			// param filtered by id also came back stripped; but a displayName-filtered
			// read with no attributes param returned meta twice. So: read by id with the
			// default projection; if that still carries no meta, resolve the group's
			// displayName via the honored id,displayName projection and re-read by
			// displayName. Only meta-less entries trigger fallback reads, and results
			// are not cached across calls.
			if g.GroupClass() == databricks.UnknownGroupClass {
				l.Debug("databricks-connector: list entry missing meta; classifying via filtered single-group read",
					zap.String("group_id", g.ID))
				found, _, _, err := r.client.FindGroupByID(ctx, workspaceId, g.ID)
				if err != nil {
					return nil, nil, fmt.Errorf("databricks-connector: failed to fetch group %s (%s) to classify it: %w", g.ID, g.DisplayName, err)
				}
				if len(found) == 0 {
					// The group vanished mid-sync (deleted, or not visible on this plane);
					// skip it like the other NotFound-on-item cases.
					l.Debug("databricks-connector: skipping group missing at read time", zap.String("group_id", g.ID))
					continue
				}
				if found[0].ID != g.ID {
					// The vendor ignored the id filter and returned a different group;
					// trusting it would grant another group's roles to the wrong principal.
					return nil, nil, uhttp.WrapErrors(codes.Internal,
						fmt.Sprintf("databricks-connector: filtered read for group %s returned group %s instead; "+
							"failing sync so existing grants are preserved", g.ID, found[0].ID))
				}
				g = found[0]
				fromFallback = true
				if g.GroupClass() == databricks.UnknownGroupClass {
					// The default-projection id-filtered read was stripped too. Resolve the
					// display name (the id,displayName projection IS honored) and read by
					// name, the one shape proven to carry meta on this tenant.
					named, _, _, err := r.client.FindGroupByID(ctx, workspaceId, g.ID, databricks.NewGroupAttrVars())
					if err != nil {
						return nil, nil, fmt.Errorf("databricks-connector: failed to fetch group %s display name to classify it: %w", g.ID, err)
					}
					if len(named) == 0 {
						// Vanished between reads.
						l.Debug("databricks-connector: skipping group missing at read time", zap.String("group_id", g.ID))
						continue
					}
					byName, _, _, err := r.client.FindGroupByName(ctx, workspaceId, named[0].DisplayName)
					if err != nil {
						return nil, nil, fmt.Errorf("databricks-connector: failed to fetch group %s (%s) to classify it: %w", g.ID, named[0].DisplayName, err)
					}
					if len(byName) != 0 {
						if byName[0].ID != g.ID {
							// The vendor ignored the displayName filter and returned a
							// different group; trusting it would grant another group's roles
							// to the wrong principal.
							return nil, nil, uhttp.WrapErrors(codes.Internal,
								fmt.Sprintf("databricks-connector: filtered read for group %s returned group %s instead; "+
									"failing sync so existing grants are preserved", g.ID, byName[0].ID))
						}
						g = byName[0]
						fromFallback = true
					}
					// byName empty or still meta-less: g keeps no meta and the switch below
					// fails closed.
				}
			}

			switch g.GroupClass() {
			case databricks.UnknownGroupClass:
				// Even the fallback reads carry no meta: the group is genuinely
				// unclassifiable, so fail closed to preserve existing grants.
				return nil, nil, uhttp.WrapErrors(codes.Internal,
					fmt.Sprintf("databricks-connector: group %s (%s) missing meta.resourceType in SCIM response "+
						"despite attributes=roles,entitlements,meta; cannot classify account vs workspace group; "+
						"failing sync so existing grants are preserved", g.ID, g.DisplayName))
			case databricks.WorkspaceGroup:
				l.Debug("databricks-connector: skipping workspace-scoped group", zap.String("group_id", g.ID))
				continue
			case databricks.AccountGroup:
			}

			// A fallback-classified group may have been read with a projection the vendor
			// stripped: an absent roles/entitlements key decodes to nil, which is
			// indistinguishable from genuinely holding none. Fail-closing on that would
			// permanently break sync on tenants that never include these keys (the CI
			// tenant's account-plane responses never carry them), so surface it as a
			// sampled Warn instead of silently classifying the group as role-less. The
			// grant is still dropped when the vendor stripped the field — the log makes
			// that visible, nothing more.
			fromFallbackUnverifiable := false
			if isWorkspaceRole {
				fromFallbackUnverifiable = fromFallback && len(g.Entitlements) == 0
			} else {
				fromFallbackUnverifiable = fromFallback && len(g.Roles) == 0
			}
			if fromFallbackUnverifiable {
				if count := r.unverifiableRoleWarns.Add(1); count == 1 || count == 10 || count == 100 || count%1000 == 0 {
					l.Warn("databricks-connector: group carries no permission attributes in the vendor response; "+
						"its grants cannot be verified on this tenant and may be reported as deleted "+
						"(no read shape on this tenant returns roles/entitlements; remediation: re-enumerate "+
						"group-held roles via the account access-control grant_rules API)",
						zap.String("group_id", g.ID), zap.String("role", roleName),
						zap.Int64("total_occurrences", count))
				}
			}

			if (!isWorkspaceRole && g.HaveRole(roleName)) || (isWorkspaceRole && g.HaveEntitlement(roleName)) {
				accountId := r.client.GetAccountId()
				accountResourceId, err := rs.NewResourceID(accountResourceType, accountId)
				if err != nil {
					return rv, nil, err
				}
				resourceId, expandAnnotation, err := groupGrantExpansion(ctx, g.ID, accountResourceId)
				if err != nil {
					return rv, nil, err
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, resourceId, grant.WithAnnotation(expandAnnotation)))
			}
		}

		token, tokErr := prepareNextToken(page, len(groups), total, pageProgressCheck{r.pageProgress, "roles/groups/" + roleName, func() string { return groups[0].ID }})
		if tokErr != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to prepare next page token: %w", tokErr)
		}
		err = bag.Next(token)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	case servicePrincipalResourceType.Id:
		servicePrincipals, total, _, err := r.client.ListServicePrincipals(
			ctx,
			workspaceId,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewServicePrincipalRolesAttrVars(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to list service principals: %w", err)
		}

		// check if service principal has the role
		for _, sp := range servicePrincipals {
			if !isWorkspaceRole && sp.HaveRole(roleName) {
				spID, err := rs.NewResourceID(servicePrincipalResourceType, sp.ID)
				if err != nil {
					return nil, nil, fmt.Errorf("databricks-connector: failed to create service principal resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, spID))
			} else if isWorkspaceRole && sp.HaveEntitlement(roleName) {
				spID, err := rs.NewResourceID(servicePrincipalResourceType, sp.ID)
				if err != nil {
					return nil, nil, fmt.Errorf("databricks-connector: failed to create service principal resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, spID))
			}
		}

		token, tokErr := prepareNextToken(page, len(servicePrincipals), total, pageProgressCheck{r.pageProgress, "roles/service-principals/" + roleName, func() string { return servicePrincipals[0].ID }})
		if tokErr != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to prepare next page token: %w", tokErr)
		}
		err = bag.Next(token)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	default:
		return nil, nil, fmt.Errorf("databricks-connector: invalid resource type: %s", bag.ResourceTypeID())
	}

	nextPage, err := bag.Marshal()
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

// Get rebuilds a single role resource, used to re-sync it after a RESOURCE_CHANGE event.
// Roles are synthetic (not fetched from an API), so this just reconstructs the resource
// from its resource ID the same way List does.
func (r *roleBuilder) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	roleName := resourceId.Resource
	if parentResourceId.GetResourceType() == workspaceResourceType.Id {
		_, name, found := strings.Cut(resourceId.Resource, ":")
		if !found {
			return nil, nil, fmt.Errorf("databricks-connector: invalid workspace role resource id: %s", resourceId.Resource)
		}
		roleName = name
	}

	resource, err := roleResource(ctx, roleName, parentResourceId)
	if err != nil {
		return nil, nil, err
	}

	ctxzap.Extract(ctx).Debug("databricks-connector: Get: refreshed role",
		zap.String("resource_id", resourceId.Resource),
		zap.String("parent_resource_id", parentResourceId.GetResource()),
	)
	return resource, nil, nil
}

func (r *roleBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can be granted role membership",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can be granted role membership")
	}

	parentType, parentID, err := getParentInfoFromProfile(rs.GetProfile(entitlement.Resource))
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from role profile: %w", err)
	}

	isWorkspaceRole := parentType == workspaceResourceType.Id
	permissionName := entitlement.Resource.Id.Resource
	var workspaceId string
	if isWorkspaceRole {
		workspaceId = parentID
		permissionName = prepareWorkspaceRole(permissionName)
	}

	switch principal.Id.ResourceType {
	case userResourceType.Id:
		u, _, err := r.client.GetUser(ctx, workspaceId, principal.Id.Resource)
		if err != nil {
			return nil, principalLookupError(err, "user", principal.Id.Resource, isWorkspaceRole, workspaceId)
		}

		addPermissions(isWorkspaceRole, &u.Permissions, permissionName)

		_, err = r.client.UpdateUser(ctx, workspaceId, u)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to add role: %w", err)
		}

	case groupResourceType.Id:
		_, groupPrincipalId, err := parseResourceId(principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to parse principal resource id: %w", err)
		}

		g, _, err := r.client.GetGroupNoCache(ctx, workspaceId, groupPrincipalId.Resource, databricks.NewGroupRolesAttrVars())
		if err != nil {
			return nil, principalLookupError(err, "group", groupPrincipalId.Resource, isWorkspaceRole, workspaceId)
		}

		if groupHasPermission(g, isWorkspaceRole, permissionName) {
			return annotations.New(&v2.GrantAlreadyExists{}), nil
		}

		// Account roles and workspace entitlements are patched onto the group, so only the new value is written.
		_, err = r.client.PatchGroup(ctx, workspaceId, groupPrincipalId.Resource, addValueOp(groupPermissionAttribute(isWorkspaceRole), permissionName))
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to add role: %w", err)
		}

	case servicePrincipalResourceType.Id:
		sp, _, err := r.client.GetServicePrincipal(ctx, workspaceId, principal.Id.Resource)
		if err != nil {
			return nil, principalLookupError(err, "service principal", principal.Id.Resource, isWorkspaceRole, workspaceId)
		}

		addPermissions(isWorkspaceRole, &sp.Permissions, permissionName)

		_, err = r.client.UpdateServicePrincipal(ctx, workspaceId, sp)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to add role: %w", err)
		}

	default:
		return nil, fmt.Errorf("databricks-connector: invalid principal type: %s", principal.Id.ResourceType)
	}

	return nil, nil
}

func (r *roleBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	principal := grant.Principal
	entitlement := grant.Entitlement

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can have role membership revoked",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can have role membership revoked")
	}

	parentType, parentID, err := getParentInfoFromProfile(rs.GetProfile(entitlement.Resource))
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from role profile: %w", err)
	}

	isWorkspaceRole := parentType == workspaceResourceType.Id
	permissionName := entitlement.Resource.Id.Resource
	var workspaceId string
	if isWorkspaceRole {
		workspaceId = parentID
		permissionName = prepareWorkspaceRole(permissionName)
	}

	switch principal.Id.ResourceType {
	case userResourceType.Id:
		u, _, err := r.client.GetUser(ctx, workspaceId, principal.Id.Resource)
		if err != nil {
			return nil, principalLookupError(err, "user", principal.Id.Resource, isWorkspaceRole, workspaceId)
		}

		removePermissions(isWorkspaceRole, &u.Permissions, permissionName)

		_, err = r.client.UpdateUser(ctx, workspaceId, u)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to remove role: %w", err)
		}

	case groupResourceType.Id:
		_, groupPrincipalId, err := parseResourceId(principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to parse principal resource id: %w", err)
		}

		g, _, err := r.client.GetGroupNoCache(ctx, workspaceId, groupPrincipalId.Resource, databricks.NewGroupRolesAttrVars())
		if isNotFoundError(err) {
			// group not found (deleted, or not assigned to this workspace)
			// Unlike the user and service principal branches, a missing group has nothing left
			// to revoke, so a 404 is a no-op here instead of going through principalLookupError.
			return annotations.New(&v2.GrantAlreadyRevoked{}), nil
		}
		if err != nil {
			return nil, principalLookupError(err, "group", groupPrincipalId.Resource, isWorkspaceRole, workspaceId)
		}

		// The workspace SCIM API answers 400 to a remove on an empty attribute, so a role or
		// entitlement the group does not have is left as is instead of being patched.
		if !groupHasPermission(g, isWorkspaceRole, permissionName) {
			return annotations.New(&v2.GrantAlreadyRevoked{}), nil
		}

		_, err = r.client.PatchGroup(ctx, workspaceId, groupPrincipalId.Resource, removeValueOp(groupPermissionAttribute(isWorkspaceRole), permissionName))
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to remove role: %w", err)
		}

	case servicePrincipalResourceType.Id:
		sp, _, err := r.client.GetServicePrincipal(ctx, workspaceId, principal.Id.Resource)
		if err != nil {
			return nil, principalLookupError(err, "service principal", principal.Id.Resource, isWorkspaceRole, workspaceId)
		}

		removePermissions(isWorkspaceRole, &sp.Permissions, permissionName)

		_, err = r.client.UpdateServicePrincipal(ctx, workspaceId, sp)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to remove role: %w", err)
		}

	default:
		return nil, fmt.Errorf("databricks-connector: invalid principal type: %s", principal.Id.ResourceType)
	}

	return nil, nil
}

// groupHasPermission reports whether the group already holds the account role, or the workspace
// entitlement when isWorkspaceRole is set.
func groupHasPermission(g *databricks.Group, isWorkspaceRole bool, name string) bool {
	if isWorkspaceRole {
		return g.HaveEntitlement(name)
	}
	return g.HaveRole(name)
}

// principalLookupError explains a failed principal lookup for a role grant or revoke. The workspace
// SCIM API answers 404 for principals that are not assigned to the workspace.
func principalLookupError(err error, principalLabel, principalId string, isWorkspaceRole bool, workspaceId string) error {
	if isWorkspaceRole && isNotFoundError(err) {
		return uhttp.WrapErrors(
			codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: %s %s is not assigned to workspace %s; grant workspace membership first",
				principalLabel, principalId, workspaceId),
			err,
		)
	}

	return fmt.Errorf("databricks-connector: failed to get %s: %w", principalLabel, err)
}

func newRoleBuilder(client *databricks.Client) *roleBuilder {
	return &roleBuilder{
		client:                client,
		resourceType:          roleResourceType,
		unverifiableRoleWarns: &atomic.Int64{},
		pageProgress:          newPaginationProgress(),
	}
}
