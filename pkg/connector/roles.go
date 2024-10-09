package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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
}

func (r *roleBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return roleResourceType
}

func roleResource(ctx context.Context, role string, parent *v2.ResourceId) (*v2.Resource, error) {
	var roleID string
	profile := map[string]interface{}{
		"role_name":   role,
		"parent_type": parent.ResourceType,
		"parent_id":   parent.Resource,
	}

	// To differentiate between what type of role does the resource represent.
	if parent.ResourceType == workspaceResourceType.Id {
		roleID = fmt.Sprintf("%s:%s", parent.Resource, role)
	} else if parent.ResourceType == accountResourceType.Id {
		roleID = role
	}

	roleTraitOptions := []rs.RoleTraitOption{
		rs.WithRoleProfile(profile),
	}

	resource, err := rs.NewRoleResource(
		role,
		roleResourceType,
		roleID,
		roleTraitOptions,
		rs.WithParentResourceID(parent),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the roles from the database as resource objects.
// Roles include a RoleTrait because they are the 'shape' of a standard role.
func (r *roleBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	var rv []*v2.Resource
	if parentResourceID.ResourceType == accountResourceType.Id {
		for _, role := range roles {
			rr, err := roleResource(ctx, role, parentResourceID)
			if err != nil {
				return nil, "", nil, err
			}

			rv = append(rv, rr)
		}
	}

	if parentResourceID.ResourceType == workspaceResourceType.Id {
		for _, ent := range entitlements {
			er, err := roleResource(ctx, ent, parentResourceID)
			if err != nil {
				return nil, "", nil, err
			}

			rv = append(rv, er)
		}
	}

	return rv, "", nil, nil
}

// Entitlements returns membership entitlements for a given role.
func (r *roleBuilder) Entitlements(
	_ context.Context,
	resource *v2.Resource,
	_ *pagination.Token,
) (
	[]*v2.Entitlement,
	string,
	annotations.Annotations,
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
	}, "", nil, nil
}

// Grants returns all the grants for a given role.
// Since Databricks API does not support listing grants for a role, so that it aligns with the sdk API,
// we have to go through all the users, groups and servicePrincipals to check if they have the role.
func (r *roleBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var rv []*v2.Grant

	roleTrait, err := rs.GetRoleTrait(resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get role trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(roleTrait.Profile)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent info from role profile: %w", err)
	}

	isWorkspaceRole := parentType == workspaceResourceType.Id

	// If the role is a workspace role, we need to update the client config to use the workspace API.
	if isWorkspaceRole {
		r.client.SetWorkspaceConfig(parentID)
	} else {
		r.client.SetAccountConfig()
	}

	roleName, ok := rs.GetProfileStringValue(roleTrait.Profile, "role_name")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get role type: %w", err)
	}

	bag, page, err := parsePageToken(pToken.Token, &v2.ResourceId{ResourceType: roleResourceType.Id})
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
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
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewUserRolesAttrVars(),
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list users: %w", err)
		}

		// check if user has the role
		for _, u := range users {
			if !isWorkspaceRole && u.HaveRole(roleName) {
				uID, err := rs.NewResourceID(userResourceType, u.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create user resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, uID))
			} else if isWorkspaceRole && u.HaveEntitlement(roleName) {
				uID, err := rs.NewResourceID(userResourceType, u.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create user resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, uID))
			}
		}

		token := prepareNextToken(page, len(users), total)
		err = bag.Next(token)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	case groupResourceType.Id:
		groups, total, _, err := r.client.ListGroups(
			ctx,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewGroupRolesAttrVars(),
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list groups: %w", err)
		}

		// check if group has the role
		for _, g := range groups {
			// skip workspace specific groups (admins and users)
			if !g.IsAccountGroup() {
				continue
			}

			if (!isWorkspaceRole && g.HaveRole(roleName)) || (isWorkspaceRole && g.HaveEntitlement(roleName)) {
				memberResource, annotation, err := expandGrantForGroup(g.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to expand grant for group %s: %w", g.ID, err)
				}
				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, memberResource.Id, grant.WithAnnotation(annotation)))
			}
		}

		token := prepareNextToken(page, len(groups), total)
		err = bag.Next(token)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	case servicePrincipalResourceType.Id:
		servicePrincipals, total, _, err := r.client.ListServicePrincipals(
			ctx,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewServicePrincipalRolesAttrVars(),
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list service principals: %w", err)
		}

		// check if service principal has the role
		for _, sp := range servicePrincipals {
			if !isWorkspaceRole && sp.HaveRole(roleName) {
				spID, err := rs.NewResourceID(servicePrincipalResourceType, sp.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create service principal resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, spID))
			} else if isWorkspaceRole && sp.HaveEntitlement(roleName) {
				spID, err := rs.NewResourceID(servicePrincipalResourceType, sp.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create service principal resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, spID))
			}
		}

		token := prepareNextToken(page, len(servicePrincipals), total)
		err = bag.Next(token)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	default:
		return nil, "", nil, fmt.Errorf("databricks-connector: invalid resource type: %s", bag.ResourceTypeID())
	}

	nextPage, err := bag.Marshal()
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, nextPage, nil, nil
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

	roleTrait, err := rs.GetRoleTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get role trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(roleTrait.Profile)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from role profile: %w", err)
	}

	isWorkspaceRole := parentType == workspaceResourceType.Id
	permissionName := entitlement.Resource.Id.Resource
	if isWorkspaceRole {
		r.client.SetWorkspaceConfig(parentID)
		permissionName = prepareWorkspaceRole(permissionName)
	} else {
		r.client.SetAccountConfig()
	}

	switch principal.Id.ResourceType {
	case userResourceType.Id:
		u, _, err := r.client.GetUser(ctx, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get user: %w", err)
		}

		addPermissions(isWorkspaceRole, &u.Permissions, permissionName)

		_, err = r.client.UpdateUser(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to add role: %w", err)
		}

	case groupResourceType.Id:
		g, _, err := r.client.GetGroup(ctx, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group: %w", err)
		}

		addPermissions(isWorkspaceRole, &g.Permissions, permissionName)

		_, err = r.client.UpdateGroup(ctx, g)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to add role: %w", err)
		}

	case servicePrincipalResourceType.Id:
		sp, _, err := r.client.GetServicePrincipal(ctx, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get service principal: %w", err)
		}

		addPermissions(isWorkspaceRole, &sp.Permissions, permissionName)

		_, err = r.client.UpdateServicePrincipal(ctx, sp)
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

	roleTrait, err := rs.GetRoleTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get role trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(roleTrait.Profile)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from role profile: %w", err)
	}

	isWorkspaceRole := parentType == workspaceResourceType.Id
	permissionName := entitlement.Resource.Id.Resource
	if isWorkspaceRole {
		r.client.SetWorkspaceConfig(parentID)
		permissionName = prepareWorkspaceRole(permissionName)
	} else {
		r.client.SetAccountConfig()
	}

	switch principal.Id.ResourceType {
	case userResourceType.Id:
		u, _, err := r.client.GetUser(ctx, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get user: %w", err)
		}

		removePermissions(isWorkspaceRole, &u.Permissions, permissionName)

		_, err = r.client.UpdateUser(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to remove role: %w", err)
		}

	case groupResourceType.Id:
		g, _, err := r.client.GetGroup(ctx, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group: %w", err)
		}

		removePermissions(isWorkspaceRole, &g.Permissions, permissionName)

		_, err = r.client.UpdateGroup(ctx, g)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to remove role: %w", err)
		}

	case servicePrincipalResourceType.Id:
		sp, _, err := r.client.GetServicePrincipal(ctx, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get service principal: %w", err)
		}

		removePermissions(isWorkspaceRole, &sp.Permissions, permissionName)

		_, err = r.client.UpdateServicePrincipal(ctx, sp)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to remove role: %w", err)
		}

	default:
		return nil, fmt.Errorf("databricks-connector: invalid principal type: %s", principal.Id.ResourceType)
	}

	return nil, nil
}

func newRoleBuilder(client *databricks.Client) *roleBuilder {
	return &roleBuilder{
		client:       client,
		resourceType: roleResourceType,
	}
}
