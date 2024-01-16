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

func roleResource(ctx context.Context, role string, ent bool, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"role_id": role,
	}

	if ent {
		profile["type"] = EntitlementType
	} else {
		profile["type"] = RoleType
	}

	roleTraitOptions := []rs.RoleTraitOption{
		rs.WithRoleProfile(profile),
	}

	resource, err := rs.NewRoleResource(
		role,
		roleResourceType,
		role,
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
	for _, role := range roles {
		rr, err := roleResource(ctx, role, false, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, rr)
	}

	for _, ent := range entitlements {
		er, err := roleResource(ctx, ent, true, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, er)
	}

	return rv, "", nil, nil
}

func (r *roleBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	entitlementOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s role", resource.DisplayName)),
		ent.WithDescription(fmt.Sprintf("%s Databricks role", resource.DisplayName)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(resource, RoleMemberEntitlement, entitlementOptions...))

	return rv, "", nil, nil
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

	rType, ok := rs.GetProfileStringValue(roleTrait.Profile, "type")
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
		users, total, err := r.client.ListUsers(
			ctx,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewUserRolesAttrVars(),
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list users: %w", err)
		}

		// check if user has the role
		for _, u := range users {
			if rType == RoleType && u.HaveRole(resource.Id.Resource) {
				uID, err := rs.NewResourceID(userResourceType, u.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create user resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, uID))
			} else if rType == EntitlementType && u.HaveEntitlement(resource.Id.Resource) {
				uID, err := rs.NewResourceID(userResourceType, u.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create user resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, uID))
			}
		}

		token := prepareNextToken(page, uint(len(users)), total)
		err = bag.Next(token)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	case groupResourceType.Id:
		// groups don't contain roles, only entitlements
		if rType == RoleType {
			bag.Pop()
			break
		}

		groups, total, err := r.client.ListGroups(
			ctx,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewGroupRolesAttrVars(),
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list groups: %w", err)
		}

		// check if group has the role
		for _, g := range groups {
			if rType == EntitlementType && g.HaveEntitlement(resource.Id.Resource) {
				gID, err := rs.NewResourceID(groupResourceType, g.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create group resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, gID))
			}
		}

		token := prepareNextToken(page, uint(len(groups)), total)
		err = bag.Next(token)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
		}

	case servicePrincipalResourceType.Id:
		servicePrincipals, total, err := r.client.ListServicePrincipals(
			ctx,
			databricks.NewPaginationVars(page, ResourcesPageSize),
			databricks.NewServicePrincipalRolesAttrVars(),
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list service principals: %w", err)
		}

		// check if service principal has the role
		for _, sp := range servicePrincipals {
			if rType == RoleType && sp.HaveRole(resource.Id.Resource) {
				spID, err := rs.NewResourceID(servicePrincipalResourceType, sp.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create service principal resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, spID))
			} else if rType == EntitlementType && sp.HaveEntitlement(resource.Id.Resource) {
				spID, err := rs.NewResourceID(servicePrincipalResourceType, sp.ID)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to create service principal resource id: %w", err)
				}

				rv = append(rv, grant.NewGrant(resource, RoleMemberEntitlement, spID))
			}
		}

		token := prepareNextToken(page, uint(len(servicePrincipals)), total)
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

func newRoleBuilder(client *databricks.Client) *roleBuilder {
	return &roleBuilder{
		client:       client,
		resourceType: roleResourceType,
	}
}
