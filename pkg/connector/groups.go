package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const groupMemberEntitlement = "member"

type groupBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (g *groupBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return groupResourceType
}

func groupResource(ctx context.Context, group *databricks.Group, parent *v2.ResourceId) (*v2.Resource, error) {
	members := make([]string, len(group.Members))

	for i, member := range group.Members {
		members[i] = member.Ref
	}

	profile := map[string]interface{}{
		"display_name": group.DisplayName,
	}

	if len(members) > 0 {
		profile["members"] = strings.Join(members, ",")
	}

	groupTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	resource, err := rs.NewGroupResource(
		group.DisplayName,
		groupResourceType,
		group.ID,
		groupTraitOptions,
		rs.WithParentResourceID(parent),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the groups from the database as resource objects.
// Groups include a GroupTrait because they are the 'shape' of a standard group.
func (g *groupBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pg *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	bag, page, err := parsePageToken(pg.Token, &v2.ResourceId{ResourceType: groupResourceType.Id})
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	groups, total, err := g.client.ListGroups(
		ctx,
		databricks.NewPaginationVars(page, ResourcesPageSize),
		databricks.NewGroupAttrVars(),
	)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, group := range groups {
		gCopy := group

		gr, err := groupResource(ctx, &gCopy, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, gr)
	}

	token := prepareNextToken(page, uint(len(groups)), total)
	nextPage, err := bag.NextToken(token)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, nextPage, nil, nil
}

// Entitlements return all entitlements relevant to the group.
// Databricks Group can be linked with group in some workspace
// and this function takes this in consideration.
// The difference is that you can setup `entitlements` for the group in the workspace.
// Group can have members, which represent membership entitlements,
// it can have permissions assigned to it, which represent role permissions entitlements,
// it can have roles assigned to it, which represent entitlements in Role resource,
// and it can have entitlements assigned to it, which represent entitlements in the workspace.
func (g *groupBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	// membership entitlement - for user members
	memberAssignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s %s", resource.DisplayName, groupMemberEntitlement)),
		ent.WithDescription(fmt.Sprintf("%s %s in Databricks", resource.DisplayName, groupMemberEntitlement)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(resource, groupMemberEntitlement, memberAssignmentOptions...))

	// role permissions entitlements
	// get all assignable roles for this specific group resource
	roles, err := g.client.ListRoles(context.Background(), "groups", resource.Id.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list roles for group %s: %w", resource.Id.Resource, err)
	}

	for _, role := range roles {
		rolePermissionOptions := []ent.EntitlementOption{
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
			ent.WithDisplayName(fmt.Sprintf("%s role", role.Name)),
			ent.WithDescription(fmt.Sprintf("%s role in Databricks", role.Name)),
		}

		rv = append(rv, ent.NewPermissionEntitlement(resource, role.Name, rolePermissionOptions...))
	}

	return rv, "", nil, nil
}

// Grants return all grants relevant to the group.
// Databricks Groups have membership and role grants.
func (g *groupBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var rv []*v2.Grant

	// membership grants
	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	membersPayload, ok := rs.GetProfileStringValue(groupTrait.Profile, "members")
	if ok {
		members := strings.Split(membersPayload, ",")

		for _, m := range members {
			pp := strings.Split(m, "/")
			if len(pp) != 2 {
				return nil, "", nil, fmt.Errorf("databricks-connector: invalid member format of %s: %w", m, err)
			}

			memberType, memberID := pp[0], pp[1]
			var resourceId *v2.ResourceId

			switch memberType {
			case "Users":
				resourceId = &v2.ResourceId{ResourceType: userResourceType.Id, Resource: memberID}
			case "ServicePrincipals":
				resourceId = &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: memberID}
			default:
				return nil, "", nil, fmt.Errorf("databricks-connector: invalid member type: %s", memberType)
			}

			rv = append(rv, grant.NewGrant(resource, groupMemberEntitlement, resourceId))
		}
	}

	// role grants
	ruleSets, err := g.client.ListRuleSets(ctx, "groups", resource.Id.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list role rule sets for group %s: %w", resource.Id.Resource, err)
	}

	for _, ruleSet := range ruleSets {
		for _, p := range ruleSet.Principals {
			pp := strings.Split(p, "/")
			if len(pp) != 2 {
				return nil, "", nil, fmt.Errorf("databricks-connector: invalid principal format: %s", p)
			}

			principalType, principal := pp[0], pp[1]
			var resourceId *v2.ResourceId

			switch principalType {
			case UsersType:
				su, _, err := g.client.ListUsers(
					ctx,
					&databricks.PaginationVars{Count: 1},
					databricks.NewFilterVars(fmt.Sprintf("userName eq '%s'", principal)),
				)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to list users: %w", err)
				}

				if len(su) == 0 {
					return nil, "", nil, fmt.Errorf("databricks-connector: user %s not found", principal)
				}

				resourceId = &v2.ResourceId{ResourceType: userResourceType.Id, Resource: su[0].ID}
			case GroupsType:
				sg, _, err := g.client.ListGroups(
					ctx,
					&databricks.PaginationVars{Count: 1},
					databricks.NewFilterVars(fmt.Sprintf("displayName eq '%s'", principal)),
				)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to list groups: %w", err)
				}

				if len(sg) == 0 {
					return nil, "", nil, fmt.Errorf("databricks-connector: group %s not found", principal)
				}

				resourceId = &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: sg[0].ID}
			case ServicePrincipalsType:
				ss, _, err := g.client.ListServicePrincipals(
					ctx,
					&databricks.PaginationVars{Count: 1},
					databricks.NewFilterVars(fmt.Sprintf("displayName eq '%s'", principal)),
				)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to list service principals: %w", err)
				}

				if len(ss) == 0 {
					return nil, "", nil, fmt.Errorf("databricks-connector: service principal %s not found", principal)
				}

				resourceId = &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: principal}
			default:
				return nil, "", nil, fmt.Errorf("databricks-connector: invalid principal type: %s", principalType)
			}

			rv = append(rv, grant.NewGrant(resource, ruleSet.Role, resourceId))
		}
	}

	return rv, "", nil, nil
}

func newGroupBuilder(client *databricks.Client) *groupBuilder {
	return &groupBuilder{
		client:       client,
		resourceType: groupResourceType,
	}
}
