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
	"google.golang.org/protobuf/reflect/protoreflect"
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
		// Ref contains both the type and the ID of the member
		members[i] = member.Ref
	}

	profile := map[string]interface{}{
		"display_name": group.DisplayName,
		"group_id":     group.ID,
		"parent_type":  parent.ResourceType,
		"parent_id":    parent.Resource,
	}

	if len(members) > 0 {
		profile["members"] = strings.Join(members, ",")
	}

	groupTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	// keep the parent resource id, only if the parent resource is account
	var options []rs.ResourceOption
	if parent.ResourceType == accountResourceType.Id {
		options = append(options, rs.WithParentResourceID(parent))
	}

	resource, err := rs.NewGroupResource(
		group.DisplayName,
		groupResourceType,
		group.ID,
		groupTraitOptions,
		options...,
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

	if parentResourceID.ResourceType == workspaceResourceType.Id {
		g.client.SetWorkspaceConfig(parentResourceID.Resource)
	} else {
		g.client.SetAccountConfig()
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
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list groups: %w", err)
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

	token := prepareNextToken(page, len(groups), total)
	nextPage, err := bag.NextToken(token)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, nextPage, nil, nil
}

// Entitlements return all entitlements relevant to the group.
// Group can have members, which represent membership entitlements,
// it can have permissions assigned to it, which represent role permissions entitlements,
// and it can also have entitlements assigned to it, which are represented in role resource type.
func (g *groupBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	parentType, ok := rs.GetProfileStringValue(groupTrait.Profile, "parent_type")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent type from group profile")
	}

	parentID, ok := rs.GetProfileStringValue(groupTrait.Profile, "parent_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent id from group profile")
	}

	if parentType == workspaceResourceType.Id {
		g.client.SetWorkspaceConfig(parentID)
	} else {
		g.client.SetAccountConfig()
	}

	// membership entitlement - for group members
	memberAssignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
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
// Databricks Groups have membership and role permissions grants (granting identity resource some permission to this specific group, e.g. group manager).
func (g *groupBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var rv []*v2.Grant

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	parentType, ok := rs.GetProfileStringValue(groupTrait.Profile, "parent_type")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent type from group profile")
	}

	parentID, ok := rs.GetProfileStringValue(groupTrait.Profile, "parent_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent id from group profile")
	}

	isWorkspaceGroup := parentType == workspaceResourceType.Id

	if isWorkspaceGroup {
		g.client.SetWorkspaceConfig(parentID)
	} else {
		g.client.SetAccountConfig()
	}

	// membership grants
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
			var anns []protoreflect.ProtoMessage

			switch memberType {
			case "Users":
				resourceId = &v2.ResourceId{ResourceType: userResourceType.Id, Resource: memberID}
			case "Groups":
				resourceId = &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: memberID}
				anns = append(anns, expandGrantForGroup(memberID))
			case "ServicePrincipals":
				resourceId = &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: memberID}
			default:
				return nil, "", nil, fmt.Errorf("databricks-connector: invalid member type: %s", memberType)
			}

			rv = append(rv, grant.NewGrant(resource, groupMemberEntitlement, resourceId, grant.WithAnnotation(anns...)))
		}
	}

	// role permissions grants
	ruleSets, err := g.client.ListRuleSets(ctx, "groups", resource.Id.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list role rule sets for group %s: %w", resource.Id.Resource, err)
	}

	for _, ruleSet := range ruleSets {
		for _, p := range ruleSet.Principals {
			resourceId, err := prepareResourceID(ctx, g.client, p)
			if err != nil {
				return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource id for principal %s: %w", p, err)
			}

			var annotations []protoreflect.ProtoMessage
			if resourceId.ResourceType == groupResourceType.Id {
				annotations = append(annotations, expandGrantForGroup(resourceId.Resource))
			}

			rv = append(rv, grant.NewGrant(resource, ruleSet.Role, resourceId, grant.WithAnnotation(annotations...)))
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
