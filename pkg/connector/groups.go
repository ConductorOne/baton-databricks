package connector

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
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
	roles, err := g.client.ListRoles(context.Background(), GroupsType, resource.Id.Resource)
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

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
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
	ruleSets, err := g.client.ListRuleSets(ctx, GroupsType, resource.Id.Resource)
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

func (g *groupBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can be granted group permissions",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can be granted group permissions")
	}

	groupTrait, err := rs.GetGroupTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
	}

	if parentType == workspaceResourceType.Id {
		g.client.SetWorkspaceConfig(parentID)
	} else {
		g.client.SetAccountConfig()
	}

	groupID := entitlement.Resource.Id.Resource

	if entitlement.Slug == groupMemberEntitlement {
		group, err := g.client.GetGroup(ctx, groupID)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupID, err)
		}

		for _, member := range group.Members {
			if member.ID == principal.Id.Resource {
				l.Info(
					"databricks-connector: group already has the member added",
					zap.String("principal_id", principal.Id.Resource),
					zap.String("entitlement", entitlement.Slug),
				)

				return nil, nil
			}
		}

		// add the member to the group
		group.Members = append(group.Members, databricks.Member{
			ID: principal.Id.Resource,
		})

		err = g.client.UpdateGroup(ctx, group)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to update group %s: %w", groupID, err)
		}
	} else {
		ruleSets, err := g.client.ListRuleSets(ctx, GroupsType, groupID)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to list rule sets for group %s (%s): %w", principal.Id.Resource, groupID, err)
		}

		principalID, err := preparePrincipalID(ctx, g.client, principal.Id.ResourceType, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", err)
		}

		found := false

		for i, ruleSet := range ruleSets {
			if ruleSet.Role == entitlement.Slug {
				found = true

				// check if it contains the principals and add principal to the rule set
				if slices.Contains(ruleSet.Principals, principalID) {
					l.Info(
						"databricks-connector: group already has the entitlement",
						zap.String("principal_id", principalID),
						zap.String("entitlement", entitlement.Slug),
					)

					return nil, nil
				}

				// add the principal to the rule set
				ruleSets[i].Principals = append(ruleSet.Principals, principalID)
			}
		}

		if len(ruleSets) == 0 || !found {
			ruleSets = append(ruleSets, databricks.RuleSet{
				Role:       entitlement.Slug,
				Principals: []string{principalID},
			})
		}

		err = g.client.UpdateRuleSets(ctx, GroupsType, groupID, ruleSets)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to update rule sets for group %s (%s): %w", principal.Id.Resource, groupID, err)
		}
	}

	return nil, nil
}

func (g *groupBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	principal := grant.Principal
	entitlement := grant.Entitlement

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can have group permissions revoked",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can have group permissions revoked")
	}

	groupTrait, err := rs.GetGroupTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
	}

	if parentType == workspaceResourceType.Id {
		g.client.SetWorkspaceConfig(parentID)
	} else {
		g.client.SetAccountConfig()
	}

	groupID := entitlement.Resource.Id.Resource

	if entitlement.Slug == groupMemberEntitlement {
		group, err := g.client.GetGroup(ctx, groupID)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupID, err)
		}

		for i, member := range group.Members {
			if member.ID == principal.Id.Resource {
				group.Members = slices.Delete(group.Members, i, i+1)
				break
			}
		}

		err = g.client.UpdateGroup(ctx, group)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to update group %s: %w", groupID, err)
		}
	} else {
		ruleSets, err := g.client.ListRuleSets(ctx, GroupsType, groupID)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to list rule sets for group %s (%s): %w", principal.Id.Resource, groupID, err)
		}

		if len(ruleSets) == 0 {
			l.Info(
				"databricks-connector: group already does not have the entitlement",
				zap.String("principal_id", principal.Id.Resource),
				zap.String("entitlement", entitlement.Slug),
			)

			return nil, nil
		}

		principalID, err := preparePrincipalID(ctx, g.client, principal.Id.ResourceType, principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", err)
		}

		for i, ruleSet := range ruleSets {
			if ruleSet.Role == entitlement.Slug {
				// check if it contains the principals and remove the principal to the rule set
				if slices.Contains(ruleSet.Principals, principalID) {
					// if there is only one principal, remove the whole rule set
					if len(ruleSet.Principals) == 1 {
						ruleSets = slices.Delete(ruleSets, i, i+1)
						break
					} else {
						pI := slices.Index(ruleSet.Principals, principalID)
						ruleSets[i].Principals = slices.Delete(ruleSet.Principals, pI, pI+1)
						break
					}
				}

				l.Info(
					"databricks-connector: group already does not have the entitlement",
					zap.String("principal_id", principalID),
					zap.String("entitlement", entitlement.Slug),
				)

				return nil, nil
			}
		}

		err = g.client.UpdateRuleSets(ctx, GroupsType, groupID, ruleSets)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to update rule sets for group %s (%s): %w", principal.Id.Resource, groupID, err)
		}
	}

	return nil, nil
}

func newGroupBuilder(client *databricks.Client) *groupBuilder {
	return &groupBuilder{
		client:       client,
		resourceType: groupResourceType,
	}
}
