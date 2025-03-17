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
const groupManagerEntitlement = "roles/group.manager"

type groupBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (g *groupBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return groupResourceType
}

func groupResourceId(_ context.Context, groupId string, parentResourceId *v2.ResourceId) string {
	if parentResourceId == nil {
		return strings.Join([]string{groupResourceType.Id, groupId}, "/")
	}

	return strings.Join([]string{parentResourceId.ResourceType, parentResourceId.Resource, groupResourceType.Id, groupId}, "/")
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
		"parent_type":  parent.GetResourceType(),
		"parent_id":    parent.GetResource(),
	}

	if len(members) > 0 {
		profile["members"] = strings.Join(members, ",")
	}

	groupTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	var options []rs.ResourceOption
	if parent != nil {
		options = append(options, rs.WithParentResourceID(parent))
	}
	groupId := groupResourceId(ctx, group.ID, parent)

	resource, err := rs.NewGroupResource(
		group.DisplayName,
		groupResourceType,
		groupId,
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

	workspaceId := ""
	if parentResourceID.ResourceType == workspaceResourceType.Id {
		workspaceId = parentResourceID.Resource
	}

	bag, page, err := parsePageToken(pg.Token, &v2.ResourceId{ResourceType: groupResourceType.Id})
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	groups, total, _, err := g.client.ListGroups(
		ctx,
		workspaceId,
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
func (g *groupBuilder) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	var workspaceId string
	if resource.GetParentResourceId().GetResourceType() == workspaceResourceType.Id {
		workspaceId = resource.ParentResourceId.Resource
	}

	// membership entitlement - for group members
	memberAssignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s %s", resource.DisplayName, groupMemberEntitlement)),
		ent.WithDescription(fmt.Sprintf("%s %s in Databricks", resource.DisplayName, groupMemberEntitlement)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(resource, groupMemberEntitlement, memberAssignmentOptions...))

	_, groupId, err := parseResourceId(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse group resource id: %w", err)
	}

	// role permissions entitlements
	// get all assignable roles for this specific group resource
	roles, _, err := g.client.ListRoles(ctx, workspaceId, GroupsType, groupId.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list roles for group %s: %w", groupId.Resource, err)
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

	parentId, groupId, err := parseResourceId(resource.Id.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse group resource id: %w", err)
	}

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	var workspaceId string
	isWorkspaceGroup := parentId.ResourceType == workspaceResourceType.Id
	if isWorkspaceGroup {
		workspaceId = parentId.Resource
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
				rid, expandAnnotation, err := groupGrantExpansion(ctx, memberID, parentId)
				if err != nil {
					return rv, "", nil, err
				}
				resourceId = rid
				anns = append(anns, expandAnnotation)
			case "ServicePrincipals":
				resourceId = &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: memberID}
			default:
				return nil, "", nil, fmt.Errorf("databricks-connector: invalid member type: %s", memberType)
			}

			rv = append(rv, grant.NewGrant(resource, groupMemberEntitlement, resourceId, grant.WithAnnotation(anns...)))
		}
	}

	// role permissions grants
	ruleSets, _, err := g.client.ListRuleSets(ctx, workspaceId, GroupsType, groupId.Resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list role rule sets for group %s: %w", resource.Id.Resource, err)
	}

	for _, ruleSet := range ruleSets {
		for _, p := range ruleSet.Principals {
			resourceId, err := prepareResourceId(ctx, g.client, workspaceId, p)
			if err != nil {
				return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource id for principal %s: %w", p, err)
			}
			var annotations []protoreflect.ProtoMessage
			if resourceId.ResourceType == groupResourceType.Id {
				rid, expandAnnotation, err := groupGrantExpansion(ctx, resourceId.Resource, resource.ParentResourceId)
				if err != nil {
					return rv, "", nil, err
				}
				resourceId = rid
				annotations = append(annotations, expandAnnotation)
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

	var parentId *v2.ResourceId
	var principalId *v2.ResourceId
	var err error
	if principal.Id.ResourceType == groupResourceType.Id {
		parentId, principalId, err = parseResourceId(principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to parse principal resource id: %w", err)
		}
	} else {
		parentId = principal.ParentResourceId
		principalId = principal.Id
	}

	var workspaceId string
	if parentId != nil && parentId.ResourceType == workspaceResourceType.Id {
		workspaceId = parentId.Resource
	}

	parentGroupId, groupId, err := parseResourceId(entitlement.Resource.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to parse group resource id: %w", err)
	}
	if parentGroupId != nil && parentGroupId.ResourceType == workspaceResourceType.Id {
		workspaceId = parentGroupId.Resource
	}

	membershipEntitlementID := ent.NewEntitlementID(entitlement.Resource, groupMemberEntitlement)
	managerEntitlementID := ent.NewEntitlementID(entitlement.Resource, groupManagerEntitlement)
	if entitlement.Id == membershipEntitlementID {
		group, _, err := g.client.GetGroup(ctx, workspaceId, groupId.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupId.Resource, err)
		}

		for _, member := range group.Members {
			if member.ID == principalId.Resource {
				l.Info(
					"databricks-connector: group already has the member added",
					zap.String("principal_id", principal.Id.Resource),
					zap.String("entitlement", groupMemberEntitlement),
				)

				return nil, nil
			}
		}

		// add the member to the group
		group.Members = append(group.Members, databricks.Member{
			ID: principal.Id.Resource,
		})

		_, err = g.client.UpdateGroup(ctx, workspaceId, group)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to update group %s: %w", groupId.Resource, err)
		}

		return nil, nil
	}

	// If the entitlement is a role permission entitlement
	ruleSets, _, err := g.client.ListRuleSets(ctx, workspaceId, GroupsType, groupId.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list rule sets for group %s (%s): %w", principal.Id.Resource, groupId.Resource, err)
	}

	principalID, err := preparePrincipalId(ctx, g.client, workspaceId, principal.Id.ResourceType, principal.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", err)
	}

	role := groupManagerEntitlement
	if workspaceId == "" && entitlement.Id != managerEntitlementID {
		return nil, fmt.Errorf("databricks-connector: only group manager entitlement is supported for role permissions for group %s", groupId.Resource)
	} else if workspaceId != "" {
		role = entitlement.Slug
	}

	if role == "" {
		return nil, fmt.Errorf("databricks-connector: role is empty")
	}

	found := false

	for i, ruleSet := range ruleSets {
		if ruleSet.Role == role {
			found = true

			// check if it contains the principals and add principal to the rule set
			if slices.Contains(ruleSet.Principals, principalID) {
				l.Info(
					"databricks-connector: group already has the entitlement",
					zap.String("principal_id", principalID),
					zap.String("entitlement", role),
				)

				return nil, nil
			}

			// add the principal to the rule set
			ruleSets[i].Principals = append(ruleSets[i].Principals, principalID)
		}
	}

	if !found {
		ruleSets = append(ruleSets, databricks.RuleSet{
			Role:       role,
			Principals: []string{principalID},
		})
	}

	_, err = g.client.UpdateRuleSets(ctx, workspaceId, GroupsType, groupId.Resource, ruleSets)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for group %s (%s): %w", principal.Id.Resource, groupId.Resource, err)
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

	principalId := principal.Id.Resource

	var err error
	if principal.Id.ResourceType == groupResourceType.Id {
		_, principal, err := parseResourceId(principal.Id.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to parse principal resource id: %w", err)
		}
		principalId = principal.Resource
	}

	parentResourceId, groupId, err := parseResourceId(entitlement.Resource.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to parse entitlement resource id: %w", err)
	}

	if parentResourceId == nil {
		return nil, fmt.Errorf("databricks-connector: parent resource id not found")
	}
	parentID := parentResourceId.GetResource()
	parentType := parentResourceId.GetResourceType()

	var workspaceId string
	if parentType == workspaceResourceType.Id {
		workspaceId = parentID
	}

	membershipEntitlementID := ent.NewEntitlementID(entitlement.Resource, groupMemberEntitlement)
	managerEntitlementID := ent.NewEntitlementID(entitlement.Resource, groupManagerEntitlement)
	if entitlement.Id == membershipEntitlementID {
		group, _, err := g.client.GetGroup(ctx, workspaceId, groupId.Resource)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupId.Resource, err)
		}

		for i, member := range group.Members {
			if member.ID == principalId {
				group.Members = slices.Delete(group.Members, i, i+1)
				break
			}
		}

		_, err = g.client.UpdateGroup(ctx, workspaceId, group)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to update group %s: %w", groupId.Resource, err)
		}
		return nil, nil
	}

	role := groupManagerEntitlement
	if workspaceId == "" && entitlement.Id != managerEntitlementID {
		return nil, fmt.Errorf("databricks-connector: only group manager entitlement is supported for role permissions for group %s", groupId.Resource)
	} else if workspaceId != "" {
		role = entitlement.Slug
	}

	if role == "" {
		return nil, fmt.Errorf("databricks-connector: role is empty")
	}

	ruleSets, _, err := g.client.ListRuleSets(ctx, workspaceId, GroupsType, groupId.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list rule sets for group %s (%s): %w", principal.Id.Resource, groupId.Resource, err)
	}

	if len(ruleSets) == 0 {
		l.Info(
			"databricks-connector: group already does not have the entitlement",
			zap.String("principal_id", principal.Id.Resource),
			zap.String("entitlement", role),
		)

		return nil, nil
	}

	principalId, prepareErr := preparePrincipalId(ctx, g.client, workspaceId, principal.Id.ResourceType, principal.Id.Resource)
	if prepareErr != nil {
		return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", err)
	}

	for i, ruleSet := range ruleSets {
		if ruleSet.Role != role {
			continue
		}

		// check if it contains the principals and remove the principal to the rule set
		if slices.Contains(ruleSet.Principals, principalId) {
			// if there is only one principal, remove the whole rule set
			if len(ruleSet.Principals) == 1 {
				ruleSets = slices.Delete(ruleSets, i, i+1)
			} else {
				pI := slices.Index(ruleSet.Principals, principalId)
				ruleSets[i].Principals = slices.Delete(ruleSet.Principals, pI, pI+1)
			}
			break
		}

		l.Info(
			"databricks-connector: group already does not have the entitlement",
			zap.String("principal_id", principalId),
			zap.String("entitlement", role),
		)

		return nil, nil
	}

	_, err = g.client.UpdateRuleSets(ctx, workspaceId, GroupsType, groupId.Resource, ruleSets)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for group %s (%s): %w", principal.Id.Resource, groupId.Resource, err)
	}

	return nil, nil
}

func newGroupBuilder(client *databricks.Client) *groupBuilder {
	return &groupBuilder{
		client:       client,
		resourceType: groupResourceType,
	}
}
