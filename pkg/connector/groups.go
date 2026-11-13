package connector

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"errors"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const groupMemberEntitlement = "member"
const groupManagerEntitlement = "roles/group.manager"

type groupBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
	sync         syncPrincipals
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

// workspaceAdminsGroupName is the name Databricks gives the built-in admins
// group in every workspace. The group is addressed by this name, not by an id,
// so the listing filter and the alias check must agree on it.
const workspaceAdminsGroupName = "admins"

// workspaceMembershipReadOnly is what Grant and Revoke answer for a workspace group.
// The workspace SCIM API would accept the PATCH and remove the admin, so the refusal
// has to come from here.
const workspaceMembershipReadOnly = "databricks-connector: membership of a workspace admins group is read-only; manage it in Databricks"

// groupResource builds a group resource. workspaceName qualifies the display name as
// "<group> (<workspace>)", because Databricks names the built-in group "admins" in every
// workspace. It is empty for an account group, and the unqualified name stays in the profile.
func groupResource(ctx context.Context, group *databricks.Group, parent *v2.ResourceId, workspaceName string) (*v2.Resource, error) {
	profile := map[string]any{
		"display_name": group.DisplayName,
		"group_id":     group.ID,
		"parent_type":  parent.GetResourceType(),
		"parent_id":    parent.GetResource(),
	}

	options := []rs.ResourceOption{
		rs.WithResourceProfile(profile),
	}
	if parent != nil {
		options = append(options, rs.WithParentResourceID(parent))
	}
	groupId := groupResourceId(ctx, group.ID, parent)
	// RawId is the resource ID so the same Databricks group synced under the account and
	// under workspaces stays unique for match_baton_id.
	options = append(options, rs.WithAnnotation(&v2.RawId{Id: groupId}))

	displayName := group.DisplayName
	if workspaceName != "" {
		displayName = fmt.Sprintf("%s (%s)", group.DisplayName, workspaceName)
	}

	resource, err := rs.NewGroupResource(
		displayName,
		groupResourceType,
		groupId,
		nil,
		options...,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// workspaceDisplayName returns the name of the workspace a group hangs off, or an empty
// string for an account group. GetWorkspace reuses the account workspace listing, which
// the workspace syncer has already made in this sync, so it answers from the cache.
func (g *groupBuilder) workspaceDisplayName(ctx context.Context, deploymentName string, annos *annotations.Annotations) (string, error) {
	if deploymentName == "" {
		return "", nil
	}

	workspace, rateLimitData, err := g.client.GetWorkspace(ctx, deploymentName)
	if rateLimitData != nil {
		annos.WithRateLimiting(rateLimitData)
	}
	if err != nil {
		return "", fmt.Errorf("databricks-connector: failed to read the workspace %s a group hangs off: %w", deploymentName, err)
	}

	return workspace.Name, nil
}

// List returns groups as resource objects, with a GroupTrait because they are the
// 'shape' of a standard group. Under the account it returns every group. Under a
// workspace parent it returns only the built-in admins group, on a single page.
func (g *groupBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil {
		return nil, nil, nil
	}

	workspaceId := ""
	vars := []databricks.Vars{databricks.NewGroupAttrVars()}
	if parentResourceID.ResourceType == workspaceResourceType.Id {
		workspaceId = parentResourceID.Resource
		vars = append(vars, databricks.NewFilterVars(`displayName eq "`+workspaceAdminsGroupName+`"`))
	}

	bag, page, err := parsePageToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: groupResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	groups, total, _, err := g.client.ListGroups(
		ctx,
		workspaceId,
		append(vars, databricks.NewPaginationVars(page, ResourcesPageSize))...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list groups: %w", err)
	}

	annos := annotations.Annotations{}
	workspaceName, err := g.workspaceDisplayName(ctx, workspaceId, &annos)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	var rv []*v2.Resource
	for _, group := range groups {
		gCopy := group

		gr, err := groupResource(ctx, &gCopy, parentResourceID, workspaceName)
		if err != nil {
			return nil, nil, err
		}

		rv = append(rv, gr)
	}

	// Under a workspace the listing is filtered to the one admins group, so there is no second page.
	token := ""
	if workspaceId == "" {
		token = prepareNextToken(page, len(groups), total)
	}
	nextPage, err := bag.NextToken(token)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage, Annotations: annos}, nil
}

// Entitlements return all entitlements relevant to the group.
// Group can have members, which represent membership entitlements,
// it can have permissions assigned to it, which represent role permissions entitlements,
// and it can also have entitlements assigned to it, which are represented in role resource type.
func (g *groupBuilder) Entitlements(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	var rv []*v2.Entitlement

	var workspaceId string
	if resource.GetParentResourceId().GetResourceType() == workspaceResourceType.Id {
		workspaceId = resource.ParentResourceId.Resource
	}

	// membership entitlement - for group members
	memberAssignmentOptions := []ent.EntitlementOption{
		ent.WithDisplayName(fmt.Sprintf("%s %s", resource.DisplayName, groupMemberEntitlement)),
		ent.WithDescription(fmt.Sprintf("%s %s in Databricks", resource.DisplayName, groupMemberEntitlement)),
	}
	// Membership of a workspace admins group is read-only, so it is not grantable.
	if workspaceId == "" {
		memberAssignmentOptions = append(memberAssignmentOptions,
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType))
	}

	rv = append(rv, ent.NewAssignmentEntitlement(resource, groupMemberEntitlement, memberAssignmentOptions...))
	if workspaceId != "" {
		return rv, nil, nil
	}

	_, groupId, err := parseResourceId(resource.Id.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse group resource id: %w", err)
	}

	// role permissions entitlements
	// get all assignable roles for this specific group resource
	roles, _, err := g.client.ListRoles(ctx, workspaceId, GroupsType, groupId.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list roles for group %s: %w", groupId.Resource, err)
	}

	for _, role := range roles {
		rolePermissionOptions := []ent.EntitlementOption{
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
			ent.WithDisplayName(fmt.Sprintf("%s role", role.Name)),
			ent.WithDescription(fmt.Sprintf("%s role in Databricks", role.Name)),
		}

		rv = append(rv, ent.NewPermissionEntitlement(resource, role.Name, rolePermissionOptions...))
	}

	return rv, nil, nil
}

// splitMemberRef splits a SCIM member ref such as "Users/123" into its type and id.
func splitMemberRef(ref string) (string, string, error) {
	memberType, memberID, found := strings.Cut(ref, "/")
	if !found || memberType == "" || memberID == "" || strings.Contains(memberID, "/") {
		return "", "", fmt.Errorf("databricks-connector: invalid member format of %s", ref)
	}

	return memberType, memberID, nil
}

// memberResourceType maps a SCIM member type to the resource type the connector emits for
// it, or an empty string for a type the connector does not model.
func memberResourceType(memberType string) string {
	switch memberType {
	case "Users":
		return userResourceType.Id
	case "Groups":
		return groupResourceType.Id
	case "ServicePrincipals":
		return servicePrincipalResourceType.Id
	default:
		return ""
	}
}

// accountHeld reports, per member type, which member ids the account holds. It resolves the
// whole page in one request per type, and a member the account does not hold is absent. A
// member this sync cannot emit is left out without a request, so the caller skips it.
func (g *groupBuilder) accountHeld(ctx context.Context, members []databricks.Member) (map[string]map[string]struct{}, *v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)
	idsByType := make(map[string][]string)
	for _, member := range members {
		memberType, memberID, err := splitMemberRef(member.Ref)
		if err != nil {
			return nil, nil, err
		}

		resourceType := memberResourceType(memberType)
		if resourceType == "" || !emitPrincipal(g.sync, resourceType) {
			l.Debug("databricks-connector: skipping a member this sync does not emit",
				zap.String("member_type", memberType),
				zap.String("member_id", memberID))

			continue
		}

		idsByType[memberType] = append(idsByType[memberType], memberID)
	}

	held := make(map[string]map[string]struct{}, len(idsByType))
	var rateLimitData *v2.RateLimitDescription

	for memberType, ids := range idsByType {
		heldIDs, typeRateLimitData, err := g.client.FindAccountHeldIDs(ctx, memberType, ids)
		if typeRateLimitData != nil {
			rateLimitData = typeRateLimitData
		}

		if err != nil {
			return nil, rateLimitData, fmt.Errorf("databricks-connector: failed to look up %s on the account: %w", memberType, err)
		}

		held[memberType] = heldIDs
	}

	return held, rateLimitData, nil
}

// Grants return all grants relevant to the group.
// Databricks Groups have membership and role permissions grants (granting identity resource some permission to this specific group, e.g. group manager).
func (g *groupBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)

	var rv []*v2.Grant

	parentId, groupId, err := parseResourceId(resource.Id.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse group resource id: %w", err)
	}

	var workspaceId string
	isWorkspaceGroup := parentId.ResourceType == workspaceResourceType.Id
	if isWorkspaceGroup {
		workspaceId = parentId.Resource
	}

	bag, cursor, err := parseCursorToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: groupResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	// membership grants
	// Always fetch the group with members attribute to ensure we get the members
	// regardless of authentication type (OAuth vs personal access token)
	group, rateLimitData, err := g.client.GetGroup(ctx, workspaceId, groupId.Resource, databricks.NewGroupMembersAttrVars())
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupId.Resource, err)
	}

	annos := annotations.Annotations{}
	if rateLimitData != nil {
		annos.WithRateLimiting(rateLimitData)
	}

	// The API returns every member at once and has no member pagination, so members are paged
	// locally to keep each response under the Lambda payload limit. The API returns members in a
	// different order on every call, so pageAfter sorts them for pages to line up across re-fetches.
	members, nextCursor := pageAfter(group.Members, func(m databricks.Member) string { return m.Ref }, cursor)

	l.Debug("grants: group resource",
		zap.String("group_id", groupId.Resource),
		zap.Int("member_count", len(group.Members)),
		zap.Int("page_member_count", len(members)),
		zap.String("cursor", cursor),
	)

	// Only the account syncs members, so one it does not hold is workspace-local and has no
	// resource to point a grant at.
	var held map[string]map[string]struct{}
	if isWorkspaceGroup {
		heldByType, rateLimitData, err := g.accountHeld(ctx, members)
		if rateLimitData != nil {
			annos.WithRateLimiting(rateLimitData)
		}
		if err != nil {
			return nil, nil, err
		}

		held = heldByType
	}

	for _, member := range members {
		// member.Ref contains the type and ID separated by "/", e.g., "Users/123" or "Groups/456"
		memberType, memberID, err := splitMemberRef(member.Ref)
		if err != nil {
			return nil, nil, err
		}

		var resourceId *v2.ResourceId
		var anns []protoreflect.ProtoMessage

		if isWorkspaceGroup {
			if _, ok := held[memberType][memberID]; !ok {
				l.Debug("databricks-connector: skipping a workspace group member with no synced principal",
					zap.String("group", resource.Id.Resource),
					zap.String("member", member.Ref))
				continue
			}
			// The entitlement is not grantable, so the grant says so too.
			anns = append(anns, &v2.GrantImmutable{})
		}

		switch memberType {
		case "Users":
			resourceId = &v2.ResourceId{ResourceType: userResourceType.Id, Resource: memberID}
		case "Groups":
			memberParent := parentId
			if isWorkspaceGroup {
				memberParent = &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: g.client.GetAccountId()}
			}
			rid, expandAnnotation, err := groupGrantExpansion(ctx, memberID, memberParent)
			if err != nil {
				return rv, nil, err
			}
			resourceId = rid
			anns = append(anns, expandAnnotation)
		case "ServicePrincipals":
			resourceId = &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: memberID}
		default:
			return nil, nil, fmt.Errorf("databricks-connector: invalid member type: %s", memberType)
		}

		rv = append(rv, grant.NewGrant(resource, groupMemberEntitlement, resourceId, grant.WithAnnotation(anns...)))
	}

	nextPage, err := bag.NextToken(nextCursor)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	// role permissions grants are only emitted with the first page of members
	if cursor != "" || isWorkspaceGroup {
		return rv, &rs.SyncOpResults{NextPageToken: nextPage, Annotations: annos}, nil
	}

	ruleSets, rateLimitDataRuleSets, err := g.client.ListRuleSets(ctx, workspaceId, GroupsType, groupId.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list role rule sets for group %s: %w", resource.Id.Resource, err)
	}

	if rateLimitDataRuleSets != nil {
		annos.WithRateLimiting(rateLimitDataRuleSets)
	}

	for _, ruleSet := range ruleSets {
		for _, p := range ruleSet.Principals {
			resourceId, err := prepareResourceId(ctx, g.client, workspaceId, p)
			if err != nil {
				return nil, nil, fmt.Errorf("databricks-connector: failed to prepare resource id for principal %s: %w", p, err)
			}
			var annotations []protoreflect.ProtoMessage
			if resourceId.ResourceType == groupResourceType.Id {
				rid, expandAnnotation, err := groupGrantExpansion(ctx, resourceId.Resource, resource.ParentResourceId)
				if err != nil {
					return rv, nil, err
				}
				resourceId = rid
				annotations = append(annotations, expandAnnotation)
			}

			rv = append(rv, grant.NewGrant(resource, ruleSet.Role, resourceId, grant.WithAnnotation(annotations...)))
		}
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage, Annotations: annos}, nil
}

// Get re-fetches a single group, used to re-sync it after a RESOURCE_CHANGE event; fetched
// without members since groupResource doesn't need them (Grants fetches them separately).
func (g *groupBuilder) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	parentId, groupId, err := parseResourceId(resourceId.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse group resource id: %w", err)
	}

	var workspaceId string
	if parentId != nil && parentId.ResourceType == workspaceResourceType.Id {
		workspaceId = parentId.Resource
	}

	group, rateLimitData, err := g.client.GetGroup(databricks.WithoutCache(ctx), workspaceId, groupId.Resource, databricks.NewGroupAttrVars())
	annos := annotations.Annotations{}
	if rateLimitData != nil {
		annos.WithRateLimiting(rateLimitData)
	}
	if err != nil {
		return nil, annos, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupId.Resource, err)
	}

	workspaceName, err := g.workspaceDisplayName(ctx, workspaceId, &annos)
	if err != nil {
		return nil, annos, err
	}

	resource, err := groupResource(ctx, group, parentResourceId, workspaceName)
	if err != nil {
		return nil, annos, err
	}

	ctxzap.Extract(ctx).Debug("databricks-connector: Get: refreshed group",
		zap.String("resource_id", resourceId.Resource),
		zap.String("parent_resource_id", parentResourceId.GetResource()),
	)
	return resource, annos, nil
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
		if parentGroupId.GetResourceType() == workspaceResourceType.Id {
			return nil, status.Error(codes.FailedPrecondition, workspaceMembershipReadOnly)
		}

		group, _, err := g.client.GetGroupNoCache(ctx, workspaceId, groupId.Resource, databricks.NewGroupMembersAttrVars())
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

				return annotations.New(&v2.GrantAlreadyExists{}), nil
			}
		}

		// The group is patched so only the new member is written and the rest of the group is kept.
		_, err = g.client.PatchGroup(ctx, workspaceId, groupId.Resource, addValueOp("members", principalId.Resource))
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
		var apiErr *databricks.APIError
		if errors.As(err, &apiErr) {
			if apiErr.StatusCode == http.StatusConflict {
				if apiErr.Detail == databricks.AlreadyExists {
					return nil, nil
				}
			}
		}
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
		if parentType == workspaceResourceType.Id {
			return nil, status.Error(codes.FailedPrecondition, workspaceMembershipReadOnly)
		}

		group, _, err := g.client.GetGroupNoCache(ctx, workspaceId, groupId.Resource, databricks.NewGroupMembersAttrVars())
		if isNotFoundError(err) {
			// group not found (deleted, or not assigned to this workspace)
			return annotations.New(&v2.GrantAlreadyRevoked{}), nil
		}
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to get group %s: %w", groupId.Resource, err)
		}

		// Only account groups reach this far. A principal that is not a member is left as
		// is instead of being patched, so a repeated revoke stays idempotent.
		if !slices.ContainsFunc(group.Members, func(m databricks.Member) bool { return m.ID == principalId }) {
			return annotations.New(&v2.GrantAlreadyRevoked{}), nil
		}

		_, err = g.client.PatchGroup(ctx, workspaceId, groupId.Resource, removeValueOp("members", principalId))
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
	if errors.Is(prepareErr, errPrincipalNotFound) {
		// the principal no longer exists, so it does not have the entitlement: the revoke is already satisfied
		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}
	if prepareErr != nil {
		return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", prepareErr)
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
		var apiErr *databricks.APIError
		if errors.As(err, &apiErr) {
			if apiErr.StatusCode == http.StatusConflict {
				if apiErr.Detail == databricks.AlreadyExists {
					return nil, nil
				}
			}
		}
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for group %s (%s): %w", principal.Id.Resource, groupId.Resource, err)
	}

	return nil, nil
}

func newGroupBuilder(client *databricks.Client, sync syncPrincipals) *groupBuilder {
	return &groupBuilder{
		client:       client,
		resourceType: groupResourceType,
		sync:         sync,
	}
}
