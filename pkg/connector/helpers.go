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
	"google.golang.org/protobuf/types/known/structpb"
)

func parseResourceId(resourceId string) (*v2.ResourceId, *v2.ResourceId, error) {
	parts := strings.Split(resourceId, "/")
	switch len(parts) {
	case 2:
		return nil, &v2.ResourceId{
			ResourceType: parts[0],
			Resource:     parts[1],
		}, nil
	case 4:
		return &v2.ResourceId{
				ResourceType: parts[0],
				Resource:     parts[1],
			},
			&v2.ResourceId{
				ResourceType: parts[2],
				Resource:     parts[3],
			}, nil
	}

	return nil, nil, fmt.Errorf("invalid resource ID: %s", resourceId)
}

func groupGrantExpansion(ctx context.Context, groupId string, parentResource *v2.ResourceId) (*v2.ResourceId, *v2.GrantExpandable, error) {
	groupResourceStr := groupResourceId(ctx, groupId, parentResource)
	resourceId, err := rs.NewResourceID(groupResourceType, groupResourceStr)
	if err != nil {
		return nil, nil, err
	}

	return resourceId, &v2.GrantExpandable{
		EntitlementIds: []string{fmt.Sprintf("group:%s:%s", groupResourceStr, groupMemberEntitlement)},
	}, nil
}

func annotationsForUserResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	return annos
}

// prepareResourceId prepares a resource ID for a user, group, or service principal.
// It's used when we need to parse results from listing rule sets.
func prepareResourceId(ctx context.Context, c *databricks.Client, workspaceId string, principal string) (*v2.ResourceId, error) {
	pp := strings.Split(principal, "/")
	if len(pp) != 2 {
		return nil, fmt.Errorf("invalid principal format: %s", principal)
	}

	// principalID represent user's username, service principal's application ID, or group display name,
	// so we need to find the actual user, service principal or group ID
	principalType, principal := pp[0], pp[1]
	var resourceId *v2.ResourceId

	switch principalType {
	case UsersType:
		userID, _, err := c.FindUserID(ctx, workspaceId, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to find user %s: %w", principal, err)
		}

		resourceId = &v2.ResourceId{ResourceType: userResourceType.Id, Resource: userID}
	case GroupsType:
		groupID, _, err := c.FindGroupID(ctx, workspaceId, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to find group %s: %w", principal, err)
		}

		resourceId = &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: groupID}
	case ServicePrincipalsType:
		servicePrincipalID, _, err := c.FindServicePrincipalID(ctx, workspaceId, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to find service principal %s: %w", principal, err)
		}

		resourceId = &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: servicePrincipalID}
	default:
		return nil, fmt.Errorf("invalid principal type: %s", principalType)
	}

	return resourceId, nil
}

// prepareResourceType prepares a resource type for a user, group, or service principal.
// It's used when we need to parse results from listing rule sets.
func prepareResourceType(principal *databricks.WorkspacePrincipal) (*v2.ResourceType, error) {
	switch {
	case principal.UserName != "":
		return userResourceType, nil
	case principal.GroupDisplayName != "":
		return groupResourceType, nil
	case principal.ServicePrincipalAppID != "":
		return servicePrincipalResourceType, nil
	default:
		return nil, fmt.Errorf("invalid principal: %v", principal)
	}
}

// preparePrincipalId prepares a principal ID for a user, group, or service principal.
// It's used when we need edit rule sets with new principals.
func preparePrincipalId(ctx context.Context, c *databricks.Client, workspaceId, principalType, principalId string) (string, error) {
	var result string

	switch principalType {
	case userResourceType.Id:
		username, _, err := c.FindUsername(ctx, workspaceId, principalId)
		if err != nil {
			return "", fmt.Errorf("failed to find user %s/%s: %w", workspaceId, principalId, err)
		}

		result = fmt.Sprintf("%s/%s", UsersType, username)
	case groupResourceType.Id:
		displayName, _, err := c.FindGroupDisplayName(ctx, workspaceId, principalId)
		if err != nil {
			return "", fmt.Errorf("failed to find group %s/%s: %w", workspaceId, principalId, err)
		}

		result = fmt.Sprintf("%s/%s", GroupsType, displayName)
	case servicePrincipalResourceType.Id:
		appID, _, err := c.FindServicePrincipalAppID(ctx, workspaceId, principalId)
		if err != nil {
			return "", fmt.Errorf("failed to find service principal %s/%s: %w", workspaceId, principalId, err)
		}

		result = fmt.Sprintf("%s/%s", ServicePrincipalsType, appID)
	default:
		return "", fmt.Errorf("invalid principal type: %s", principalType)
	}

	return result, nil
}

func isValidPrincipal(principal *v2.ResourceId) bool {
	return principal.ResourceType == userResourceType.Id ||
		principal.ResourceType == groupResourceType.Id ||
		principal.ResourceType == servicePrincipalResourceType.Id
}

func getParentInfoFromProfile(profile *structpb.Struct) (string, string, error) {
	parentType, ok := rs.GetProfileStringValue(profile, "parent_type")
	if !ok {
		return "", "", fmt.Errorf("parent type not found")
	}

	parentID, ok := rs.GetProfileStringValue(profile, "parent_id")
	if !ok {
		return "", "", fmt.Errorf("parent id not found")
	}

	return parentType, parentID, nil
}

func addPermissions(isWorkspaceRole bool, perms *databricks.Permissions, entitlement string) {
	if !isWorkspaceRole {
		perms.Roles = append(perms.Roles, databricks.PermissionValue{
			Value: entitlement,
		})
	} else {
		perms.Entitlements = append(perms.Entitlements, databricks.PermissionValue{
			Value: entitlement,
		})
	}
}

func removePermissions(isWorkspaceRole bool, perms *databricks.Permissions, entitlement string) {
	if !isWorkspaceRole {
		for i, r := range perms.Roles {
			if r.Value == entitlement {
				perms.Roles = slices.Delete(perms.Roles, i, i+1)
				break
			}
		}
	} else {
		for i, e := range perms.Entitlements {
			if e.Value == entitlement {
				perms.Entitlements = slices.Delete(perms.Entitlements, i, i+1)
				break
			}
		}
	}
}

func prepareWorkspaceRole(entitlement string) string {
	parts := strings.Split(entitlement, ":")
	if len(parts) != 2 {
		return ""
	}

	return parts[1]
}
