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

type ResourceCache struct {
	// Map of API IDs to resources
	resources map[string]*v2.Resource
}

func (c *ResourceCache) Get(resourceId string) *v2.Resource {
	return c.resources[resourceId]
}

func (c *ResourceCache) Set(resourceId string, resource *v2.Resource) {
	c.resources[resourceId] = resource
}

func NewResourceCache() *ResourceCache {
	return &ResourceCache{
		resources: make(map[string]*v2.Resource),
	}
}

var resourceCache = NewResourceCache()

func annotationsForUserResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	return annos
}

// prepareResourceID prepares a resource ID for a user, group, or service principal.
// It's used when we need to parse results from listing rule sets.
func prepareResourceID(ctx context.Context, c *databricks.Client, principal string) (*v2.ResourceId, error) {
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
		userID, err := c.FindUserID(ctx, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to find user %s: %w", principal, err)
		}

		resourceId = &v2.ResourceId{ResourceType: userResourceType.Id, Resource: userID}
	case GroupsType:
		groupID, err := c.FindGroupID(ctx, principal)
		if err != nil {
			return nil, fmt.Errorf("failed to find group %s: %w", principal, err)
		}

		resourceId = &v2.ResourceId{ResourceType: groupResourceType.Id, Resource: groupID}
	case ServicePrincipalsType:
		servicePrincipalID, err := c.FindServicePrincipalID(ctx, principal)
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

// preparePrincipalID prepares a principal ID for a user, group, or service principal.
// It's used when we need edit rule sets with new principals.
func preparePrincipalID(ctx context.Context, c *databricks.Client, principalType, principalID string) (string, error) {
	var result string

	switch principalType {
	case userResourceType.Id:
		username, err := c.FindUsername(ctx, principalID)
		if err != nil {
			return "", fmt.Errorf("failed to find user %s: %w", principalID, err)
		}

		result = fmt.Sprintf("%s/%s", UsersType, username)
	case groupResourceType.Id:
		displayName, err := c.FindGroupDisplayName(ctx, principalID)
		if err != nil {
			return "", fmt.Errorf("failed to find group %s: %w", principalID, err)
		}

		result = fmt.Sprintf("%s/%s", GroupsType, displayName)
	case servicePrincipalResourceType.Id:
		appID, err := c.FindServicePrincipalAppID(ctx, principalID)
		if err != nil {
			return "", fmt.Errorf("failed to find service principal %s: %w", principalID, err)
		}

		result = fmt.Sprintf("%s/%s", ServicePrincipalsType, appID)
	default:
		return "", fmt.Errorf("invalid principal type: %s", principalType)
	}

	return result, nil
}

func expandGrantForGroup(id string) (*v2.Resource, *v2.GrantExpandable, error) {
	memberResource := resourceCache.Get(id)
	if memberResource == nil {
		return nil, nil, fmt.Errorf("databricks-connector: group %s not found in cache", id)
	}

	return memberResource, &v2.GrantExpandable{
		EntitlementIds: []string{fmt.Sprintf("group:%s:%s", memberResource.Id.Resource, groupMemberEntitlement)},
	}, nil
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
