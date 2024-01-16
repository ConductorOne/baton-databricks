package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

const ResourcesPageSize uint = 50

func annotationsForUserResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	return annos
}

func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, uint, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, 0, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	page, err := convertPageToken(b.PageToken())
	if err != nil {
		return nil, 0, err
	}

	return b, page, nil
}

// convertPageToken converts a string token into an int.
func convertPageToken(token string) (uint, error) {
	if token == "" {
		return 1, nil
	}

	page, err := strconv.ParseUint(token, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse page token: %w", err)
	}

	return uint(page), nil
}

// prepareNextToken prepares the next page token.
// It calculates the next page number and returns it as a string.
func prepareNextToken(page uint, pageTotal int, total uint) string {
	var token string

	next := page + uint(pageTotal)
	if next < total+1 {
		token = strconv.Itoa(int(next))
	}

	return token
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
