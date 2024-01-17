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

const (
	// Roles relevant to Account API (Grantable to User or ServicePrincipal).
	AccountAdminRole     = "account_admin"
	MarketplaceAdminRole = "marketplace.admin"

	// Roles (or Entitlements) relevant to Workspace API (Grantable to User, Group or ServicePrincipal).
	WorkspaceAccessRole    = "workspace_access"
	SQLAccessRole          = "databricks-sql-access"
	ClusterCreateRole      = "allow-cluster-create"
	InstancePoolCreateRole = "allow-instance-pool-create"

	UsersType             = "users"
	GroupsType            = "groups"
	ServicePrincipalsType = "servicePrincipals"
)

type accountBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (a *accountBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return accountResourceType
}

func accountResource(ctx context.Context, accID string) (*v2.Resource, error) {
	resource, err := rs.NewResource(
		accID,
		accountResourceType,
		accID,
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: userResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: groupResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: servicePrincipalResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: workspaceResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: roleResourceType.Id},
		),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (a *accountBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var rv []*v2.Resource

	ur, err := accountResource(ctx, a.client.GetAccountId())
	if err != nil {
		return nil, "", nil, err
	}

	rv = append(rv, ur)

	return rv, "", nil, nil
}

// Entitlements returns slice of entitlements for marketplace admins under account.
func (a *accountBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	permissiongOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType, servicePrincipalResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s %s role", resource.DisplayName, MarketplaceAdminRole)),
		ent.WithDescription(fmt.Sprintf("%s %s role in Databricks", resource.DisplayName, MarketplaceAdminRole)),
	}

	rv = append(rv, ent.NewPermissionEntitlement(resource, MarketplaceAdminRole, permissiongOptions...))

	return rv, "", nil, nil
}

// Grants returns grants for marketplace admins under account.
func (a *accountBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var rv []*v2.Grant

	// list rule sets for the account
	ruleSets, err := a.client.ListRuleSets(ctx, "", "")
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list rule sets for account %s: %w", resource.Id.Resource, err)
	}

	for _, ruleSet := range ruleSets {
		// rule set contains role and its principals, each one with resource type and resource id seperated by "/"
		if strings.Contains(ruleSet.Role, MarketplaceAdminRole) {
			for _, p := range ruleSet.Principals {
				resourceId, anns, err := prepareResourceID(ctx, a.client, p)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource id for principal %s: %w", p, err)
				}

				rv = append(rv, grant.NewGrant(resource, MarketplaceAdminRole, resourceId, grant.WithAnnotation(anns...)))
			}
		}
	}

	return rv, "", nil, nil
}

func newAccountBuilder(client *databricks.Client) *accountBuilder {
	return &accountBuilder{
		client:       client,
		resourceType: accountResourceType,
	}
}
