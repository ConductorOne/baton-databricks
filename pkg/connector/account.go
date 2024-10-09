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

const (
	// Roles relevant to Account API (Grantable to User or ServicePrincipal).
	AccountAdminRole     = "account_admin"
	MarketplaceAdminRole = "marketplace.admin"

	// Roles (or Entitlements) relevant to Workspace API (Grantable to User, Group or ServicePrincipal).
	WorkspaceAccessRole    = "workspace-access"
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

func accountResource(ctx context.Context, accID string, accAPIAvailable bool) (*v2.Resource, error) {
	children := []protoreflect.ProtoMessage{
		&v2.ChildResourceType{ResourceTypeId: workspaceResourceType.Id},
	}

	if accAPIAvailable {
		children = append(children,
			&v2.ChildResourceType{ResourceTypeId: userResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: groupResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: servicePrincipalResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: roleResourceType.Id},
		)
	}

	resource, err := rs.NewResource(
		accID,
		accountResourceType,
		accID,
		rs.WithAnnotation(children...),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (a *accountBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var rv []*v2.Resource

	ur, err := accountResource(ctx, a.client.GetAccountId(), a.client.IsAccountAPIAvailable())
	if err != nil {
		return nil, "", nil, err
	}

	rv = append(rv, ur)

	return rv, "", nil, nil
}

// Entitlements returns slice of entitlements for marketplace admins under account.
func (a *accountBuilder) Entitlements(
	_ context.Context,
	resource *v2.Resource,
	_ *pagination.Token,
) (
	[]*v2.Entitlement,
	string,
	annotations.Annotations,
	error,
) {
	if !a.client.IsAccountAPIAvailable() {
		return nil, "", nil, nil
	}
	return []*v2.Entitlement{
		ent.NewPermissionEntitlement(
			resource,
			MarketplaceAdminRole,
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
			ent.WithDisplayName(fmt.Sprintf("%s %s role", resource.DisplayName, MarketplaceAdminRole)),
			ent.WithDescription(fmt.Sprintf("%s %s role in Databricks", resource.DisplayName, MarketplaceAdminRole)),
		),
	}, "", nil, nil
}

// Grants returns grants for marketplace admins under account.
// To get marketplace admins, we can only use the account API.
func (a *accountBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	if !a.client.IsAccountAPIAvailable() {
		return nil, "", nil, nil
	}

	a.client.SetAccountConfig()

	var rv []*v2.Grant

	// list rule sets for the account
	ruleSets, _, err := a.client.ListRuleSets(ctx, "", "")
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list rule sets for account %s: %w", resource.Id.Resource, err)
	}

	for _, ruleSet := range ruleSets {
		// rule set contains role and its principals, each one with resource type and resource id seperated by "/"
		if strings.Contains(ruleSet.Role, MarketplaceAdminRole) {
			for _, p := range ruleSet.Principals {
				resourceId, err := prepareResourceID(ctx, a.client, p)
				if err != nil {
					return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource id for principal %s: %w", p, err)
				}

				var annotations []protoreflect.ProtoMessage
				if resourceId.ResourceType == groupResourceType.Id {
					memberResource, annotation, err := expandGrantForGroup(resourceId.Resource)
					if err != nil {
						return nil, "", nil, fmt.Errorf("databricks-connector: failed to expand grant for group %s: %w", resourceId.Resource, err)
					}
					annotations = append(annotations, annotation)
					resourceId = memberResource.Id
				}

				rv = append(rv, grant.NewGrant(resource, MarketplaceAdminRole, resourceId, grant.WithAnnotation(annotations...)))
			}
		}
	}

	return rv, "", nil, nil
}

func (a *accountBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can be granted account permissions",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can be granted account permissions")
	}

	accID := entitlement.Resource.Id.Resource
	ruleSets, _, err := a.client.ListRuleSets(ctx, "", "")
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list rule sets for account %s: %w", accID, err)
	}

	principalID, err := preparePrincipalID(ctx, a.client, principal.Id.ResourceType, principal.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to prepare principal id for principal %s: %w", principal.Id.Resource, err)
	}

	found := false

	for i, ruleSet := range ruleSets {
		if !strings.Contains(ruleSet.Role, entitlement.Slug) {
			continue
		}

		found = true

		// check if it contains the principals and add principal to the rule set
		if slices.Contains(ruleSet.Principals, principalID) {
			l.Info(
				"databricks-connector: account already has the entitlement",
				zap.String("principal_id", principalID),
				zap.String("entitlement", entitlement.Slug),
			)

			return nil, nil
		}

		// add the principal to the rule set
		ruleSets[i].Principals = append(ruleSets[i].Principals, principalID)
	}

	if !found {
		ruleSets = append(ruleSets, databricks.RuleSet{
			Role:       fmt.Sprintf("roles/%s", entitlement.Slug),
			Principals: []string{principalID},
		})
	}

	_, err = a.client.UpdateRuleSets(ctx, "", "", ruleSets)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for account %s: %w", accID, err)
	}

	return nil, nil
}

func (a *accountBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	principal := grant.Principal
	entitlement := grant.Entitlement

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users and service principals can have account permissions revoked",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users and service principals can have account permissions revoked")
	}

	accID := entitlement.Resource.Id.Resource
	ruleSets, _, err := a.client.ListRuleSets(ctx, "", "")
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list rule sets for account %s: %w", accID, err)
	}

	if len(ruleSets) == 0 {
		l.Info(
			"databricks-connector: account already does not have the entitlement",
			zap.String("principal_id", principal.Id.Resource),
			zap.String("entitlement", entitlement.Slug),
		)

		return nil, nil
	}

	principalID, err := preparePrincipalID(ctx, a.client, principal.Id.ResourceType, principal.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", err)
	}

	for i, ruleSet := range ruleSets {
		if strings.Contains(ruleSet.Role, entitlement.Slug) {
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
				"databricks-connector: account already does not have the entitlement",
				zap.String("principal_id", principalID),
				zap.String("entitlement", entitlement.Slug),
			)

			return nil, nil
		}
	}

	_, err = a.client.UpdateRuleSets(ctx, "", "", ruleSets)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for account %s: %w", accID, err)
	}

	return nil, nil
}

func newAccountBuilder(client *databricks.Client) *accountBuilder {
	return &accountBuilder{
		client:       client,
		resourceType: accountResourceType,
	}
}
