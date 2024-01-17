package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type servicePrincipalBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (s *servicePrincipalBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return servicePrincipalResourceType
}

func servicePrincipalResource(ctx context.Context, servicePrincipal *databricks.ServicePrincipal, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"application_id": servicePrincipal.ApplicationID,
		"display_name":   servicePrincipal.DisplayName,
	}

	servicePrincipalTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	resource, err := rs.NewGroupResource(
		servicePrincipal.DisplayName,
		servicePrincipalResourceType,
		servicePrincipal.ID,
		servicePrincipalTraitOptions,
		rs.WithParentResourceID(parent),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the servicePrincipals from the database as resource objects.
func (s *servicePrincipalBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	bag, page, err := parsePageToken(pToken.Token, &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id})
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	servicePrincipals, total, err := s.client.ListServicePrincipals(
		ctx,
		databricks.NewPaginationVars(page, ResourcesPageSize),
		databricks.NewServicePrincipalAttrVars(),
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list service principals: %w", err)
	}

	var rv []*v2.Resource
	for _, servicePrincipal := range servicePrincipals {
		gCopy := servicePrincipal

		gr, err := servicePrincipalResource(ctx, &gCopy, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, gr)
	}

	token := prepareNextToken(page, len(servicePrincipals), total)
	nextPage, err := bag.NextToken(token)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, nextPage, nil, nil
}

// Entitlements return all entitlements relevant to the servicePrincipal.
func (s *servicePrincipalBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get application_id from service principal profile")
	}

	// role permissions entitlements
	// get all assignable roles for this specific service principal resource
	roles, err := s.client.ListRoles(context.Background(), "servicePrincipals", applicationId)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list roles for service principal %s (%s): %w", resource.Id.Resource, applicationId, err)
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

// Grants return all grants relevant to the servicePrincipal.
// Databricks ServicePrincipals have membership and role grants.
func (s *servicePrincipalBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get application_id from service principal profile")
	}

	ruleSets, err := s.client.ListRuleSets(ctx, "servicePrincipals", applicationId)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list rule sets for service principal %s (%s): %w", resource.Id.Resource, applicationId, err)
	}

	var rv []*v2.Grant
	for _, ruleSet := range ruleSets {
		for _, p := range ruleSet.Principals {
			resourceId, anns, err := prepareResourceID(ctx, s.client, p)
			if err != nil {
				return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource id for principal %s: %w", p, err)
			}

			rv = append(rv, grant.NewGrant(resource, ruleSet.Role, resourceId, grant.WithAnnotation(anns...)))
		}
	}

	return rv, "", nil, nil
}

func newServicePrincipalBuilder(client *databricks.Client) *servicePrincipalBuilder {
	return &servicePrincipalBuilder{
		client:       client,
		resourceType: servicePrincipalResourceType,
	}
}
