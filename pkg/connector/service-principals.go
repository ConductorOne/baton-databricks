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
	roles := make([]string, len(servicePrincipal.Roles))
	ents := make([]string, len(servicePrincipal.Entitlements))

	for i, r := range servicePrincipal.Roles {
		roles[i] = r.Value
	}

	for i, e := range servicePrincipal.Entitlements {
		ents[i] = e.Value
	}

	profile := map[string]interface{}{
		"application_id": servicePrincipal.ApplicationID,
		"display_name":   servicePrincipal.DisplayName,
	}

	if len(roles) > 0 {
		profile["roles"] = strings.Join(roles, ",")
	}

	if len(ents) > 0 {
		profile["entitlements"] = strings.Join(ents, ",")
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

	servicePrincipals, total, err := s.client.ListServicePrincipals(ctx, databricks.NewPaginationVars(page, ResourcesPageSize))
	if err != nil {
		return nil, "", nil, err
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

	var token string

	next := page + uint(len(servicePrincipals))
	if next < total+1 {
		token = strconv.Itoa(int(next))
	}

	nextPage, err := bag.NextToken(token)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, nextPage, nil, nil
}

// Entitlements return all entitlements relevant to the servicePrincipal.
// Similar to Groups, ServicePrincipals have:
// - role entitlements (can have account_admin or marketplace_admin roles)
// - role permissions entitlements (can have specific roles - e.g. manager or user)
// - workspace permissions entitlements
func (s *servicePrincipalBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, err
	}

	// role entitlements
	rolesPayload, ok := rs.GetProfileStringValue(groupTrait.Profile, "roles")
	if ok {
		roles := strings.Split(rolesPayload, ",")
		for _, role := range roles {
			rolePermissionOptions := []ent.EntitlementOption{
				ent.WithGrantableTo(servicePrincipalResourceType),
				ent.WithDisplayName(fmt.Sprintf("%s role", role)),
				ent.WithDescription(fmt.Sprintf("%s role in Databricks", role)),
			}

			rv = append(rv, ent.NewPermissionEntitlement(resource, role, rolePermissionOptions...))
		}
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("failed to get application_id from service principal profile")
	}

	// role permissions entitlements
	// get all assignable roles for this specific service principal resource
	roles, err := s.client.ListRoles(context.Background(), "servicePrincipals", applicationId)
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to list roles for service principal %s (%s): %w", resource.Id.Resource, applicationId, err)
	}

	for _, role := range roles {
		rolePermissionOptions := []ent.EntitlementOption{
			ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
			ent.WithDisplayName(fmt.Sprintf("%s role", role.Name)),
			ent.WithDescription(fmt.Sprintf("%s role in Databricks", role.Name)),
		}

		rv = append(rv, ent.NewPermissionEntitlement(resource, role.Name, rolePermissionOptions...))
	}

	// workspace permissions entitlements
	entitlementsPayload, ok := rs.GetProfileStringValue(groupTrait.Profile, "entitlements")
	if ok {
		entitlements := strings.Split(entitlementsPayload, ",")
		for _, e := range entitlements {
			rolePermissionOptions := []ent.EntitlementOption{
				ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
				ent.WithDisplayName(fmt.Sprintf("%s entitlement", e)),
				ent.WithDescription(fmt.Sprintf("%s entitlement in Databricks", e)),
			}

			rv = append(rv, ent.NewPermissionEntitlement(resource, e, rolePermissionOptions...))
		}
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

	var rv []*v2.Grant

	rolesPayload, ok := rs.GetProfileStringValue(groupTrait.Profile, "roles")
	if ok {
		roles := strings.Split(rolesPayload, ",")
		for _, role := range roles {
			rv = append(rv, grant.NewGrant(resource, role, resource.Id))
		}
	}

	entsPayload, ok := rs.GetProfileStringValue(groupTrait.Profile, "entitlements")
	if ok {
		ents := strings.Split(entsPayload, ",")
		for _, e := range ents {
			rv = append(rv, grant.NewGrant(resource, e, resource.Id))
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
