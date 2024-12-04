package connector

import (
	"context"
	"fmt"
	"slices"

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
		"parent_type":    parent.ResourceType,
		"parent_id":      parent.Resource,
	}

	servicePrincipalTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	// keep the parent resource id, only if the parent resource is account
	var options []rs.ResourceOption
	if parent.ResourceType == accountResourceType.Id {
		options = append(options, rs.WithParentResourceID(parent))
	}

	resource, err := rs.NewGroupResource(
		servicePrincipal.DisplayName,
		servicePrincipalResourceType,
		servicePrincipal.ID,
		servicePrincipalTraitOptions,
		options...,
	)

	if err != nil {
		return nil, err
	}

	resourceCache.Set(servicePrincipal.ID, resource)
	return resource, nil
}

// List returns all the servicePrincipals from the database as resource objects.
func (s *servicePrincipalBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	var workspaceId string
	if parentResourceID.ResourceType == workspaceResourceType.Id {
		workspaceId = parentResourceID.Resource
	}

	bag, page, err := parsePageToken(pToken.Token, &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id})
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	servicePrincipals, total, _, err := s.client.ListServicePrincipals(
		ctx,
		workspaceId,
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
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
	}

	var workspaceId string
	if parentType == workspaceResourceType.Id {
		workspaceId = parentID
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get application_id from service principal profile")
	}

	// role permissions entitlements
	// get all assignable roles for this specific service principal resource
	roles, _, err := s.client.ListRoles(context.Background(), workspaceId, ServicePrincipalsType, applicationId)
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
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
	}

	var workspaceId string
	if parentType == workspaceResourceType.Id {
		workspaceId = parentID
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get application_id from service principal profile")
	}

	ruleSets, _, err := s.client.ListRuleSets(ctx, workspaceId, ServicePrincipalsType, applicationId)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list rule sets for service principal %s (%s): %w", resource.Id.Resource, applicationId, err)
	}

	var rv []*v2.Grant
	for _, ruleSet := range ruleSets {
		for _, p := range ruleSet.Principals {
			resourceId, err := prepareResourceID(ctx, s.client, p)
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

			rv = append(rv, grant.NewGrant(resource, ruleSet.Role, resourceId, grant.WithAnnotation(annotations...)))
		}
	}

	return rv, "", nil, nil
}

func (s *servicePrincipalBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can be granted service principal permissions",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can be granted service principal permissions")
	}

	groupTrait, err := rs.GetGroupTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
	}

	var workspaceId string
	if parentType == workspaceResourceType.Id {
		workspaceId = parentID
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: failed to get application_id from service principal profile")
	}

	ruleSets, _, err := s.client.ListRuleSets(ctx, workspaceId, ServicePrincipalsType, applicationId)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list rule sets for service principal %s (%s): %w", principal.Id.Resource, applicationId, err)
	}

	principalID, err := preparePrincipalID(ctx, s.client, principal.Id.ResourceType, principal.Id.Resource)
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
					"databricks-connector: service principal already has the entitlement",
					zap.String("principal_id", principalID),
					zap.String("entitlement", entitlement.Slug),
				)

				return nil, nil
			}

			// add the principal to the rule set
			ruleSets[i].Principals = append(ruleSets[i].Principals, principalID)
		}
	}

	if !found {
		ruleSets = append(ruleSets, databricks.RuleSet{
			Role:       entitlement.Slug,
			Principals: []string{principalID},
		})
	}

	_, err = s.client.UpdateRuleSets(ctx, workspaceId, ServicePrincipalsType, applicationId, ruleSets)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for service principal %s (%s): %w", principal.Id.Resource, applicationId, err)
	}

	return nil, nil
}

func (s *servicePrincipalBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	principal := grant.Principal
	entitlement := grant.Entitlement

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can have service principal permissions revoked",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can have service principal permissions revoked")
	}

	groupTrait, err := rs.GetGroupTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	parentType, parentID, err := getParentInfoFromProfile(groupTrait.Profile)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get parent info from group profile: %w", err)
	}

	var workspaceId string
	if parentType == workspaceResourceType.Id {
		workspaceId = parentID
	}

	applicationId, ok := rs.GetProfileStringValue(groupTrait.Profile, "application_id")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: failed to get application_id from service principal profile")
	}

	ruleSets, _, err := s.client.ListRuleSets(ctx, workspaceId, ServicePrincipalsType, applicationId)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list rule sets for service principal %s (%s): %w", principal.Id.Resource, applicationId, err)
	}

	if len(ruleSets) == 0 {
		l.Info(
			"databricks-connector: service principal already does not have the entitlement",
			zap.String("principal_id", principal.Id.Resource),
			zap.String("entitlement", entitlement.Slug),
		)

		return nil, nil
	}

	principalID, err := preparePrincipalID(ctx, s.client, principal.Id.ResourceType, principal.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to prepare principal id: %w", err)
	}

	for i, ruleSet := range ruleSets {
		if ruleSet.Role != entitlement.Slug {
			continue
		}

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
			"databricks-connector: service principal already does not have the entitlement",
			zap.String("principal_id", principalID),
			zap.String("entitlement", entitlement.Slug),
		)

		return nil, nil
	}

	_, err = s.client.UpdateRuleSets(ctx, workspaceId, ServicePrincipalsType, applicationId, ruleSets)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to update rule sets for service principal %s (%s): %w", principal.Id.Resource, applicationId, err)
	}

	return nil, nil
}

func newServicePrincipalBuilder(client *databricks.Client) *servicePrincipalBuilder {
	return &servicePrincipalBuilder{
		client:       client,
		resourceType: servicePrincipalResourceType,
	}
}
