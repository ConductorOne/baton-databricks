package connector

import (
	"context"
	"fmt"
	"strconv"

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

const workspaceMemberEntitlement = "member"

type workspaceBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
	workspaces   map[string]struct{}
}

func (w *workspaceBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return workspaceResourceType
}

func minimalWorkspaceResource(ctx context.Context, workspace *databricks.Workspace, parent *v2.ResourceId) (*v2.Resource, error) {
	resource, err := rs.NewGroupResource(
		workspace.DeploymentName,
		workspaceResourceType,
		workspace.DeploymentName,
		nil,
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: userResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: groupResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: servicePrincipalResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: roleResourceType.Id},
		),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func workspaceResource(_ context.Context, workspace *databricks.Workspace, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"workspace_id": workspace.ID,
	}

	resource, err := rs.NewGroupResource(
		workspace.Name,
		workspaceResourceType,
		workspace.DeploymentName,
		[]rs.GroupTraitOption{
			rs.WithGroupProfile(profile),
		},
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: roleResourceType.Id},
		),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the workspaces from the database as resource objects.
func (w *workspaceBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		return nil, "", nil, nil
	}

	var rv []*v2.Resource
	if w.client.IsAccountAPIAvailable() {
		workspaces, _, err := w.client.ListWorkspaces(ctx)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
		}

		for _, workspace := range workspaces {
			// If workspaces are specified, skip all the workspaces that are not in the list.
			if _, ok := w.workspaces[workspace.DeploymentName]; !ok && len(w.workspaces) > 0 {
				continue
			}

			wCopy := workspace

			wr, err := workspaceResource(ctx, &wCopy, parentResourceID)
			if err != nil {
				return nil, "", nil, err
			}

			rv = append(rv, wr)
		}
	} else {
		for workspace := range w.workspaces {
			ws := &databricks.Workspace{
				DeploymentName: workspace,
			}

			wr, err := minimalWorkspaceResource(ctx, ws, parentResourceID)
			if err != nil {
				return nil, "", nil, err
			}

			rv = append(rv, wr)
		}
	}

	return rv, "", nil, nil
}

// Entitlements returns slice of entitlements representing workspace members.
// To get workspace members, we can only use the account API.
func (w *workspaceBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	if !w.client.IsAccountAPIAvailable() {
		return nil, "", nil, nil
	}

	var rv []*v2.Entitlement

	memberAssignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s %s", resource.DisplayName, workspaceMemberEntitlement)),
		ent.WithDescription(fmt.Sprintf("%s %s in Databricks", resource.DisplayName, workspaceMemberEntitlement)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(resource, workspaceMemberEntitlement, memberAssignmentOptions...))

	return rv, "", nil, nil
}

// Grants returns slice of grants representing workspace members.
// To get workspace members, we can only use the account API.
func (w *workspaceBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	if !w.client.IsAccountAPIAvailable() {
		return nil, "", nil, nil
	}

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	workspaceId, ok := rs.GetProfileInt64Value(groupTrait.Profile, "workspace_id")
	if !ok {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to get workspace ID: %w", err)
	}

	workspace := strconv.Itoa(int(workspaceId))
	assignments, _, err := w.client.ListWorkspaceMembers(ctx, workspace)
	if err != nil {
		return nil, "", nil, fmt.Errorf("databricks-connector: failed to list workspace members: %w", err)
	}

	var rv []*v2.Grant
	for _, assignment := range assignments {
		resourceType, err := prepareResourceType(assignment.Principal)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource type: %w", err)
		}

		resourceID, err := rs.NewResourceID(resourceType, assignment.Principal.ID)
		if err != nil {
			return nil, "", nil, fmt.Errorf("databricks-connector: failed to prepare resource ID: %w", err)
		}

		var annotations []protoreflect.ProtoMessage
		if resourceType == groupResourceType {
			rid, expandAnnotation, err := groupGrantExpansion(ctx, resourceID.Resource, resource.ParentResourceId)
			if err != nil {
				return rv, "", nil, err
			}
			resourceID = rid
			annotations = append(annotations, expandAnnotation)
		}

		rv = append(rv, grant.NewGrant(resource, workspaceMemberEntitlement, resourceID, grant.WithAnnotation(annotations...)))
	}

	return rv, "", nil, nil
}

func (w *workspaceBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can be granted workspace membership",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can be granted workspace membership")
	}

	groupTrait, err := rs.GetGroupTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	workspaceID, ok := rs.GetProfileInt64Value(groupTrait.Profile, "workspace_id")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: failed to get workspace ID: %w", err)
	}

	workspace := strconv.Itoa(int(workspaceID))
	_, err = w.client.CreateOrUpdateWorkspaceMember(ctx, workspace, principal.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to create or update workspace member: %w", err)
	}

	return nil, nil
}

func (w *workspaceBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	principal := grant.Principal
	entitlement := grant.Entitlement

	if !isValidPrincipal(principal.Id) {
		l.Warn(
			"databricks-connector: only users, groups and service principals can have workspace membership revoked",
			zap.String("principal_id", principal.Id.String()),
			zap.String("principal_type", principal.Id.ResourceType),
		)

		return nil, fmt.Errorf("databricks-connector: only users, groups and service principals can have workspace membership revoked")
	}

	groupTrait, err := rs.GetGroupTrait(entitlement.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	workspaceID, ok := rs.GetProfileInt64Value(groupTrait.Profile, "workspace_id")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: failed to get workspace ID: %w", err)
	}

	workspace := strconv.Itoa(int(workspaceID))
	_, err = w.client.RemoveWorkspaceMember(ctx, workspace, principal.Id.Resource)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to create or update workspace member: %w", err)
	}

	return nil, nil
}

func newWorkspaceBuilder(client *databricks.Client, workspaces []string) *workspaceBuilder {
	wMap := make(map[string]struct{}, len(workspaces))
	for _, w := range workspaces {
		wMap[w] = struct{}{}
	}

	return &workspaceBuilder{
		client:       client,
		resourceType: workspaceResourceType,
		workspaces:   wMap,
	}
}
