package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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
}

func (w *workspaceBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return workspaceResourceType
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
func (w *workspaceBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil {
		return nil, nil, nil
	}

	var rv []*v2.Resource

	workspaces, _, err := w.client.ListWorkspaces(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}

	for _, workspace := range workspaces {
		wCopy := workspace

		wr, err := workspaceResource(ctx, &wCopy, parentResourceID)
		if err != nil {
			return nil, nil, err
		}

		rv = append(rv, wr)
	}

	return rv, nil, nil
}

// Entitlements returns slice of entitlements representing workspace members.
// To get workspace members, we can only use the account API.
func (w *workspaceBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	if !w.client.IsAccountAPIAvailable() {
		return nil, nil, nil
	}

	var rv []*v2.Entitlement

	memberAssignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType),
		ent.WithDisplayName(fmt.Sprintf("%s %s", resource.DisplayName, workspaceMemberEntitlement)),
		ent.WithDescription(fmt.Sprintf("%s %s in Databricks", resource.DisplayName, workspaceMemberEntitlement)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(resource, workspaceMemberEntitlement, memberAssignmentOptions...))

	return rv, nil, nil
}

// Grants returns slice of grants representing workspace members.
// To get workspace members, we can only use the account API.
func (w *workspaceBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)

	if !w.client.IsAccountAPIAvailable() {
		return nil, nil, nil
	}

	groupTrait, err := rs.GetGroupTrait(resource)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to get group trait: %w", err)
	}

	workspaceId, ok := rs.GetProfileInt64Value(groupTrait.Profile, "workspace_id")
	if !ok {
		return nil, nil, fmt.Errorf("databricks-connector: failed to get workspace ID: %w", err)
	}

	workspace := strconv.Itoa(int(workspaceId))
	assignments, rateLimitDesc, err := w.client.ListWorkspaceMembers(ctx, workspace)
	annos := annotations.Annotations{}
	if err != nil {
		if rateLimitDesc != nil {
			annos.WithRateLimiting(rateLimitDesc)
		}
		// Check if this is the specific error for workspaces without permissions API
		var apiErr *databricks.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusBadRequest {
			// Check for the specific error message that indicates permissions API is not available
			if strings.Contains(apiErr.Message, "Permission assignment APIs are not available for this workspace") {
				l := ctxzap.Extract(ctx)
				l.Info("Workspace does not have permissions API available - skipping",
					zap.String("workspace_id", workspace),
					zap.String("workspace_name", resource.DisplayName),
				)
				// Return empty grants for workspaces without permissions API
				return []*v2.Grant{}, &rs.SyncOpResults{Annotations: annos}, nil
			}
			// If it's a 400 but not the specific permissions API error, log it and return error
			l := ctxzap.Extract(ctx)
			l.Warn("Received 400 error from workspace API, but not the expected permissions API error",
				zap.String("workspace_id", workspace),
				zap.String("workspace_name", resource.DisplayName),
				zap.String("error_message", apiErr.Message),
				zap.String("error_detail", apiErr.Detail),
			)
		}
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: failed to list workspace members: %w", err)
	}

	var rv []*v2.Grant
	l.Debug("grants: workspace resource", zap.String("workspace_id", workspace), zap.Int("assignments_count", len(assignments)))
	for _, assignment := range assignments {
		resourceType, err := prepareResourceType(assignment.Principal)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to prepare resource type: %w", err)
		}

		resourceID, err := rs.NewResourceID(resourceType, assignment.Principal.ID)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to prepare resource ID: %w", err)
		}

		var annotations []protoreflect.ProtoMessage
		if resourceType == groupResourceType {
			rid, expandAnnotation, err := groupGrantExpansion(ctx, resourceID.Resource, resource.ParentResourceId)
			if err != nil {
				return rv, nil, err
			}
			resourceID = rid
			annotations = append(annotations, expandAnnotation)
		}

		rv = append(rv, grant.NewGrant(resource, workspaceMemberEntitlement, resourceID, grant.WithAnnotation(annotations...)))
	}

	return rv, nil, nil
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

func newWorkspaceBuilder(client *databricks.Client) *workspaceBuilder {
	return &workspaceBuilder{
		client:       client,
		resourceType: workspaceResourceType,
	}
}
