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
	workspaces   map[string]struct{}
}

func (w *workspaceBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return workspaceResourceType
}

// minimalWorkspaceResource builds a workspace from just its deployment name, for
// token auth where the Account API (and its numeric workspace IDs) is unreachable.
// Deployment names are unique per Databricks cloud (they form the workspace's
// canonical hostname), so they're safe as the resource ID here.
// Users, groups and service principals hang off the workspace here instead of the account.
func minimalWorkspaceResource(_ context.Context, workspace *databricks.Workspace, parent *v2.ResourceId) (*v2.Resource, error) {
	return rs.NewGroupResource(
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
}

func workspaceResource(_ context.Context, workspace *databricks.Workspace, parent *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"workspace_id": workspace.ID,
	}

	resource, err := rs.NewGroupResource(
		workspace.Name,
		workspaceResourceType,
		workspace.DeploymentName,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: roleResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: workspacePATResourceType.Id},
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

	if w.client.IsTokenAuth() {
		for workspace := range w.workspaces {
			if w.client.IsWorkspaceNameExcluded(workspace) {
				continue
			}

			ws := &databricks.Workspace{DeploymentName: workspace}

			wr, err := minimalWorkspaceResource(ctx, ws, parentResourceID)
			if err != nil {
				return nil, nil, err
			}

			rv = append(rv, wr)
		}

		if len(w.workspaces) > 0 && len(rv) == 0 {
			ctxzap.Extract(ctx).Warn("databricks-connector: all configured workspaces are excluded, sync will be empty",
				zap.Strings("workspaces", configuredWorkspaceNames(w.workspaces)),
			)
		}

		return rv, nil, nil
	}

	workspaces, _, err := w.client.ListWorkspaces(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}

	matchedConfigured := make(map[string]struct{}, len(w.workspaces))
	for _, workspace := range workspaces {
		// Skip workspaces outside the configured set when one was provided.
		if len(w.workspaces) > 0 {
			cfg, ok := matchConfiguredWorkspace(w.workspaces, workspace.DeploymentName, workspace.Name, strconv.Itoa(workspace.ID))
			if !ok {
				continue
			}
			matchedConfigured[cfg] = struct{}{}
		}

		wCopy := workspace

		wr, err := workspaceResource(ctx, &wCopy, parentResourceID)
		if err != nil {
			return nil, nil, err
		}

		rv = append(rv, wr)
	}

	l := ctxzap.Extract(ctx)
	if len(w.workspaces) > 0 && len(matchedConfigured) == 0 {
		l.Warn("databricks-connector: none of the configured workspaces matched any account workspace, sync will be empty",
			zap.Strings("workspaces", configuredWorkspaceNames(w.workspaces)),
		)
	}
	for workspace := range w.workspaces {
		if _, ok := matchedConfigured[workspace]; ok {
			continue
		}
		if w.client.IsWorkspaceNameExcluded(workspace) {
			l.Debug("databricks-connector: configured workspace was excluded from sync",
				zap.String("workspace", workspace),
			)
			continue
		}
		l.Debug("databricks-connector: configured workspace not found among account workspaces",
			zap.String("workspace", workspace),
		)
	}

	return rv, nil, nil
}

func configuredWorkspaceNames(configured map[string]struct{}) []string {
	names := make([]string, 0, len(configured))
	for name := range configured {
		names = append(names, name)
	}
	return names
}

// matchConfiguredWorkspace looks up a workspace by deployment name, name, or numeric
// ID case-insensitively (mirroring Client.IsWorkspaceNameExcluded), returning the matched
// key so warnings can report the value the user configured.
func matchConfiguredWorkspace(configured map[string]struct{}, candidates ...string) (string, bool) {
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, ok := configured[candidate]; ok {
			return candidate, true
		}
		for cfg := range configured {
			if strings.EqualFold(cfg, candidate) {
				return cfg, true
			}
		}
	}
	return "", false
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

	profile := rs.GetProfile(resource)

	workspaceId, ok := rs.GetProfileInt64Value(profile, "workspace_id")
	if !ok {
		return nil, nil, fmt.Errorf("databricks-connector: failed to get workspace ID")
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

	profile := rs.GetProfile(entitlement.Resource)

	workspaceID, ok := rs.GetProfileInt64Value(profile, "workspace_id")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: failed to get workspace ID")
	}

	workspace := strconv.Itoa(int(workspaceID))
	_, err := w.client.CreateOrUpdateWorkspaceMember(ctx, workspace, principal.Id.Resource)
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

	profile := rs.GetProfile(entitlement.Resource)

	workspaceID, ok := rs.GetProfileInt64Value(profile, "workspace_id")
	if !ok {
		return nil, fmt.Errorf("databricks-connector: failed to get workspace ID")
	}

	workspace := strconv.Itoa(int(workspaceID))
	_, err := w.client.RemoveWorkspaceMember(ctx, workspace, principal.Id.Resource)
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
