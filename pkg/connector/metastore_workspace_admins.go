package connector

import (
	"context"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const workspaceAdminsPrefix = "_workspace_admins_"

// workspaceAdminResolver handles Unity Catalog's system alias for the workspace
// admins group. Its scope is one Grants page, not a cache shared across syncs.
type workspaceAdminResolver struct {
	metastore  *metastoreBuilder
	annos      *annotations.Annotations
	workspaces map[string]databricks.Workspace
	running    map[string]bool
}

func (r *workspaceAdminResolver) resolve(ctx context.Context, assignment databricks.PrivilegeAssignment) (*v2.ResourceId, error) {
	// The alias ends in the numeric workspace ID. Names contain underscores and change,
	// so neither the name nor the read route identifies the workspace.
	workspaceID := assignment.Principal[strings.LastIndex(assignment.Principal, "_")+1:]
	if workspaceID == "" {
		return nil, nil
	}
	if r.workspaces == nil {
		workspaces, running, err := r.metastore.scopedWorkspaces(ctx, r.annos)
		if err != nil {
			return nil, err
		}
		r.workspaces = workspaces
		r.running = running
	}
	workspace, ok := r.workspaces[workspaceID]
	if !ok {
		return nil, nil // The workspace's group is outside the configured sync scope.
	}
	if !r.running[workspaceID] {
		// A workspace being deleted never becomes RUNNING, so waiting fails every sync.
		if strings.EqualFold(workspace.Status, workspaceStatusCancelling) {
			return nil, nil
		}

		return nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: workspace %s is %s; cannot resolve its metastore admins privileges until it is RUNNING",
			workspace.DeploymentName, workspace.Status)
	}

	id := assignment.PrincipalID.String()
	if id == "" || id == "0" {
		var rateLimit *v2.RateLimitDescription
		var err error
		id, rateLimit, err = r.metastore.client.FindGroupID(ctx, workspace.DeploymentName, workspaceAdminsGroupName)
		noteRateLimit(r.annos, rateLimit)
		if err != nil {
			return nil, refuseUnreadable("databricks-connector: failed to resolve workspace admins group", err)
		}
		if id == "" {
			return nil, nil
		}
	} else {
		name, rateLimit, err := r.metastore.client.FindGroupDisplayName(ctx, workspace.DeploymentName, id)
		noteRateLimit(r.annos, rateLimit)
		if err != nil {
			return nil, refuseUnreadable("databricks-connector: failed to resolve workspace admins group", err)
		}
		if name != workspaceAdminsGroupName {
			return nil, nil // Never map an alias to another group or reuse a deleted ID's name.
		}
	}

	return &v2.ResourceId{
		ResourceType: groupResourceType.Id,
		Resource: groupResourceId(ctx, id, &v2.ResourceId{
			ResourceType: workspaceResourceType.Id,
			Resource:     workspace.DeploymentName,
		}),
	}, nil
}
