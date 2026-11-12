package connector

import (
	"strconv"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

type syncSessionKey struct{}

func catalogRoutesKey(metastoreID string) string {
	return "unity-catalog/routes/" + metastoreID
}

func catalogRoutesForWorkspaces(workspaces []databricks.Workspace, owner string) []catalogRoute {
	routes := make([]catalogRoute, 0, len(workspaces))
	for _, workspace := range workspaces {
		routes = append(routes, catalogRoute{
			Workspace:      workspace.DeploymentName,
			WorkspaceID:    strconv.Itoa(workspace.ID),
			WorkspaceName:  workspace.Name,
			MetastoreOwner: owner,
		})
	}
	return routes
}
