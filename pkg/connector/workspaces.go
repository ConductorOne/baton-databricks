package connector

import (
	"context"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type workspaceBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (w *workspaceBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return workspaceResourceType
}

func workspaceResource(ctx context.Context, workspace *databricks.Workspace, parent *v2.ResourceId) (*v2.Resource, error) {
	resource, err := rs.NewResource(
		workspace.Name,
		workspaceResourceType,
		workspace.ID,
		rs.WithParentResourceID(parent),
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

	workspaces, err := w.client.ListWorkspaces(ctx, &databricks.PaginationVars{Count: ResourcesPageSize})
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, workspace := range workspaces {
		wCopy := workspace

		wr, err := workspaceResource(ctx, &wCopy, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, wr)
	}

	return rv, "", nil, nil
}

func (w *workspaceBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	// TODO: Implement Workspace API support

	return nil, "", nil, nil
}

func (w *workspaceBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	// TODO: Implement Workspace API support

	return nil, "", nil, nil
}

func newWorkspaceBuilder(client *databricks.Client) *workspaceBuilder {
	return &workspaceBuilder{
		client:       client,
		resourceType: workspaceResourceType,
	}
}
