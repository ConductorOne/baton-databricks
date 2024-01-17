package connector

import (
	"context"
	"fmt"
	"io"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

type Databricks struct {
	client  *databricks.Client
	configs []databricks.Config
}

// ResourceSyncers returns a ResourceSyncer for each resource type that should be synced from the upstream service.
func (d *Databricks) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		newAccountBuilder(d.client),
		newGroupBuilder(d.client),
		newServicePrincipalBuilder(d.client),
		newUserBuilder(d.client),
		// TODO: implement workspace builder that will be able to use different clients for different workspaces
		newWorkspaceBuilder(d.client),
		newRoleBuilder(d.client),
	}
}

// Asset takes an input AssetRef and attempts to fetch it using the connector's authenticated http client
// It streams a response, always starting with a metadata object, following by chunked payloads for the asset.
func (d *Databricks) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

// Metadata returns metadata about the connector.
func (d *Databricks) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "Databricks",
		Description: "Connector syncing Databricks workspaces, users, groups, service principals and roles to Baton",
	}, nil
}

// Validate is called to ensure that the connector is properly configured. It should exercise any API credentials
// to be sure that they are valid.
func (d *Databricks) Validate(ctx context.Context) (annotations.Annotations, error) {
	_, _, err := d.client.ListUsers(ctx, &databricks.PaginationVars{Count: 1})
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to validate client: %w", err)
	}

	return nil, nil
}

// New returns a new instance of the connector.
func New(ctx context.Context, acc string, configs []databricks.Config) (*Databricks, error) {
	httpClient, err := configs[0].GetClient(ctx)
	if err != nil {
		return nil, err
	}

	client := databricks.NewClient(httpClient, acc, configs[0])

	return &Databricks{
		client,
		configs,
	}, nil
}
