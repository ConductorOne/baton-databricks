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
	client     *databricks.Client
	workspaces []string
}

// ResourceSyncers returns a ResourceSyncer for each resource type that should be synced from the upstream service.
func (d *Databricks) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		newAccountBuilder(d.client),
		newGroupBuilder(d.client),
		newServicePrincipalBuilder(d.client),
		newUserBuilder(d.client),
		newWorkspaceBuilder(d.client, d.workspaces),
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
// to be sure that they are valid. Since this connector works with two APIs and can have different types of credentials
// it is important to validate that the connector is properly configured before attempting to sync.
func (d *Databricks) Validate(ctx context.Context) (annotations.Annotations, error) {
	isAccAPIAvailable := false
	isWSAPIAvailable := false

	// Check if we can list users from Account API (unless we are using token auth specific to a single workspace).
	if !d.client.IsTokenAuth() {
		_, _, err := d.client.ListRoles(ctx, "", "", "")
		if err == nil {
			isAccAPIAvailable = true
		}
	}

	// Validate that credentials are valid for each targeted workspace.
	if len(d.workspaces) > 0 {
		for _, workspace := range d.workspaces {
			_, _, err := d.client.ListRoles(ctx, workspace, "", "")
			if err != nil && !isAccAPIAvailable {
				return nil, fmt.Errorf("databricks-connector: failed to validate credentials for workspace %s: %w", workspace, err)
			}

			isWSAPIAvailable = true
		}
	}

	// Validate that credentials are valid for every workspace.
	if len(d.workspaces) == 0 {
		workspaces, _, err := d.client.ListWorkspaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
		}

		for _, workspace := range workspaces {
			_, _, err := d.client.ListRoles(ctx, workspace.DeploymentName, "", "")
			if err != nil && !isAccAPIAvailable {
				return nil, fmt.Errorf("databricks-connector: failed to validate credentials for workspace %s: %w", workspace.DeploymentName, err)
			}

			isWSAPIAvailable = true
		}
	}

	// Resolve the result.
	if !isAccAPIAvailable && !isWSAPIAvailable {
		return nil, fmt.Errorf("databricks-connector: failed to validate credentials")
	}

	d.client.UpdateAvailability(isAccAPIAvailable, isWSAPIAvailable)

	return nil, nil
}

// New returns a new instance of the connector.
func New(
	ctx context.Context,
	hostname,
	accountHostname,
	accountID string,
	auth databricks.Auth,
	workspaces []string,
) (*Databricks, error) {
	httpClient, err := auth.GetClient(ctx)
	if err != nil {
		return nil, err
	}

	client, err := databricks.NewClient(ctx, httpClient, hostname, accountHostname, accountID, auth)
	if err != nil {
		return nil, err
	}

	return &Databricks{
		client,
		workspaces,
	}, nil
}
