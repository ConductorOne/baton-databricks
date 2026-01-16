package connector

import (
	"context"
	"fmt"
	"io"

	"github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type Databricks struct {
	client *databricks.Client
}

// ResourceSyncers returns a ResourceSyncerV2 for each resource type that should be synced from the upstream service.
func (d *Databricks) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	syncers := []connectorbuilder.ResourceSyncerV2{
		newAccountBuilder(d.client),
		newGroupBuilder(d.client),
		newServicePrincipalBuilder(d.client),
		newUserBuilder(d.client),
		newWorkspaceBuilder(d.client),
		newRoleBuilder(d.client),
	}

	return syncers
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
		AccountCreationSchema: &v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
				"email": {
					DisplayName: "Email",
					Required:    true,
					Description: "The email address of the user.",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "Email",
					Order:       1,
				},
				"displayName": {
					DisplayName: "Display Name",
					Required:    true,
					Description: "User's display name",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "Display Name",
					Order:       2,
				},
				"givenName": {
					DisplayName: "Given Name",
					Required:    false,
					Description: "User's given name",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "Given Name",
					Order:       3,
				},
				"familyName": {
					DisplayName: "Family Name",
					Required:    false,
					Description: "User's family name",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "Family Name",
					Order:       4,
				},
				"active": {
					DisplayName: "Active",
					Required:    false,
					Description: "if the user is active",
					Field: &v2.ConnectorAccountCreationSchema_Field_BoolField{
						BoolField: &v2.ConnectorAccountCreationSchema_BoolField{},
					},
					Placeholder: "active",
					Order:       5,
				},
			},
		},
	}, nil
}

// Validate is called to ensure that the connector is properly configured. It should exercise any API credentials
// to be sure that they are valid. Since this connector works with two APIs and can have different types of credentials
// it is important to validate that the connector is properly configured before attempting to sync.
func (d *Databricks) Validate(ctx context.Context) (annotations.Annotations, error) {
	isAccAPIAvailable := false
	isWSAPIAvailable := false

	// Check if we can list users from Account API.
	_, _, err := d.client.ListRoles(ctx, "", "", "")
	if err == nil {
		isAccAPIAvailable = true
	}

	// Validate that credentials are valid for every workspace.
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
		client: client,
	}, nil
}

// NewConnector returns a new connector builder from a configuration struct.
func NewConnector(ctx context.Context, cfg *config.Databricks, opts *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	l := ctxzap.Extract(ctx)

	accountHostname := getAccountHostname(cfg, cfg.Hostname)
	auth := prepareClientAuth(ctx, cfg, l)

	cb, err := New(
		ctx,
		cfg.Hostname,
		accountHostname,
		cfg.AccountId,
		auth,
	)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, nil, err
	}

	return cb, nil, nil
}

func prepareClientAuth(_ context.Context, cfg *config.Databricks, l *zap.Logger) databricks.Auth {
	accountID := cfg.AccountId
	databricksClientId := cfg.DatabricksClientId
	databricksClientSecret := cfg.DatabricksClientSecret
	accountHostname := getAccountHostname(cfg, cfg.Hostname)

	return databricks.NewOAuth2(
		accountID,
		databricksClientId,
		databricksClientSecret,
		accountHostname,
	)
}

// getAccountHostname returns the account hostname from config if set, otherwise calculates it from hostname.
func getAccountHostname(cfg *config.Databricks, hostname string) string {
	if cfg.AccountHostname != "" {
		return cfg.AccountHostname
	}
	return databricks.GetAccountHostname(hostname)
}
