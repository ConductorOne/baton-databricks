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
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type Databricks struct {
	client     *databricks.Client
	workspaces []string
}

// ResourceSyncers returns a ResourceSyncerV2 for each resource type that should be synced from the upstream service.
func (d *Databricks) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	syncers := []connectorbuilder.ResourceSyncer{
		newAccountBuilder(d.client),
		newGroupBuilder(d.client),
		newServicePrincipalBuilder(d.client),
		newUserBuilder(d.client),
		newWorkspaceBuilder(d.client, d.workspaces),
		newRoleBuilder(d.client),
	}

	// Convert ResourceSyncer to ResourceSyncerV2
	v2Syncers := make([]connectorbuilder.ResourceSyncerV2, len(syncers))
	for i, syncer := range syncers {
		v2Syncers[i] = &resourceSyncerV1toV2Adapter{syncer: syncer}
	}
	return v2Syncers
}

// resourceSyncerV1toV2Adapter adapts a ResourceSyncer to ResourceSyncerV2.
type resourceSyncerV1toV2Adapter struct {
	syncer connectorbuilder.ResourceSyncer
}

func (a *resourceSyncerV1toV2Adapter) ResourceType(ctx context.Context) *v2.ResourceType {
	return a.syncer.ResourceType(ctx)
}

func (a *resourceSyncerV1toV2Adapter) List(ctx context.Context, parentResourceID *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	resources, pageToken, annos, err := a.syncer.List(ctx, parentResourceID, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return resources, ret, err
}

func (a *resourceSyncerV1toV2Adapter) Entitlements(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	ents, pageToken, annos, err := a.syncer.Entitlements(ctx, r, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return ents, ret, err
}

func (a *resourceSyncerV1toV2Adapter) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	grants, pageToken, annos, err := a.syncer.Grants(ctx, r, &opts.PageToken)
	ret := &resource.SyncOpResults{NextPageToken: pageToken, Annotations: annos}
	return grants, ret, err
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
		client:     client,
		workspaces: workspaces,
	}, nil
}

// NewConnector returns a new connector builder from a configuration struct.
func NewConnector(ctx context.Context, cfg *config.Databricks, opts *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	l := ctxzap.Extract(ctx)

	err := config.ValidateConfig(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	hostname := getHostname(cfg)
	accountHostname := databricks.GetAccountHostname(hostname)
	auth := prepareClientAuth(ctx, cfg, l)

	cb, err := New(
		ctx,
		hostname,
		accountHostname,
		cfg.AccountId,
		auth,
		cfg.Workspaces,
	)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, nil, err
	}

	return cb, nil, nil
}

func areTokensSet(workspaces []string, tokens []string) bool {
	return len(tokens) > 0 &&
		len(workspaces) == len(tokens)
}

func prepareClientAuth(_ context.Context, cfg *config.Databricks, l *zap.Logger) databricks.Auth {
	accountID := cfg.AccountId
	databricksClientId := cfg.DatabricksClientId
	databricksClientSecret := cfg.DatabricksClientSecret
	username := cfg.Username
	password := cfg.Password
	workspaces := cfg.Workspaces
	tokens := cfg.WorkspaceTokens
	accountHostname := databricks.GetAccountHostname(getHostname(cfg))

	switch {
	case username != "" && password != "":
		l.Info(
			"using basic auth",
			zap.String("account-id", accountID),
			zap.String("username", username),
		)
		cAuth := databricks.NewBasicAuth(
			username,
			password,
		)
		return cAuth
	case databricksClientId != "" && databricksClientSecret != "":
		l.Info(
			"using oauth",
			zap.String("account-id", accountID),
			zap.String("client-id", databricksClientId),
		)
		cAuth := databricks.NewOAuth2(
			accountID,
			databricksClientId,
			databricksClientSecret,
			accountHostname,
		)
		return cAuth
	case areTokensSet(workspaces, tokens):
		l.Info(
			"using access token",
			zap.String("account-id", accountID),
		)
		cAuth := databricks.NewTokenAuth(
			workspaces,
			tokens,
		)
		return cAuth
	default:
		l.Warn(
			"no valid authentication method detected, falling back to NoAuth. " +
				"This will likely cause API calls to fail. " +
				"Please configure one of: OAuth (client-id + client-secret), " +
				"username/password, or personal access token (workspace-tokens + workspaces)",
		)
		return &databricks.NoAuth{}
	}
}

func getHostname(cfg *config.Databricks) string {
	const defaultHost = "cloud.databricks.com"
	if cfg.Hostname == "" {
		return defaultHost
	}
	return cfg.Hostname
}
