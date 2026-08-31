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
	client     *databricks.Client
	workspaces []string
}

// ResourceSyncers returns a ResourceSyncerV2 for each resource type that should be synced from the upstream service.
func (d *Databricks) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	syncers := []connectorbuilder.ResourceSyncerV2{
		newAccountBuilder(d.client),
		newGroupBuilder(d.client),
		newServicePrincipalBuilder(d.client),
		newUserBuilder(d.client),
		newWorkspaceBuilder(d.client, d.workspaces),
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

	// The Account API is unreachable with workspace tokens, so only probe it for OAuth.
	if !d.client.IsTokenAuth() {
		_, _, err := d.client.ListRoles(ctx, "", "", "")
		if err == nil {
			isAccAPIAvailable = true
		}
	}

	// With an explicit workspace list (always the case for token auth), validate each
	// configured workspace. Otherwise discover every workspace from the Account API.
	workspaceNames := d.workspaces
	if len(workspaceNames) == 0 {
		workspaces, _, err := d.client.ListWorkspaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
		}

		workspaceNames = make([]string, 0, len(workspaces))
		for _, workspace := range workspaces {
			workspaceNames = append(workspaceNames, workspace.DeploymentName)
		}
	}

	for _, workspace := range workspaceNames {
		_, _, err := d.client.ListRoles(ctx, workspace, "", "")
		if err != nil && !isAccAPIAvailable {
			return nil, fmt.Errorf("databricks-connector: failed to validate credentials for workspace %s: %w", workspace, err)
		}

		isWSAPIAvailable = true
	}

	// Resolve the result.
	if !isAccAPIAvailable && !isWSAPIAvailable {
		return nil, fmt.Errorf("databricks-connector: failed to validate credentials")
	}

	d.client.UpdateAvailability(isAccAPIAvailable, isWSAPIAvailable)

	// Account plane down (always under token auth, possible under OAuth) silently drops
	// account entitlements/grants and re-parents identities onto workspaces; log it for operators.
	if !isAccAPIAvailable && isWSAPIAvailable {
		ctxzap.Extract(ctx).Debug(
			"databricks-connector: account API unreachable; syncing workspace-scoped data only. " +
				"Account entitlements and grants, and workspace-membership entitlements, will not be synced, " +
				"and identities are parented under their workspace instead of the account",
		)
	}

	return nil, nil
}

// New returns a new instance of the connector.
func New(
	ctx context.Context,
	hostname,
	accountHostname,
	accountID,
	baseURL string,
	auth databricks.Auth,
	excludeWorkspaces []string,
	workspaces []string,
) (*Databricks, error) {
	httpClient, err := auth.GetClient(ctx)
	if err != nil {
		return nil, err
	}

	client, err := databricks.NewClient(ctx, httpClient, hostname, accountHostname, accountID, baseURL, auth, excludeWorkspaces)
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

	authMethod := ""
	if opts != nil {
		authMethod = opts.SelectedAuthMethod
	}

	if err := config.ValidateConfig(ctx, cfg, authMethod); err != nil {
		return nil, nil, err
	}

	accountHostname := getAccountHostname(cfg, cfg.Hostname)
	auth := prepareClientAuth(ctx, cfg, authMethod, l)

	cb, err := New(
		ctx,
		cfg.Hostname,
		accountHostname,
		cfg.AccountId,
		cfg.BaseUrl,
		auth,
		cfg.DatabricksExcludeWorkspaces,
		cfg.Workspaces,
	)
	if err != nil {
		return nil, nil, err
	}

	return cb, nil, nil
}

func prepareClientAuth(_ context.Context, cfg *config.Databricks, authMethod string, l *zap.Logger) databricks.Auth {
	if authMethod == config.DatabricksWorkspaceTokenGroup {
		l.Debug("using workspace token auth", zap.String("account-id", cfg.AccountId))
		return databricks.NewTokenAuth(cfg.Workspaces, cfg.WorkspaceTokens)
	}

	l.Debug("using oauth", zap.String("account-id", cfg.AccountId))
	return databricks.NewOAuth2(
		cfg.AccountId,
		cfg.DatabricksClientId,
		cfg.DatabricksClientSecret,
		getAccountHostname(cfg, cfg.Hostname),
	)
}

// getAccountHostname returns the account hostname from config if set, otherwise calculates it from hostname.
func getAccountHostname(cfg *config.Databricks, hostname string) string {
	if cfg.AccountHostname != "" {
		return cfg.AccountHostname
	}
	return databricks.GetAccountHostname(hostname)
}
