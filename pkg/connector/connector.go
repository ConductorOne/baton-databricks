package connector

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/config"
	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

type Databricks struct {
	client *databricks.Client
	// clientID is the applicationId of the service principal the connector
	// authenticates as. Grant visibility is decided against it.
	clientID   string
	workspaces []string
	opts       *cli.ConnectorOpts
}

// ResourceSyncers returns a ResourceSyncerV2 for each resource type that should be synced from the upstream service.
func (d *Databricks) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	workspaces := configuredWorkspaceSet(d.workspaces)
	principals := syncPrincipals{
		groups:            d.willSyncResourceType(groupResourceType.Id),
		servicePrincipals: d.willSyncResourceType(servicePrincipalResourceType.Id),
	}

	return []connectorbuilder.ResourceSyncerV2{
		newAccountBuilder(d.client),
		newGroupBuilder(d.client),
		newServicePrincipalBuilder(d.client),
		newUserBuilder(d.client),
		newWorkspaceBuilder(d.client, d.workspaces),
		newRoleBuilder(d.client),
		newMetastoreBuilder(d.client, d.clientID, workspaces, principals),
		newCatalogBuilder(d.client, d.clientID, workspaces, principals),
		newSchemaBuilder(d.client, d.clientID, workspaces, principals),
		newTableBuilder(d.client, d.clientID, workspaces, principals),
		newVolumeBuilder(d.client, d.clientID, workspaces, principals),
	}
}

// willSyncResourceType is true when the type is in this sync. Nil opts (the
// capabilities prototype) means no filter, so every advertised type is in scope.
func (d *Databricks) willSyncResourceType(resourceTypeID string) bool {
	if d.opts == nil {
		return true
	}
	return d.opts.WillSyncResourceType(resourceTypeID)
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
		Description: "Connector syncing Databricks workspaces, users, groups, service principals and roles to Baton, " +
			"along with Unity Catalog data assets — metastores, catalogs, schemas, tables and volumes — and the privileges granted on them",
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

// Validate is called to ensure that the connector is properly configured. It exercises the
// OAuth credentials against the Account API.
func (d *Databricks) Validate(ctx context.Context) (annotations.Annotations, error) {
	// A failed account API check is a fixable misconfiguration, so fail instead of
	// silently dropping account-level data.
	if _, _, err := d.client.ListRoles(ctx, "", "", ""); err != nil {
		return nil, fmt.Errorf("databricks-connector: account API validation failed: %w", err)
	}

	// Workspace enumeration is the other account-plane call every sync depends on.
	// Per-workspace probing is deliberately not done here: a workspace the service
	// principal can't reach is skipped during sync rather than failing validation.
	workspaces, _, err := d.client.ListWorkspaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}

	if len(d.workspaces) > 0 && !slices.ContainsFunc(workspaces, func(w databricks.Workspace) bool {
		_, ok := matchConfiguredWorkspace(configuredWorkspaceSet(d.workspaces), w.DeploymentName, w.Name, strconv.Itoa(w.ID))
		return ok
	}) {
		return nil, fmt.Errorf("databricks-connector: none of the configured workspaces (%s) exists on the account", strings.Join(d.workspaces, ", "))
	}

	return nil, nil
}

// New returns a new instance of the connector. clientID is the application id of
// the service principal the connector authenticates as.
func New(
	ctx context.Context,
	hostname,
	accountHostname,
	accountID,
	baseURL,
	clientID string,
	auth databricks.Auth,
	excludeWorkspaces []string,
	workspaces []string,
	opts *cli.ConnectorOpts,
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
		clientID:   clientID,
		workspaces: workspaces,
		opts:       opts,
	}, nil
}

// NewConnector returns a new connector builder from a configuration struct.
func NewConnector(ctx context.Context, cfg *config.Databricks, opts *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	accountHostname := getAccountHostname(cfg, cfg.Hostname)
	auth := prepareClientAuth(cfg)

	cb, err := New(
		ctx,
		cfg.Hostname,
		accountHostname,
		cfg.AccountId,
		cfg.BaseUrl,
		cfg.DatabricksClientId,
		auth,
		cfg.DatabricksExcludeWorkspaces,
		cfg.Workspaces,
		opts,
	)
	if err != nil {
		return nil, nil, err
	}

	return cb, nil, nil
}

func prepareClientAuth(cfg *config.Databricks) databricks.Auth {
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
