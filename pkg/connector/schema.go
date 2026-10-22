package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*schemaBuilder)(nil)

const (
	securableTypeSchema = "SCHEMA"

	// informationSchema is the read-only schema Databricks puts in every catalog.
	// Its privileges are not grantable, so it is left out of the sync.
	informationSchema = "information_schema"

	// schemaNameSeparator joins the catalog and the schema into the dotted full
	// name the permissions endpoint addresses a schema by. Databricks rejects a
	// name holding a period, so splitting on it is unambiguous.
	schemaNameSeparator = "."

	profileKeySchemaName = "schema_name"

	// profileKeyCatalogOwner carries the owner of the parent catalog. Owning the
	// catalog is one of the things that makes a schema's privilege read complete,
	// and Grants is handed only the schema, so List writes the owner down instead
	// of listing the catalogs again on every call.
	profileKeyCatalogOwner = "catalog_owner"

	// schemaWriteRemedy is what a 403 on the PATCH tells the operator.
	schemaWriteRemedy = "the connector must own the metastore, the parent catalog or the schema, or hold MANAGE on the schema or the parent catalog"

	// schemaTrustRemedy is what a refusal to report a partial privilege list
	// tells the operator.
	schemaTrustRemedy = "the connector's service principal must own the metastore, the parent catalog or the schema, hold MANAGE on the schema or " +
		"on the parent catalog, or be assigned Admin directly on the workspace whose admins group owns the parent catalog"

	// schemaListRemedy is what a refusal to sync a catalog whose schema listing
	// would be partial tells the operator.
	schemaListRemedy = "the connector's service principal must own the metastore or the catalog, or hold MANAGE, USE_SCHEMA or ALL_PRIVILEGES on the catalog"
)

// schemaPrivileges is the catalog set minus the three that stop at the catalog.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
var schemaPrivileges = slices.DeleteFunc(slices.Clone(catalogPrivileges), func(privilege string) bool {
	return slices.Contains([]string{"BROWSE", "CREATE_SCHEMA", "USE_CATALOG"}, privilege)
})

// schemaListingPrivileges each make ListSchemas return every schema in the
// catalog. Unlike a privilege read, a complete listing does not need MANAGE:
// USE_SCHEMA and ALL_PRIVILEGES carry it too, which was measured on a tenant
// where neither made the privilege list complete.
var schemaListingPrivileges = []string{privilegeManage, "USE_SCHEMA", "ALL_PRIVILEGES"}

var schemaLevel = securableLevel{
	securableType:   databricks.SecurableSchema,
	noun:            "schema",
	privileges:      schemaPrivileges,
	writeRemedy:     schemaWriteRemedy,
	trustRemedy:     schemaTrustRemedy,
	permissionsName: schemaPermissionsName,
	trusts:          schemaGrantsAreTrustworthy,
}

type schemaBuilder struct {
	client *databricks.Client
	// clientID is the applicationId of the service principal the connector
	// authenticates as, which is what grant visibility is decided against.
	clientID string
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
	sync       syncPrincipals
}

func newSchemaBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *schemaBuilder {
	return &schemaBuilder{client: client, clientID: clientID, workspaces: workspaces, sync: sync}
}

func (b *schemaBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return schemaResourceType
}

// schemaResourceID keys a schema by its metastore and its dotted full name, which
// extends the catalog's "{metastore_id}::{catalog_name}".
func schemaResourceID(metastoreID, catalogName, name string) string {
	return metastoreID + securableKeySeparator + catalogName + schemaNameSeparator + name
}

// parseSchemaID reads a schema resource id, "{metastore_id}::{catalog}.{schema}".
func parseSchemaID(resourceID string) (string, string, string, error) {
	metastoreID, fullName, ok := strings.Cut(resourceID, securableKeySeparator)
	if !ok {
		return "", "", "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema resource id %q", resourceID)
	}

	catalogName, name, ok := strings.Cut(fullName, schemaNameSeparator)
	if !ok || metastoreID == "" || catalogName == "" || name == "" {
		return "", "", "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema resource id %q", resourceID)
	}

	return metastoreID, catalogName, name, nil
}

// schemaPermissionsName: the permissions endpoint addresses a schema by its
// dotted full name. The metastore is carried by the host.
func schemaPermissionsName(resourceID string) (string, error) {
	_, catalogName, name, err := parseSchemaID(resourceID)
	if err != nil {
		return "", err
	}

	return catalogName + schemaNameSeparator + name, nil
}

func schemaResource(
	schema *databricks.Schema,
	metastoreID, catalogName string,
	parent *v2.ResourceId,
	route schemaRoute,
) (*v2.Resource, error) {
	fullName := catalogName + schemaNameSeparator + schema.Name
	profile := map[string]any{
		profileKeyMetastoreID:    metastoreID,
		profileKeyMetastoreOwner: route.MetastoreOwner,
		profileKeyFullName:       fullName,
		profileKeyCatalogName:    catalogName,
		profileKeyCatalogOwner:   route.CatalogOwner,
		profileKeySchemaName:     schema.Name,
		profileKeyOwner:          schema.Owner,
		profileKeySecurableType:  securableTypeSchema,
		profileKeyWorkspace:      route.Workspace,
		profileKeyWorkspaceID:    route.WorkspaceID,
		profileKeyWorkspaceName:  route.WorkspaceName,
	}

	resourceID := schemaResourceID(metastoreID, catalogName, schema.Name)

	// The display name is the full name rather than the leaf: main.sales and
	// finance.sales would otherwise be two rows both reading "sales" in C1.
	return rs.NewAppResource(
		fullName,
		schemaResourceType,
		resourceID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: resourceID}),
	)
}

// schemaRoute is the workspace a catalog's schemas are listed through, carried in
// the page token so later pages neither re-resolve the route nor prove again that
// the listing is complete.
type schemaRoute struct {
	Workspace     string `json:"w"`
	WorkspaceID   string `json:"i"`
	WorkspaceName string `json:"n"`
	// MetastoreOwner and CatalogOwner ride along so each schema's profile can
	// carry them. See profileKeyCatalogOwner.
	MetastoreOwner string `json:"o"`
	CatalogOwner   string `json:"c"`
}

// List pages a catalog's schemas through the one workspace the route picked.
// Databricks answers a caller without catalog-wide visibility with only the
// schemas it owns or can use, at 200, so that listing is refused rather than
// synced.
func (b *schemaBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != catalogResourceType.Id {
		return nil, nil, nil
	}

	metastoreID, catalogName, err := parseCatalogID(parentResourceID.GetResource())
	if err != nil {
		return nil, nil, err
	}

	annos := annotations.Annotations{}
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(attr.PageToken.Token); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema page token: %v", err)
	}

	if bag.Current() == nil {
		if err := b.pushRoute(ctx, metastoreID, catalogName, bag, &annos); err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if bag.Current() == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: no workspace in sync scope lists the catalog; listing no schemas",
				zap.String("metastore", metastoreID),
				zap.String("catalog", catalogName))

			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}
	}

	var route schemaRoute
	if err := json.Unmarshal([]byte(bag.ResourceID()), &route); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid schema page token: %v", err)
	}

	schemas, next, rateLimit, err := b.client.ListSchemas(ctx, route.Workspace, catalogName, bag.PageToken(), ResourcesPageSize)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, schemaListingError(catalogName, route.Workspace, err)
	}

	var rv []*v2.Resource
	for _, schema := range schemas {
		if schema.Name == "" || schema.Name == informationSchema {
			continue
		}

		built, err := schemaResource(&schema, metastoreID, catalogName, parentResourceID, route)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to build the resource for schema %s.%s: %w", catalogName, schema.Name, err)
		}
		rv = append(rv, built)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
}

// pushRoute records the one workspace the schemas are read through: the first
// running workspace attached to the metastore. An empty stack means no workspace
// this sync can use serves it. A catalog listed there whose schemas cannot all be
// read fails instead: a short listing would make ConductorOne delete the schemas
// it hides, and their whole subtree.
func (b *schemaBuilder) pushRoute(
	ctx context.Context,
	metastoreID, catalogName string,
	bag *pagination.Bag,
	annos *annotations.Annotations,
) error {
	scope, err := scopedWorkspaces(ctx, b.client, b.workspaces, annos)
	if err != nil {
		return err
	}

	// An assignment list that cannot be read, including a 404, leaves which
	// workspaces serve the metastore unknown. Listing no schemas here would make
	// ConductorOne delete every schema of the catalog, so the sync fails and keeps
	// what it already holds.
	usable, inScope, err := scope.attached(ctx, b.client, metastoreID, annos)
	if err != nil {
		return fmt.Errorf(
			"databricks-connector: failed to list the workspaces assigned to metastore %s, so the sync cannot tell which serve it: %w",
			metastoreID, err)
	}
	if len(usable) == 0 {
		if inScope {
			return status.Errorf(codes.FailedPrecondition,
				"databricks-connector: metastore %s has no running workspace in sync scope", metastoreID)
		}

		return nil
	}

	owner, err := metastoreOwner(ctx, b.client, metastoreID, annos)
	if err != nil {
		return err
	}

	workspace := usable[0]
	// The catalog listing answers who owns the catalog, which is what the
	// completeness rule is decided against. A catalog this workspace does not
	// list has no schemas on this path.
	catalog, err := b.catalogThrough(ctx, workspace.DeploymentName, metastoreID, catalogName, annos)
	if err != nil {
		return err
	}
	if catalog == nil {
		ctxzap.Extract(ctx).Debug("databricks-connector: the workspace does not list the catalog, so it lists no schemas",
			zap.String("catalog", catalogName),
			zap.String("workspace", workspace.DeploymentName))

		return nil
	}

	route := schemaRoute{
		Workspace:      workspace.DeploymentName,
		WorkspaceID:    strconv.Itoa(workspace.ID),
		WorkspaceName:  workspace.Name,
		MetastoreOwner: owner,
		CatalogOwner:   catalog.Owner,
	}

	complete, err := schemaListingIsComplete(ctx, b.client, securableTrust{
		ownPrincipal:   b.clientID,
		metastoreOwner: owner,
		parentOwner:    catalog.Owner,
		fullName:       catalogName,
		workspace:      route.Workspace,
		workspaceID:    route.WorkspaceID,
	}, annos)
	if err != nil {
		return err
	}
	if !complete {
		return status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot list every schema in catalog %s, and Databricks answers a partial listing with 200, "+
				"so syncing it would delete the schemas ConductorOne cannot see along with their grants; %s",
			catalogName, schemaListRemedy)
	}

	encoded, err := json.Marshal(route)
	if err != nil {
		return err
	}
	bag.Push(pagination.PageState{ResourceTypeID: schemaResourceType.Id, ResourceID: string(encoded)})

	return nil
}

// catalogThrough finds one catalog in the listing of workspace, nil when that
// workspace does not list it.
func (b *schemaBuilder) catalogThrough(
	ctx context.Context,
	workspace, metastoreID, catalogName string,
	annos *annotations.Annotations,
) (*databricks.Catalog, error) {
	pageToken := ""
	for {
		catalogs, next, rateLimit, err := b.client.ListCatalogs(ctx, workspace, pageToken, ResourcesPageSize)
		noteRateLimit(annos, rateLimit)
		if err != nil {
			return nil, catalogListingError(metastoreID, workspace, err)
		}

		for _, catalog := range catalogs {
			if catalog.Name == catalogName && (catalog.MetastoreID == "" || catalog.MetastoreID == metastoreID) {
				return &catalog, nil
			}
		}

		if next == "" {
			return nil, nil
		}
		// Databricks ignores a page token it cannot deserialize and restarts at
		// page one, which would walk the same page forever.
		if next == pageToken {
			return nil, status.Errorf(codes.Internal,
				"databricks-connector: the catalogs of metastore %s repeat page token %q through workspace %s",
				metastoreID, next, workspace)
		}
		pageToken = next
	}
}

// schemaListingError reads a workspace that refuses the schema listing as
// unknown, not as listing nothing: a short list would read as deletions in C1,
// and the SDK reads a NotFound as a securable with no children.
func schemaListingError(catalogName, workspace string, err error) error {
	switch apiStatus(err) {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: the schemas of catalog %s cannot be listed through workspace %s, "+
				"so which schemas are in scope is unknown; %s", catalogName, workspace, schemaListRemedy),
			err)
	}

	return fmt.Errorf("databricks-connector: failed to list the schemas of catalog %s through workspace %s: %w",
		catalogName, workspace, err)
}

// schemaListingIsComplete reports whether ListSchemas returns every schema in the
// catalog. Databricks answers a caller without catalog-wide visibility with the
// schemas it owns or holds USE_SCHEMA on, at 200, which is indistinguishable from
// a catalog with that many schemas. Measured on a workspace Admin that does not
// own the metastore: the system catalog came back with 2 schemas instead of 15,
// and a catalog grant of MANAGE alone did list a schema owned by someone else.
// Workspace Admin is therefore not an authority here; owning the catalog is.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
func schemaListingIsComplete(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner sees everything below it, and the metastore's owner is its admin.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) || ownsPrincipal(trust.ownPrincipal, trust.parentOwner) {
		return true, nil
	}

	// The workspace admins group acts as the owner for a direct Admin of that workspace.
	if workspaceAdminsGroupOwns(trust.parentOwner, trust.workspaceID) {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, trust.workspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	return holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, trust.fullName, schemaListingPrivileges, annos)
}

// schemaGrantsAreTrustworthy reports whether the connector reads every privilege
// assignment on a schema. Databricks shows the full list to the metastore admin,
// to the owner of the schema or of the catalog above it, and to a holder of
// MANAGE on either; a direct workspace Admin also sees it when the catalog is
// owned by that workspace's admins group. Anyone else reads only their own rows,
// with a 200. See ensureGrantsAreTrustworthy.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/admin-privileges
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/
func schemaGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	// On the live tenant the connector owns the metastore, so this settles the
	// whole rule with no extra call at all.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.parentOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.owner) {
		return true, nil
	}

	if workspaceAdminsGroupOwns(trust.parentOwner, trust.workspaceID) {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, trust.workspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	// MANAGE on the schema is the cheaper of the two reads, so it goes first.
	manages, err := holdsPrivilegeOn(ctx, client, trust, databricks.SecurableSchema, trust.fullName, managePrivilege, annos)
	if err != nil || manages {
		return manages, err
	}

	// MANAGE on the catalog carries down to every schema in it.
	catalogName, _, _ := strings.Cut(trust.fullName, schemaNameSeparator)

	return holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, catalogName, managePrivilege, annos)
}

func (b *schemaBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(schemaPrivileges)+1)
	for _, privilege := range schemaPrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a schema in Databricks", privilege)),
		))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a schema in Databricks"),
	))

	return rv, nil, nil
}

func (b *schemaBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (b *schemaBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return syncSecurableGrants(ctx, b.client, b.clientID, b.sync, schemaLevel, resource, attr)
}

func (b *schemaBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return grantSecurable(ctx, b.client, b.clientID, schemaLevel, principal, entitlement)
}

func (b *schemaBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return revokeSecurable(ctx, b.client, b.clientID, schemaLevel, revoked)
}
