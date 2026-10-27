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

	// errorCodeSchemaGone is what Databricks names in error_code when the schema
	// a request addresses does not exist.
	errorCodeSchemaGone = "SCHEMA_DOES_NOT_EXIST"

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
	// Dropping the catalog drops its schemas, so a revoke on a schema can meet
	// either code and both mean the schema is gone.
	goneErrorCodes: []string{errorCodeSchemaGone, errorCodeCatalogGone},
}

type schemaBuilder struct {
	client   *databricks.Client
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
		rs.WithAnnotation(
			// Without these annotations the table and volume syncers' List is
			// never invoked.
			&v2.ChildResourceType{ResourceTypeId: tableResourceType.Id},
			&v2.ChildResourceType{ResourceTypeId: volumeResourceType.Id},
			&v2.RawId{Id: resourceID},
		),
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

// List pages a catalog's schemas through a workspace that can see every schema.
// Databricks answers a caller without catalog-wide visibility with only the
// schemas it owns or can use, at 200. For the built-in system catalog that
// short list is left out. For every other catalog the sync stops, so
// ConductorOne keeps the schemas it already has.
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

	if next != "" && next == bag.PageToken() {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.Internal,
			"databricks-connector: listing schemas of catalog %s repeated page token %q", catalogName, next)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
}

// pushRoute records a workspace that can list every schema of the catalog. It
// walks the running in-scope workspaces until one both lists the catalog and
// can read every schema. An empty stack means no workspace this sync can use
// serves the metastore. A catalog none of them list fails the sync: returning
// no schemas would make ConductorOne delete them. The built-in system catalog
// is the exception, because this connector has never listed it completely.
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

	usable, inScope, err := scope.attached(ctx, b.client, metastoreID, annos)
	if err != nil {
		return assignmentListError(metastoreID, err)
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

	var sawCatalog bool
	for i := range usable {
		workspace := usable[i]
		catalog, err := catalogThrough(ctx, b.client, workspace.DeploymentName, metastoreID, catalogName, annos)
		if err != nil {
			return err
		}
		if catalog == nil {
			continue
		}
		sawCatalog = true

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
			continue
		}

		encoded, err := json.Marshal(route)
		if err != nil {
			return err
		}
		bag.Push(pagination.PageState{ResourceTypeID: schemaResourceType.Id, ResourceID: string(encoded)})

		return nil
	}
	if !sawCatalog {
		return status.Errorf(codes.FailedPrecondition,
			"databricks-connector: no workspace in sync scope lists catalog %s of metastore %s", catalogName, metastoreID)
	}
	// system is built in. A workspace admin sees 2 of its schemas out of 15, and
	// this connector has never synced the rest, so leaving it out deletes nothing.
	// Any other catalog was listed in full before: a short list would delete the
	// schemas ConductorOne can no longer see, so the sync stops and keeps them.
	if catalogName == "system" {
		ctxzap.Extract(ctx).Debug("databricks-connector: skipping schemas of the system catalog, which the connector cannot fully list")

		return nil
	}

	return status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot list every schema in catalog %s; %s",
		catalogName, schemaListRemedy)
}

// schemaThrough finds one schema in the listing of workspace, nil when that
// workspace does not list it. Its caller has already proven the listing holds
// every schema in the catalog, so an absence here is the schema being gone.
func schemaThrough(
	ctx context.Context,
	client *databricks.Client,
	workspace, catalogName, schemaName string,
	annos *annotations.Annotations,
) (*databricks.Schema, error) {
	pageToken := ""
	for {
		schemas, next, rateLimit, err := client.ListSchemas(ctx, workspace, catalogName, pageToken, ResourcesPageSize)
		noteRateLimit(annos, rateLimit)
		if err != nil {
			return nil, schemaListingError(catalogName, workspace, err)
		}

		for _, schema := range schemas {
			if schema.Name == schemaName {
				return &schema, nil
			}
		}

		if next == "" {
			return nil, nil
		}
		if next == pageToken {
			return nil, status.Errorf(codes.Internal,
				"databricks-connector: the schemas of catalog %s repeat page token %q through workspace %s",
				catalogName, next, workspace)
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

// schemaListingIsComplete is true when ListSchemas returns every schema. USE_SCHEMA and ALL_PRIVILEGES count here. They do not make a privilege read complete.
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

// schemaGrantsAreTrustworthy is true for an owner of the schema, catalog, or
// metastore, a holder of MANAGE on the schema or catalog, or a direct Admin
// when the workspace admins group owns the catalog.
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
