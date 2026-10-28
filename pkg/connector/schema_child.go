package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// profileKeySchemaOwner carries the owner of the parent schema. Owning the
// schema is one of the things that makes a table's or a volume's privilege read
// complete, and Grants is handed only the table, so List writes the owner down
// instead of listing the schemas again on every call.
const profileKeySchemaOwner = "schema_owner"

// schemaChildRoute is the workspace a schema's tables or volumes are listed
// through, carried in the page token so later pages neither re-resolve the route
// nor prove again that the parent listing is complete. The owners ride along so
// each resource's profile can carry them. See profileKeySchemaOwner.
type schemaChildRoute struct {
	Workspace      string `json:"w"`
	WorkspaceID    string `json:"i"`
	WorkspaceName  string `json:"n"`
	MetastoreOwner string `json:"o"`
	CatalogOwner   string `json:"c"`
	SchemaOwner    string `json:"s"`
}

// listSchemaChildren pages one level that hangs from a schema — a table or a
// volume. pushSchemaChildRoute re-runs the schema syncer's completeness proof
// on the first page of every schema, once for tables and again for volumes.
// The privilege reads in that proof bypass the HTTP cache. The owners the
// proof learns are written onto each resource, so Grants does not repeat it.
//
// fetch reads one page through the routed workspace and turns it into resources.
func listSchemaChildren(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	workspaces map[string]struct{},
	resourceType *v2.ResourceType,
	noun string,
	parentResourceID *v2.ResourceId,
	attr rs.SyncOpAttrs,
	fetch func(ctx context.Context, route schemaChildRoute, metastoreID, catalogName, schemaName, pageToken string) (
		[]*v2.Resource, string, *v2.RateLimitDescription, error),
) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != schemaResourceType.Id {
		return nil, nil, nil
	}

	metastoreID, catalogName, schemaName, err := parseSchemaID(parentResourceID.GetResource())
	if err != nil {
		return nil, nil, err
	}
	// The schema syncer leaves Databricks' own read-only schema out, so nothing
	// under it is ever synced either.
	if schemaName == informationSchema {
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(attr.PageToken.Token); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid %s page token: %v", noun, err)
	}

	if bag.Current() == nil {
		if err := pushSchemaChildRoute(ctx, client, clientID, workspaces, resourceType, noun,
			metastoreID, catalogName, schemaName, bag, &annos); err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		if bag.Current() == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: no workspace in sync scope lists the schema; listing none of its children",
				zap.String("resource_type", resourceType.Id),
				zap.String("schema", parentResourceID.GetResource()))

			return nil, &rs.SyncOpResults{Annotations: annos}, nil
		}
	}

	var route schemaChildRoute
	if err := json.Unmarshal([]byte(bag.ResourceID()), &route); err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: invalid %s page token: %v", noun, err)
	}

	rv, next, rateLimit, err := fetch(ctx, route, metastoreID, catalogName, schemaName, bag.PageToken())
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos},
			schemaChildListingError(noun, catalogName+schemaNameSeparator+schemaName, route.Workspace, err)
	}

	if next != "" && next == bag.PageToken() {
		return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.Internal,
			"databricks-connector: listing %ss of schema %s repeated page token %q", noun, catalogName+schemaNameSeparator+schemaName, next)
	}

	nextToken, err := bag.NextToken(next)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return rv, &rs.SyncOpResults{Annotations: annos, NextPageToken: nextToken}, nil
}

// childListRemedy is what a refusal to sync a schema whose table or volume
// listing is partial tells the operator to fix. USE_SCHEMA and MANAGE list
// schemas; they do not list every table or volume.
const childListRemedy = "the connector's service principal must own the metastore, the catalog or the schema, " +
	"or hold ALL_PRIVILEGES on the catalog"

// pushSchemaChildRoute records a workspace that can list every table or volume
// of the schema. It walks the running in-scope workspaces until one lists the
// catalog and the child listing is complete. An error from a workspace stops
// the walk. An empty result is an error: it would make ConductorOne delete
// the tables and volumes.
func pushSchemaChildRoute(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	workspaces map[string]struct{},
	resourceType *v2.ResourceType,
	noun string,
	metastoreID, catalogName, schemaName string,
	bag *pagination.Bag,
	annos *annotations.Annotations,
) error {
	scope, err := scopedWorkspaces(ctx, client, workspaces, annos)
	if err != nil {
		return err
	}

	usable, inScope, err := scope.attached(ctx, client, metastoreID, annos)
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

	owner, err := metastoreOwner(ctx, client, metastoreID, annos)
	if err != nil {
		return err
	}

	fullName := catalogName + schemaNameSeparator + schemaName
	var sawCatalog bool
	for i := range usable {
		workspace := usable[i]
		catalog, err := catalogThrough(ctx, client, workspace.DeploymentName, metastoreID, catalogName, annos)
		if err != nil {
			return err
		}
		if catalog == nil {
			continue
		}
		sawCatalog = true

		route := schemaChildRoute{
			Workspace:      workspace.DeploymentName,
			WorkspaceID:    strconv.Itoa(workspace.ID),
			WorkspaceName:  workspace.Name,
			MetastoreOwner: owner,
			CatalogOwner:   catalog.Owner,
		}
		trust := securableTrust{
			ownPrincipal:   clientID,
			metastoreOwner: owner,
			parentOwner:    catalog.Owner,
			fullName:       catalogName,
			workspace:      route.Workspace,
			workspaceID:    route.WorkspaceID,
		}
		schemasComplete, err := schemaListingIsComplete(ctx, client, trust, annos)
		if err != nil {
			return err
		}

		schema, err := schemaThrough(ctx, client, route.Workspace, catalogName, schemaName, annos)
		if err != nil {
			return err
		}
		if schemasComplete && schema == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: the schema is no longer listed; listing none of its children",
				zap.String("resource_type", resourceType.Id),
				zap.String("schema", fullName),
				zap.String("workspace", route.Workspace))

			return nil
		}
		// A partial schema list still contains a schema the caller owns. That
		// ownership is enough to list every table and volume in it. Anything
		// else hidden by the partial list cannot be told from a schema that is gone.
		if schema == nil || (!schemasComplete && !ownsPrincipal(clientID, schema.Owner)) {
			continue
		}

		trust.schemaOwner = schema.Owner
		childrenComplete, err := childListingIsComplete(ctx, client, trust, annos)
		if err != nil {
			return err
		}
		if !childrenComplete {
			continue
		}
		route.SchemaOwner = schema.Owner

		encoded, err := json.Marshal(route)
		if err != nil {
			return err
		}
		bag.Push(pagination.PageState{ResourceTypeID: resourceType.Id, ResourceID: string(encoded)})

		return nil
	}
	if !sawCatalog {
		return status.Errorf(codes.FailedPrecondition,
			"databricks-connector: no workspace in sync scope lists catalog %s of metastore %s", catalogName, metastoreID)
	}

	return status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot list every %s in schema %s; %s", noun, fullName, childListRemedy)
}

// childListingIsComplete is true when ListTables or ListVolumes returns every
// child of the schema. USE_SCHEMA and MANAGE make the schema list complete.
// They do not make the table or volume list complete.
func childListingIsComplete(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.parentOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.schemaOwner) {
		return true, nil
	}

	if workspaceAdminsGroupOwns(trust.parentOwner, trust.workspaceID) {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, trust.workspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	return holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, trust.fullName, []string{privilegeAllPrivileges}, annos)
}

// schemaChildListingError reads a workspace that refuses the listing as unknown,
// not as listing nothing: a short list would read as deletions in C1, and the SDK
// reads a NotFound as a securable with no children.
func schemaChildListingError(noun, schemaFullName, workspace string, err error) error {
	switch apiStatus(err) {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: the %ss of schema %s cannot be listed through workspace %s, "+
				"so which of them are in scope is unknown; %s", noun, schemaFullName, workspace, childListRemedy),
			err)
	}

	return fmt.Errorf("databricks-connector: failed to list the %ss of schema %s through workspace %s: %w",
		noun, schemaFullName, workspace, err)
}

// schemaChildResourceID keys a table or a volume by its metastore and its dotted
// full name, which extends the schema's "{metastore_id}::{catalog}.{schema}".
func schemaChildResourceID(metastoreID, catalogName, schemaName, name string) string {
	return metastoreID + securableKeySeparator +
		strings.Join([]string{catalogName, schemaName, name}, schemaNameSeparator)
}

// parseSchemaChildID reads a table or volume resource id,
// "{metastore_id}::{catalog}.{schema}.{name}".
func parseSchemaChildID(resourceID, noun string) (string, string, string, string, error) {
	invalid := status.Errorf(codes.InvalidArgument, "databricks-connector: invalid %s resource id %q", noun, resourceID)

	metastoreID, fullName, ok := strings.Cut(resourceID, securableKeySeparator)
	if !ok {
		return "", "", "", "", invalid
	}

	parts := strings.Split(fullName, schemaNameSeparator)
	if len(parts) != 3 || metastoreID == "" || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", "", invalid
	}

	return metastoreID, parts[0], parts[1], parts[2], nil
}

// schemaChildProfile is what every table and volume carries, before the level
// adds its own name and type.
func schemaChildProfile(metastoreID, catalogName, schemaName, fullName, owner, securableType string, route schemaChildRoute) map[string]any {
	return map[string]any{
		profileKeyMetastoreID:    metastoreID,
		profileKeyMetastoreOwner: route.MetastoreOwner,
		profileKeyFullName:       fullName,
		profileKeyCatalogName:    catalogName,
		profileKeyCatalogOwner:   route.CatalogOwner,
		profileKeySchemaName:     schemaName,
		profileKeySchemaOwner:    route.SchemaOwner,
		profileKeyOwner:          owner,
		profileKeySecurableType:  securableType,
		profileKeyWorkspace:      route.Workspace,
		profileKeyWorkspaceID:    route.WorkspaceID,
		profileKeyWorkspaceName:  route.WorkspaceName,
	}
}

// schemaChildGrantsAreTrustworthy is true for an owner of the table or volume, its schema, its catalog, or the metastore, or for a holder of MANAGE on any of those.
func schemaChildGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	securableType string,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	// On the live tenant the connector owns the metastore, so this settles the
	// whole rule with no extra call at all.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.parentOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.schemaOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.owner) {
		return true, nil
	}

	// The workspace admins group acts as the owner for a direct Admin of that workspace.
	if workspaceAdminsGroupOwns(trust.parentOwner, trust.workspaceID) {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, trust.workspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	// MANAGE on the securable itself is the cheapest of the three reads, and
	// MANAGE above it carries down, so they are tried from the inside out.
	manages, err := holdsPrivilegeOn(ctx, client, trust, securableType, trust.fullName, managePrivilege, annos)
	if err != nil || manages {
		return manages, err
	}

	catalogName, rest, _ := strings.Cut(trust.fullName, schemaNameSeparator)
	schemaName, _, _ := strings.Cut(rest, schemaNameSeparator)
	manages, err = holdsPrivilegeOn(ctx, client, trust, databricks.SecurableSchema,
		catalogName+schemaNameSeparator+schemaName, managePrivilege, annos)
	if err != nil || manages {
		return manages, err
	}

	return holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, catalogName, managePrivilege, annos)
}
