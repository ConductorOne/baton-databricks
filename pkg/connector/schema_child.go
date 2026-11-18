package connector

import (
	"context"
	"encoding/json"
	"errors"
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
	"google.golang.org/protobuf/proto"
)

// profileKeySchemaOwner carries the parent schema's owner, which is one of the
// things that makes a table's or a volume's privilege read complete. Grants is
// handed only the table, so List writes it down.
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
// volume — through a workspace proved to list every one of them, calling fetch
// for each page. The owners that proof learns are written onto each resource,
// so Grants does not run it again.
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
	ctx = context.WithValue(ctx, syncSessionKey{}, attr.Session)
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
			"databricks-connector: listing the %ss of schema %s.%s repeated page token %q", noun, catalogName, schemaName, next)
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
// of the schema, walking the running in-scope workspaces until one serves the
// catalog, serves the schema and proves the child listing complete. A workspace
// that answers neither is skipped and the walk goes on. Pushing nothing empties
// the listing, so that is reserved for a parent every workspace named as gone
// with none refusing a read; any refusal left standing is an error instead.
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
	// The metastore syncer removes the parent in the same sync.
	if errors.Is(err, errMetastoreGone) {
		return nil
	}
	if err != nil {
		return err
	}

	fullName := catalogName + schemaNameSeparator + schemaName
	var sawCatalog, sawGone, provenGone, sawForeign bool
	var lastDeny error
	for i := range usable {
		workspace := usable[i]
		catalog, err := catalogThrough(ctx, client, workspace.DeploymentName, metastoreID, catalogName, annos)
		if err != nil {
			// A 404 carrying no code reaches the SDK as NotFound, which it reads as
			// a warning: the children would be emitted as none and C1 would delete
			// them. A workspace that does not serve the catalog says nothing about
			// the schema below it, so the walk goes on instead.
			if errors.Is(err, errCatalogGone) {
				sawGone = true

				continue
			}
			if deniesTrust(err) {
				lastDeny = err

				continue
			}

			return err
		}
		if catalog == nil {
			// A catalog of another metastore is an answer, not an absence, so it
			// cannot stand in for proof that this metastore's catalog is gone.
			sawForeign = true

			continue
		}
		sawCatalog = true
		// The reads run in order, so a workspace serving the catalog now supersedes a
		// gone answer an earlier one gave.
		provenGone = false

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
			// A workspace the catalog is not bound to refuses with 403, which
			// deniesTrust takes above, so this read names it missing only when it is.
			if errors.Is(err, errCatalogGone) {
				provenGone = true

				continue
			}

			return err
		}

		schema, err := schemaThrough(ctx, client, route.Workspace, catalogName, schemaName, annos)
		// A schema named as missing is read like one absent from the listing:
		// proof of deletion only for a caller that sees the whole catalog.
		if errors.Is(err, errSchemaGone) {
			schema, err = nil, nil
		}
		if err != nil {
			// A workspace that refuses the schema read says nothing about the ones
			// that have not been asked yet, so the walk goes on as it does for the
			// catalog read above.
			if deniesTrust(err) {
				lastDeny = err

				continue
			}

			return err
		}
		if schemasComplete && schema == nil {
			ctxzap.Extract(ctx).Debug("databricks-connector: the schema is no longer there; listing none of its children",
				zap.String("resource_type", resourceType.Id),
				zap.String("schema", fullName),
				zap.String("workspace", route.Workspace))

			return nil
		}
		// Owning the schema is enough to list its children. Anything else a
		// partial list hides cannot be told from a schema that is gone.
		if schema == nil || (!schemasComplete && !ownsPrincipal(clientID, schema.Owner)) {
			continue
		}

		trust.schemaOwner = schema.Owner
		childrenComplete, err := childListingIsComplete(ctx, client, trust, annos)
		if err != nil {
			if errors.Is(err, errCatalogGone) {
				provenGone = true

				continue
			}

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
	// A catalog proved gone holds no tables or volumes, and the catalog syncer
	// drops the parent in the same sync. A refusal anywhere leaves the picture mixed.
	if provenGone && lastDeny == nil {
		return nil
	}
	if !sawCatalog {
		// A workspace that does not serve the catalog answers like a gone one.
		if sawGone && lastDeny == nil && !sawForeign {
			return nil
		}
		msg := fmt.Sprintf("databricks-connector: no workspace in sync scope lists catalog %s of metastore %s", catalogName, metastoreID)
		if lastDeny != nil {
			msg = fmt.Sprintf("%s: %s", msg, lastDeny.Error())
		}

		return status.Errorf(codes.FailedPrecondition, "%s", msg)
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

	if ownerWorkspaceID, ok := catalogAdminWorkspaceID(trust.parentOwner); ok {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, ownerWorkspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	complete, err := holdsPrivilegeOn(ctx, client, trust, databricks.SecurableCatalog, trust.fullName,
		[]string{privilegeAllPrivileges}, []string{errorCodeCatalogGone}, annos)
	if errors.Is(err, errSecurableGone) {
		return false, errCatalogGone
	}

	return complete, err
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

// schemaChildGrantsAreTrustworthy is true for an owner of the table or volume,
// its schema, its catalog or the metastore, for a holder of MANAGE on any of
// those, or for a direct Admin when the workspace admins group owns the catalog.
func schemaChildGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	securableType string,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	if ownsPrincipal(trust.ownPrincipal, trust.metastoreOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.parentOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.schemaOwner) ||
		ownsPrincipal(trust.ownPrincipal, trust.owner) {
		return true, nil
	}

	// The workspace admins group acts as the owner for a direct Admin of its workspace.
	if ownerWorkspaceID, ok := catalogAdminWorkspaceID(trust.parentOwner); ok {
		admin, err := isWorkspaceAdmin(ctx, client, trust.ownPrincipal, ownerWorkspaceID, annos)
		if err != nil || admin {
			return admin, err
		}
	}

	// MANAGE on the securable, its schema or its catalog makes the privilege
	// list complete, and one effective read reports all three.
	return holdsEffectivePrivilegeOn(ctx, client, trust, securableType, trust.fullName, managePrivilege, annos)
}

// liveChildRoute rebuilds the route and the owners above a table or a volume
// from what Databricks reports now. The metastore owner comes from a live
// listing, the catalog from the recorded workspace when this sync still admits
// it and otherwise from the first running in-scope workspace that serves it,
// and the schema from a read through that workspace.
func liveChildRoute(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	workspaces map[string]struct{},
	resource *v2.Resource,
	metastoreID, catalogName, schemaName string,
	annos *annotations.Annotations,
) (schemaChildRoute, error) {
	owner, err := metastoreOwner(ctx, client, metastoreID, annos)
	if err != nil {
		return schemaChildRoute{}, err
	}

	route, catalog, err := liveCatalogRoute(ctx, client, clientID, workspaces, resource, metastoreID, catalogName, owner, annos)
	if err != nil {
		return schemaChildRoute{}, err
	}

	fullName := catalogName + schemaNameSeparator + schemaName
	schema, rateLimit, err := client.GetSchema(ctx, route.Workspace, fullName)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		// Only Databricks naming the securable proves it gone; an unbound workspace
		// refuses with 403, and reading that as an absence would report a false revoke.
		if databricks.IsSecurableGone(err, errorCodeSchemaGone, errorCodeCatalogGone) {
			return schemaChildRoute{}, errSecurableGone
		}

		return schemaChildRoute{}, refuseUnreadable(fmt.Sprintf(
			"databricks-connector: failed to read schema %s through workspace %s", fullName, route.Workspace), err)
	}

	return schemaChildRoute{
		Workspace:      route.Workspace,
		WorkspaceID:    route.WorkspaceID,
		WorkspaceName:  route.WorkspaceName,
		MetastoreOwner: owner,
		CatalogOwner:   catalog.Owner,
		SchemaOwner:    schema.Owner,
	}, nil
}

// childEntitlementOnLiveRoute rebuilds the entitlement's table or volume from
// what Databricks reports now: the four owners the trust check short-circuits
// on — metastore, catalog, schema and the securable itself — ride on the
// profile, where they can be hours old, so a transfer away from the connector
// would otherwise let a truncated privilege list decide the write. read is the
// level's own leaf read, which also answers whether the securable is gone.
func childEntitlementOnLiveRoute(
	ctx context.Context,
	client *databricks.Client,
	clientID string,
	workspaces map[string]struct{},
	noun string,
	entitlement *v2.Entitlement,
	annos *annotations.Annotations,
	read func(ctx context.Context, route schemaChildRoute, metastoreID, catalogName, schemaName, name string,
		parent *v2.ResourceId, annos *annotations.Annotations) (*v2.Resource, error),
) (*v2.Entitlement, error) {
	resource := entitlement.GetResource()
	metastoreID, catalogName, schemaName, name, err := parseSchemaChildID(resource.GetId().GetResource(), noun)
	if err != nil {
		return nil, err
	}

	route, err := liveChildRoute(ctx, client, clientID, workspaces, resource, metastoreID, catalogName, schemaName, annos)
	if err != nil {
		return nil, err
	}

	routed, err := read(ctx, route, metastoreID, catalogName, schemaName, name, resource.GetParentResourceId(), annos)
	if err != nil {
		return nil, err
	}

	copyEntitlement, ok := proto.Clone(entitlement).(*v2.Entitlement)
	if !ok {
		return nil, status.Errorf(codes.Internal, "databricks-connector: cannot copy the %s entitlement", noun)
	}
	copyEntitlement.Resource = routed

	return copyEntitlement, nil
}

// schemaChildIsGone settles a 404 that a missing table or volume and a workspace
// that cannot serve its catalog both answer with. The parent is read first
// through that same workspace: only a schema it answers 200 for makes the
// securable's own 404 proof of deletion. Any other answer leaves the absence
// unproven, which is what keeps C1 from removing what it already has.
func schemaChildIsGone(
	ctx context.Context,
	client *databricks.Client,
	noun, fullName, workspace string,
	annos *annotations.Annotations,
	readSecurable func(ctx context.Context, workspace, fullName string) (*v2.RateLimitDescription, error),
) (bool, error) {
	cut := strings.LastIndex(fullName, schemaNameSeparator)
	if cut <= 0 {
		return false, status.Errorf(codes.InvalidArgument,
			"databricks-connector: %s %s has no parent schema in its name", noun, fullName)
	}
	schemaFullName := fullName[:cut]

	_, rateLimit, err := client.GetSchema(ctx, workspace, schemaFullName)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return false, fmt.Errorf(
			"databricks-connector: workspace %s does not answer for schema %s, so a %s missing there cannot be told from one it cannot serve: %w",
			workspace, schemaFullName, noun, err)
	}

	rateLimit, err = readSecurable(ctx, workspace, fullName)
	noteRateLimit(annos, rateLimit)
	switch {
	case err == nil:
		return false, nil
	case apiStatus(err) == http.StatusNotFound:
		// The workspace serves the parent and still has no such securable.
		return true, nil
	}

	return false, err
}
