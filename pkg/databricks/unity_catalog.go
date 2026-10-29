package databricks

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// A metastore lives on the account plane; every securable below it is only
// reachable through a workspace host the metastore is assigned to.
const (
	// https://docs.databricks.com/api/account/accountmetastores/list
	accountMetastoresEndpoint = "/api/2.0/accounts/%s/metastores"

	// https://docs.databricks.com/api/account/accountmetastoreassignments/list
	accountMetastoreWorkspacesEndpoint = "/api/2.0/accounts/%s/metastores/%s/workspaces"

	// https://docs.databricks.com/api/workspace/catalogs/list
	unityCatalogCatalogsEndpoint = "/api/2.1/unity-catalog/catalogs"

	// https://docs.databricks.com/api/workspace/schemas/list
	unityCatalogSchemasEndpoint = "/api/2.1/unity-catalog/schemas"

	// https://docs.databricks.com/api/workspace/tables/list
	unityCatalogTablesEndpoint = "/api/2.1/unity-catalog/tables"

	// https://docs.databricks.com/api/workspace/volumes/list
	unityCatalogVolumesEndpoint = "/api/2.1/unity-catalog/volumes"

	// GET https://docs.databricks.com/api/workspace/grants/get
	// PATCH https://docs.databricks.com/api/workspace/grants/update
	unityCatalogPermissionsEndpoint = "/api/2.1/unity-catalog/permissions"

	// Path segments for the permissions endpoint, not interchangeable with the
	// uppercase securable_type a listed securable carries. A metastore is
	// addressed by its UUID: its name is rejected.
	SecurableMetastore = "metastore"
	SecurableCatalog   = "catalog"
	SecurableSchema    = "schema"
	SecurableTable     = "table"
	SecurableVolume    = "volume"

	// The permissions endpoint rejects any max_results from 1 to 149 with a 400.
	// Zero selects the paginated response that replaces the unpaginated form.
	permissionsMaxResults uint = 0

	// The tables endpoint rejects a max_results above this ceiling.
	unityCatalogTablesMaxResults uint = 50
)

// ListMetastores (GET /api/2.0/accounts/{account_id}/metastores). Not paginated.
func (c *Client) ListMetastores(
	ctx context.Context,
) (
	[]Metastore,
	*v2.RateLimitDescription,
	error,
) {
	var res metastoresResponse

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountMetastoresEndpoint, c.accountId))
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, fmt.Errorf("failed to list metastores: %w", err)
	}

	return res.Metastores, ratelimitData, nil
}

// ListMetastoreWorkspaces (GET /api/2.0/accounts/{account_id}/metastores/{metastore_id}/workspaces)
// returns the numeric workspace ids assigned to that metastore.
func (c *Client) ListMetastoreWorkspaces(
	ctx context.Context,
	metastoreID string,
) (
	[]string,
	*v2.RateLimitDescription,
	error,
) {
	var res metastoreWorkspacesResponse

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountMetastoreWorkspacesEndpoint, c.accountId, metastoreID))
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, fmt.Errorf("failed to list workspaces for metastore %s: %w", metastoreID, err)
	}

	ids := make([]string, 0, len(res.WorkspaceIDs))
	for _, id := range res.WorkspaceIDs {
		if id == "" {
			continue
		}
		ids = append(ids, id.String())
	}

	return ids, ratelimitData, nil
}

// ListCatalogs (GET /api/2.1/unity-catalog/catalogs) reads one page of the catalogs
// of the metastore attached to workspaceId, which is a deployment name. A catalog
// bound to a subset of the workspaces is listed only by the ones it is bound to.
func (c *Client) ListCatalogs(
	ctx context.Context,
	workspaceId, pageToken string,
	maxResults uint,
	includeUnbound bool,
) (
	[]Catalog,
	string,
	*v2.RateLimitDescription,
	error,
) {
	var res catalogsResponse

	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogCatalogsEndpoint)
	ratelimitData, err := c.Get(ctx, u, &res, &unityCatalogVars{
		maxResults:     &maxResults,
		pageToken:      pageToken,
		includeUnbound: includeUnbound,
	})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId, fmt.Errorf("failed to list catalogs: %w", err))
	}

	return res.Catalogs, res.NextPageToken, ratelimitData, nil
}

// GetCatalog (GET /api/2.1/unity-catalog/catalogs/{name}) reads one catalog
// through workspaceId, which is a deployment name. It answers what draining
// ListCatalogs cannot: a 404 naming CATALOG_DOES_NOT_EXIST is the catalog not
// existing, while any other refusal is this workspace not seeing it.
func (c *Client) GetCatalog(
	ctx context.Context,
	workspaceId, name string,
) (
	*Catalog,
	*v2.RateLimitDescription,
	error,
) {
	var res Catalog

	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogCatalogsEndpoint, name)
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, nameWorkspace403Remedy(workspaceId, fmt.Errorf("failed to read catalog %s: %w", name, err))
	}

	return &res, ratelimitData, nil
}

// ListSchemas (GET /api/2.1/unity-catalog/schemas?catalog_name=) reads one page of
// the schemas of catalogName, through workspaceId, which is a deployment name.
// Unlike the catalogs listing, this one pages, so the cursor is passed through.
func (c *Client) ListSchemas(
	ctx context.Context,
	workspaceId, catalogName, pageToken string,
	maxResults uint,
) (
	[]Schema,
	string,
	*v2.RateLimitDescription,
	error,
) {
	var res schemasResponse

	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogSchemasEndpoint)
	ratelimitData, err := c.Get(ctx, u, &res, &unityCatalogVars{
		catalogName: catalogName,
		maxResults:  &maxResults,
		pageToken:   pageToken,
	})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId,
			fmt.Errorf("failed to list schemas in catalog %s: %w", catalogName, err))
	}

	return res.Schemas, res.NextPageToken, ratelimitData, nil
}

// ListTables (GET /api/2.1/unity-catalog/tables?catalog_name=&schema_name=) pages
// the tables, views and materialized views in a schema. schema_name is mandatory,
// and a max_results above the endpoint's ceiling of 50 is clamped rather than sent.
func (c *Client) ListTables(
	ctx context.Context,
	workspaceId, catalogName, schemaName, pageToken string,
	maxResults uint,
) (
	[]Table,
	string,
	*v2.RateLimitDescription,
	error,
) {
	if maxResults > unityCatalogTablesMaxResults {
		maxResults = unityCatalogTablesMaxResults
	}

	var res tablesResponse

	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogTablesEndpoint)
	ratelimitData, err := c.Get(ctx, u, &res, &unityCatalogVars{
		catalogName: catalogName,
		schemaName:  schemaName,
		maxResults:  &maxResults,
		pageToken:   pageToken,
		// Without this the listing carries every column and property of every
		// table, which is the bulk of the response and none of it is read here.
		omitTablePayload: true,
	})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId,
			fmt.Errorf("failed to list tables in schema %s.%s: %w", catalogName, schemaName, err))
	}

	return res.Tables, res.NextPageToken, ratelimitData, nil
}

// ListVolumes (GET /api/2.1/unity-catalog/volumes?catalog_name=&schema_name=) pages a schema's volumes.
func (c *Client) ListVolumes(
	ctx context.Context,
	workspaceId, catalogName, schemaName, pageToken string,
	maxResults uint,
) (
	[]Volume,
	string,
	*v2.RateLimitDescription,
	error,
) {
	var res volumesResponse

	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogVolumesEndpoint)
	ratelimitData, err := c.Get(ctx, u, &res, &unityCatalogVars{
		catalogName: catalogName,
		schemaName:  schemaName,
		maxResults:  &maxResults,
		pageToken:   pageToken,
	})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId,
			fmt.Errorf("failed to list volumes in schema %s.%s: %w", catalogName, schemaName, err))
	}

	return res.Volumes, res.NextPageToken, ratelimitData, nil
}

// ListPermissions (GET /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}).
// The principal query filters effectively, not literally: an inherited assignment
// comes back naming the group it is inherited through, so a caller must read every
// row's privileges and never match on the principal field. Empty lists every row.
func (c *Client) ListPermissions(
	ctx context.Context,
	workspaceId, securableType, fullName, principal, pageToken string,
) (
	[]PrivilegeAssignment,
	string,
	*v2.RateLimitDescription,
	error,
) {
	return c.listPermissions(ctx, workspaceId, securableType, fullName, principal, pageToken, false)
}

func (c *Client) listPermissions(
	ctx context.Context,
	workspaceId, securableType, fullName, principal, pageToken string,
	uncached bool,
) (
	[]PrivilegeAssignment,
	string,
	*v2.RateLimitDescription,
	error,
) {
	u, err := c.permissionsUrl(workspaceId, securableType, fullName)
	if err != nil {
		return nil, "", nil, err
	}

	var res permissionsResponse

	maxResults := permissionsMaxResults
	read := c.Get
	if uncached {
		read = c.GetUncached
	}
	ratelimitData, err := read(ctx, u, &res, &unityCatalogVars{
		principal:  principal,
		maxResults: &maxResults,
		pageToken:  pageToken,
	})
	if err != nil {
		if principal != "" {
			return nil, "", ratelimitData, fmt.Errorf("failed to list permissions for principal %s on %s %s: %w", principal, securableType, fullName, err)
		}

		return nil, "", ratelimitData, fmt.Errorf("failed to list permissions on %s %s: %w", securableType, fullName, err)
	}

	return res.PrivilegeAssignments, res.NextPageToken, ratelimitData, nil
}

// DrainPermissions walks ListPermissions to exhaustion for every principal,
// bypassing the response cache. Its only caller is the pre-read that decides
// whether a Grant still has to PATCH, and that decision is wrong when it is made
// against a page the cache has been holding since before the last change.
func (c *Client) DrainPermissions(
	ctx context.Context,
	workspaceId, securableType, fullName string,
) (
	[]PrivilegeAssignment,
	*v2.RateLimitDescription,
	error,
) {
	return drainPagesFrom(ctx, "", func(pageToken string) ([]PrivilegeAssignment, string, *v2.RateLimitDescription, error) {
		return c.listPermissions(ctx, workspaceId, securableType, fullName, "", pageToken, true)
	})
}

// ForEachUncachedPermissionsPage walks ListPermissions past the cache, without
// retaining the pages, for the pre-read that decides whether a Revoke still has
// to PATCH. See DrainPermissions.
func (c *Client) ForEachUncachedPermissionsPage(
	ctx context.Context,
	workspaceId, securableType, fullName, principal string,
	visit func([]PrivilegeAssignment) (bool, error),
) (*v2.RateLimitDescription, error) {
	return forEachPageFrom(ctx, "", func(pageToken string) ([]PrivilegeAssignment, string, *v2.RateLimitDescription, error) {
		return c.listPermissions(ctx, workspaceId, securableType, fullName, principal, pageToken, true)
	}, visit)
}

// UpdatePermissions (PATCH /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}).
// Databricks no-ops a privilege already held or already absent, so the echoed
// assignments are the only confirmation the change landed — and that echo is
// itself paginated, so a principal on a later page must not read as success.
func (c *Client) UpdatePermissions(
	ctx context.Context,
	workspaceId, securableType, fullName string,
	changes []PermissionsChange,
) (
	[]PrivilegeAssignment,
	*v2.RateLimitDescription,
	error,
) {
	var assignments []PrivilegeAssignment
	ratelimitData, err := c.updatePermissions(ctx, workspaceId, securableType, fullName, changes,
		func(page []PrivilegeAssignment) (bool, error) {
			assignments = append(assignments, page...)
			return false, nil
		})
	if err != nil {
		return nil, ratelimitData, err
	}

	return assignments, ratelimitData, nil
}

// UpdatePermissionsUntil is UpdatePermissions for a caller that can stop early.
func (c *Client) UpdatePermissionsUntil(
	ctx context.Context,
	workspaceId, securableType, fullName string,
	changes []PermissionsChange,
	visit func([]PrivilegeAssignment) (bool, error),
) (*v2.RateLimitDescription, error) {
	return c.updatePermissions(ctx, workspaceId, securableType, fullName, changes, visit)
}

func (c *Client) updatePermissions(
	ctx context.Context,
	workspaceId, securableType, fullName string,
	changes []PermissionsChange,
	visit func([]PrivilegeAssignment) (bool, error),
) (*v2.RateLimitDescription, error) {
	if len(changes) == 0 {
		return nil, invalidArgument("no permission changes given for %s %s", securableType, fullName)
	}

	for i, change := range changes {
		if err := change.validate(); err != nil {
			return nil, fmt.Errorf("invalid permission change %d for %s %s: %w", i, securableType, fullName, err)
		}
	}

	u, err := c.permissionsUrl(workspaceId, securableType, fullName)
	if err != nil {
		return nil, err
	}

	payload := struct {
		Changes []PermissionsChange `json:"changes"`
	}{
		Changes: changes,
	}

	var res permissionsResponse

	ratelimitData, err := c.Patch(ctx, u, payload, &res)
	if err != nil {
		return ratelimitData, fmt.Errorf("failed to update permissions on %s %s: %w", securableType, fullName, err)
	}

	// The provisioning pre-read stored page one under the key the sync reads,
	// and a PATCH does not invalidate it. Re-reading past the cache overwrites
	// that entry with the post-change state. The write already landed, so a
	// failed refresh must not fail it. A read that stops before the last page
	// leaves those later pages until the next cycle; max_results 0 makes that rare.
	_, _, refreshRateLimit, refreshErr := c.listPermissions(ctx, workspaceId, securableType, fullName, "", "", true)
	if refreshErr != nil {
		ctxzap.Extract(ctx).Debug("could not refresh the permissions cache after the write",
			zap.String("securable_type", securableType),
			zap.String("full_name", fullName),
			zap.Error(refreshErr),
		)
	} else if refreshRateLimit != nil {
		ratelimitData = refreshRateLimit
	}

	stop, err := visit(res.PrivilegeAssignments)
	if err != nil || stop || res.NextPageToken == "" {
		return ratelimitData, err
	}

	// The PATCH body is already page one. The remaining pages confirm the change
	// landed, so they are read past the cache for the same reason the pre-read is.
	drainRateLimit, drainErr := forEachPageFrom(ctx, res.NextPageToken,
		func(pageToken string) ([]PrivilegeAssignment, string, *v2.RateLimitDescription, error) {
			return c.listPermissions(ctx, workspaceId, securableType, fullName, "", pageToken, true)
		}, visit)
	if drainRateLimit != nil {
		ratelimitData = drainRateLimit
	}
	if drainErr != nil {
		return ratelimitData, fmt.Errorf(
			"failed to read the remaining permissions pages after updating %s %s: %w",
			securableType, fullName, drainErr)
	}

	return ratelimitData, nil
}

// A name that does not survive URL assembly addresses a different securable,
// which on a PATCH writes someone else's permissions.
func (c *Client) permissionsUrl(workspaceId, securableType, fullName string) (*url.URL, error) {
	if securableType == "" || fullName == "" {
		return nil, invalidArgument("securable type and full name are required, got %q and %q", securableType, fullName)
	}

	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogPermissionsEndpoint, securableType, fullName)
	if !addressesSecurable(u, "/"+securableType+"/"+fullName) {
		return nil, invalidArgument("cannot address %s %q: the name does not survive URL assembly", securableType, fullName)
	}

	return u, nil
}

// JoinPath reads each element as pre-escaped, so a bare "%" in a name drops the
// whole path, and the request layer's unescape round trip then turns a "?" into
// a query string and a "#" into a fragment.
func addressesSecurable(u *url.URL, suffix string) bool {
	unescaped, err := url.PathUnescape(u.String())
	if err != nil {
		return false
	}

	sent, err := url.Parse(unescaped)
	if err != nil {
		return false
	}

	return sent.RawQuery == "" && sent.Fragment == "" && strings.HasSuffix(sent.Path, suffix)
}
