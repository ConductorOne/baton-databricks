package databricks

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

	// GET https://docs.databricks.com/api/workspace/grants/get
	// PATCH https://docs.databricks.com/api/workspace/grants/update
	unityCatalogPermissionsEndpoint = "/api/2.1/unity-catalog/permissions"

	// Path segments for the permissions endpoint, not interchangeable with the
	// uppercase securable_type a listed securable carries. A metastore is
	// addressed by its UUID: its name is rejected.
	SecurableMetastore = "metastore"

	// The permissions endpoint rejects any max_results from 1 to 149 with a 400.
	// Zero selects the paginated response that replaces the unpaginated form.
	permissionsMaxResults uint = 0
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

// listSecurables reads one page off a workspace-scoped securable endpoint.
func listSecurables[R any, T any](
	ctx context.Context,
	c *Client,
	workspaceId, endpoint string,
	page func(res *R) ([]T, string),
	vars ...Vars,
) (
	[]T,
	string,
	*v2.RateLimitDescription,
	error,
) {
	var res R

	u := c.workspaceUrl(workspaceId).JoinPath(endpoint)
	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId, err)
	}

	items, nextPageToken := page(&res)

	return items, nextPageToken, ratelimitData, nil
}

// listCatalogs (GET /api/2.1/unity-catalog/catalogs) pages the catalogs of the
// metastore attached to workspaceId, which is a deployment name.
func (c *Client) listCatalogs(
	ctx context.Context,
	workspaceId, pageToken string,
	maxResults uint,
) (
	[]Catalog,
	string,
	*v2.RateLimitDescription,
	error,
) {
	catalogs, next, ratelimitData, err := listSecurables(ctx, c, workspaceId, unityCatalogCatalogsEndpoint,
		func(res *catalogsResponse) ([]Catalog, string) { return res.Catalogs, res.NextPageToken },
		&unityCatalogVars{
			maxResults: &maxResults,
			pageToken:  pageToken,
		})
	if err != nil {
		return nil, "", ratelimitData, fmt.Errorf("failed to list catalogs: %w", err)
	}

	return catalogs, next, ratelimitData, nil
}

// ForEachCatalogPage walks listCatalogs without retaining the pages.
func (c *Client) ForEachCatalogPage(
	ctx context.Context,
	workspaceId string,
	maxResults uint,
	visit func([]Catalog) (bool, error),
) (*v2.RateLimitDescription, error) {
	return forEachPageFrom(ctx, "", func(pageToken string) ([]Catalog, string, *v2.RateLimitDescription, error) {
		return c.listCatalogs(ctx, workspaceId, pageToken, maxResults)
	}, visit)
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
	// failed refresh must not fail it.
	if _, _, refreshRateLimit, refreshErr := c.listPermissions(ctx, workspaceId, securableType, fullName, "", "", true); refreshErr == nil && refreshRateLimit != nil {
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
