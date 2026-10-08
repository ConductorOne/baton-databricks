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

	// https://docs.databricks.com/api/account/accountmetastoreassignments/get
	accountWorkspaceMetastoreEndpoint = "/api/2.0/accounts/%s/workspaces/%s/metastore"

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

// GetWorkspaceMetastore (GET /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/metastore),
// empty when the workspace has none. workspaceID is the numeric account-level id,
// not the deployment name the securable endpoints are addressed by.
func (c *Client) GetWorkspaceMetastore(
	ctx context.Context,
	workspaceID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	var res workspaceMetastoreResponse

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspaceMetastoreEndpoint, c.accountId, workspaceID))
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return "", ratelimitData, fmt.Errorf("failed to get metastore assignment for workspace %s: %w", workspaceID, err)
	}

	return res.MetastoreAssignment.MetastoreID, ratelimitData, nil
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
	u, err := c.permissionsUrl(workspaceId, securableType, fullName)
	if err != nil {
		return nil, "", nil, err
	}

	var res permissionsResponse

	maxResults := permissionsMaxResults
	ratelimitData, err := c.Get(ctx, u, &res, &unityCatalogVars{
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

// DrainPermissions walks ListPermissions to exhaustion for every principal.
func (c *Client) DrainPermissions(
	ctx context.Context,
	workspaceId, securableType, fullName string,
) (
	[]PrivilegeAssignment,
	*v2.RateLimitDescription,
	error,
) {
	return drainPagesFrom(ctx, "", func(pageToken string) ([]PrivilegeAssignment, string, *v2.RateLimitDescription, error) {
		return c.ListPermissions(ctx, workspaceId, securableType, fullName, "", pageToken)
	})
}

// ForEachPermissionsPage walks ListPermissions without retaining the pages.
func (c *Client) ForEachPermissionsPage(
	ctx context.Context,
	workspaceId, securableType, fullName, principal string,
	visit func([]PrivilegeAssignment) (bool, error),
) (*v2.RateLimitDescription, error) {
	return forEachPageFrom(ctx, "", func(pageToken string) ([]PrivilegeAssignment, string, *v2.RateLimitDescription, error) {
		return c.ListPermissions(ctx, workspaceId, securableType, fullName, principal, pageToken)
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

	stop, err := visit(res.PrivilegeAssignments)
	if err != nil || stop || res.NextPageToken == "" {
		return ratelimitData, err
	}

	// The PATCH body is already page one.
	drainRateLimit, drainErr := forEachPageFrom(ctx, res.NextPageToken,
		func(pageToken string) ([]PrivilegeAssignment, string, *v2.RateLimitDescription, error) {
			return c.ListPermissions(ctx, workspaceId, securableType, fullName, "", pageToken)
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
