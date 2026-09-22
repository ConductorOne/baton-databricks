package databricks

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	unityCatalogCatalogsEndpoint    = "/api/2.1/unity-catalog/catalogs"
	unityCatalogSchemasEndpoint     = "/api/2.1/unity-catalog/schemas"
	unityCatalogTablesEndpoint      = "/api/2.1/unity-catalog/tables"
	unityCatalogPermissionsEndpoint = "/api/2.1/unity-catalog/permissions"

	// Securable types as they appear in the Unity Catalog permissions path.
	SecurableCatalog = "catalog"
	SecurableSchema  = "schema"
	SecurableTable   = "table"
)

// Catalog is a Unity Catalog top-level data container.
type Catalog struct {
	Name        string `json:"name"`
	Owner       string `json:"owner"`
	Comment     string `json:"comment"`
	MetastoreID string `json:"metastore_id"`
}

// Schema is a grouping of tables/views within a catalog.
type Schema struct {
	Name        string `json:"name"`
	CatalogName string `json:"catalog_name"`
	FullName    string `json:"full_name"`
	Owner       string `json:"owner"`
	Comment     string `json:"comment"`
}

// Table is a data asset (table or view) within a schema.
type Table struct {
	Name        string `json:"name"`
	CatalogName string `json:"catalog_name"`
	SchemaName  string `json:"schema_name"`
	FullName    string `json:"full_name"`
	TableType   string `json:"table_type"`
	Owner       string `json:"owner"`
}

// PrivilegeAssignment maps a principal to the privileges it holds on a securable.
type PrivilegeAssignment struct {
	Principal  string   `json:"principal"`
	Privileges []string `json:"privileges"`
}

// PermissionsChange is a single add/remove delta applied to a securable's grants.
type PermissionsChange struct {
	Principal string   `json:"principal"`
	Add       []string `json:"add,omitempty"`
	Remove    []string `json:"remove,omitempty"`
}

type catalogsResponse struct {
	Catalogs      []Catalog `json:"catalogs"`
	NextPageToken string    `json:"next_page_token"`
}

type schemasResponse struct {
	Schemas       []Schema `json:"schemas"`
	NextPageToken string   `json:"next_page_token"`
}

type tablesResponse struct {
	Tables        []Table `json:"tables"`
	NextPageToken string  `json:"next_page_token"`
}

type permissionsResponse struct {
	PrivilegeAssignments []PrivilegeAssignment `json:"privilege_assignments"`
}

// ucListVars applies Unity Catalog list/query parameters.
type ucListVars struct {
	catalogName string
	schemaName  string
	principal   string
	pageToken   string
	maxResults  uint
}

func (v *ucListVars) Apply(params *url.Values) {
	if v.catalogName != "" {
		params.Add("catalog_name", v.catalogName)
	}
	if v.schemaName != "" {
		params.Add("schema_name", v.schemaName)
	}
	if v.principal != "" {
		params.Add("principal", v.principal)
	}
	if v.maxResults > 0 {
		params.Add("max_results", strconv.FormatUint(uint64(v.maxResults), 10))
	}
	if v.pageToken != "" {
		params.Add("page_token", v.pageToken)
	}
}

// ListCatalogs returns a page of catalogs in the workspace's metastore.
func (c *Client) ListCatalogs(
	ctx context.Context,
	workspaceId string,
	pageToken string,
	maxResults uint,
) (
	[]Catalog,
	string,
	*v2.RateLimitDescription,
	error,
) {
	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogCatalogsEndpoint)

	var res catalogsResponse
	ratelimitData, err := c.Get(ctx, u, &res, &ucListVars{pageToken: pageToken, maxResults: maxResults})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId, err)
	}

	return res.Catalogs, res.NextPageToken, ratelimitData, nil
}

// ListSchemas returns a page of schemas within a catalog.
func (c *Client) ListSchemas(
	ctx context.Context,
	workspaceId string,
	catalogName string,
	pageToken string,
	maxResults uint,
) (
	[]Schema,
	string,
	*v2.RateLimitDescription,
	error,
) {
	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogSchemasEndpoint)

	var res schemasResponse
	ratelimitData, err := c.Get(ctx, u, &res, &ucListVars{catalogName: catalogName, pageToken: pageToken, maxResults: maxResults})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId, err)
	}

	return res.Schemas, res.NextPageToken, ratelimitData, nil
}

// ListTables returns a page of tables within a schema.
func (c *Client) ListTables(
	ctx context.Context,
	workspaceId string,
	catalogName string,
	schemaName string,
	pageToken string,
	maxResults uint,
) (
	[]Table,
	string,
	*v2.RateLimitDescription,
	error,
) {
	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogTablesEndpoint)

	var res tablesResponse
	ratelimitData, err := c.Get(ctx, u, &res, &ucListVars{catalogName: catalogName, schemaName: schemaName, pageToken: pageToken, maxResults: maxResults})
	if err != nil {
		return nil, "", ratelimitData, nameWorkspace403Remedy(workspaceId, err)
	}

	return res.Tables, res.NextPageToken, ratelimitData, nil
}

// ListPermissions returns the direct privilege assignments on a securable.
// securableType is one of Securable{Catalog,Schema,Table}; fullName is the
// dotted Unity Catalog name (catalog, catalog.schema, or catalog.schema.table).
func (c *Client) ListPermissions(
	ctx context.Context,
	workspaceId string,
	securableType string,
	fullName string,
) (
	[]PrivilegeAssignment,
	*v2.RateLimitDescription,
	error,
) {
	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogPermissionsEndpoint, securableType, fullName)

	var res permissionsResponse
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, nameWorkspace403Remedy(workspaceId, err)
	}

	return res.PrivilegeAssignments, ratelimitData, nil
}

// UpdatePermissions applies add/remove privilege changes to a securable.
func (c *Client) UpdatePermissions(
	ctx context.Context,
	workspaceId string,
	securableType string,
	fullName string,
	changes []PermissionsChange,
) (
	*v2.RateLimitDescription,
	error,
) {
	u := c.workspaceUrl(workspaceId).JoinPath(unityCatalogPermissionsEndpoint, securableType, fullName)

	body := map[string]interface{}{"changes": changes}

	var res permissionsResponse
	ratelimitData, err := c.doRequest(ctx, u, http.MethodPatch, body, &res)
	if err != nil {
		return ratelimitData, nameWorkspace403Remedy(workspaceId, err)
	}

	return ratelimitData, nil
}
