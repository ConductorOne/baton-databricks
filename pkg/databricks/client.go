package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const (
	Base        = "cloud.databricks.com"
	APIEndpoint = "/api/2.0"

	// Helper endpoints.
	PreviewEndpoint       = "/preview"
	AccessControlEndpoint = "/access-control"
	SCIMEndpoint          = "/scim/v2"
	AccountsEndpoint      = "/accounts"

	// Base hosts.
	AccountBaseHost   = "accounts." + Base + APIEndpoint
	WorkspaceBaseHost = "%s." + Base + APIEndpoint + PreviewEndpoint

	// Resource endpoints.
	UsersEndpoint                = "/Users"
	GroupsEndpoint               = "/Groups"
	ServicePrincipalsEndpoint    = "/ServicePrincipals"
	RolesEndpoint                = "/assignable-roles"
	RuleSetsEndpoint             = "/rule-sets"
	WorkspacesEndpoint           = "/workspaces"
	WorkspaceAssignmentsEndpoint = "/permissionassignments"
)

type Client struct {
	httpClient *http.Client
	baseUrl    *url.URL
	cfg        Config
	auth       Auth
	etag       string
	acc        string

	isAccAPIAvailable bool
	isWSAPIAvailable  bool
}

func NewClient(httpClient *http.Client, acc string, auth Auth) *Client {
	return &Client{
		httpClient: httpClient,
		auth:       auth,
		acc:        acc,
	}
}

func (c *Client) IsWorkspaceAPIAvailable() bool {
	return c.isWSAPIAvailable
}

func (c *Client) IsAccountAPIAvailable() bool {
	return c.isAccAPIAvailable
}

func (c *Client) UpdateAvailability(accAPI, wsAPI bool) {
	c.isAccAPIAvailable = accAPI
	c.isWSAPIAvailable = wsAPI
}

func (c *Client) GetCurrentConfig() Config {
	return c.cfg
}

func (c *Client) IsAccountConfig() bool {
	_, ok := c.cfg.(*AccountConfig)
	return ok
}

func (c *Client) IsTokenAuth() bool {
	_, ok := c.auth.(*TokenAuth)
	return ok
}

func (c *Client) SetWorkspaceConfig(workspace string) {
	wc, ok := c.cfg.(*WorkspaceConfig)
	if ok && wc.Workspace() == workspace {
		return
	}

	c.cfg = NewWorkspaceConfig(c.acc, workspace)
	c.baseUrl = c.cfg.BaseUrl()

	if tokenAuth, ok := c.auth.(*TokenAuth); ok {
		tokenAuth.SetWorkspace(workspace)
	}
}

func (c *Client) SetAccountConfig() {
	if _, ok := c.cfg.(*AccountConfig); ok {
		return
	}

	c.cfg = NewAccountConfig(c.acc)
	c.baseUrl = c.cfg.BaseUrl()
}

func (c *Client) UpdateConfig(cfg Config) {
	c.cfg = cfg
	c.baseUrl = c.cfg.BaseUrl()
}

func (c *Client) UpdateEtag(etag string) {
	c.etag = etag
}

func (c *Client) GetAccountId() string {
	return c.acc
}

type ListResponse[T any] struct {
	Resources []T  `json:"Resources"`
	Total     uint `json:"totalResults"`
}

func (c *Client) ListUsers(ctx context.Context, vars ...Vars) ([]User, uint, error) {
	var res ListResponse[User]
	u, err := c.cfg.ResolvePath(c.baseUrl, UsersEndpoint)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare url to fetch users: %w", err)
	}

	err = c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, err
	}

	return res.Resources, res.Total, nil
}

func (c *Client) FindUserID(ctx context.Context, username string) (string, error) {
	users, _, err := c.ListUsers(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("userName eq '%s'", username)),
	)

	if err != nil {
		return "", err
	}

	if len(users) == 0 {
		return "", err
	}

	return users[0].ID, nil
}

func (c *Client) ListGroups(ctx context.Context, vars ...Vars) ([]Group, uint, error) {
	var res ListResponse[Group]
	u, err := c.cfg.ResolvePath(c.baseUrl, GroupsEndpoint)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	err = c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, err
	}

	return res.Resources, res.Total, nil
}

func (c *Client) FindGroupID(ctx context.Context, displayName string) (string, error) {
	groups, _, err := c.ListGroups(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("displayName eq '%s'", displayName)),
	)

	if err != nil {
		return "", err
	}

	if len(groups) == 0 {
		return "", err
	}

	return groups[0].ID, nil
}

func (c *Client) ListServicePrincipals(ctx context.Context, vars ...Vars) ([]ServicePrincipal, uint, error) {
	var res ListResponse[ServicePrincipal]
	u, err := c.cfg.ResolvePath(c.baseUrl, ServicePrincipalsEndpoint)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	err = c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, err
	}

	return res.Resources, res.Total, nil
}

func (c *Client) FindServicePrincipalID(ctx context.Context, appID string) (string, error) {
	servicePrincipals, _, err := c.ListServicePrincipals(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("applicationId eq '%s'", appID)),
	)

	if err != nil {
		return "", err
	}

	if len(servicePrincipals) == 0 {
		return "", err
	}

	return servicePrincipals[0].ID, nil
}

func (c *Client) ListRoles(ctx context.Context, resourceType, resourceId string) ([]Role, error) {
	var res struct {
		Roles []Role `json:"roles"`
	}

	u, err := c.cfg.ResolvePath(c.baseUrl, RolesEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch roles: %w", err)
	}

	resourcePayload, err := url.JoinPath("accounts", c.acc, resourceType, resourceId)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare resource payload: %w", err)
	}

	err = c.Get(ctx, u, &res, NewResourceVars(resourcePayload))
	if err != nil {
		return nil, err
	}

	return res.Roles, nil
}

func (c *Client) ListWorkspaces(ctx context.Context) ([]Workspace, error) {
	var res []Workspace

	u, err := c.cfg.ResolvePath(c.baseUrl, WorkspacesEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch workspaces: %w", err)
	}

	err = c.Get(ctx, u, &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) ListWorkspaceMembers(ctx context.Context, workspaceID int) ([]WorkspaceAssignment, error) {
	var res struct {
		Assignments []WorkspaceAssignment `json:"permission_assignments"`
	}

	id := strconv.Itoa(workspaceID)
	u := *c.baseUrl
	baseEndpoint := fmt.Sprintf("%s/%s", AccountsEndpoint, c.acc)
	path, err := url.JoinPath(baseEndpoint, WorkspacesEndpoint, id, WorkspaceAssignmentsEndpoint)
	if err != nil {
		return nil, err
	}
	u.Path = path

	err = c.Get(ctx, &u, &res)
	if err != nil {
		return nil, err
	}

	return res.Assignments, nil
}

func (c *Client) ListRuleSets(ctx context.Context, resourceType, resourceId string) ([]RuleSet, error) {
	var res struct {
		RuleSets []RuleSet `json:"grant_rules"`
		Etag     string    `json:"etag"`
	}

	u, err := c.cfg.ResolvePath(c.baseUrl, RuleSetsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch roles: %w", err)
	}

	resourcePayload, err := url.JoinPath("accounts", c.acc, resourceType, resourceId, "ruleSets", "default")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare resource payload: %w", err)
	}

	err = c.Get(ctx, u, &res, NewNameVars(resourcePayload, c.etag))
	if err != nil {
		return nil, err
	}

	c.UpdateEtag(res.Etag)

	return res.RuleSets, nil
}

func (c *Client) Get(ctx context.Context, urlAddress *url.URL, response interface{}, params ...Vars) error {
	return c.doRequest(ctx, urlAddress, http.MethodGet, nil, response, params...)
}

func (c *Client) doRequest(ctx context.Context, urlAddress *url.URL, method string, body io.Reader, response interface{}, params ...Vars) error {
	u, err := url.PathUnescape(urlAddress.String())
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}

	if len(params) > 0 {
		query := url.Values{}

		for _, param := range params {
			param.Apply(&query)
		}

		req.URL.RawQuery = query.Encode()
	}

	c.auth.Apply(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}

	return nil
}
