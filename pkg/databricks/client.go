package databricks

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const (
	base        = "cloud.databricks.com"
	apiEndpoint = "/api/2.0"

	// Helper endpoints.
	previewEndpoint       = "/preview"
	accessControlEndpoint = "/access-control"
	scimEndpoint          = "/scim/v2"
	accountsEndpoint      = "/accounts"

	// Base hosts.
	accountBaseHost   = "accounts." + base + apiEndpoint
	workspaceBaseHost = "%s." + base + apiEndpoint + previewEndpoint

	// Resource endpoints. Some of these are case sensitive.
	usersEndpoint                = "/Users"
	groupsEndpoint               = "/Groups"
	servicePrincipalsEndpoint    = "/ServicePrincipals"
	rolesEndpoint                = "/assignable-roles"
	ruleSetsEndpoint             = "/rule-sets"
	workspacesEndpoint           = "/workspaces"
	workspaceAssignmentsEndpoint = "/permissionassignments"
)

type Client struct {
	httpClient *uhttp.BaseHttpClient
	baseUrl    *url.URL
	cfg        Config
	auth       Auth
	etag       string
	accountId  string

	isAccAPIAvailable bool
	isWSAPIAvailable  bool
}

func NewClient(ctx context.Context, httpClient *http.Client, hostname, accountHostname, accountID string, auth Auth) (*Client, error) {
	cli, err := uhttp.NewBaseHttpClientWithContext(ctx, httpClient)
	return &Client{
		httpClient: cli,
		auth:       auth,
		accountId:  accountID,
	}, err
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

	c.cfg = NewWorkspaceConfig("", c.accountId, workspace)
	c.baseUrl = c.cfg.BaseUrl()

	if tokenAuth, ok := c.auth.(*TokenAuth); ok {
		tokenAuth.SetWorkspace(workspace)
	}
}

func (c *Client) SetAccountConfig() {
	if _, ok := c.cfg.(*AccountConfig); ok {
		return
	}

	c.cfg = NewAccountConfig("", c.accountId)
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
	return c.accountId
}

type ListResponse[T any] struct {
	Resources []T  `json:"Resources"`
	Total     uint `json:"totalResults"`
}

func (c *Client) ListUsers(
	ctx context.Context,
	vars ...Vars,
) (
	[]User,
	uint,
	*v2.RateLimitDescription,
	error,
) {
	var res ListResponse[User]
	u, err := c.cfg.ResolvePath(c.baseUrl, usersEndpoint)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to prepare url to fetch users: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, ratelimitData, err
	}

	return res.Resources, res.Total, ratelimitData, nil
}

func (c *Client) GetUser(
	ctx context.Context,
	userID string,
) (
	*User,
	*v2.RateLimitDescription,
	error,
) {
	var res *User
	u, err := c.cfg.ResolvePath(c.baseUrl, usersEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch users: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, userID)

	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res, ratelimitData, nil
}

func (c *Client) UpdateUser(ctx context.Context, user *User) (*v2.RateLimitDescription, error) {
	u, err := c.cfg.ResolvePath(c.baseUrl, usersEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch users: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, user.ID)

	return c.Put(ctx, u, user, nil)
}

func (c *Client) FindUserID(
	ctx context.Context,
	username string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	users, _, ratelimitData, err := c.ListUsers(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("userName eq '%s'", username)),
	)

	if err != nil {
		return "", ratelimitData, err
	}

	if len(users) == 0 {
		return "", ratelimitData, err
	}

	return users[0].ID, ratelimitData, nil
}

func (c *Client) FindUsername(
	ctx context.Context,
	userID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	users, _, ratelimitData, err := c.ListUsers(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("id eq '%s'", userID)),
	)

	if err != nil {
		return "", ratelimitData, err
	}

	if len(users) == 0 {
		return "", ratelimitData, err
	}

	return users[0].UserName, ratelimitData, nil
}

func (c *Client) ListGroups(
	ctx context.Context,
	vars ...Vars,
) (
	[]Group,
	uint,
	*v2.RateLimitDescription,
	error,
) {
	var res ListResponse[Group]
	u, err := c.cfg.ResolvePath(c.baseUrl, groupsEndpoint)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, ratelimitData, err
	}

	return res.Resources, res.Total, ratelimitData, nil
}

func (c *Client) GetGroup(
	ctx context.Context,
	groupID string,
) (
	*Group,
	*v2.RateLimitDescription,
	error,
) {
	var res *Group
	u, err := c.cfg.ResolvePath(c.baseUrl, groupsEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, groupID)

	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res, ratelimitData, nil
}

func (c *Client) UpdateGroup(ctx context.Context, group *Group) (*v2.RateLimitDescription, error) {
	u, err := c.cfg.ResolvePath(c.baseUrl, groupsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, group.ID)

	return c.Put(ctx, u, group, nil)
}

func (c *Client) FindGroupID(
	ctx context.Context,
	displayName string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	groups, _, ratelimitData, err := c.ListGroups(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("displayName eq '%s'", displayName)),
	)

	if err != nil {
		return "", ratelimitData, err
	}

	if len(groups) == 0 {
		return "", ratelimitData, err
	}

	return groups[0].ID, ratelimitData, nil
}

func (c *Client) FindGroupDisplayName(
	ctx context.Context,
	groupID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	groups, _, ratelimitData, err := c.ListGroups(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("id eq '%s'", groupID)),
	)

	if err != nil {
		return "", ratelimitData, err
	}

	if len(groups) == 0 {
		return "", ratelimitData, err
	}

	return groups[0].DisplayName, ratelimitData, nil
}

func (c *Client) ListServicePrincipals(
	ctx context.Context,
	vars ...Vars,
) (
	[]ServicePrincipal,
	uint,
	*v2.RateLimitDescription,
	error,
) {
	var res ListResponse[ServicePrincipal]
	u, err := c.cfg.ResolvePath(c.baseUrl, servicePrincipalsEndpoint)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, ratelimitData, err
	}

	return res.Resources, res.Total, ratelimitData, nil
}

func (c *Client) GetServicePrincipal(
	ctx context.Context,
	servicePrincipalID string,
) (
	*ServicePrincipal,
	*v2.RateLimitDescription,
	error,
) {
	var res *ServicePrincipal
	u, err := c.cfg.ResolvePath(c.baseUrl, servicePrincipalsEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, servicePrincipalID)

	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res, ratelimitData, nil
}

func (c *Client) UpdateServicePrincipal(
	ctx context.Context,
	servicePrincipal *ServicePrincipal,
) (
	*v2.RateLimitDescription,
	error,
) {
	u, err := c.cfg.ResolvePath(c.baseUrl, servicePrincipalsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, servicePrincipal.ID)

	return c.Put(ctx, u, servicePrincipal, nil)
}

func (c *Client) FindServicePrincipalID(
	ctx context.Context,
	appID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	servicePrincipals, _, ratelimitData, err := c.ListServicePrincipals(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("applicationId eq '%s'", appID)),
	)

	if err != nil {
		return "", ratelimitData, err
	}

	if len(servicePrincipals) == 0 {
		return "", ratelimitData, err
	}

	return servicePrincipals[0].ID, ratelimitData, nil
}

func (c *Client) FindServicePrincipalAppID(
	ctx context.Context,
	servicePrincipalID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	servicePrincipals, _, ratelimitData, err := c.ListServicePrincipals(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("id eq '%s'", servicePrincipalID)),
	)

	if err != nil {
		return "", ratelimitData, err
	}

	if len(servicePrincipals) == 0 {
		return "", ratelimitData, err
	}

	return servicePrincipals[0].ApplicationID, ratelimitData, nil
}

func (c *Client) ListRoles(
	ctx context.Context,
	resourceType string,
	resourceId string,
) (
	[]Role,
	*v2.RateLimitDescription,
	error,
) {
	var res struct {
		Roles []Role `json:"roles"`
	}

	u, err := c.cfg.ResolvePath(c.baseUrl, rolesEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch roles: %w", err)
	}

	resourcePayload, err := url.JoinPath("accounts", c.accountId, resourceType, resourceId)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare resource payload: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res, NewResourceVars(resourcePayload))
	if err != nil {
		return nil, ratelimitData, err
	}

	return res.Roles, ratelimitData, nil
}

func (c *Client) ListWorkspaces(
	ctx context.Context,
) (
	[]Workspace,
	*v2.RateLimitDescription,
	error,
) {
	var res []Workspace

	u, err := c.cfg.ResolvePath(c.baseUrl, workspacesEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch workspaces: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res, ratelimitData, nil
}

func (c *Client) prepareURLForWorkspaceMembers(params ...string) (*url.URL, error) {
	u := *c.baseUrl

	baseEndpoint := fmt.Sprintf("%s/%s", accountsEndpoint, c.accountId)
	path, err := url.JoinPath(baseEndpoint, params...)
	if err != nil {
		return nil, err
	}
	u.Path = path

	return &u, nil
}

func (c *Client) ListWorkspaceMembers(
	ctx context.Context,
	workspaceID int,
) (
	[]WorkspaceAssignment,
	*v2.RateLimitDescription,
	error,
) {
	var res struct {
		Assignments []WorkspaceAssignment `json:"permission_assignments"`
	}

	id := strconv.Itoa(workspaceID)
	u, err := c.prepareURLForWorkspaceMembers(workspacesEndpoint, id, workspaceAssignmentsEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch workspace members: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res.Assignments, ratelimitData, nil
}

func (c *Client) CreateOrUpdateWorkspaceMember(
	ctx context.Context,
	workspaceID int64,
	principalID string,
) (
	*v2.RateLimitDescription,
	error,
) {
	wID := strconv.Itoa(int(workspaceID))
	u, err := c.prepareURLForWorkspaceMembers(
		workspacesEndpoint,
		wID,
		workspaceAssignmentsEndpoint,
		"principals",
		principalID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to create workspace member: %w", err)
	}

	payload := struct {
		Permission []string `json:"permissions"`
	}{
		Permission: []string{"USER"},
	}

	return c.Put(ctx, u, payload, nil)
}

func (c *Client) RemoveWorkspaceMember(
	ctx context.Context,
	workspaceID int64,
	principalID string,
) (*v2.RateLimitDescription, error) {
	wID := strconv.Itoa(int(workspaceID))

	u, err := c.prepareURLForWorkspaceMembers(
		workspacesEndpoint,
		wID,
		workspaceAssignmentsEndpoint,
		"principals",
		principalID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to create workspace member: %w", err)
	}

	return c.Put(ctx, u, nil, nil)
}

func (c *Client) ListRuleSets(
	ctx context.Context,
	resourceType string,
	resourceId string,
) (
	[]RuleSet,
	*v2.RateLimitDescription,
	error,
) {
	var res struct {
		RuleSets []RuleSet `json:"grant_rules"`
		Etag     string    `json:"etag"`
	}

	u, err := c.cfg.ResolvePath(c.baseUrl, ruleSetsEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare url to fetch rule sets: %w", err)
	}

	resourcePayload, err := url.JoinPath("accounts", c.accountId, resourceType, resourceId, "ruleSets", "default")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare resource payload: %w", err)
	}

	ratelimitData, err := c.Get(ctx, u, &res, NewNameVars(resourcePayload, c.etag))
	if err != nil {
		return nil, ratelimitData, err
	}

	c.UpdateEtag(res.Etag)

	return res.RuleSets, ratelimitData, nil
}

func (c *Client) UpdateRuleSets(
	ctx context.Context,
	resourceType, resourceId string,
	ruleSets []RuleSet,
) (
	*v2.RateLimitDescription,
	error,
) {
	u, err := c.cfg.ResolvePath(c.baseUrl, ruleSetsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch rule sets: %w", err)
	}

	resourcePayload, err := url.JoinPath("accounts", c.accountId, resourceType, resourceId, "ruleSets", "default")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare resource payload: %w", err)
	}

	payload := struct {
		Name    string `json:"name"`
		RuleSet struct {
			Name       string    `json:"name"`
			Etag       string    `json:"etag"`
			GrantRules []RuleSet `json:"grant_rules"`
		} `json:"rule_set"`
	}{
		Name: resourcePayload,
		RuleSet: struct {
			Name       string    `json:"name"`
			Etag       string    `json:"etag"`
			GrantRules []RuleSet `json:"grant_rules"`
		}{
			Name:       resourcePayload,
			Etag:       c.etag,
			GrantRules: ruleSets,
		},
	}

	return c.Put(ctx, u, payload, nil, NewNameVars(resourcePayload, c.etag))
}
