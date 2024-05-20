package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const (
	Base            = "cloud.databricks.com"
	APIEndpoint     = "/api/2.0"
	JSONContentType = "application/json"

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
	httpClient *uhttp.BaseHttpClient
	baseUrl    *url.URL
	cfg        Config
	auth       Auth
	etag       string
	acc        string

	isAccAPIAvailable bool
	isWSAPIAvailable  bool
}

func NewClient(httpClient *http.Client, acc string, auth Auth) *Client {
	cli := uhttp.NewBaseHttpClient(httpClient)
	return &Client{
		httpClient: cli,
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

func (c *Client) GetUser(ctx context.Context, userID string) (*User, error) {
	var res *User
	u, err := c.cfg.ResolvePath(c.baseUrl, UsersEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch users: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, userID)

	err = c.Get(ctx, u, &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) UpdateUser(ctx context.Context, user *User) error {
	u, err := c.cfg.ResolvePath(c.baseUrl, UsersEndpoint)
	if err != nil {
		return fmt.Errorf("failed to prepare url to fetch users: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, user.ID)

	b, err := json.Marshal(user)
	if err != nil {
		return err
	}

	return c.Put(ctx, u, bytes.NewReader(b), nil)
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

func (c *Client) FindUsername(ctx context.Context, userID string) (string, error) {
	users, _, err := c.ListUsers(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("id eq '%s'", userID)),
	)

	if err != nil {
		return "", err
	}

	if len(users) == 0 {
		return "", err
	}

	return users[0].UserName, nil
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

func (c *Client) GetGroup(ctx context.Context, groupID string) (*Group, error) {
	var res *Group
	u, err := c.cfg.ResolvePath(c.baseUrl, GroupsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, groupID)

	err = c.Get(ctx, u, &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) UpdateGroup(ctx context.Context, group *Group) error {
	u, err := c.cfg.ResolvePath(c.baseUrl, GroupsEndpoint)
	if err != nil {
		return fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, group.ID)

	b, err := json.Marshal(group)
	if err != nil {
		return err
	}

	return c.Put(ctx, u, bytes.NewReader(b), nil)
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

func (c *Client) FindGroupDisplayName(ctx context.Context, groupID string) (string, error) {
	groups, _, err := c.ListGroups(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("id eq '%s'", groupID)),
	)

	if err != nil {
		return "", err
	}

	if len(groups) == 0 {
		return "", err
	}

	return groups[0].DisplayName, nil
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

func (c *Client) GetServicePrincipal(ctx context.Context, servicePrincipalID string) (*ServicePrincipal, error) {
	var res *ServicePrincipal
	u, err := c.cfg.ResolvePath(c.baseUrl, ServicePrincipalsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, servicePrincipalID)

	err = c.Get(ctx, u, &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Client) UpdateServicePrincipal(ctx context.Context, servicePrincipal *ServicePrincipal) error {
	u, err := c.cfg.ResolvePath(c.baseUrl, ServicePrincipalsEndpoint)
	if err != nil {
		return fmt.Errorf("failed to prepare url to fetch groups: %w", err)
	}

	u.Path = fmt.Sprintf("%s/%s", u.Path, servicePrincipal.ID)

	b, err := json.Marshal(servicePrincipal)
	if err != nil {
		return err
	}

	return c.Put(ctx, u, bytes.NewReader(b), nil)
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

func (c *Client) FindServicePrincipalAppID(ctx context.Context, servicePrincipalID string) (string, error) {
	servicePrincipals, _, err := c.ListServicePrincipals(
		ctx,
		&PaginationVars{Count: 1},
		NewFilterVars(fmt.Sprintf("id eq '%s'", servicePrincipalID)),
	)

	if err != nil {
		return "", err
	}

	if len(servicePrincipals) == 0 {
		return "", err
	}

	return servicePrincipals[0].ApplicationID, nil
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

func (c *Client) prepareURLForWorkspaceMembers(params ...string) (*url.URL, error) {
	u := *c.baseUrl

	baseEndpoint := fmt.Sprintf("%s/%s", AccountsEndpoint, c.acc)
	path, err := url.JoinPath(baseEndpoint, params...)
	if err != nil {
		return nil, err
	}
	u.Path = path

	return &u, nil
}

func (c *Client) ListWorkspaceMembers(ctx context.Context, workspaceID int) ([]WorkspaceAssignment, error) {
	var res struct {
		Assignments []WorkspaceAssignment `json:"permission_assignments"`
	}

	id := strconv.Itoa(workspaceID)
	u, err := c.prepareURLForWorkspaceMembers(WorkspacesEndpoint, id, WorkspaceAssignmentsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch workspace members: %w", err)
	}

	err = c.Get(ctx, u, &res)
	if err != nil {
		return nil, err
	}

	return res.Assignments, nil
}

func (c *Client) CreateOrUpdateWorkspaceMember(ctx context.Context, workspaceID int64, principalID string) error {
	wID := strconv.Itoa(int(workspaceID))
	u, err := c.prepareURLForWorkspaceMembers(WorkspacesEndpoint, wID, WorkspaceAssignmentsEndpoint, "principals", principalID)
	if err != nil {
		return fmt.Errorf("failed to prepare url to create workspace member: %w", err)
	}

	payload := struct {
		Permission []string `json:"permissions"`
	}{
		Permission: []string{"USER"},
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return c.Put(ctx, u, bytes.NewReader(b), nil)
}

func (c *Client) RemoveWorkspaceMember(ctx context.Context, workspaceID int64, principalID string) error {
	wID := strconv.Itoa(int(workspaceID))

	u, err := c.prepareURLForWorkspaceMembers(WorkspacesEndpoint, wID, WorkspaceAssignmentsEndpoint, "principals", principalID)
	if err != nil {
		return fmt.Errorf("failed to prepare url to create workspace member: %w", err)
	}

	return c.Put(ctx, u, nil, nil)
}

func (c *Client) ListRuleSets(ctx context.Context, resourceType, resourceId string) ([]RuleSet, error) {
	var res struct {
		RuleSets []RuleSet `json:"grant_rules"`
		Etag     string    `json:"etag"`
	}

	u, err := c.cfg.ResolvePath(c.baseUrl, RuleSetsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare url to fetch rule sets: %w", err)
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

func (c *Client) UpdateRuleSets(ctx context.Context, resourceType, resourceId string, ruleSets []RuleSet) error {
	u, err := c.cfg.ResolvePath(c.baseUrl, RuleSetsEndpoint)
	if err != nil {
		return fmt.Errorf("failed to prepare url to fetch rule sets: %w", err)
	}

	resourcePayload, err := url.JoinPath("accounts", c.acc, resourceType, resourceId, "ruleSets", "default")
	if err != nil {
		return fmt.Errorf("failed to prepare resource payload: %w", err)
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

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return c.Put(ctx, u, bytes.NewReader(b), nil, NewNameVars(resourcePayload, c.etag))
}

func (c *Client) Get(ctx context.Context, urlAddress *url.URL, response interface{}, params ...Vars) error {
	return c.doRequest(ctx, urlAddress, http.MethodGet, nil, response, params...)
}

func (c *Client) Put(ctx context.Context, urlAddress *url.URL, body io.Reader, response interface{}, params ...Vars) error {
	return c.doRequest(ctx, urlAddress, http.MethodPut, body, response, params...)
}

func parseJSON(body io.Reader, res interface{}) error {
	// Databricks seems to return content-type text/plain even though it's json, so don't check content type
	if err := json.NewDecoder(body).Decode(res); err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}

	return nil
}

func WithJSONBody(body interface{}) uhttp.RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		buffer := new(bytes.Buffer)
		err := json.NewEncoder(buffer).Encode(body)
		if err != nil {
			return nil, nil, err
		}

		_, headers, err := uhttp.WithContentTypeJSONHeader()()
		if err != nil {
			return nil, nil, err
		}

		return buffer, headers, nil
	}
}

func WithJSONResponse(response interface{}) uhttp.DoOption {
	return func(resp *uhttp.WrapperResponse) error {
		return json.Unmarshal(resp.Body, response)
	}
}

func (c *Client) doRequest(ctx context.Context, urlAddress *url.URL, method string, body io.Reader, response interface{}, params ...Vars) error {
	var (
		req *http.Request
		err error
	)
	u, err := url.PathUnescape(urlAddress.String())
	if err != nil {
		return err
	}

	uri, err := url.Parse(u)
	if err != nil {
		return err
	}

	switch method {
	case http.MethodGet:
		req, err = c.httpClient.NewRequest(ctx,
			http.MethodGet,
			uri,
			uhttp.WithAcceptJSONHeader(),
		)
	case http.MethodPut:
		req, err = c.httpClient.NewRequest(ctx,
			http.MethodPut,
			uri,
			uhttp.WithAcceptJSONHeader(),
			uhttp.WithContentTypeJSONHeader(),
			WithJSONBody(body),
		)
	default:
		return fmt.Errorf("databricks-connector: invalid http method: %s", method)
	}

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
	resp, err := c.httpClient.Do(req, WithJSONResponse(&response))
	if err != nil {
		return fmt.Errorf("databricks-connector: error: %s %v", err.Error(), resp.Body)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var res struct {
			Detail  string `json:"detail"`
			Message string `json:"message"`
		}
		if err := parseJSON(resp.Body, &res); err != nil {
			return err
		}

		message := strings.Join([]string{res.Detail, res.Message}, " ")
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, message)
	}

	return nil
}
