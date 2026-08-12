package databricks

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const (
	defaultHost = "cloud.databricks.com" // aws
	azureHost   = "azuredatabricks.net"
	gcpHost     = "gcp.databricks.com"

	// Some of these are case sensitive.
	usersEndpoint             = "/api/2.0/preview/scim/v2/Users"
	groupsEndpoint            = "/api/2.0/preview/scim/v2/Groups"
	servicePrincipalsEndpoint = "/api/2.0/preview/scim/v2/ServicePrincipals"
	rolesEndpoint             = "/api/2.0/preview/accounts/access-control/assignable-roles"
	ruleSetsEndpoint          = "/api/2.0/preview/accounts/access-control/rule-sets"

	accountUsersEndpoint             = "/api/2.0/accounts/%s/scim/v2/Users"
	accountGroupsEndpoint            = "/api/2.0/accounts/%s/scim/v2/Groups"
	accountServicePrincipalsEndpoint = "/api/2.0/accounts/%s/scim/v2/ServicePrincipals"
	accountRolesEndpoint             = "/api/2.0/preview/accounts/%s/access-control/assignable-roles"
	accountRuleSetsEndpoint          = "/api/2.0/preview/accounts/%s/access-control/rule-sets"

	accountWorkspacesEndpoint           = "/api/2.0/accounts/%s/workspaces"
	accountWorkspaceAssignmentsEndpoint = "/api/2.0/accounts/%s/workspaces/%s/permissionassignments"
)

type Client struct {
	httpClient        *uhttp.BaseHttpClient
	baseUrl           *url.URL
	accountBaseUrl    *url.URL
	auth              Auth
	etag              string
	accountId         string
	excludeWorkspaces map[string]struct{}

	isAccAPIAvailable bool
	isWSAPIAvailable  bool
}

func GetAccountHostname(hostname string) string {
	if strings.HasSuffix(hostname, azureHost) {
		return "accounts." + azureHost
	} else if strings.HasSuffix(hostname, gcpHost) {
		return "accounts." + gcpHost
	}
	return "accounts." + hostname
}

func NewClient(ctx context.Context, httpClient *http.Client, hostname, accountHostname, accountID, baseURL string, auth Auth, excludeWorkspaces []string) (*Client, error) {
	var baseUrl *url.URL
	var err error

	excludeSet := make(map[string]struct{}, len(excludeWorkspaces))
	for _, w := range excludeWorkspaces {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		excludeSet[strings.ToLower(w)] = struct{}{}
	}

	// If baseURL is provided, use it directly (for testing)
	if baseURL != "" {
		baseUrl, err = url.Parse(baseURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse base URL: %w", err)
		}
	} else {
		baseUrl = &url.URL{
			Scheme: "https",
			Host:   hostname,
		}
	}

	accountBaseUrl := &url.URL{
		Scheme: "https",
		Host:   accountHostname,
	}

	cli, err := uhttp.NewBaseHttpClientWithContext(ctx, httpClient)
	return &Client{
		httpClient:        cli,
		auth:              auth,
		accountId:         accountID,
		accountBaseUrl:    accountBaseUrl,
		baseUrl:           baseUrl,
		excludeWorkspaces: excludeSet,
	}, err
}

// isWorkspaceExcluded case-insensitively matches w against the exclude set (name,
// deployment name, or ID) — Databricks lowercases deployment_name but not workspace_name.
// Returns every exclude-set key that matched, since a workspace can match more than one.
func (c *Client) isWorkspaceExcluded(w Workspace) ([]string, bool) {
	if len(c.excludeWorkspaces) == 0 {
		return nil, false
	}
	var keys []string
	for _, key := range []string{w.Name, w.DeploymentName, strconv.Itoa(w.ID)} {
		key = strings.ToLower(key)
		if _, ok := c.excludeWorkspaces[key]; ok {
			keys = append(keys, key)
		}
	}
	return keys, len(keys) > 0
}

// IsWorkspaceExcluded reports whether deploymentName matches the
// databricks-exclude-workspaces set.
func (c *Client) IsWorkspaceExcluded(deploymentName string) bool {
	_, ok := c.isWorkspaceExcluded(Workspace{DeploymentName: deploymentName})
	return ok
}

func (c *Client) workspaceUrl(workspaceId string) *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   workspaceId + "." + c.baseUrl.Host,
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

func (c *Client) IsTokenAuth() bool {
	_, ok := c.auth.(*TokenAuth)
	return ok
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
	workspaceId string,
	vars ...Vars,
) (
	[]User,
	uint,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountUsersEndpoint, c.accountId))
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(usersEndpoint)
	}

	var res ListResponse[User]
	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, ratelimitData, err
	}

	return res.Resources, res.Total, ratelimitData, nil
}

func (c *Client) GetUser(
	ctx context.Context,
	workspaceId string,
	userId string,
) (
	*User,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountUsersEndpoint, c.accountId), userId)
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(usersEndpoint, userId)
	}

	var res *User
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res, ratelimitData, nil
}

func (c *Client) UpdateUser(ctx context.Context, workspaceId string, user *User) (*v2.RateLimitDescription, error) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountUsersEndpoint, c.accountId), user.ID)
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(usersEndpoint, user.ID)
	}

	return c.Put(ctx, u, user, nil)
}

func (c *Client) FindUserID(
	ctx context.Context,
	workspaceId string,
	username string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	users, _, ratelimitData, err := c.ListUsers(
		ctx,
		workspaceId,
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
	workspaceId string,
	userID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	users, _, ratelimitData, err := c.ListUsers(
		ctx,
		workspaceId,
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
	workspaceId string,
	vars ...Vars,
) (
	[]Group,
	uint,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountGroupsEndpoint, c.accountId))
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(groupsEndpoint)
	}

	var res ListResponse[Group]
	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, ratelimitData, err
	}

	return res.Resources, res.Total, ratelimitData, nil
}

func (c *Client) GetGroup(ctx context.Context, workspaceId, groupId string, vars ...Vars) (
	*Group,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountGroupsEndpoint, c.accountId), groupId)
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(groupsEndpoint, groupId)
	}

	var res *Group
	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res, ratelimitData, nil
}

func (c *Client) UpdateGroup(ctx context.Context, workspaceId string, group *Group) (*v2.RateLimitDescription, error) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountGroupsEndpoint, c.accountId), group.ID)
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(groupsEndpoint, group.ID)
	}

	return c.Put(ctx, u, group, nil)
}

func (c *Client) FindGroupID(
	ctx context.Context,
	workspaceId string,
	displayName string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	groups, _, ratelimitData, err := c.ListGroups(
		ctx,
		workspaceId,
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
	workspaceId,
	groupID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	groups, _, ratelimitData, err := c.ListGroups(
		ctx,
		workspaceId,
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
	workspaceId string,
	vars ...Vars,
) (
	[]ServicePrincipal,
	uint,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountServicePrincipalsEndpoint, c.accountId))
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(servicePrincipalsEndpoint)
	}

	var res ListResponse[ServicePrincipal]
	ratelimitData, err := c.Get(ctx, u, &res, vars...)
	if err != nil {
		return nil, 0, ratelimitData, err
	}

	return res.Resources, res.Total, ratelimitData, nil
}

func (c *Client) GetServicePrincipal(
	ctx context.Context,
	workspaceId string,
	servicePrincipalID string,
) (
	*ServicePrincipal,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountServicePrincipalsEndpoint, c.accountId), servicePrincipalID)
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(servicePrincipalsEndpoint, servicePrincipalID)
	}

	var res *ServicePrincipal
	ratelimitData, err := c.Get(ctx, u, &res)
	return res, ratelimitData, err
}

func (c *Client) UpdateServicePrincipal(
	ctx context.Context,
	workspaceId string,
	servicePrincipal *ServicePrincipal,
) (
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountServicePrincipalsEndpoint, c.accountId), servicePrincipal.ID)
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(servicePrincipalsEndpoint, servicePrincipal.ID)
	}
	return c.Put(ctx, u, servicePrincipal, nil)
}

func (c *Client) FindServicePrincipalID(
	ctx context.Context,
	workspaceId string,
	appID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	servicePrincipals, _, ratelimitData, err := c.ListServicePrincipals(
		ctx,
		workspaceId,
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
	workspaceId string,
	servicePrincipalID string,
) (
	string,
	*v2.RateLimitDescription,
	error,
) {
	servicePrincipals, _, ratelimitData, err := c.ListServicePrincipals(
		ctx,
		workspaceId,
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
	workspaceId string,
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

	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountRolesEndpoint, c.accountId))
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(rolesEndpoint)
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

// ListWorkspaces returns the account's workspaces, minus any excluded by the
// databricks-exclude-workspaces field. GET /accounts/{id}/workspaces (unpaginated).
func (c *Client) ListWorkspaces(
	ctx context.Context,
) (
	[]Workspace,
	*v2.RateLimitDescription,
	error,
) {
	var res []Workspace

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspacesEndpoint, c.accountId))
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	if len(c.excludeWorkspaces) == 0 {
		return res, ratelimitData, nil
	}

	matched := make(map[string]struct{}, len(c.excludeWorkspaces))
	filtered := make([]Workspace, 0, len(res))
	for _, w := range res {
		if keys, ok := c.isWorkspaceExcluded(w); ok {
			for _, key := range keys {
				matched[key] = struct{}{}
			}
			continue
		}
		filtered = append(filtered, w)
	}

	unmatched := make([]string, 0, len(c.excludeWorkspaces))
	for key := range c.excludeWorkspaces {
		if _, ok := matched[key]; !ok {
			unmatched = append(unmatched, key)
		}
	}
	ctxzap.Extract(ctx).Debug("databricks: applied workspace exclusions",
		zap.Int("excluded", len(res)-len(filtered)),
		zap.Strings("unmatched_exclude_entries", unmatched),
	)

	return filtered, ratelimitData, nil
}

func (c *Client) ListWorkspaceMembers(
	ctx context.Context,
	workspaceId string,
) (
	[]WorkspaceAssignment,
	*v2.RateLimitDescription,
	error,
) {
	var res struct {
		Assignments []WorkspaceAssignment `json:"permission_assignments"`
	}

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, c.accountId, workspaceId))
	ratelimitData, err := c.Get(ctx, u, &res)
	if err != nil {
		return nil, ratelimitData, err
	}

	return res.Assignments, ratelimitData, nil
}

func (c *Client) CreateOrUpdateWorkspaceMember(
	ctx context.Context,
	workspaceId string,
	principalId string,
) (
	*v2.RateLimitDescription,
	error,
) {
	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, c.accountId, workspaceId), "principals", principalId)
	payload := struct {
		Permission []string `json:"permissions"`
	}{
		Permission: []string{"USER"},
	}

	return c.Put(ctx, u, payload, nil)
}

func (c *Client) RemoveWorkspaceMember(
	ctx context.Context,
	workspaceId string,
	principalId string,
) (*v2.RateLimitDescription, error) {
	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, c.accountId, workspaceId), "principals", principalId)
	return c.Put(ctx, u, nil, nil)
}

func (c *Client) ListRuleSets(
	ctx context.Context,
	workspaceId string,
	resourceType string,
	resourceId string,
) (
	[]RuleSet,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountRuleSetsEndpoint, c.accountId))
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(ruleSetsEndpoint)
	}

	resourcePayload, err := url.JoinPath("accounts", c.accountId, resourceType, resourceId, "ruleSets", "default")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare resource payload: %w", err)
	}

	var res struct {
		RuleSets []RuleSet `json:"grant_rules"`
		Etag     string    `json:"etag"`
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
	workspaceId, resourceType, resourceId string,
	ruleSets []RuleSet,
) (
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountRuleSetsEndpoint, c.accountId))
	} else {
		u = c.workspaceUrl(workspaceId).JoinPath(ruleSetsEndpoint)
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

type Name struct {
	GivenName  string `json:"givenName"`
	FamilyName string `json:"familyName"`
}

type CreateUserBody struct {
	// this is actually the email:
	//	https://docs.databricks.com/api/account/accountusers/create#userName
	UserName    string `json:"userName"`
	Name        Name   `json:"name"`
	Id          string `json:"id,omitempty"` // Not currently supported, reserved for future use.
	Active      bool   `json:"active"`
	DisplayName string `json:"displayName"`
}

type CreateUserResponse CreateUserBody

// https://docs.databricks.com/api/account/accountusers/create
func (c *Client) CreateUser(
	ctx context.Context,
	workspaceId string,
	body *CreateUserBody,
) (
	*CreateUserResponse,
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountUsersEndpoint, c.accountId))
	} else {
		return nil, nil, fmt.Errorf("creating users is not implemented yet for workspaces")
	}

	var res CreateUserResponse
	ratelimitData, err := c.Post(ctx, u, body, &res)
	if err != nil {
		return nil, ratelimitData, err
	}
	return &res, ratelimitData, nil
}

// https://docs.databricks.com/api/account/accountusers/delete
func (c *Client) DeleteUser(
	ctx context.Context,
	workspaceId string,
	userId string,
) (
	*v2.RateLimitDescription,
	error,
) {
	var u *url.URL
	if workspaceId == "" {
		u = c.accountBaseUrl.JoinPath(fmt.Sprintf(accountUsersEndpoint+"/%s", c.accountId, userId))
	} else {
		return nil, fmt.Errorf("deleting users is not implemented yet for workspaces")
	}

	ratelimitData, err := c.Delete(ctx, u)
	if err != nil {
		return ratelimitData, err
	}
	return ratelimitData, nil
}
