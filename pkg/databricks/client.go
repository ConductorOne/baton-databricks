package databricks

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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
}

// hostMatches reports whether hostname equals suffix or sits under it at a DNS
// label boundary. Plain strings.HasSuffix would let evilazuredatabricks.net
// match azuredatabricks.net; requiring the leading dot prevents that.
func hostMatches(hostname, suffix string) bool {
	return hostname == suffix || strings.HasSuffix(hostname, "."+suffix)
}

func GetAccountHostname(hostname string) string {
	hostname = strings.ToLower(strings.TrimSuffix(hostname, "."))
	switch {
	case hostMatches(hostname, azureHost):
		return "accounts." + azureHost
	case hostMatches(hostname, gcpHost):
		return "accounts." + gcpHost
	case hostMatches(hostname, defaultHost):
		return "accounts." + defaultHost
	default:
		// Unknown hosts have no canonical account suffix, so prefix as given.
		return "accounts." + hostname
	}
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

		// OAuth is the only auth method, and Validate hard-fails when the account
		// plane is unreachable, so availability is an invariant rather than
		// something callers must wait for Validate to establish.
		isAccAPIAvailable: true,
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

// IsWorkspaceNameExcluded reports whether deploymentName matches the
// databricks-exclude-workspaces set. Checks the name only, not via
// isWorkspaceExcluded: that also matches on ID, and a zero-value ID here would
// let an exclude entry of "0" match every workspace.
func (c *Client) IsWorkspaceNameExcluded(deploymentName string) bool {
	if len(c.excludeWorkspaces) == 0 {
		return false
	}
	_, ok := c.excludeWorkspaces[strings.ToLower(deploymentName)]
	return ok
}

func (c *Client) workspaceUrl(workspaceId string) *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   workspaceId + "." + c.baseUrl.Host,
	}
}

func (c *Client) IsAccountAPIAvailable() bool {
	return c.isAccAPIAvailable
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
		return nil, 0, ratelimitData, nameWorkspace403Remedy(workspaceId, err)
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
		return nil, 0, ratelimitData, nameWorkspace403Remedy(workspaceId, err)
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
		return nil, 0, ratelimitData, nameWorkspace403Remedy(workspaceId, err)
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
		return nil, ratelimitData, nameWorkspace403Remedy(workspaceId, err)
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

// CreateOrUpdateWorkspaceMember grants the "USER" workspace permission to a
// principal.
//
// The account-level permission-assignments endpoint
// (PUT .../permissionassignments/principals/{principal_id}) fully replaces the
// principal's permissions array - Databricks' docs for this endpoint state
// that omitting (or sending an unsupported) "permissions" value has "the same
// effect as providing an empty list, which will result in the deletion of all
// permissions for the principal." There is no PATCH for this specific
// account-level endpoint (a PATCH exists only for the distinct
// workspace-scoped /api/2.0/identity/workspace-assignments/{principal_id}
// identity API, which is a different endpoint/auth scope). So granting
// "member" naively by PUTting {"permissions":["USER"]} silently destroys an
// existing ADMIN assignment.
//
// To stay safe against that full-replacement semantics, read the principal's
// current permissions first and union "USER" into them before writing the
// full set back, so an existing ADMIN is preserved.
func (c *Client) CreateOrUpdateWorkspaceMember(
	ctx context.Context,
	workspaceId string,
	principalId string,
) (
	*v2.RateLimitDescription,
	error,
) {
	current, ratelimitData, err := c.currentWorkspacePermissions(ctx, workspaceId, principalId)
	if err != nil {
		return ratelimitData, fmt.Errorf("failed to list current workspace permission assignments: %w", err)
	}

	permissionSet := map[string]struct{}{WorkspaceUserPermission: {}}
	for _, permission := range current {
		permissionSet[permission] = struct{}{}
	}

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, c.accountId, workspaceId), "principals", principalId)
	payload := struct {
		Permissions []string `json:"permissions"`
	}{
		Permissions: sortedPermissions(permissionSet),
	}

	putRatelimitData, err := c.Put(ctx, u, payload, nil)
	if putRatelimitData != nil {
		ratelimitData = putRatelimitData
	}

	return ratelimitData, err
}

// RemoveWorkspaceMember revokes the "USER" workspace permission from a
// principal.
//
// This endpoint is the same full-replacement PUT described in
// CreateOrUpdateWorkspaceMember, so unconditionally PUTting an empty/nil body
// would strip every permission the principal holds - including ADMIN - when
// only the "member" (USER) grant should be revoked. Read the principal's
// current permissions first, drop "USER", and write back whatever remains
// (ADMIN, if the principal independently holds it).
func (c *Client) RemoveWorkspaceMember(
	ctx context.Context,
	workspaceId string,
	principalId string,
) (*v2.RateLimitDescription, error) {
	current, ratelimitData, err := c.currentWorkspacePermissions(ctx, workspaceId, principalId)
	if err != nil {
		return ratelimitData, fmt.Errorf("failed to list current workspace permission assignments: %w", err)
	}

	permissionSet := make(map[string]struct{}, len(current))
	for _, permission := range current {
		if permission == WorkspaceUserPermission {
			continue
		}
		permissionSet[permission] = struct{}{}
	}

	u := c.accountBaseUrl.JoinPath(fmt.Sprintf(accountWorkspaceAssignmentsEndpoint, c.accountId, workspaceId), "principals", principalId)
	payload := struct {
		Permissions []string `json:"permissions"`
	}{
		Permissions: sortedPermissions(permissionSet),
	}

	putRatelimitData, err := c.Put(ctx, u, payload, nil)
	if putRatelimitData != nil {
		ratelimitData = putRatelimitData
	}

	return ratelimitData, err
}

// currentWorkspacePermissions returns the permissions the given principal
// currently holds on the workspace. There is no API to fetch a single
// principal's assignment directly, so this lists every assignment on the
// workspace and matches by principal ID.
func (c *Client) currentWorkspacePermissions(
	ctx context.Context,
	workspaceId string,
	principalId string,
) (
	[]string,
	*v2.RateLimitDescription,
	error,
) {
	assignments, ratelimitData, err := c.ListWorkspaceMembers(ctx, workspaceId)
	if err != nil {
		return nil, ratelimitData, err
	}

	for _, assignment := range assignments {
		if assignment.Principal == nil {
			continue
		}
		if strconv.Itoa(assignment.Principal.ID) != principalId {
			continue
		}

		return assignment.Permissions, ratelimitData, nil
	}

	return nil, ratelimitData, nil
}

// sortedPermissions returns the set's members in a deterministic (sorted)
// order so requests and tests are stable.
func sortedPermissions(permissionSet map[string]struct{}) []string {
	permissions := make([]string, 0, len(permissionSet))
	for permission := range permissionSet {
		permissions = append(permissions, permission)
	}
	sort.Strings(permissions)

	return permissions
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
		return nil, ratelimitData, nameWorkspace403Remedy(workspaceId, err)
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
