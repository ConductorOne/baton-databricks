package databricks

import (
	"fmt"
	"net/url"
)

type Config interface {
	BaseUrl() *url.URL
	ResolvePath(base *url.URL, endpoint string) (*url.URL, error)
}

// Account Config for account API.
type AccountConfig struct {
	accountId       string
	accountHostname string
}

func NewAccountConfig(accountHostname, accountId string) *AccountConfig {
	if accountHostname == "" {
		accountHostname = AccountBaseHost
	}
	return &AccountConfig{
		accountId,
		accountHostname,
	}
}

func (c *AccountConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   c.accountHostname,
	}
}

func (c AccountConfig) ResolvePath(base *url.URL, endpoint string) (*url.URL, error) {
	u := *base

	baseEndpoint := fmt.Sprintf("%s/%s", AccountsEndpoint, c.accountId)

	var pathParts []string

	switch endpoint {
	case UsersEndpoint, GroupsEndpoint, ServicePrincipalsEndpoint:
		pathParts = []string{baseEndpoint, SCIMEndpoint, endpoint}
	case RolesEndpoint, RuleSetsEndpoint:
		pathParts = []string{PreviewEndpoint, baseEndpoint, AccessControlEndpoint, endpoint}
	case WorkspacesEndpoint:
		pathParts = []string{baseEndpoint, endpoint}
	default:
		return nil, fmt.Errorf("unknown endpoint %s", endpoint)
	}

	path, err := url.JoinPath(pathParts[0], pathParts[1:]...)
	if err != nil {
		return nil, err
	}

	u.Path = path

	return &u, nil
}

// Workspace Config for workspace API.
type WorkspaceConfig struct {
	workspace string
	hostname  string
}

func NewWorkspaceConfig(hostname, accountId, workspace string) *WorkspaceConfig {
	if hostname == "" {
		hostname = WorkspaceBaseHost
	}
	return &WorkspaceConfig{
		workspace,
		hostname,
	}
}

func (c *WorkspaceConfig) Workspace() string {
	return c.workspace
}

func (c *WorkspaceConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf(c.hostname, c.workspace),
	}
}

func (c WorkspaceConfig) ResolvePath(base *url.URL, endpoint string) (*url.URL, error) {
	u := *base

	var pathParts []string

	switch endpoint {
	case UsersEndpoint, GroupsEndpoint, ServicePrincipalsEndpoint:
		pathParts = []string{SCIMEndpoint, endpoint}
	case RolesEndpoint, RuleSetsEndpoint:
		pathParts = []string{AccountsEndpoint, AccessControlEndpoint, endpoint}
	default:
		return nil, fmt.Errorf("unknown endpoint %s", endpoint)
	}

	path, err := url.JoinPath(pathParts[0], pathParts[1:]...)
	if err != nil {
		return nil, err
	}

	u.Path = path

	return &u, nil
}
