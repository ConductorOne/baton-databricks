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
	acc string
}

func NewAccountConfig(acc string) *AccountConfig {
	return &AccountConfig{acc}
}

func (c *AccountConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   AccountBaseHost,
	}
}

func (c AccountConfig) ResolvePath(base *url.URL, endpoint string) (*url.URL, error) {
	u := *base

	baseEndpoint := fmt.Sprintf("%s/%s", AccountsEndpoint, c.acc)

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
}

func NewWorkspaceConfig(acc, workspace string) *WorkspaceConfig {
	return &WorkspaceConfig{workspace}
}

func (c *WorkspaceConfig) Workspace() string {
	return c.workspace
}

func (c *WorkspaceConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf(WorkspaceBaseHost, c.workspace),
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
