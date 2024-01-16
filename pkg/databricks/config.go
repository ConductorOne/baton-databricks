package databricks

import (
	"fmt"
	"net/http"
	"net/url"
)

type Config interface {
	BaseUrl() *url.URL
	ResolvePath(base *url.URL, endpoint string) (*url.URL, error)
	ApplyAuth(req *http.Request)
	IsScopedToWorkspace() bool
}

// Account Config for account API
type AccountConfig struct {
	acc  string
	auth Auth
}

func NewAccountConfig(acc string, auth Auth) *AccountConfig {
	return &AccountConfig{
		acc,
		auth,
	}
}

func (c *AccountConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   AccountBaseHost,
	}
}

func (c *AccountConfig) ApplyAuth(req *http.Request) {
	c.auth.Apply(req)
}

func (c *AccountConfig) IsScopedToWorkspace() bool {
	return false
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

// Workspace Config for workspace API
type WorkspaceConfig struct {
	workspace string
	auth      Auth
}

func NewWorkspaceConfig(acc, workspace string, auth Auth) *WorkspaceConfig {
	return &WorkspaceConfig{
		workspace,
		auth,
	}
}

func (c *WorkspaceConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf(WorkspaceBaseHost, c.workspace),
	}
}

func (c *WorkspaceConfig) IsScopedToWorkspace() bool {
	return true
}

func (c *WorkspaceConfig) ApplyAuth(req *http.Request) {
	c.auth.Apply(req)
}

func (c WorkspaceConfig) ResolvePath(base *url.URL, endpoint string) (*url.URL, error) {
	u := *base

	var pathParts []string

	switch endpoint {
	case UsersEndpoint, GroupsEndpoint, ServicePrincipalsEndpoint:
		pathParts = []string{SCIMEndpoint, endpoint}
	case RolesEndpoint:
		pathParts = []string{AccountsEndpoint, AccessControlEndpoint, RolesEndpoint}
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
