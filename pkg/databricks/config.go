package databricks

import (
	"fmt"
	"net/http"
	"net/url"
)

type Auth interface {
	Apply(req *http.Request)
}

type TokenAuth struct {
	Token string
}

func NewTokenAuth(token string) *TokenAuth {
	return &TokenAuth{
		Token: token,
	}
}

func (t *TokenAuth) Apply(req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.Token))
}

type BasicAuth struct {
	Username string
	Password string
}

func NewBasicAuth(username, password string) *BasicAuth {
	return &BasicAuth{
		Username: username,
		Password: password,
	}
}

func (b *BasicAuth) Apply(req *http.Request) {
	req.SetBasicAuth(b.Username, b.Password)
}

type WorkspaceConfig struct {
	baseHost string
	auth     Auth
}

func NewWorkspaceConfig(baseHost string, auth Auth) *WorkspaceConfig {
	return &WorkspaceConfig{
		baseHost,
		auth,
	}
}

func (c *WorkspaceConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   c.baseHost,
	}
}

func (c *WorkspaceConfig) ApplyAuth(req *http.Request) {
	c.auth.Apply(req)
}

type AccountConfig struct {
	accountId string
	auth      Auth
}

func NewAccountConfig(accountId string, auth Auth) *AccountConfig {
	return &AccountConfig{
		accountId,
		auth,
	}
}

func (c *AccountConfig) BaseUrl() *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   AccountBaseHost,
		Path:   fmt.Sprintf(AccountEndpoint, c.accountId),
	}
}

func (c *AccountConfig) ApplyAuth(req *http.Request) {
	c.auth.Apply(req)
}

type Config interface {
	BaseUrl() *url.URL
	ApplyAuth(req *http.Request)
}
