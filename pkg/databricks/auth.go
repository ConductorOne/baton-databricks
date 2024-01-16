package databricks

import (
	"fmt"
	"net/http"
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
