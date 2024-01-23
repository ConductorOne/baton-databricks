package databricks

import (
	"context"
	"fmt"
	"net/http"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type Auth interface {
	Apply(req *http.Request)
	GetClient(ctx context.Context) (*http.Client, error)
}

type TokenAuth struct {
	Tokens           map[string]string
	CurrentWorkspace string
}

func NewTokenAuth(workspaces, tokens []string) *TokenAuth {
	tokensMap := make(map[string]string)

	for i, workspace := range workspaces {
		tokensMap[workspace] = tokens[i]
	}

	return &TokenAuth{
		Tokens:           tokensMap,
		CurrentWorkspace: workspaces[0],
	}
}

func (t *TokenAuth) Apply(req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.Tokens[t.CurrentWorkspace]))
}

func (t *TokenAuth) SetWorkspace(workspace string) {
	t.CurrentWorkspace = workspace
}

func (t *TokenAuth) GetClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	return httpClient, nil
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

func (b *BasicAuth) GetClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	return httpClient, nil
}

type OAuth2 struct {
	cfg *clientcredentials.Config
}

func NewOAuth2(accId, clientId, clientSecret string) *OAuth2 {
	return &OAuth2{
		cfg: &clientcredentials.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			TokenURL:     fmt.Sprintf("https://accounts.cloud.databricks.com/oidc/accounts/%s/v1/token", accId),
			Scopes:       []string{"all-apis"},
		},
	}
}

func (o *OAuth2) GetClient(ctx context.Context) (*http.Client, error) {
	ts := o.cfg.TokenSource(ctx)
	httpClient := oauth2.NewClient(ctx, ts)

	return httpClient, nil
}

func (o *OAuth2) Apply(req *http.Request) {
	// No need to set the Authorization header here, the oauth2 client does it automatically
}
