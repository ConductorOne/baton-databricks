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

type NoAuth struct{}

func (n *NoAuth) Apply(req *http.Request) {}

func (n *NoAuth) GetClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	return httpClient, nil
}

type OAuth2 struct {
	cfg *clientcredentials.Config
}

func NewOAuth2(accId, clientId, clientSecret, accountHostname string) *OAuth2 {
	return &OAuth2{
		cfg: &clientcredentials.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			TokenURL:     fmt.Sprintf("https://%s/oidc/accounts/%s/v1/token", accountHostname, accId),
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
