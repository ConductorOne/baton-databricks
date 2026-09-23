package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

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

func (o *OAuth2) Apply(req *http.Request) {}

// TokenFederation authenticates via RFC 8693 token exchange.
// It presents an externally-issued JWT (e.g. SPIFFE JWT-SVID, Kubernetes SA token)
// and exchanges it for a short-lived Databricks OAuth token.
type TokenFederation struct {
	clientID    string
	tokenURL    string
	tokenFile   string
	staticToken string
}

func NewTokenFederation(accId, clientId, tokenFile, token, accountHostname string) *TokenFederation {
	return &TokenFederation{
		clientID:    clientId,
		tokenURL:    fmt.Sprintf("https://%s/oidc/accounts/%s/v1/token", accountHostname, accId),
		tokenFile:   tokenFile,
		staticToken: token,
	}
}

func (tf *TokenFederation) readSubjectToken() (string, error) {
	if tf.tokenFile != "" {
		data, err := os.ReadFile(tf.tokenFile)
		if err != nil {
			return "", fmt.Errorf("failed to read token file %s: %w", tf.tokenFile, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return tf.staticToken, nil
}

// Token performs an RFC 8693 token exchange and returns the resulting OAuth2 token.
func (tf *TokenFederation) Token() (*oauth2.Token, error) {
	subjectToken, err := tf.readSubjectToken()
	if err != nil {
		return nil, err
	}
	if subjectToken == "" {
		return nil, fmt.Errorf("empty subject token for workload identity federation")
	}

	form := url.Values{
		"grant_type":         {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"subject_token":      {subjectToken},
		"subject_token_type": {"urn:ietf:params:oauth:token-type:jwt"},
		"client_id":          {tf.clientID},
		"scope":              {"all-apis"},
	}

	resp, err := http.PostForm(tf.tokenURL, form)
	if err != nil {
		return nil, fmt.Errorf("token exchange request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read token exchange response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token exchange response: %w", err)
	}

	token := &oauth2.Token{
		AccessToken: tokenResp.AccessToken,
		TokenType:   tokenResp.TokenType,
	}
	if tokenResp.ExpiresIn > 0 {
		token.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	}

	return token, nil
}

func (tf *TokenFederation) GetClient(ctx context.Context) (*http.Client, error) {
	ts := oauth2.ReuseTokenSource(nil, tf)
	httpClient := oauth2.NewClient(ctx, ts)
	return httpClient, nil
}

func (tf *TokenFederation) Apply(req *http.Request) {}
