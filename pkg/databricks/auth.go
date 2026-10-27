package databricks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc/codes"
)

const (
	scopeAllAPIs = "all-apis"

	tokenPathOIDC     = "oidc"
	tokenPathAccounts = "accounts"
	tokenPathVersion  = "v1"
	tokenPathToken    = "token"

	//nolint:gosec // G101: these are the public RFC 8693 URNs, not credentials.
	grantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	//nolint:gosec // G101: these are the public RFC 8693 URNs, not credentials.
	subjectTokenTypeJWT = "urn:ietf:params:oauth:token-type:jwt"

	tokenExchangeTimeout = 30 * time.Second

	fallbackTokenLifetime = 5 * time.Minute
)

type tokenExchangeError struct {
	Code        string `json:"error"`
	Description string `json:"error_description"`
	RequestID   string `json:"request_id"`
}

type tokenExchangeResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

type Auth interface {
	Apply(req *http.Request)
	GetClient(ctx context.Context) (*http.Client, error)
}

type NoAuth struct{}

type OAuth2 struct {
	cfg *clientcredentials.Config
}

type TokenFederation struct {
	clientID  string
	tokenURL  string
	token     string
	tokenFile string
}

type federationTokenSource struct {
	ctx        context.Context
	auth       *TokenFederation
	httpClient *uhttp.BaseHttpClient
}

func (e *tokenExchangeError) Message() string {
	msg := e.Description
	if msg == "" {
		msg = e.Code
	}
	if e.RequestID != "" {
		msg = fmt.Sprintf("%s (request id %s)", msg, e.RequestID)
	}

	return msg
}

// accountTokenURL is the account-level token endpoint, shared by both auth methods.
func accountTokenURL(accountHostname, accountID string) string {
	u := &url.URL{Scheme: "https", Host: accountHostname}

	return u.JoinPath(tokenPathOIDC, tokenPathAccounts, accountID, tokenPathVersion, tokenPathToken).String()
}

// subjectToken returns the JWT. A file is read on every call, so a rotated token is
// picked up without restarting the connector.
func (t *TokenFederation) subjectToken() (string, error) {
	if t.tokenFile == "" {
		if t.token == "" {
			return "", uhttp.WrapErrors(codes.FailedPrecondition, "no workload identity token configured")
		}

		return t.token, nil
	}

	raw, err := os.ReadFile(t.tokenFile)
	if err != nil {
		return "", uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("failed to read workload identity token file %q", t.tokenFile), err)
	}

	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("workload identity token file %q is empty", t.tokenFile))
	}

	return token, nil
}

// tokenExpiry never returns the zero time, which oauth2 reads as a token that
// never expires.
func tokenExpiry(now time.Time, expiresIn int64, accessToken string) time.Time {
	if expiresIn > 0 {
		return now.Add(time.Duration(expiresIn) * time.Second)
	}

	if exp, ok := jwtExpiry(accessToken); ok && exp.After(now) {
		return exp
	}

	return now.Add(fallbackTokenLifetime)
}

// jwtExpiry reads the exp claim without verifying the signature. The claim only
// decides when to exchange again.
func jwtExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}

	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp <= 0 {
		return time.Time{}, false
	}

	return time.Unix(claims.Exp, 0), true
}

func (n *NoAuth) Apply(req *http.Request) {}

func (n *NoAuth) GetClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	return httpClient, nil
}

func NewOAuth2(accId, clientId, clientSecret, accountHostname string) *OAuth2 {
	return &OAuth2{
		cfg: &clientcredentials.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			TokenURL:     accountTokenURL(accountHostname, accId),
			Scopes:       []string{scopeAllAPIs},
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

// NewTokenFederation builds the federation auth. Exactly one of token or tokenFile
// carries the JWT.
func NewTokenFederation(accID, clientID, token, tokenFile, accountHostname string) *TokenFederation {
	return &TokenFederation{
		clientID:  clientID,
		tokenURL:  accountTokenURL(accountHostname, accID),
		token:     token,
		tokenFile: tokenFile,
	}
}

func (t *TokenFederation) GetClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	exchangeClient, err := uhttp.NewBaseHttpClientWithContext(ctx, httpClient)
	if err != nil {
		return nil, err
	}

	source := &federationTokenSource{ctx: ctx, auth: t, httpClient: exchangeClient}

	return oauth2.NewClient(ctx, oauth2.ReuseTokenSource(nil, source)), nil
}

func (t *TokenFederation) Apply(req *http.Request) {}

// Token exchanges the JWT for a short-lived Databricks OAuth token.
// POST https://{accountHostname}/oidc/accounts/{accountId}/v1/token, RFC 8693.
// It needs a service principal federation policy trusting the JWT's issuer, subject and audience.
// The context is the one the client was built with, because oauth2.TokenSource takes none.
func (s *federationTokenSource) Token() (*oauth2.Token, error) {
	subjectToken, err := s.auth.subjectToken()
	if err != nil {
		return nil, err
	}

	form := url.Values{
		"grant_type":         {grantTypeTokenExchange},
		"subject_token":      {subjectToken},
		"subject_token_type": {subjectTokenTypeJWT},
		"client_id":          {s.auth.clientID},
		"scope":              {scopeAllAPIs},
	}

	ctx, cancel := context.WithTimeout(s.ctx, tokenExchangeTimeout)
	defer cancel()

	endpoint, err := url.Parse(s.auth.tokenURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the token endpoint: %w", err)
	}

	req, err := s.httpClient.NewRequest(ctx, http.MethodPost, endpoint,
		uhttp.WithFormBody(form.Encode()),
		uhttp.WithAcceptJSONHeader(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build the token exchange request: %w", err)
	}

	var exchanged tokenExchangeResponse
	var exchangeErr tokenExchangeError

	resp, err := s.httpClient.Do(req,
		uhttp.WithJSONResponse(&exchanged),
		uhttp.WithErrorResponse(&exchangeErr),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if exchanged.AccessToken == "" {
		return nil, uhttp.WrapErrors(codes.FailedPrecondition, "token exchange returned no access token")
	}

	tokenType := exchanged.TokenType
	if tokenType == "" {
		tokenType = "Bearer"
	}

	return &oauth2.Token{
		AccessToken: exchanged.AccessToken,
		TokenType:   tokenType,
		Expiry:      tokenExpiry(time.Now(), exchanged.ExpiresIn, exchanged.AccessToken),
	}, nil
}
