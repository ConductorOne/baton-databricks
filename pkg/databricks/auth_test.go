package databricks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// exchangeServer serves the token endpoint and records the last form it received.
func exchangeServer(t *testing.T, handler func(form url.Values) (int, string)) (*httptest.Server, *url.Values) {
	t.Helper()

	received := &url.Values{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		*received = r.PostForm

		status, body := handler(r.PostForm)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if _, err := w.Write([]byte(body)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	return server, received
}

func newTestTokenSource(t *testing.T, tokenURL, token, tokenFile string) *federationTokenSource {
	t.Helper()

	ctx := context.Background()
	httpClient, err := uhttp.NewBaseHttpClientWithContext(ctx, &http.Client{})
	if err != nil {
		t.Fatalf("NewBaseHttpClientWithContext: %v", err)
	}

	return &federationTokenSource{
		ctx:        ctx,
		auth:       &TokenFederation{clientID: "sp-client-id", tokenURL: tokenURL, token: token, tokenFile: tokenFile},
		httpClient: httpClient,
	}
}

func writeTokenFile(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "token.jwt")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	return path
}

func TestTokenFederationExchange(t *testing.T) {
	server, received := exchangeServer(t, func(url.Values) (int, string) {
		return http.StatusOK, `{"access_token":"databricks-token","token_type":"Bearer","expires_in":3600}`
	})

	source := newTestTokenSource(t, server.URL, "subject-jwt", "")

	token, err := source.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}

	if token.AccessToken != "databricks-token" {
		t.Errorf("AccessToken = %q, want %q", token.AccessToken, "databricks-token")
	}
	if token.TokenType != "Bearer" {
		t.Errorf("TokenType = %q, want %q", token.TokenType, "Bearer")
	}
	// A zero Expiry passes token.Valid(), so assert the value itself.
	if d := time.Until(token.Expiry); d < 55*time.Minute || d > time.Hour {
		t.Errorf("token expires in %v, want about an hour from now", d)
	}

	want := map[string]string{
		"grant_type":         grantTypeTokenExchange,
		"subject_token":      "subject-jwt",
		"subject_token_type": subjectTokenTypeJWT,
		"client_id":          "sp-client-id",
		"scope":              scopeAllAPIs,
	}
	for key, value := range want {
		if got := received.Get(key); got != value {
			t.Errorf("form %q = %q, want %q", key, got, value)
		}
	}
}

// A rotated token file must be picked up without restarting the connector.
func TestTokenFederationRereadsTokenFile(t *testing.T) {
	server, received := exchangeServer(t, func(url.Values) (int, string) {
		return http.StatusOK, `{"access_token":"databricks-token","expires_in":3600}`
	})

	path := writeTokenFile(t, "first-jwt\n")
	source := newTestTokenSource(t, server.URL, "", path)

	if _, err := source.Token(); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if got := received.Get("subject_token"); got != "first-jwt" {
		t.Fatalf("first subject_token = %q, want %q (trailing newline must be trimmed)", got, "first-jwt")
	}

	if err := os.WriteFile(path, []byte("rotated-jwt"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := source.Token(); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := received.Get("subject_token"); got != "rotated-jwt" {
		t.Errorf("second subject_token = %q, want %q", got, "rotated-jwt")
	}
}

func TestTokenFederationExchangeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{
			name:    "rejected by the federation policy",
			status:  http.StatusUnauthorized,
			body:    `{"error":"invalid_request","error_description":"no matching federation policy"}`,
			wantErr: "no matching federation policy",
		},
		{
			name:    "response without an access token",
			status:  http.StatusOK,
			body:    `{"token_type":"Bearer","expires_in":3600}`,
			wantErr: "no access token",
		},
		{
			name:    "response that is not JSON",
			status:  http.StatusOK,
			body:    `<html>gateway</html>`,
			wantErr: "unmarshal json response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, _ := exchangeServer(t, func(url.Values) (int, string) {
				return tt.status, tt.body
			})

			_, err := newTestTokenSource(t, server.URL, "subject-jwt", "").Token()
			if err == nil {
				t.Fatal("Token succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// A failed exchange must reach the syncer with a gRPC code. An unclassified error
// reads as Unknown, so a throttled token endpoint would end the sync instead of retrying.
func TestTokenFederationExchangeErrorsAreClassified(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		want   codes.Code
	}{
		{"rejected by the federation policy", http.StatusBadRequest, codes.InvalidArgument},
		{"bad credentials", http.StatusUnauthorized, codes.Unauthenticated},
		{"forbidden", http.StatusForbidden, codes.PermissionDenied},
		{"throttled stays retryable", http.StatusTooManyRequests, codes.Unavailable},
		{"server error stays retryable", http.StatusServiceUnavailable, codes.Unavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, _ := exchangeServer(t, func(url.Values) (int, string) {
				return tt.status, `{"error":"invalid_grant","error_description":"no matching federation policy"}`
			})

			_, err := newTestTokenSource(t, server.URL, "subject-jwt", "").Token()
			if err == nil {
				t.Fatal("Token succeeded, want an error")
			}

			if got := status.Code(err); got != tt.want {
				t.Errorf("grpc code = %v, want %v", got, tt.want)
			}

			// wrapTransportAuthError runs on this error at the API call sites and must
			// not downgrade the code.
			if got := status.Code(wrapTransportAuthError(err)); got != tt.want {
				t.Errorf("grpc code after wrapTransportAuthError = %v, want %v", got, tt.want)
			}

			if !strings.Contains(err.Error(), "no matching federation policy") {
				t.Errorf("error = %q, want it to keep the vendor's reason", err)
			}
		})
	}
}

func TestTokenFederationSubjectToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		wantErr string
	}{
		{"missing file", filepath.Join(t.TempDir(), "absent.jwt"), "failed to read"},
		{"empty file", writeTokenFile(t, "   \n"), "is empty"},
		{"no credential at all", "", "no workload identity token configured"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newTestTokenSource(t, "", "", tt.file).Token()
			if err == nil {
				t.Fatal("Token succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("grpc code = %v, want %v", got, codes.FailedPrecondition)
			}
		})
	}
}

// A zero Expiry reads as "never expires" in oauth2, pinning the first exchange.
func TestTokenExpiry(t *testing.T) {
	t.Parallel()

	// A token that carries no exp claim to fall back to.
	const notAJWT = "an-opaque-string"

	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		expiresIn   int64
		accessToken string
		want        time.Time
	}{
		{
			name:      "expires_in wins",
			expiresIn: 3600,
			want:      now.Add(time.Hour),
		},
		{
			name:        "falls back to the exp claim",
			accessToken: testJWT(t, now.Add(30*time.Minute)),
			want:        now.Add(30 * time.Minute),
		},
		{
			name:        "falls back to a short lifetime for an opaque token",
			accessToken: notAJWT,
			want:        now.Add(fallbackTokenLifetime),
		},
		{
			name:        "falls back to a short lifetime when the exp claim is in the past",
			accessToken: testJWT(t, now.Add(-time.Hour)),
			want:        now.Add(fallbackTokenLifetime),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tokenExpiry(now, tt.expiresIn, tt.accessToken); !got.Equal(tt.want) {
				t.Errorf("tokenExpiry = %v, want %v", got, tt.want)
			}
		})
	}
}

func testJWT(t *testing.T, expiry time.Time) string {
	t.Helper()

	claims, err := json.Marshal(map[string]int64{"exp": expiry.Unix()})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	encode := base64.RawURLEncoding.EncodeToString

	return fmt.Sprintf("%s.%s.%s", encode([]byte(`{"alg":"RS256"}`)), encode(claims), encode([]byte("signature")))
}

func TestAccountTokenURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		accountHostname string
		accountID       string
		want            string
	}{
		{
			name:            "account id",
			accountHostname: "accounts.cloud.databricks.com",
			accountID:       "8c6f99ec-78ce-4654-8f92-e716b3dd67a7",
			want:            "https://accounts.cloud.databricks.com/oidc/accounts/8c6f99ec-78ce-4654-8f92-e716b3dd67a7/v1/token",
		},
		{
			name:            "azure account host",
			accountHostname: "accounts.azuredatabricks.net",
			accountID:       "acc-1",
			want:            "https://accounts.azuredatabricks.net/oidc/accounts/acc-1/v1/token",
		},
		{
			// A '?' must be escaped, or it truncates the path before /v1/token.
			name:            "account id is escaped",
			accountHostname: "accounts.cloud.databricks.com",
			accountID:       "acc?x=1",
			want:            "https://accounts.cloud.databricks.com/oidc/accounts/acc%3Fx=1/v1/token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := accountTokenURL(tt.accountHostname, tt.accountID); got != tt.want {
				t.Errorf("accountTokenURL = %q, want %q", got, tt.want)
			}
		})
	}
}
