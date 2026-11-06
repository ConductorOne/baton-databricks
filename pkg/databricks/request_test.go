package databricks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWrapTransportAuthError(t *testing.T) {
	retrieveErr := &oauth2.RetrieveError{ErrorCode: "invalid_client"}
	retrieveWithStatus := func(status int) *oauth2.RetrieveError {
		return &oauth2.RetrieveError{Response: &http.Response{StatusCode: status}}
	}

	tests := []struct {
		name string
		in   error
		want codes.Code
	}{
		{"nil stays nil", nil, codes.OK},
		{"oauth2 retrieve error without response maps to unauthenticated", retrieveErr, codes.Unauthenticated},
		{"wrapped oauth2 retrieve error maps to unauthenticated", fmt.Errorf("get workspaces: %w", retrieveErr), codes.Unauthenticated},
		{"401 maps to unauthenticated", retrieveWithStatus(http.StatusUnauthorized), codes.Unauthenticated},
		{"403 maps to permission denied", retrieveWithStatus(http.StatusForbidden), codes.PermissionDenied},
		{"429 stays retryable as unavailable", retrieveWithStatus(http.StatusTooManyRequests), codes.Unavailable},
		{"503 stays retryable as unavailable", retrieveWithStatus(http.StatusServiceUnavailable), codes.Unavailable},
		{"unrelated error is left unknown", errors.New("boom"), codes.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := status.Code(wrapTransportAuthError(tt.in))
			if got != tt.want {
				t.Errorf("wrapTransportAuthError(%v) grpc code = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestNameWorkspace403Remedy(t *testing.T) {
	forbidden := &APIError{StatusCode: http.StatusForbidden, Message: "Unauthorized access to Org: 123"}
	badRequest := &APIError{StatusCode: http.StatusBadRequest, Message: "bad"}

	tests := []struct {
		name        string
		workspaceId string
		err         error
		wantRemedy  bool
		wantSameErr bool
	}{
		{"workspace 403 gets remedy", "123", forbidden, true, false},
		{"account-scoped 403 passes through", "", forbidden, false, true},
		{"non-403 passes through", "123", badRequest, false, true},
		{"nil passes through", "123", nil, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nameWorkspace403Remedy(tt.workspaceId, tt.err)

			if tt.wantSameErr {
				if !errors.Is(got, tt.err) {
					t.Fatalf("expected untouched error, got %v", got)
				}
				return
			}

			if !tt.wantRemedy {
				return
			}
			if !strings.Contains(got.Error(), tt.workspaceId) {
				t.Errorf("remedy should name the workspace, got %q", got.Error())
			}
			if !strings.Contains(got.Error(), "databricks-exclude-workspaces") {
				t.Errorf("remedy should point at the exclude flag, got %q", got.Error())
			}
			var apiErr *APIError
			if !errors.As(got, &apiErr) {
				t.Errorf("remedy should keep the wrapped APIError reachable")
			}
		})
	}
}

// TestPatchGroup checks a SCIM PATCH succeeds on the API's empty 204 answer and keeps the
// error detail on a rejected operation.
func TestPatchGroup(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantDetail string
	}{
		{name: "empty 204", status: http.StatusNoContent},
		{name: "rejected op", status: http.StatusBadRequest, body: `{"detail":"Unsupported role passed into operation"}`, wantDetail: "Unsupported role passed into operation"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotMethod, gotPath string
			c := newServerClient(t, func(w http.ResponseWriter, req *http.Request) {
				gotMethod, gotPath = req.Method, req.URL.Path
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})

			_, err := c.PatchGroup(context.Background(), "", "g1", PatchOperation{Op: "add", Path: "roles", Value: []PermissionValue{{Value: "account_admin"}}})

			if gotMethod != http.MethodPatch || gotPath != "/api/2.0/accounts/acc-1/scim/v2/Groups/g1" {
				t.Errorf("request = %s %s, want PATCH /api/2.0/accounts/acc-1/scim/v2/Groups/g1", gotMethod, gotPath)
			}
			if tc.wantDetail == "" {
				if err != nil {
					t.Fatalf("PatchGroup returned error: %v", err)
				}
				return
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != tc.status || apiErr.Detail != tc.wantDetail {
				t.Errorf("PatchGroup error = %v, want APIError %d with detail %q", err, tc.status, tc.wantDetail)
			}
		})
	}
}

// TestGetGroupCaching: GetGroup is served from the HTTP client's response cache, while
// GetGroupNoCache always reaches the server.
func TestGetGroupCaching(t *testing.T) {
	tests := []struct {
		name      string
		get       func(c *Client) (*Group, *v2.RateLimitDescription, error)
		wantCalls int
	}{
		{
			name: "GetGroup is cached",
			get: func(c *Client) (*Group, *v2.RateLimitDescription, error) {
				return c.GetGroup(context.Background(), "", "g1")
			},
			wantCalls: 1,
		},
		{
			name: "GetGroupNoCache bypasses the cache",
			get: func(c *Client) (*Group, *v2.RateLimitDescription, error) {
				return c.GetGroupNoCache(context.Background(), "", "g1")
			},
			wantCalls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c := newServerClient(t, func(w http.ResponseWriter, req *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"g1","displayName":"G"}`))
			})

			for i := 0; i < 2; i++ {
				if _, _, err := tc.get(c); err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
			}
			if calls != tc.wantCalls {
				t.Errorf("server saw %d requests for 2 reads, want %d", calls, tc.wantCalls)
			}
		})
	}
}

// TestErrorBodyNotJSON: an HTTP error whose body is HTML, whitespace or zero bytes keeps
// its HTTP cause: an *APIError with the status code, the gRPC code uhttp derived from the
// status, and the rate-limit data. A JSON error body keeps its message and detail.
func TestErrorBodyNotJSON(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		want        codes.Code
		wantMessage string
	}{
		{"403 html", http.StatusForbidden, "<html><body>denied</body></html>", codes.PermissionDenied, ""},
		{"403 empty", http.StatusForbidden, "", codes.PermissionDenied, ""},
		{"404 html", http.StatusNotFound, "<html>not here</html>", codes.NotFound, ""},
		{"429 whitespace", http.StatusTooManyRequests, " \n\t ", codes.Unavailable, ""},
		{"429 empty", http.StatusTooManyRequests, "", codes.Unavailable, ""},
		{"503 html", http.StatusServiceUnavailable, "<html>unavailable</html>", codes.Unavailable, ""},
		{"503 empty", http.StatusServiceUnavailable, "", codes.Unavailable, ""},
		{"503 truncated json", http.StatusServiceUnavailable, `{"message":"try`, codes.Unavailable, ""},
		{"503 json", http.StatusServiceUnavailable, `{"message":"try later"}`, codes.Unavailable, "try later"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newServerClient(t, func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})

			rl, err := c.SetUserActive(context.Background(), "42", false)
			if err == nil {
				t.Fatalf("SetUserActive succeeded on HTTP %d", tc.status)
			}
			if rl == nil {
				t.Error("rate-limit data must be returned with an HTTP error")
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("error %v does not reach an *APIError", err)
			}
			if apiErr.StatusCode != tc.status {
				t.Errorf("APIError.StatusCode = %d, want %d", apiErr.StatusCode, tc.status)
			}
			if apiErr.Message != tc.wantMessage {
				t.Errorf("APIError.Message = %q, want %q", apiErr.Message, tc.wantMessage)
			}
			if got := status.Code(err); got != tc.want {
				t.Errorf("grpc code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}
