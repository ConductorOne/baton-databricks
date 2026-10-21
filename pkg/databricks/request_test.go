package databricks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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

type rewriteToServer struct {
	target *url.URL
}

func (r *rewriteToServer) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = r.target.Scheme
	req.URL.Host = r.target.Host
	return http.DefaultTransport.RoundTrip(req)
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
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				gotMethod, gotPath = req.Method, req.URL.Path
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)
			target, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("parse server url: %v", err)
			}

			c, err := NewClient(context.Background(), &http.Client{Transport: &rewriteToServer{target: target}},
				"example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "", &NoAuth{}, nil)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}

			_, err = c.PatchGroup(context.Background(), "", "g1", PatchOperation{Op: "add", Path: "roles", Value: []PermissionValue{{Value: "account_admin"}}})

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
