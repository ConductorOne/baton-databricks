package databricks

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWrapTransportAuthError(t *testing.T) {
	retrieveErr := &oauth2.RetrieveError{ErrorCode: "invalid_client"}

	tests := []struct {
		name string
		in   error
		want codes.Code
	}{
		{"nil stays nil", nil, codes.OK},
		{"oauth2 retrieve error maps to unauthenticated", retrieveErr, codes.Unauthenticated},
		{"wrapped oauth2 retrieve error maps to unauthenticated", fmt.Errorf("get workspaces: %w", retrieveErr), codes.Unauthenticated},
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
