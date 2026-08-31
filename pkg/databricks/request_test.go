package databricks

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

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
