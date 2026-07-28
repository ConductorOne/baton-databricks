package connector

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

func TestIsWorkspaceAccessForbidden(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"403 API error", &databricks.APIError{StatusCode: http.StatusForbidden}, true},
		{"wrapped 403 API error", fmt.Errorf("list: %w", &databricks.APIError{StatusCode: http.StatusForbidden}), true},
		{"400 API error", &databricks.APIError{StatusCode: http.StatusBadRequest}, false},
		{"500 API error", &databricks.APIError{StatusCode: http.StatusInternalServerError}, false},
		{"non-API error", fmt.Errorf("network timeout"), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isWorkspaceAccessForbidden(tt.err); got != tt.want {
				t.Errorf("isWorkspaceAccessForbidden(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
