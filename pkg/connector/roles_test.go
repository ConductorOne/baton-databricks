package connector

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

func TestIsApiError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		want       bool
	}{
		{"403 API error", &databricks.APIError{StatusCode: http.StatusForbidden}, http.StatusForbidden, true},
		{"wrapped 403 API error", fmt.Errorf("list: %w", &databricks.APIError{StatusCode: http.StatusForbidden}), http.StatusForbidden, true},
		{"403 API error, want 400", &databricks.APIError{StatusCode: http.StatusForbidden}, http.StatusBadRequest, false},
		{"400 API error", &databricks.APIError{StatusCode: http.StatusBadRequest}, http.StatusForbidden, false},
		{"500 API error", &databricks.APIError{StatusCode: http.StatusInternalServerError}, http.StatusForbidden, false},
		{"non-API error", fmt.Errorf("network timeout"), http.StatusForbidden, false},
		{"nil error", nil, http.StatusForbidden, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isApiError(tt.err, tt.statusCode); got != tt.want {
				t.Errorf("isApiError(%v, %d) = %v, want %v", tt.err, tt.statusCode, got, tt.want)
			}
		})
	}
}
