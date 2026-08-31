package databricks

import (
	"errors"
	"fmt"
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
