package connector

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// rolesTransport answers the assignable-roles calls Validate makes. failAccount
// makes the account-plane check (host "accounts.*") fail so isAccAPIAvailable stays
// false while the workspace check still succeeds.
type rolesTransport struct{ failAccount bool }

func (t rolesTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.failAccount && strings.HasPrefix(req.URL.Host, "accounts.") {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"boom"}`)),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"roles":[]}`)),
		Request:    req,
	}, nil
}

func newValidateConnector(t *testing.T, auth databricks.Auth, tr http.RoundTripper) *Databricks {
	t.Helper()
	client, err := databricks.NewClient(
		context.Background(), &http.Client{Transport: tr},
		"cloud.databricks.com", "accounts.cloud.databricks.com",
		"acct-1", "", auth, nil,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return &Databricks{client: client, workspaces: []string{"ws1"}}
}

// Under OAuth a failed account check is a fixable misconfiguration, so Validate
// fails rather than silently dropping account-level data.
func TestValidateOAuthAccountCheckFailureReturnsError(t *testing.T) {
	d := newValidateConnector(t, &databricks.NoAuth{}, rolesTransport{failAccount: true})

	if _, err := d.Validate(context.Background()); err == nil {
		t.Fatal("Validate: want error on OAuth account check failure, got nil")
	}
}
