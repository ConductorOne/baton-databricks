package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

// redirectTransport rewrites every request's host/scheme to a fixed target so
// tests run against a local httptest.Server without real credentials.
type redirectTransport struct {
	scheme string
	host   string
}

func (t *redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.scheme
	clone.URL.Host = t.host
	clone.Host = t.host
	return http.DefaultTransport.RoundTrip(clone)
}

// newTestDatabricksClient creates a Client that points all HTTP traffic to srv.
func newTestDatabricksClient(t *testing.T, srv *httptest.Server) *databricks.Client {
	t.Helper()

	parsed, err := url.Parse(srv.URL)
	require.NoError(t, err)

	transport := &redirectTransport{scheme: parsed.Scheme, host: parsed.Host}
	httpClient := &http.Client{Transport: transport}

	client, err := databricks.NewClient(
		context.Background(),
		httpClient,
		"workspace.cloud.databricks.com",
		"accounts.cloud.databricks.com",
		"test-account-id",
		srv.URL,
		&databricks.NoAuth{},
	)
	require.NoError(t, err)
	return client
}

// getSecretTrait extracts the SecretTrait from a resource's annotations.
func getSecretTrait(t *testing.T, r *v2.Resource) *v2.SecretTrait {
	t.Helper()
	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(r.GetAnnotations())
	ok, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, ok, "SecretTrait not found on resource %s", r.Id.Resource)
	return trait
}
