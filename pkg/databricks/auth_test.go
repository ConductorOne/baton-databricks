package databricks

import (
	"net/http"
	"net/url"
	"testing"
)

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

func TestTokenAuthApply(t *testing.T) {
	auth := NewTokenAuth(
		[]string{"dbc-abc123", "adb-2531901403506481.1"},
		[]string{"aws-token", "azure-token"},
	)

	cases := []struct {
		name      string
		host      string
		wantToken string
	}{
		{"aws deployment name (no dot)", "dbc-abc123.cloud.databricks.com", "aws-token"},
		{"azure deployment name (dotted)", "adb-2531901403506481.1.azuredatabricks.net", "azure-token"},
		{"account host matches nothing", "accounts.azuredatabricks.net", ""},
		{"unknown workspace matches nothing", "dbc-other.cloud.databricks.com", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{URL: mustURL(t, "https://"+tc.host+"/api/2.0/preview/scim/v2/Users"), Header: http.Header{}}
			auth.Apply(req)

			got := req.Header.Get("Authorization")
			want := ""
			if tc.wantToken != "" {
				want = "Bearer " + tc.wantToken
			}
			if got != want {
				t.Fatalf("Authorization = %q, want %q", got, want)
			}
		})
	}
}

// A workspace name that prefixes another must not steal the longer one's token.
func TestTokenAuthApplyPrefixCollision(t *testing.T) {
	auth := NewTokenAuth([]string{"dbc-1", "dbc-12"}, []string{"token-1", "token-12"})

	req := &http.Request{URL: mustURL(t, "https://dbc-12.cloud.databricks.com/x"), Header: http.Header{}}
	auth.Apply(req)

	if got := req.Header.Get("Authorization"); got != "Bearer token-12" {
		t.Fatalf("Authorization = %q, want %q", got, "Bearer token-12")
	}
}
