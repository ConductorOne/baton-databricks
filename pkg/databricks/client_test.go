package databricks

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newTestClient(t *testing.T, exclude []string) *Client {
	t.Helper()
	c, err := NewClient(context.Background(), &http.Client{}, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "", nil, exclude)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestIsWorkspaceExcluded(t *testing.T) {
	ws := Workspace{ID: 12345, Name: "prod", DeploymentName: "dbc-abc"}

	tests := []struct {
		name    string
		exclude []string
		want    bool
	}{
		{"no exclusions", nil, false},
		{"by name", []string{"prod"}, true},
		{"by deployment name", []string{"dbc-abc"}, true},
		{"by numeric id", []string{"12345"}, true},
		{"no match", []string{"staging"}, false},
		{"empty and whitespace entries ignored", []string{"", "   "}, false},
		{"space-padded entry still matches", []string{" prod"}, true},
		{"name match is case-insensitive", []string{"PROD"}, true},
		{"deployment name match is case-insensitive", []string{"DBC-ABC"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.exclude)
			if _, got := c.isWorkspaceExcluded(ws); got != tt.want {
				t.Errorf("isWorkspaceExcluded(%+v) with exclude %v = %v, want %v", ws, tt.exclude, got, tt.want)
			}
		})
	}
}

func TestGetAccountHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     string
	}{
		{"azure subdomain", "myorg.azuredatabricks.net", "accounts.azuredatabricks.net"},
		{"azure bare host", "azuredatabricks.net", "accounts.azuredatabricks.net"},
		{"azure mixed case", "MyOrg.AzureDatabricks.net", "accounts.azuredatabricks.net"},
		{"azure trailing dot", "myorg.azuredatabricks.net.", "accounts.azuredatabricks.net"},
		{"gcp subdomain", "myorg.gcp.databricks.com", "accounts.gcp.databricks.com"},
		{"aws workspace host normalises to account host", "dbc-1234abcd.cloud.databricks.com", "accounts.cloud.databricks.com"},
		{"unknown host falls through unnormalised", "myorg.example.com", "accounts.myorg.example.com"},
		// CXH-2349: a bare-suffix match would resolve this to the Azure account host.
		{"azure lookalike does not match", "evilazuredatabricks.net", "accounts.evilazuredatabricks.net"},
		{"gcp lookalike does not match", "evilgcp.databricks.com", "accounts.evilgcp.databricks.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetAccountHostname(tt.hostname); got != tt.want {
				t.Errorf("GetAccountHostname(%q) = %q, want %q", tt.hostname, got, tt.want)
			}
			// Keep-in-sync contract: every host the derivation produces must pass the
			// explicit-value validator, or a future derivation change could reject
			// previously-working configs.
			if err := ValidateAccountHostname(tt.want); err != nil {
				t.Errorf("ValidateAccountHostname(%q) = %v, want nil", tt.want, err)
			}
		})
	}
}

func TestValidateAccountHostname(t *testing.T) {
	accept := []string{
		"accounts.cloud.databricks.com",
		"accounts.azuredatabricks.net",
		"ACCOUNTS.Cloud.Databricks.com.", // case/trailing-dot leniency, matching GetAccountHostname's normalization
		// The contract is shape, not domain ownership: an accounts.-prefixed
		// lookalike passes by design (I2). Validation never rewrites the value.
		"accounts.evil.com",
	}
	for _, host := range accept {
		if err := ValidateAccountHostname(host); err != nil {
			t.Errorf("ValidateAccountHostname(%q) = %v, want nil", host, err)
		}
	}

	reject := []string{
		"cloud.databricks.com",
		"azuredatabricks.net",
		"gcp.databricks.com",
		"evilaccounts.cloud.databricks.com",
		"https://accounts.cloud.databricks.com", // scheme-prefixed is not accounts.-prefixed
		"accounts.",                             // bare prefix has no host; would reproduce the opaque remote failure
		"",
	}
	for _, host := range reject {
		err := ValidateAccountHostname(host)
		if err == nil {
			t.Errorf("ValidateAccountHostname(%q) = nil, want error", host)
			continue
		}
		if !errors.Is(err, ErrInvalidAccountHostname) {
			t.Errorf("ValidateAccountHostname(%q) error = %v, want errors.Is(ErrInvalidAccountHostname)", host, err)
		}
		if !strings.Contains(err.Error(), "account-hostname") {
			t.Errorf("ValidateAccountHostname(%q) error %q does not mention account-hostname", host, err.Error())
		}
	}
}

func TestIsWorkspaceExcludedReturnsEveryMatchedKey(t *testing.T) {
	ws := Workspace{ID: 12345, Name: "prod", DeploymentName: "dbc-abc"}
	c := newTestClient(t, []string{"prod", "dbc-abc", "staging"})

	keys, ok := c.isWorkspaceExcluded(ws)
	if !ok {
		t.Fatalf("isWorkspaceExcluded(%+v) = false, want true", ws)
	}
	want := []string{"prod", "dbc-abc"}
	if len(keys) != len(want) {
		t.Fatalf("isWorkspaceExcluded(%+v) matched keys = %v, want %v", ws, keys, want)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("isWorkspaceExcluded(%+v) matched keys = %v, want %v", ws, keys, want)
			break
		}
	}
}

// serverRewriter sends every request to the test server.
type serverRewriter struct {
	target *url.URL
}

func (s *serverRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = s.target.Scheme
	req.URL.Host = s.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

// newServerClient backs a client with a test server running handler.
func newServerClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}

	httpClient := &http.Client{Transport: &serverRewriter{target: target}}
	c, err := NewClient(context.Background(), httpClient, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "", &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestGetUserByUsername(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantUser string
		wantErr  bool
	}{
		{
			name:     "match",
			status:   http.StatusOK,
			body:     `{"totalResults":1,"Resources":[{"id":"u1","userName":"alice@example.com","displayName":"Alice","active":true}]}`,
			wantUser: "u1",
		},
		{
			name:   "no match",
			status: http.StatusOK,
			body:   `{"totalResults":0,"Resources":[]}`,
		},
		{
			name:    "server error",
			status:  http.StatusInternalServerError,
			body:    `{"detail":"boom"}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var query url.Values
			c := newServerClient(t, func(w http.ResponseWriter, r *http.Request) {
				query = r.URL.Query()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})

			user, _, err := c.GetUserByUsername(context.Background(), "", "alice@example.com")
			if tc.wantErr {
				if err == nil {
					t.Fatal("GetUserByUsername succeeded, want error")
				}
			} else {
				if err != nil {
					t.Fatalf("GetUserByUsername: %v", err)
				}
				switch {
				case tc.wantUser == "" && user != nil:
					t.Errorf("GetUserByUsername = %+v, want nil", user)
				case tc.wantUser != "" && (user == nil || user.ID != tc.wantUser):
					t.Errorf("GetUserByUsername = %+v, want user %q", user, tc.wantUser)
				}
			}

			if got, want := query.Get("filter"), "userName eq 'alice@example.com'"; got != want {
				t.Errorf("filter = %q, want %q", got, want)
			}
			if got, want := query.Get("count"), "1"; got != want {
				t.Errorf("count = %q, want %q", got, want)
			}
			wantAttrs := NewUserAttrVars().Attrs
			gotAttrs := query["attributes"]
			if len(gotAttrs) != len(wantAttrs) {
				t.Fatalf("attributes = %v, want %v", gotAttrs, wantAttrs)
			}
			for i := range wantAttrs {
				if gotAttrs[i] != wantAttrs[i] {
					t.Errorf("attributes = %v, want %v", gotAttrs, wantAttrs)
					break
				}
			}
		})
	}
}
