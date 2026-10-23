package databricks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		})
	}
}

// TestUpdateGroupRefusesEmptyDisplayName guards against the CXH-2347 full-replacement-PUT
// data-loss class: UpdateGroup must refuse to send a Group with an empty displayName,
// since displayName is never legitimately empty and a PUT with it blank would wipe it.
func TestUpdateGroupRefusesEmptyDisplayName(t *testing.T) {
	c := newTestClient(t, nil)

	_, err := c.UpdateGroup(context.Background(), "", &Group{BaseResponse: BaseResponse{ID: "g1"}})
	if err == nil {
		t.Fatal("UpdateGroup succeeded with an empty displayName, want an error")
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
