package databricks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Databricks ignores a page token it cannot deserialize and silently restarts at
// page one, so a walk that accepts a repeated token re-reads the same page forever.
func TestDrainPagesFromStartsAtSeedAndRejectsARepeat(t *testing.T) {
	t.Parallel()

	var tokens []string
	_, _, err := drainPagesFrom(context.Background(), "page-2", func(pageToken string) ([]string, string, *v2.RateLimitDescription, error) {
		tokens = append(tokens, pageToken)
		if pageToken == "page-2" {
			return []string{"from-seed"}, "page-2", nil, nil
		}
		return []string{"should-not-run"}, "", nil, nil
	})
	if err == nil {
		t.Fatal("drainPagesFrom returned nil error for a repeated token")
	}
	if len(tokens) != 1 || tokens[0] != "page-2" {
		t.Fatalf("fetched tokens %v, want only the seed page-2", tokens)
	}
}

// The PATCH echo is itself paginated, so reading just the PATCH body makes a Revoke
// report success while the principal still holds the privilege on a later page. The
// walk continues from the PATCH's own token rather than re-fetching page one.
func TestUpdatePermissionsDrainsFromPatchToken(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var gets []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPatch:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read patch body: %v", err)
			}
			if len(body) == 0 {
				t.Error("patch body is empty")
			}
			_ = json.NewEncoder(w).Encode(permissionsResponse{
				PrivilegeAssignments: []PrivilegeAssignment{{Principal: "alice", Privileges: []string{"SELECT"}}},
				NextPageToken:        "page-2",
			})
		case http.MethodGet:
			token := r.URL.Query().Get("page_token")
			gets = append(gets, token)
			principal := "bob"
			if token != "page-2" {
				principal = "unexpected"
			}
			_ = json.NewEncoder(w).Encode(permissionsResponse{
				PrivilegeAssignments: []PrivilegeAssignment{{Principal: principal, Privileges: []string{"MODIFY"}}},
			})
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)

	client := srv.Client()
	client.Transport = rewriteHost(srv.Listener.Addr().String(), client.Transport)
	c, err := NewClient(context.Background(), client, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", srv.URL, &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	got, _, err := c.UpdatePermissions(context.Background(), "ws", SecurableMetastore, "ms-1", []PermissionsChange{{
		Principal: "alice",
		Add:       []string{"CREATE_CATALOG"},
	}})
	if err != nil {
		t.Fatalf("UpdatePermissions: %v", err)
	}
	if len(gets) != 1 || gets[0] != "page-2" {
		t.Fatalf("GET page tokens %v, want [page-2] and not a re-fetch of the PATCH page", gets)
	}
	if len(got) != 2 || got[0].Principal != "alice" || got[1].Principal != "bob" {
		t.Fatalf("assignments = %+v, want alice then bob", got)
	}
}

type hostRewriter struct {
	host http.RoundTripper
	addr string
}

func rewriteHost(addr string, next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return hostRewriter{host: next, addr: addr}
}

func (h hostRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = "http"
	clone.URL.Host = h.addr
	return h.host.RoundTrip(clone)
}
