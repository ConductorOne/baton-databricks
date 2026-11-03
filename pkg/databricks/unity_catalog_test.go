package databricks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// Page one is re-read only to refresh the cache; confirmation comes from the PATCH body.
	if len(gets) != 2 || gets[0] != "" || gets[1] != "page-2" {
		t.Fatalf("GET page tokens %v, want the cache refresh then page-2", gets)
	}
	if len(got) != 2 || got[0].Principal != "alice" || got[1].Principal != "bob" {
		t.Fatalf("assignments = %+v, want alice then bob", got)
	}
}

func TestListCatalogsCanIncludeUnboundCatalogs(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")
	var includeUnbound string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		includeUnbound = r.URL.Query().Get("include_unbound")
		_ = json.NewEncoder(w).Encode(catalogsResponse{})
	}))
	t.Cleanup(srv.Close)

	client := srv.Client()
	client.Transport = rewriteHost(srv.Listener.Addr().String(), client.Transport)
	c, err := NewClient(context.Background(), client, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", srv.URL, &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, _, _, err := c.ListCatalogs(context.Background(), "ws", "", 100, true); err != nil {
		t.Fatalf("ListCatalogs: %v", err)
	}
	if includeUnbound != "true" {
		t.Fatalf("include_unbound = %q, want true", includeUnbound)
	}
}

// A catalog name holding "?", "#" or "%" does not survive URL assembly: the
// request layer unescapes the path again, so "sales?x" would read "sales". The
// caller writes privileges to whatever comes back, so the read is refused
// instead of answering with a different catalog.
func TestGetCatalogRefusesANameThatChangesTheURL(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var requested []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		_ = json.NewEncoder(w).Encode(Catalog{Name: "sales", MetastoreID: "ms-1"})
	}))
	t.Cleanup(srv.Close)

	client := srv.Client()
	client.Transport = rewriteHost(srv.Listener.Addr().String(), client.Transport)
	c, err := NewClient(context.Background(), client, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", srv.URL, &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	for _, name := range []string{"sales?x", "sales#x", "sales%x", ""} {
		got, _, err := c.GetCatalog(context.Background(), "ws", name)
		if err == nil {
			t.Fatalf("GetCatalog(%q) returned catalog %+v, want an error", name, got)
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("GetCatalog(%q) returned %v, want InvalidArgument", name, status.Code(err))
		}
	}
	if len(requested) != 0 {
		t.Fatalf("GetCatalog sent %v, want no request at all", requested)
	}
}

// Databricks answering with a catalog other than the one asked for is not an
// answer about the requested catalog, and Grant/Revoke rebuild the resource from
// what comes back.
func TestGetCatalogRefusesAMismatchedAnswer(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(Catalog{Name: "other", MetastoreID: "ms-1"})
	}))
	t.Cleanup(srv.Close)

	client := srv.Client()
	client.Transport = rewriteHost(srv.Listener.Addr().String(), client.Transport)
	c, err := NewClient(context.Background(), client, "example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", srv.URL, &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	got, _, err := c.GetCatalog(context.Background(), "ws", "sales")
	if err == nil {
		t.Fatalf("GetCatalog returned catalog %+v, want an error", got)
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetCatalog returned %v, want InvalidArgument", status.Code(err))
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
