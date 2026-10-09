package databricks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// This runs at the cache's default settings on purpose: BATON_HTTP_CACHE_TTL is
// deliberately not set, because turning the cache off is what hides this.
//
// Grant and Revoke both decide whether to PATCH by reading the securable's
// current assignments first, and both reads produce the same URL and query, so
// they share one cache entry. uhttp holds a GET 200 for an hour and a PATCH does
// not invalidate it, so a cached pre-read makes Revoke see the state from before
// the Grant: it reports GrantAlreadyRevoked and never sends the PATCH, leaving
// the principal with access that C1 believes is gone.
func TestProvisioningPreReadsAreNotServedFromTheCache(t *testing.T) {
	var gets int
	granted := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			gets++
			var assignments []PrivilegeAssignment
			if granted {
				assignments = []PrivilegeAssignment{{Principal: "alice", Privileges: []string{"SELECT"}}}
			}
			_ = json.NewEncoder(w).Encode(permissionsResponse{PrivilegeAssignments: assignments})
		case http.MethodPatch:
			granted = true
			_ = json.NewEncoder(w).Encode(permissionsResponse{
				PrivilegeAssignments: []PrivilegeAssignment{{Principal: "alice", Privileges: []string{"SELECT"}}},
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
	ctx := context.Background()

	// Grant's pre-read: nothing is held yet, so the Grant proceeds to the PATCH.
	held, _, err := c.DrainPermissions(ctx, "ws", SecurableMetastore, "ms-1")
	if err != nil {
		t.Fatalf("Grant pre-read: %v", err)
	}
	if len(held) != 0 {
		t.Fatalf("Grant pre-read saw %d assignment(s), want none", len(held))
	}

	if _, _, err := c.UpdatePermissions(ctx, "ws", SecurableMetastore, "ms-1",
		[]PermissionsChange{{Principal: "alice", Add: []string{"SELECT"}}}); err != nil {
		t.Fatalf("UpdatePermissions: %v", err)
	}

	// Revoke's pre-read, same process and same hour, against the same cache key.
	var seen []PrivilegeAssignment
	if _, err := c.ForEachUncachedPermissionsPage(ctx, "ws", SecurableMetastore, "ms-1", "",
		func(page []PrivilegeAssignment) (bool, error) {
			seen = append(seen, page...)
			return false, nil
		}); err != nil {
		t.Fatalf("Revoke pre-read: %v", err)
	}

	if len(seen) != 1 {
		t.Fatalf("Revoke pre-read saw %d assignment(s), want the privilege the Grant just added: "+
			"a cached page here reports GrantAlreadyRevoked and skips the PATCH", len(seen))
	}
	if gets != 2 {
		t.Errorf("%d GET(s) reached the API, want 2: both pre-reads must bypass the cache", gets)
	}
}
