package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

func TestPrincipalsForGrantsOwnerOnlyDoesNotListAccount(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var unfiltered int
	client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		if isUnfilteredAccountSCIM(r) {
			unfiltered++
			http.Error(w, "account listing is not allowed on the owner-only path", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/scim/v2/Users"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Resources":    []map[string]any{{"id": "u-1", "userName": "alice@example.com"}},
				"totalResults": 1,
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"Resources": []any{}, "totalResults": 0})
		}
	})

	uc := newUnityCatalog(client, "app-1", nil)
	index, _, err := uc.principalsForGrants(context.Background(), nil, "alice@example.com")
	if err != nil {
		t.Fatalf("principalsForGrants: %v", err)
	}
	if unfiltered != 0 {
		t.Fatalf("unfiltered account SCIM listings = %d, want 0", unfiltered)
	}
	got, ok := index.lookupName("alice@example.com")
	if !ok {
		t.Fatal("owner did not resolve")
	}
	if got.GetResourceType() != userResourceType.Id || got.GetResource() != "u-1" {
		t.Fatalf("owner = %s:%s, want user:u-1", got.GetResourceType(), got.GetResource())
	}
}

func TestPrincipalsForGrantsOwnerOnlyResolvesGroupAndServicePrincipal(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	tests := []struct {
		name         string
		owner        string
		resourceType string
		resource     string
	}{
		{name: "group", owner: "data-eng", resourceType: groupResourceType.Id, resource: "account/acc-1/group/g-1"},
		{name: "service-principal", owner: "11111111-1111-1111-1111-111111111111", resourceType: servicePrincipalResourceType.Id, resource: "sp-1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var unfiltered int
			client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
				if isUnfilteredAccountSCIM(r) {
					unfiltered++
					http.Error(w, "account listing is not allowed on the owner-only path", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.Contains(r.URL.Path, "/scim/v2/Groups") && strings.Contains(r.URL.Query().Get("filter"), "data-eng"):
					_ = json.NewEncoder(w).Encode(map[string]any{
						"Resources":    []map[string]any{{"id": "g-1", "displayName": "data-eng"}},
						"totalResults": 1,
					})
				case strings.Contains(r.URL.Path, "/scim/v2/ServicePrincipals") && strings.Contains(r.URL.Query().Get("filter"), "11111111-1111-1111-1111-111111111111"):
					_ = json.NewEncoder(w).Encode(map[string]any{
						"Resources":    []map[string]any{{"id": "sp-1", "applicationId": "11111111-1111-1111-1111-111111111111"}},
						"totalResults": 1,
					})
				default:
					_ = json.NewEncoder(w).Encode(map[string]any{"Resources": []any{}, "totalResults": 0})
				}
			})

			uc := newUnityCatalog(client, "app-1", nil)
			index, _, err := uc.principalsForGrants(context.Background(), nil, test.owner)
			if err != nil {
				t.Fatalf("principalsForGrants: %v", err)
			}
			if unfiltered != 0 {
				t.Fatalf("unfiltered account SCIM listings = %d, want 0", unfiltered)
			}
			got, ok := index.lookupName(test.owner)
			if !ok {
				t.Fatal("owner did not resolve")
			}
			if got.GetResourceType() != test.resourceType || got.GetResource() != test.resource {
				t.Fatalf("owner = %s:%s, want %s:%s", got.GetResourceType(), got.GetResource(), test.resourceType, test.resource)
			}
		})
	}
}

func TestPrincipalsForGrantsWorkspaceAdminsOwnerDoesNotListAccount(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var unfiltered int
	var filtered int
	client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		if isUnfilteredAccountSCIM(r) {
			unfiltered++
			http.Error(w, "account listing is not allowed on the owner-only path", http.StatusInternalServerError)
			return
		}
		if strings.Contains(r.URL.Path, "/scim/v2/") {
			filtered++
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"Resources": []any{}, "totalResults": 0})
	})

	uc := newUnityCatalog(client, "app-1", nil)
	index, _, err := uc.principalsForGrants(context.Background(), nil, "_workspace_admins_workspace-1")
	if err != nil {
		t.Fatalf("principalsForGrants: %v", err)
	}
	if unfiltered != 0 {
		t.Fatalf("unfiltered account SCIM listings = %d, want 0", unfiltered)
	}
	if filtered != 3 {
		t.Fatalf("filtered lookups = %d, want 3 (user, group, service principal)", filtered)
	}
	if _, ok := index.lookupName("_workspace_admins_workspace-1"); ok {
		t.Fatal("workspace-admins owner should stay unresolved")
	}
}

func TestPrincipalsForGrantsAssignmentsResolvePerPrincipal(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var unfiltered int
	var gets int
	client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		if isUnfilteredAccountSCIM(r) {
			unfiltered++
			http.Error(w, "account listing is not allowed on the assignment path", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/scim/v2/Users/1") {
			gets++
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "1", "userName": "alice@example.com"})
			return
		}
		http.Error(w, `{"message":"missing"}`, http.StatusNotFound)
	})

	uc := newUnityCatalog(client, "app-1", nil)
	index, _, err := uc.principalsForGrants(context.Background(), []databricks.PrivilegeAssignment{{
		Principal:   "alice@example.com",
		PrincipalID: json.Number("1"),
		Privileges:  []string{"USE_CATALOG"},
	}}, "")
	if err != nil {
		t.Fatalf("principalsForGrants: %v", err)
	}
	if unfiltered != 0 {
		t.Fatalf("unfiltered account SCIM listings = %d, want 0", unfiltered)
	}
	if gets == 0 {
		t.Fatal("expected a GetUser by principal_id")
	}
	got, ok := index.lookup(databricks.PrivilegeAssignment{Principal: "alice@example.com", PrincipalID: json.Number("1")})
	if !ok || got.GetResource() != "1" {
		t.Fatalf("assignment lookup = %v %v, want user 1", got, ok)
	}
}

func TestPrincipalsForGrantsEmptyPageMakesNoSCIMCalls(t *testing.T) {
	t.Setenv("BATON_HTTP_CACHE_TTL", "0")

	var hits int
	client, _ := newWorkspaceRoutedClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.Error(w, "no SCIM call expected", http.StatusInternalServerError)
	})

	uc := newUnityCatalog(client, "app-1", nil)
	index, _, err := uc.principalsForGrants(context.Background(), nil, "")
	if err != nil {
		t.Fatalf("principalsForGrants: %v", err)
	}
	if hits != 0 {
		t.Fatalf("SCIM calls = %d, want 0", hits)
	}
	if _, ok := index.lookupName("anyone"); ok {
		t.Fatal("empty index should resolve nobody")
	}
}

func isUnfilteredAccountSCIM(r *http.Request) bool {
	if r.URL.Query().Get("filter") != "" {
		return false
	}

	return strings.HasSuffix(r.URL.Path, "/scim/v2/Users") ||
		strings.HasSuffix(r.URL.Path, "/scim/v2/Groups") ||
		strings.HasSuffix(r.URL.Path, "/scim/v2/ServicePrincipals")
}
