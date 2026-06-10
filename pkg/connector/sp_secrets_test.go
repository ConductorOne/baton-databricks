package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

func TestSPSecretBuilder_List_Basic(t *testing.T) {
	secrets := []map[string]interface{}{
		{
			"id":          "secret-1",
			"create_time": "2025-01-15T10:00:00Z",
			"update_time": "2025-01-15T10:00:00Z",
			"secret_hash": "abc123",
			"status":      "ACTIVE",
		},
		{
			"id":          "secret-2",
			"create_time": "2025-03-20T12:30:00Z",
			"update_time": "2025-03-20T12:30:00Z",
			"secret_hash": "def456",
			"status":      "ACTIVE",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"secrets":         secrets,
			"next_page_token": "",
		})
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newServicePrincipalSecretBuilder(client)

	parentID := &v2.ResourceId{
		ResourceType: servicePrincipalResourceType.Id,
		Resource:     "12345",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Len(t, resources, 2)

	for _, r := range resources {
		require.Equal(t, servicePrincipalSecretResourceType.Id, r.Id.ResourceType)
		require.Equal(t, parentID.Resource, r.ParentResourceId.Resource)
		require.Equal(t, servicePrincipalResourceType.Id, r.ParentResourceId.ResourceType)

		trait := getSecretTrait(t, r)
		require.Equal(t, v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET, trait.GetCredentialType())
		require.Equal(t, "databricks.sp_oauth_secret", trait.GetCredentialDetail())
		require.Equal(t, parentID.Resource, trait.GetIdentityId().GetResource())
		require.Equal(t, servicePrincipalResourceType.Id, trait.GetIdentityId().GetResourceType())
	}

	ids := map[string]bool{}
	for _, r := range resources {
		ids[r.Id.Resource] = true
	}
	require.True(t, ids["secret-1"])
	require.True(t, ids["secret-2"])
}

func TestSPSecretBuilder_List_Pagination(t *testing.T) {
	page1 := []map[string]interface{}{
		{"id": "s1", "create_time": "2025-01-01T00:00:00Z", "secret_hash": "h1", "status": "ACTIVE"},
	}
	page2 := []map[string]interface{}{
		{"id": "s2", "create_time": "2025-02-01T00:00:00Z", "secret_hash": "h2", "status": "ACTIVE"},
	}

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		callCount++
		if r.URL.Query().Get("page_token") == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"secrets":         page1,
				"next_page_token": "cursor-page-2",
			})
		} else {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"secrets":         page2,
				"next_page_token": "",
			})
		}
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newServicePrincipalSecretBuilder(client)

	parentID := &v2.ResourceId{
		ResourceType: servicePrincipalResourceType.Id,
		Resource:     "99",
	}

	resources, _, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, resources, 2)
	require.Equal(t, 2, callCount)

	ids := map[string]bool{}
	for _, r := range resources {
		ids[r.Id.Resource] = true
	}
	require.True(t, ids["s1"])
	require.True(t, ids["s2"])
}

func TestSPSecretBuilder_List_WrongParentType(t *testing.T) {
	builder := newServicePrincipalSecretBuilder(nil)

	parentID := &v2.ResourceId{
		ResourceType: workspaceResourceType.Id,
		Resource:     "my-ws",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Nil(t, resources)
}

func TestSPSecretBuilder_List_NilParent(t *testing.T) {
	builder := newServicePrincipalSecretBuilder(nil)

	resources, results, err := builder.List(context.Background(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Nil(t, resources)
}

func TestSPSecretBuilder_NoEntitlementsOrGrants(t *testing.T) {
	builder := newServicePrincipalSecretBuilder(nil)

	ents, _, err := builder.Entitlements(context.Background(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, ents)

	grants, _, err := builder.Grants(context.Background(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, grants)
}

func TestSPSecretBuilder_List_CreatedAtParsed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"secrets": []map[string]interface{}{
				{
					"id":          "s-ts",
					"create_time": "2025-06-01T09:00:00Z",
					"secret_hash": "xhash",
					"status":      "ACTIVE",
				},
			},
			"next_page_token": "",
		})
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newServicePrincipalSecretBuilder(client)

	parentID := &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: "777"}
	resources, _, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, resources, 1)

	trait := getSecretTrait(t, resources[0])
	require.NotNil(t, trait.GetCreatedAt())
	// 2025-06-01T09:00:00Z = Unix 1748768400
	require.Equal(t, int64(1748768400), trait.GetCreatedAt().GetSeconds())
}
