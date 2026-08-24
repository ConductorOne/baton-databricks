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

func TestTokenBuilder_List_Basic(t *testing.T) {
	tokens := []map[string]interface{}{
		{
			"token_id":            "tok-abc",
			"comment":             "CI token",
			"creation_time":       int64(1748000000000),
			"expiry_time":         int64(1780000000000),
			"created_by_id":       int64(101),
			"created_by_username": "alice@example.com",
		},
		{
			"token_id":            "tok-def",
			"comment":             "",
			"creation_time":       int64(1745000000000),
			"expiry_time":         int64(-1),
			"created_by_id":       int64(202),
			"created_by_username": "bob@example.com",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"token_infos": tokens,
		}))
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newTokenBuilder(client)

	parentID := &v2.ResourceId{
		ResourceType: workspaceResourceType.Id,
		Resource:     "my-workspace",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Len(t, resources, 2)

	for _, r := range resources {
		require.Equal(t, workspacePATResourceType.Id, r.Id.ResourceType)
		require.Equal(t, parentID.Resource, r.ParentResourceId.Resource)
		require.Equal(t, workspaceResourceType.Id, r.ParentResourceId.ResourceType)

		trait := getSecretTrait(t, r)
		require.Equal(t, v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET, trait.GetCredentialType())
		require.Equal(t, "databricks.pat", trait.GetCredentialDetail())
		require.NotNil(t, trait.GetCreatedAt())
	}

	// tok-abc: has a real expiry; back-ref to user 101; display name = comment
	var tokABC *v2.Resource
	for _, r := range resources {
		if r.Id.Resource == "tok-abc" {
			tokABC = r
			break
		}
	}
	require.NotNil(t, tokABC)
	traitABC := getSecretTrait(t, tokABC)
	require.NotNil(t, traitABC.GetExpiresAt())
	require.Equal(t, "101", traitABC.GetIdentityId().GetResource())
	require.Equal(t, userResourceType.Id, traitABC.GetIdentityId().GetResourceType())
	require.Equal(t, "CI token", tokABC.DisplayName)

	// tok-def: expiry_time=-1 → no expires_at; comment="" → display name = token_id
	var tokDEF *v2.Resource
	for _, r := range resources {
		if r.Id.Resource == "tok-def" {
			tokDEF = r
			break
		}
	}
	require.NotNil(t, tokDEF)
	traitDEF := getSecretTrait(t, tokDEF)
	require.Nil(t, traitDEF.GetExpiresAt())
	require.Equal(t, "tok-def", tokDEF.DisplayName)
}

func TestTokenBuilder_List_Forbidden_GracefulDegrade(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "User does not have CAN_MANAGE permission",
		}))
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newTokenBuilder(client)

	parentID := &v2.ResourceId{
		ResourceType: workspaceResourceType.Id,
		Resource:     "restricted-workspace",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err, "403 must be swallowed, not returned as an error")
	require.Nil(t, results)
	require.Nil(t, resources)
}

func TestTokenBuilder_List_Unauthorized_GracefulDegrade(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "Unauthorized",
		}))
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newTokenBuilder(client)

	parentID := &v2.ResourceId{
		ResourceType: workspaceResourceType.Id,
		Resource:     "another-workspace",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err, "401 must be swallowed, not returned as an error")
	require.Nil(t, results)
	require.Nil(t, resources)
}

func TestTokenBuilder_List_WrongParentType(t *testing.T) {
	builder := newTokenBuilder(nil)

	parentID := &v2.ResourceId{
		ResourceType: servicePrincipalResourceType.Id,
		Resource:     "sp-123",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Nil(t, resources)
}

func TestTokenBuilder_List_NilParent(t *testing.T) {
	builder := newTokenBuilder(nil)

	resources, results, err := builder.List(context.Background(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Nil(t, resources)
}

func TestTokenBuilder_NoEntitlementsOrGrants(t *testing.T) {
	builder := newTokenBuilder(nil)

	ents, _, err := builder.Entitlements(context.Background(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, ents)

	grants, _, err := builder.Grants(context.Background(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, grants)
}

func TestTokenBuilder_List_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"token_infos": []interface{}{},
		}))
	}))
	defer srv.Close()

	client := newTestDatabricksClient(t, srv)
	builder := newTokenBuilder(client)

	parentID := &v2.ResourceId{
		ResourceType: workspaceResourceType.Id,
		Resource:     "empty-workspace",
	}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Nil(t, results)
	require.Empty(t, resources)
}
