package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

type credentialLifecycleConnector struct {
	servicePrincipals connectorbuilder.ResourceSyncerV2
	secrets           *servicePrincipalSecretBuilder
}

func (c *credentialLifecycleConnector) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{}, nil
}

func (c *credentialLifecycleConnector) Validate(context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (c *credentialLifecycleConnector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{c.servicePrincipals, c.secrets}
}

func newIssueEncryptionConfig(t *testing.T) *v2.EncryptionConfig {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	publicKey := (&jose.JSONWebKey{Key: privateKey}).Public()
	encoded, err := publicKey.MarshalJSON()
	require.NoError(t, err)
	return v2.EncryptionConfig_builder{
		Provider:           jwk.EncryptionProviderJwk,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{PubKey: encoded}.Build(),
	}.Build()
}

func TestServicePrincipalBuilderIssueClientSecret(t *testing.T) {
	identityID := &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: "sp-123"}
	builder := &credentialIssuingServicePrincipalBuilder{
		servicePrincipalBuilder: &servicePrincipalBuilder{resourceType: servicePrincipalResourceType},
		createSecret: func(_ context.Context, servicePrincipalID, lifetime string) (*databricks.ServicePrincipalSecret, error) {
			require.Equal(t, "sp-123", servicePrincipalID)
			require.Equal(t, "86400s", lifetime)
			return &databricks.ServicePrincipalSecret{
				ID:         "secret-456",
				Secret:     "one-time-client-secret",
				Status:     "ACTIVE",
				CreateTime: "2026-07-21T00:00:00.000Z",
				ExpireTime: "2026-07-22T00:00:00.000Z",
			}, nil
		},
	}

	output, err := builder.Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: identityID,
		CredentialOptions: v2.LocalCredentialOptions_builder{
			ClientSecret: &v2.LocalCredentialOptions_ClientSecret{},
		}.Build(),
		IssuanceConstraints: v2.CredentialIssuanceConstraints_builder{Lifetime: durationpb.New(24 * time.Hour)}.Build(),
	})
	require.NoError(t, err)
	secret, plaintexts := output.Secret, output.PlaintextData
	require.Equal(t, "secret-456", secret.GetId().GetResource())
	require.Len(t, plaintexts, 1)
	require.Equal(t, "client_secret", plaintexts[0].GetName())
	require.Equal(t, []byte("one-time-client-secret"), plaintexts[0].GetBytes())

	secretTrait := &v2.SecretTrait{}
	annos := annotations.Annotations(secret.GetAnnotations())
	found, err := annos.Pick(secretTrait)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, identityID.GetResource(), secretTrait.GetIdentityId().GetResource())
}

func TestServicePrincipalBuilderIssueRejectsTokenArm(t *testing.T) {
	builder := &credentialIssuingServicePrincipalBuilder{servicePrincipalBuilder: &servicePrincipalBuilder{}}
	_, err := builder.Issue(context.Background(), &connectorbuilder.CredentialIssueInput{IdentityID: &v2.ResourceId{
		ResourceType: servicePrincipalResourceType.Id,
		Resource:     "sp-123",
	}, CredentialOptions: v2.LocalCredentialOptions_builder{
		Token: &v2.LocalCredentialOptions_Token{},
	}.Build()})
	require.ErrorContains(t, err, "only OAuth client-secret credentials")
}

func TestServicePrincipalSecretCredentialLifecycle(t *testing.T) {
	ctx := context.Background()
	secrets := map[string]databricks.ServicePrincipalSecret{}
	deleted := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.True(t, strings.HasPrefix(r.URL.Path, "/api/2.0/accounts/account-1/servicePrincipals/sp-123/credentials/secrets"))
		switch r.Method {
		case http.MethodPost:
			secret := databricks.ServicePrincipalSecret{ID: "secret-456", Secret: "one-time-secret", Status: "ACTIVE"}
			secrets[secret.ID] = secret
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"id":     secret.ID,
				"secret": secret.Secret,
				"status": secret.Status,
			}))
		case http.MethodGet:
			listed := make([]databricks.ServicePrincipalSecret, 0, len(secrets))
			for _, secret := range secrets {
				secret.Secret = ""
				listed = append(listed, secret)
			}
			require.NoError(t, json.NewEncoder(w).Encode(databricks.ListServicePrincipalSecretsResponse{Secrets: listed}))
		case http.MethodDelete:
			require.Equal(t, "/api/2.0/accounts/account-1/servicePrincipals/sp-123/credentials/secrets/secret-456", r.URL.Path)
			delete(secrets, "secret-456")
			deleted = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := databricks.NewClient(ctx, server.Client(), "workspace.example", serverURL.Host, "account-1", server.URL, &databricks.NoAuth{})
	require.NoError(t, err)
	lifecycle := &credentialLifecycleConnector{
		servicePrincipals: newServicePrincipalBuilder(client),
		secrets:           newServicePrincipalSecretBuilder(client),
	}
	connector, err := connectorbuilder.NewConnector(ctx, lifecycle)
	require.NoError(t, err)

	identityID := v2.ResourceId_builder{ResourceType: servicePrincipalResourceType.Id, Resource: "sp-123"}.Build()
	issued, err := connector.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
		IdentityId: identityID,
		CredentialOptions: v2.CredentialOptions_builder{
			ClientSecret: &v2.CredentialOptions_ClientSecret{},
		}.Build(),
		EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "secret-456", issued.GetSecret().GetId().GetResource())
	require.Equal(t, identityID, issued.GetSecret().GetParentResourceId())

	listed, _, err := lifecycle.secrets.List(ctx, identityID, resource.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, issued.GetSecret().GetId(), listed[0].GetId())
	require.Equal(t, issued.GetSecret().GetParentResourceId(), listed[0].GetParentResourceId())

	_, err = connector.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{
		ResourceId: issued.GetSecret().GetId(), ParentResourceId: issued.GetSecret().GetParentResourceId(),
	}.Build())
	require.NoError(t, err)
	require.True(t, deleted)
	listed, _, err = lifecycle.secrets.List(ctx, identityID, resource.SyncOpAttrs{})
	require.NoError(t, err)
	require.Empty(t, listed)
}

func TestCredentialIssuanceCapabilityRequiresAccountConfiguration(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		accountID string
		wantIssue bool
	}{{"account", "account-1", true}, {"workspace only", "", false}} {
		t.Run(test.name, func(t *testing.T) {
			client, err := databricks.NewClient(ctx, http.DefaultClient, "workspace.example", "accounts.example", test.accountID, "https://workspace.example", &databricks.NoAuth{})
			require.NoError(t, err)
			lifecycle := &credentialLifecycleConnector{servicePrincipals: newServicePrincipalBuilder(client), secrets: newServicePrincipalSecretBuilder(client)}
			connector, err := connectorbuilder.NewConnector(ctx, lifecycle)
			require.NoError(t, err)
			metadata, err := connector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
			require.NoError(t, err)
			var found bool
			for _, resourceCapability := range metadata.GetMetadata().GetCapabilities().GetResourceTypeCapabilities() {
				if resourceCapability.GetResourceType().GetId() != servicePrincipalResourceType.Id {
					continue
				}
				for _, capability := range resourceCapability.GetCapabilities() {
					found = found || capability == v2.Capability_CAPABILITY_CREDENTIAL_ISSUE
				}
			}
			require.Equal(t, test.wantIssue, found)
		})
	}
}
