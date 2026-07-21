package connector

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestServicePrincipalBuilderIssueClientSecret(t *testing.T) {
	identityID := &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: "sp-123"}
	builder := &servicePrincipalBuilder{
		resourceType: servicePrincipalResourceType,
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

	secret, plaintexts, _, err := builder.Issue(context.Background(), identityID, v2.LocalCredentialOptions_builder{
		ClientSecret: v2.LocalCredentialOptions_ClientSecret_builder{Ttl: durationpb.New(24 * time.Hour)}.Build(),
	}.Build())
	require.NoError(t, err)
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
	builder := &servicePrincipalBuilder{}
	_, _, _, err := builder.Issue(context.Background(), &v2.ResourceId{
		ResourceType: servicePrincipalResourceType.Id,
		Resource:     "sp-123",
	}, v2.LocalCredentialOptions_builder{
		Token: &v2.LocalCredentialOptions_Token{},
	}.Build())
	require.ErrorContains(t, err, "only OAuth client-secret credentials")
}
