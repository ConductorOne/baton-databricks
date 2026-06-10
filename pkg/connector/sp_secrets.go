package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type servicePrincipalSecretBuilder struct {
	client *databricks.Client
}

func (b *servicePrincipalSecretBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return servicePrincipalSecretResourceType
}

func (b *servicePrincipalSecretBuilder) spSecretResource(
	secret *databricks.SecretInfo,
	spResourceID *v2.ResourceId,
) (*v2.Resource, error) {
	traitOpts := []rs.SecretTraitOption{
		rs.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET),
		rs.WithSecretDetail("databricks.sp_oauth_secret"),
		rs.WithSecretIdentityID(spResourceID),
	}

	if secret.CreateTime != "" {
		t, err := time.Parse(time.RFC3339, secret.CreateTime)
		if err == nil {
			traitOpts = append(traitOpts, rs.WithSecretCreatedAt(t))
		}
	}

	return rs.NewSecretResource(
		secret.ID,
		servicePrincipalSecretResourceType,
		secret.ID,
		traitOpts,
		rs.WithParentResourceID(spResourceID),
	)
}

// List fans out over the parent service principal and emits a secret resource
// for each OAuth credential belonging to that SP.
func (b *servicePrincipalSecretBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil || parentResourceID.ResourceType != servicePrincipalResourceType.Id {
		return nil, nil, nil
	}

	l := ctxzap.Extract(ctx)
	spID := parentResourceID.Resource

	var rv []*v2.Resource
	pageToken := ""

	for {
		secrets, nextToken, _, err := b.client.ListServicePrincipalSecrets(ctx, spID, pageToken)
		if err != nil {
			return nil, nil, fmt.Errorf("databricks-connector: failed to list SP secrets for %s: %w", spID, err)
		}

		l.Debug("listed SP OAuth secrets", zap.String("sp_id", spID), zap.Int("count", len(secrets)))

		for i := range secrets {
			r, err := b.spSecretResource(&secrets[i], parentResourceID)
			if err != nil {
				return nil, nil, err
			}
			rv = append(rv, r)
		}

		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}

	return rv, nil, nil
}

// Entitlements returns nil — secrets carry no access entitlements.
func (b *servicePrincipalSecretBuilder) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// Grants returns nil — secrets carry no grants.
func (b *servicePrincipalSecretBuilder) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func newServicePrincipalSecretBuilder(client *databricks.Client) *servicePrincipalSecretBuilder {
	return &servicePrincipalSecretBuilder{client: client}
}
