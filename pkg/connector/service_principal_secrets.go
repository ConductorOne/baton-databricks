package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

type servicePrincipalSecretBuilder struct {
	client *databricks.Client
}

func newServicePrincipalSecretBuilder(client *databricks.Client) *servicePrincipalSecretBuilder {
	return &servicePrincipalSecretBuilder{client: client}
}

func (s *servicePrincipalSecretBuilder) ResourceType(context.Context) *v2.ResourceType {
	return servicePrincipalSecretResourceType
}

func (s *servicePrincipalSecretBuilder) List(
	ctx context.Context,
	parentResourceID *v2.ResourceId,
	attr resource.SyncOpAttrs,
) ([]*v2.Resource, *resource.SyncOpResults, error) {
	if parentResourceID == nil {
		return nil, nil, nil
	}

	response, err := s.client.ListServicePrincipalSecrets(ctx, parentResourceID.GetResource(), attr.PageToken.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: list service principal secrets: %w", err)
	}

	resources := make([]*v2.Resource, 0, len(response.Secrets))
	for i := range response.Secrets {
		secret, err := servicePrincipalSecretResource(parentResourceID, &response.Secrets[i])
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, secret)
	}
	return resources, &resource.SyncOpResults{NextPageToken: response.NextPageToken}, nil
}

func (*servicePrincipalSecretBuilder) Entitlements(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

func (*servicePrincipalSecretBuilder) Grants(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

func (s *servicePrincipalBuilder) Issue(
	ctx context.Context,
	identityID *v2.ResourceId,
	credentialOptions *v2.LocalCredentialOptions,
) (*v2.Resource, []*v2.PlaintextData, annotations.Annotations, error) {
	if identityID == nil || identityID.GetResourceType() != servicePrincipalResourceType.Id {
		return nil, nil, nil, fmt.Errorf("databricks-connector: invalid service principal identity")
	}
	clientSecret := credentialOptions.GetClientSecret()
	if clientSecret == nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: only OAuth client-secret credentials are supported")
	}

	lifetime := ""
	if ttl := clientSecret.GetTtl(); ttl != nil {
		if err := ttl.CheckValid(); err != nil || ttl.AsDuration() <= 0 || ttl.GetNanos() != 0 {
			return nil, nil, nil, fmt.Errorf("databricks-connector: invalid client-secret TTL")
		}
		lifetime = fmt.Sprintf("%ds", ttl.GetSeconds())
	}

	created, err := s.createSecret(ctx, identityID.GetResource(), lifetime)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("databricks-connector: create service principal secret: %w", err)
	}
	if created.Secret == "" {
		return nil, nil, nil, fmt.Errorf("databricks-connector: create service principal secret returned no secret material")
	}

	plaintext := []byte(created.Secret)
	created.Secret = ""
	secret, err := servicePrincipalSecretResource(identityID, created)
	if err != nil {
		return nil, nil, nil, err
	}
	return secret, []*v2.PlaintextData{{
		Name:        "client_secret",
		Description: "Databricks OAuth client secret",
		Bytes:       plaintext,
	}}, nil, nil
}

func (*servicePrincipalBuilder) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	return &v2.CredentialDetailsCredentialIssue{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
	}, nil, nil
}

func servicePrincipalSecretResource(identityID *v2.ResourceId, secret *databricks.ServicePrincipalSecret) (*v2.Resource, error) {
	if secret.ID == "" {
		return nil, fmt.Errorf("databricks-connector: service principal secret has no ID")
	}

	secretOptions := []resource.SecretTraitOption{
		resource.WithSecretIdentityID(identityID),
		resource.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET),
		resource.WithSecretDetail("databricks.oauth_client_secret"),
	}
	resourceOptions := []resource.ResourceOption{
		resource.WithParentResourceID(identityID),
		resource.WithAnnotation(&v2.RawId{Id: secret.ID}),
	}
	if secret.CreateTime != "" {
		createdAt, err := time.Parse(time.RFC3339Nano, secret.CreateTime)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: parse service principal secret creation time: %w", err)
		}
		resourceOptions = append(resourceOptions, resource.WithResourceCreatedAt(createdAt))
	}
	if secret.ExpireTime != "" {
		expiresAt, err := time.Parse(time.RFC3339Nano, secret.ExpireTime)
		if err != nil {
			return nil, fmt.Errorf("databricks-connector: parse service principal secret expiry: %w", err)
		}
		secretOptions = append(secretOptions, resource.WithSecretExpiresAt(expiresAt))
	}

	return resource.NewSecretResource(
		secret.ID,
		servicePrincipalSecretResourceType,
		secret.ID,
		secretOptions,
		resourceOptions...,
	)
}
