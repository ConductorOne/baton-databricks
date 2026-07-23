package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

type servicePrincipalSecretBuilder struct {
	client       *databricks.Client
	deleteSecret func(context.Context, string, string) error
}

var _ connectorbuilder.ResourceDeleterV2 = (*servicePrincipalSecretBuilder)(nil)

func newServicePrincipalSecretBuilder(client *databricks.Client) *servicePrincipalSecretBuilder {
	return &servicePrincipalSecretBuilder{client: client, deleteSecret: client.DeleteServicePrincipalSecret}
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
	if !s.client.HasAccountConfiguration() {
		return nil, &resource.SyncOpResults{}, nil
	}

	response, err := s.client.ListServicePrincipalSecrets(ctx, parentResourceID.GetResource(), attr.PageToken.Token)
	if err != nil {
		var apiErr *databricks.APIError
		if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusForbidden || apiErr.StatusCode == http.StatusNotFound) {
			ctxzap.Extract(ctx).Warn("service principal secrets are not readable; continuing without them", zap.Int("status_code", apiErr.StatusCode))
			return nil, &resource.SyncOpResults{}, nil
		}
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

func (s *servicePrincipalSecretBuilder) Delete(ctx context.Context, resourceID, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	if resourceID == nil || resourceID.GetResourceType() != servicePrincipalSecretResourceType.Id || resourceID.GetResource() == "" {
		return nil, fmt.Errorf("databricks-connector: invalid service principal secret resource")
	}
	if parentResourceID == nil || parentResourceID.GetResourceType() != servicePrincipalResourceType.Id || parentResourceID.GetResource() == "" {
		return nil, fmt.Errorf("databricks-connector: invalid service principal parent resource")
	}
	if err := s.deleteSecret(ctx, parentResourceID.GetResource(), resourceID.GetResource()); err != nil {
		var apiErr *databricks.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("databricks-connector: delete service principal secret: %w", err)
	}
	return nil, nil
}

func (s *credentialIssuingServicePrincipalBuilder) Issue(
	ctx context.Context,
	input *connectorbuilder.CredentialIssueInput,
) (*connectorbuilder.CredentialIssueOutput, error) {
	identityID := input.IdentityID
	if identityID == nil || identityID.GetResourceType() != servicePrincipalResourceType.Id {
		return nil, fmt.Errorf("databricks-connector: invalid service principal identity")
	}
	clientSecret := input.CredentialOptions.GetClientSecret()
	if clientSecret == nil {
		return nil, fmt.Errorf("databricks-connector: only OAuth client-secret credentials are supported")
	}

	lifetime := ""
	if expiresAt := input.ExpiresAt; expiresAt != nil {
		if err := expiresAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("databricks-connector: invalid client-secret lifetime")
		}
		remaining := time.Until(expiresAt.AsTime())
		if remaining <= 0 {
			return nil, fmt.Errorf("databricks-connector: invalid client-secret lifetime")
		}
		seconds := int64(remaining / time.Second)
		if seconds < 1 {
			return nil, fmt.Errorf("databricks-connector: client-secret lifetime is below provider minimum")
		}
		lifetime = fmt.Sprintf("%ds", seconds)
	}

	created, err := s.createSecret(ctx, identityID.GetResource(), lifetime)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: create service principal secret: %w", err)
	}
	if created.Secret == "" {
		return nil, fmt.Errorf("databricks-connector: create service principal secret returned no secret material")
	}

	plaintext := []byte(created.Secret)
	created.Secret = ""
	secret, err := servicePrincipalSecretResource(identityID, created)
	if err != nil {
		return nil, err
	}
	return &connectorbuilder.CredentialIssueOutput{
		Secret:       secret,
		ResourceMode: v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		PlaintextData: []*v2.PlaintextData{{
			Name:        "client_secret",
			Description: "Databricks OAuth client secret",
			Bytes:       plaintext,
		}},
	}, nil
}

func (*credentialIssuingServicePrincipalBuilder) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	return v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
				Expiry: v2.IssuanceExpiryCapability_builder{
					Min: durationpb.New(time.Second),
				}.Build(),
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
				SecretResourceTypeId: servicePrincipalSecretResourceType.Id,
			}.Build(),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET,
	}.Build(), nil, nil
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
