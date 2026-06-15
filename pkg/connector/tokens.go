package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// millisToTime converts a Unix-millisecond timestamp to time.Time.
// Returns zero time if ms is ≤ 0 (Databricks uses -1 for "never expires").
func millisToTime(ms int64) (time.Time, bool) {
	if ms <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

type tokenBuilder struct {
	client *databricks.Client
}

func (b *tokenBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return workspacePATResourceType
}

func (b *tokenBuilder) patResource(
	token *databricks.TokenInfo,
	workspaceResourceID *v2.ResourceId,
) (*v2.Resource, error) {
	traitOpts := []rs.SecretTraitOption{
		rs.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET),
		rs.WithSecretDetail("databricks.pat"),
	}

	if t, ok := millisToTime(token.CreationTime); ok {
		traitOpts = append(traitOpts, rs.WithSecretCreatedAt(t))
	}

	if t, ok := millisToTime(token.ExpiryTime); ok {
		traitOpts = append(traitOpts, rs.WithSecretExpiresAt(t))
	}

	// Back-ref the owning user when a user ID is available.
	if token.CreatedByID > 0 {
		userID := strconv.FormatInt(token.CreatedByID, 10)
		userResourceID, err := rs.NewResourceID(userResourceType, userID)
		if err == nil {
			traitOpts = append(traitOpts, rs.WithSecretIdentityID(userResourceID))
		}
	}

	displayName := token.TokenID
	if token.Comment != "" {
		displayName = token.Comment
	}

	return rs.NewSecretResource(
		displayName,
		workspacePATResourceType,
		token.TokenID,
		traitOpts,
		rs.WithParentResourceID(workspaceResourceID),
	)
}

// List fans out over the parent workspace and emits a PAT resource for each
// token. If the configured account SP lacks workspace-admin, the endpoint
// returns 403; we log and skip that workspace rather than failing the whole sync.
func (b *tokenBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil || parentResourceID.ResourceType != workspaceResourceType.Id {
		return nil, nil, nil
	}

	l := ctxzap.Extract(ctx)
	workspaceDeploymentName := parentResourceID.Resource

	tokens, _, err := b.client.ListTokenManagementTokens(ctx, workspaceDeploymentName)
	if err != nil {
		var apiErr *databricks.APIError
		if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusForbidden || apiErr.StatusCode == http.StatusUnauthorized) {
			l.Warn("databricks-connector: skipping workspace PATs — account SP lacks workspace-admin",
				zap.String("workspace", workspaceDeploymentName),
				zap.Int("status", apiErr.StatusCode),
			)
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("databricks-connector: failed to list tokens for workspace %s: %w", workspaceDeploymentName, err)
	}

	l.Debug("listed workspace PATs", zap.String("workspace", workspaceDeploymentName), zap.Int("count", len(tokens)))

	rv := make([]*v2.Resource, 0, len(tokens))
	for i := range tokens {
		r, err := b.patResource(&tokens[i], parentResourceID)
		if err != nil {
			return nil, nil, err
		}
		rv = append(rv, r)
	}

	return rv, nil, nil
}

// Entitlements returns nil — PATs carry no access entitlements.
func (b *tokenBuilder) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// Grants returns nil — PATs carry no grants.
func (b *tokenBuilder) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func newTokenBuilder(client *databricks.Client) *tokenBuilder {
	return &tokenBuilder{client: client}
}
