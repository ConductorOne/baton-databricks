package connector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*metastoreBuilder)(nil)

type metastoreBuilder struct {
	client   *databricks.Client
	clientID string
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
	sync       syncPrincipals
}

func newMetastoreBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *metastoreBuilder {
	return &metastoreBuilder{client: client, clientID: clientID, workspaces: workspaces, sync: sync}
}

func (m *metastoreBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return metastoreResourceType
}

func metastoreResource(_ context.Context, metastore *databricks.Metastore, parent *v2.ResourceId, workspace, workspaceID, workspaceName string) (*v2.Resource, error) {
	profile := map[string]any{
		profileKeyMetastoreID:   metastore.MetastoreID,
		profileKeyRegion:        metastore.Region,
		profileKeyOwner:         metastore.Owner,
		profileKeySecurableType: securableTypeMetastore,
		profileKeyWorkspace:     workspace,
		profileKeyWorkspaceID:   workspaceID,
		profileKeyWorkspaceName: workspaceName,
	}

	name := metastore.Name
	if name == "" {
		name = metastore.MetastoreID
	}

	// The permissions endpoint addresses a metastore by UUID, so that is the id.
	return rs.NewAppResource(
		name,
		metastoreResourceType,
		metastore.MetastoreID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(
			&v2.ChildResourceType{ResourceTypeId: catalogResourceType.Id},
			&v2.RawId{Id: metastore.MetastoreID},
		),
	)
}

// metastorePermissionsName: a metastore is addressed by a bare UUID. The
// separator only appears in the composite ids of the securables below it, so
// finding one here means the wrong resource arrived.
func metastorePermissionsName(resourceID string) (string, error) {
	if resourceID == "" || strings.Contains(resourceID, securableKeySeparator) {
		return "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid metastore resource id %q", resourceID)
	}

	return resourceID, nil
}

var metastoreLevel = securableLevel{
	securableType: databricks.SecurableMetastore,
	noun:          "metastore",
	privileges:    metastorePrivileges,
	// MANAGE is not assignable on a metastore, so naming it here would send the
	// operator after a privilege Databricks rejects. The authority is ownership.
	writeRemedy:     "the connector must own the metastore or be Admin on an attached workspace",
	trustRemedy:     "the connector's service principal must own the metastore or be assigned Admin directly on the workspace",
	permissionsName: metastorePermissionsName,
	trusts:          metastoreGrantsAreTrustworthy,
}

// List emits each metastore with the workspace its permissions are read through.
// A metastore is left out only when no attached workspace is in scope or usable.
// A pending workspace fails the sync instead, so ConductorOne keeps the metastore.
func (m *metastoreBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != accountResourceType.Id {
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	metastores, rateLimit, err := m.client.ListMetastores(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, refuseUnreadable("databricks-connector: failed to list metastores", err)
	}

	scope, err := scopedWorkspaces(ctx, m.client, m.workspaces, &annos)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	l := ctxzap.Extract(ctx)
	var rv []*v2.Resource
	for _, metastore := range metastores {
		if metastore.MetastoreID == "" {
			continue
		}

		// A 404 here does not mean the metastore is gone. Skipping it would make ConductorOne delete it.
		usable, inScope, err := scope.attached(ctx, m.client, metastore.MetastoreID, &annos)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, assignmentListError(metastore.MetastoreID, err)
		}
		if len(usable) == 0 {
			if inScope {
				return nil, &rs.SyncOpResults{Annotations: annos}, status.Errorf(codes.FailedPrecondition,
					"databricks-connector: metastore %s has no running workspace in sync scope", metastore.MetastoreID)
			}
			l.Debug("databricks-connector: metastore has no workspace in sync scope; skipping it",
				zap.String("metastore", metastore.MetastoreID))
			continue
		}

		workspace, err := pickReadableWorkspace(ctx, m.client, m.clientID, metastore.Owner, metastore.MetastoreID, usable, &annos)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		mr, err := metastoreResource(ctx, &metastore, parentResourceID, workspace.DeploymentName, strconv.Itoa(workspace.ID), workspace.Name)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		rv = append(rv, mr)
	}

	return rv, &rs.SyncOpResults{Annotations: annos}, nil
}

func (m *metastoreBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(metastorePrivileges)+1)
	for _, privilege := range metastorePrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a metastore in Databricks", privilege)),
		))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a metastore in Databricks"),
	))

	return rv, nil, nil
}

func (m *metastoreBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (m *metastoreBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return syncSecurableGrants(ctx, m.client, m.clientID, m.sync, metastoreLevel, resource, attr)
}

func (m *metastoreBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	routed, err := m.entitlementOnLiveRoute(ctx, entitlement, &annos)
	if err != nil {
		if errors.Is(err, errMetastoreGone) {
			return annos, status.Errorf(codes.NotFound,
				"databricks-connector: metastore %s is no longer on the account", entitlement.GetResource().GetId().GetResource())
		}

		return annos, err
	}

	return grantSecurable(ctx, m.client, m.clientID, metastoreLevel, principal, routed)
}

func (m *metastoreBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	annos := annotations.Annotations{}
	routed, err := m.entitlementOnLiveRoute(ctx, revoked.GetEntitlement(), &annos)
	if err != nil {
		if errors.Is(err, errMetastoreGone) {
			annos.Update(&v2.GrantAlreadyRevoked{})
			return annos, nil
		}

		return annos, err
	}

	grantCopy := proto.Clone(revoked).(*v2.Grant)
	grantCopy.Entitlement = routed

	return revokeSecurable(ctx, m.client, m.clientID, metastoreLevel, grantCopy)
}

// entitlementOnLiveRoute returns the entitlement addressed at a workspace.
// A workspace on the profile is the one List proved. It is kept when that
// workspace is still running and attached. Otherwise the route is resolved live.
func (m *metastoreBuilder) entitlementOnLiveRoute(
	ctx context.Context,
	entitlement *v2.Entitlement,
	annos *annotations.Annotations,
) (*v2.Entitlement, error) {
	resource := entitlement.GetResource()
	metastoreID := resource.GetId().GetResource()
	profile := rs.GetProfile(resource)
	profileWorkspace, _ := rs.GetProfileStringValue(profile, profileKeyWorkspace)
	if profileWorkspace != "" {
		kept, err := m.profileWorkspaceStillServes(ctx, metastoreID, profileWorkspace, annos)
		if err != nil {
			return nil, err
		}
		if kept {
			return entitlement, nil
		}
	}
	metastores, rateLimit, err := m.client.ListMetastores(ctx)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return nil, fmt.Errorf("databricks-connector: failed to list metastores: %w", err)
	}

	found := slices.IndexFunc(metastores, func(metastore databricks.Metastore) bool {
		return metastore.MetastoreID == metastoreID
	})
	if found < 0 {
		return nil, errMetastoreGone
	}

	scope, err := scopedWorkspaces(ctx, m.client, m.workspaces, annos)
	if err != nil {
		return nil, err
	}
	usable, _, err := scope.attached(ctx, m.client, metastoreID, annos)
	if err != nil {
		return nil, assignmentListError(metastoreID, err)
	}
	if len(usable) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: metastore %s has no running workspace in sync scope", metastoreID)
	}

	workspace, err := pickReadableWorkspace(ctx, m.client, m.clientID, metastores[found].Owner, metastoreID, usable, annos)
	if err != nil {
		return nil, err
	}

	routed, err := metastoreResource(ctx, &metastores[found], resource.GetParentResourceId(),
		workspace.DeploymentName, strconv.Itoa(workspace.ID), workspace.Name)
	if err != nil {
		return nil, err
	}

	copyEntitlement := proto.Clone(entitlement).(*v2.Entitlement)
	copyEntitlement.Resource = routed

	return copyEntitlement, nil
}

// profileWorkspaceStillServes reports whether workspace is still a running
// workspace attached to the metastore. A profile written by an earlier sync
// can name one that no longer is.
func (m *metastoreBuilder) profileWorkspaceStillServes(
	ctx context.Context,
	metastoreID, workspace string,
	annos *annotations.Annotations,
) (bool, error) {
	scope, err := scopedWorkspaces(ctx, m.client, m.workspaces, annos)
	if err != nil {
		return false, err
	}
	usable, _, err := scope.attached(ctx, m.client, metastoreID, annos)
	if err != nil {
		return false, assignmentListError(metastoreID, err)
	}
	for i := range usable {
		if usable[i].DeploymentName == workspace {
			return true, nil
		}
	}

	return false, nil
}
