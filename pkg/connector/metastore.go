package connector

import (
	"context"
	"fmt"
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
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*metastoreBuilder)(nil)

type metastoreBuilder struct {
	client *databricks.Client
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
			// Without this annotation the catalog syncer's List is never invoked.
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
// A metastore with no usable workspace is left out: its grants cannot be read.
func (m *metastoreBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID.GetResourceType() != accountResourceType.Id {
		return nil, nil, nil
	}

	annos := annotations.Annotations{}
	metastores, rateLimit, err := m.client.ListMetastores(ctx)
	noteRateLimit(&annos, rateLimit)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("databricks-connector: failed to list metastores: %w", err)
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

		// An assignment list that cannot be read, including a 404, leaves which
		// workspaces serve the metastore unknown. Skipping it here would drop the
		// metastore and make ConductorOne remove its grants, so the sync fails and
		// keeps what it already holds. A metastore that is really gone stops being
		// returned by ListMetastores instead.
		usable, inScope, err := scope.attached(ctx, m.client, metastore.MetastoreID, &annos)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf(
				"databricks-connector: failed to list the workspaces assigned to metastore %s, so the sync cannot tell which serve it: %w",
				metastore.MetastoreID, err)
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

		workspace := usable[0]
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
	return grantSecurable(ctx, m.client, m.clientID, metastoreLevel, principal, entitlement)
}

func (m *metastoreBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return revokeSecurable(ctx, m.client, m.clientID, metastoreLevel, revoked)
}
