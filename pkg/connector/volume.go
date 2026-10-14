package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*volumeBuilder)(nil)

type volumeBuilder struct {
	securableDeps
}

func newVolumeBuilder(client *databricks.Client, uc *unityCatalog, sync syncPrincipals) *volumeBuilder {
	return &volumeBuilder{securableDeps{client: client, uc: uc, sync: sync}}
}

func (b *volumeBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return volumeResourceType
}

func (b *volumeBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	parent, err := parseSecurableRef(parentResourceID.GetResource())
	if parentResourceID.GetResourceType() == schemaResourceType.Id && err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse volume parent %q: %w", parentResourceID.GetResource(), err)
	}

	return b.listSchemaChildren(ctx, parentResourceID, attr, volumeResourceType, securableTypeVolume, "volume",
		func(ctx context.Context, workspace, pageToken string) ([]securable, string, *v2.RateLimitDescription, error) {
			volumes, next, rateLimit, err := b.client.ListVolumes(ctx, workspace, parent.catalog(), parent.schema(), pageToken, ResourcesPageSize)
			items := make([]securable, 0, len(volumes))
			for _, volume := range volumes {
				if volume.Name == "" {
					continue
				}
				items = append(items, securable{
					name:  volume.Name,
					owner: volume.Owner,
					profile: map[string]any{
						profileKeyCatalogName: parent.catalog(),
						profileKeySchemaName:  parent.schema(),
						profileKeyVolumeName:  volume.Name,
						profileKeyVolumeType:  volume.VolumeType,
					},
				})
			}

			return items, next, rateLimit, err
		})
}

var volumeKind = securableKind{
	name:        databricks.SecurableVolume,
	parse:       parseSecurableRef,
	privileges:  volumePrivileges,
	writeRemedy: catalogTrustRemedy,
	goneCodes: []string{
		databricks.ErrorCodeVolumeDoesNotExist,
		databricks.ErrorCodeSchemaDoesNotExist,
		databricks.ErrorCodeCatalogDoesNotExist,
	},
	access: func(ctx context.Context, d *securableDeps, resource *v2.Resource, ref securableRef) (string, accessState, *v2.RateLimitDescription, error) {
		return d.catalogAccess(ctx, resource, ref, true)
	},
}

func (b *volumeBuilder) StaticEntitlements(_ context.Context, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	rv := make([]*v2.Entitlement, 0, len(volumePrivileges)+1)
	for _, privilege := range volumePrivileges {
		rv = append(rv, ent.NewPermissionEntitlement(nil, privilege,
			grantable,
			ent.WithDescription(fmt.Sprintf("%s privilege on a volume in Databricks", privilege)),
		))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(nil, ownerEntitlement,
		ent.WithDescription("Owns a volume in Databricks"),
	))

	return rv, nil, nil
}

func (b *volumeBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (b *volumeBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return b.grantsOn(ctx, resource, attr, databricks.SecurableVolume, volumePrivileges)
}

func (b *volumeBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return b.grantSecurable(ctx, principal, entitlement, volumeKind)
}

func (b *volumeBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return b.revokeSecurable(ctx, revoked, volumeKind)
}
