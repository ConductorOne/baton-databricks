package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

var _ connectorbuilder.StaticEntitlementSyncerV2 = (*volumeBuilder)(nil)

const (
	securableTypeVolume = "VOLUME"

	profileKeyVolumeName = "volume_name"
	profileKeyVolumeType = "volume_type"

	// volumeWriteRemedy is what a 403 on the PATCH tells the operator.
	volumeWriteRemedy = "the connector must own the metastore, the parent catalog, the parent schema or the volume, " +
		"or hold MANAGE on the volume, the parent schema or the parent catalog"

	// volumeTrustRemedy is what a refusal to report a partial privilege list
	// tells the operator.
	volumeTrustRemedy = "the connector's service principal must own the metastore, the parent catalog, the parent schema or the volume, " +
		"hold MANAGE on the volume, the parent schema or the parent catalog, or be assigned Admin directly on the workspace whose " +
		"admins group owns the parent catalog"
)

// volumePrivileges is the set a volume takes: its files are read and written
// rather than queried, so none of the table DML applies.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
var volumePrivileges = []string{
	privilegeAllPrivileges,
	"APPLY_TAG",
	privilegeManage,
	"READ_METADATA",
	"READ_VOLUME",
	"WRITE_VOLUME",
}

var volumeLevel = securableLevel{
	securableType:   databricks.SecurableVolume,
	noun:            "volume",
	privileges:      volumePrivileges,
	writeRemedy:     volumeWriteRemedy,
	trustRemedy:     volumeTrustRemedy,
	permissionsName: volumePermissionsName,
	trusts:          volumeGrantsAreTrustworthy,
}

type volumeBuilder struct {
	client *databricks.Client
	// clientID is the applicationId of the service principal the connector
	// authenticates as, which is what grant visibility is decided against.
	clientID string
	// workspaces is the configured allowlist, empty when none was given.
	workspaces map[string]struct{}
	sync       syncPrincipals
}

func newVolumeBuilder(client *databricks.Client, clientID string, workspaces map[string]struct{}, sync syncPrincipals) *volumeBuilder {
	return &volumeBuilder{client: client, clientID: clientID, workspaces: workspaces, sync: sync}
}

func (b *volumeBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return volumeResourceType
}

// volumePermissionsName: the permissions endpoint addresses a volume by its
// dotted full name. The metastore is carried by the host.
func volumePermissionsName(resourceID string) (string, error) {
	_, catalogName, schemaName, name, err := parseSchemaChildID(resourceID, "volume")
	if err != nil {
		return "", err
	}

	return strings.Join([]string{catalogName, schemaName, name}, schemaNameSeparator), nil
}

func volumeGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	return schemaChildGrantsAreTrustworthy(ctx, client, trust, databricks.SecurableVolume, annos)
}

func volumeResource(
	volume *databricks.Volume,
	metastoreID, catalogName, schemaName string,
	parent *v2.ResourceId,
	route schemaChildRoute,
) (*v2.Resource, error) {
	fullName := strings.Join([]string{catalogName, schemaName, volume.Name}, schemaNameSeparator)
	profile := schemaChildProfile(metastoreID, catalogName, schemaName, fullName, volume.Owner, securableTypeVolume, route)
	profile[profileKeyVolumeName] = volume.Name
	profile[profileKeyVolumeType] = volume.VolumeType

	resourceID := schemaChildResourceID(metastoreID, catalogName, schemaName, volume.Name)

	// The display name is the full name rather than the leaf: main.sales.files
	// and main.finance.files would otherwise both read "files" in C1.
	return rs.NewAppResource(
		fullName,
		volumeResourceType,
		resourceID,
		nil,
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: resourceID}),
	)
}

// List pages the volumes of one schema.
func (b *volumeBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	return listSchemaChildren(ctx, b.client, b.clientID, b.workspaces, volumeResourceType, "volume", parentResourceID, attr,
		func(ctx context.Context, route schemaChildRoute, metastoreID, catalogName, schemaName, pageToken string) (
			[]*v2.Resource, string, *v2.RateLimitDescription, error) {
			volumes, next, rateLimit, err := b.client.ListVolumes(ctx, route.Workspace, catalogName, schemaName, pageToken, ResourcesPageSize)
			if err != nil {
				return nil, "", rateLimit, err
			}

			var rv []*v2.Resource
			for _, volume := range volumes {
				if volume.Name == "" {
					continue
				}

				built, err := volumeResource(&volume, metastoreID, catalogName, schemaName, parentResourceID, route)
				if err != nil {
					return nil, "", rateLimit, fmt.Errorf(
						"databricks-connector: failed to build the resource for volume %s.%s.%s: %w", catalogName, schemaName, volume.Name, err)
				}
				rv = append(rv, built)
			}

			return rv, next, rateLimit, nil
		})
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
	return syncSecurableGrants(ctx, b.client, b.clientID, b.sync, volumeLevel, resource, attr)
}

func (b *volumeBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return grantSecurable(ctx, b.client, b.clientID, volumeLevel, principal, entitlement)
}

func (b *volumeBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	return revokeSecurable(ctx, b.client, b.clientID, volumeLevel, revoked)
}
