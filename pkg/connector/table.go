package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tableBuilder struct {
	securableDeps
}

func newTableBuilder(client *databricks.Client, uc *unityCatalog, sync syncPrincipals) *tableBuilder {
	return &tableBuilder{securableDeps{client: client, uc: uc, sync: sync}}
}

func (b *tableBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return tableResourceType
}

func (b *tableBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	parent, err := parseSecurableRef(parentResourceID.GetResource())
	if parentResourceID.GetResourceType() == schemaResourceType.Id && err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse table parent %q: %w", parentResourceID.GetResource(), err)
	}

	return b.listSchemaChildren(ctx, parentResourceID, attr, tableResourceType, securableTypeTable, "table",
		func(ctx context.Context, workspace, pageToken string) ([]securable, string, *v2.RateLimitDescription, error) {
			tables, next, rateLimit, err := b.client.ListTables(ctx, workspace, parent.catalog(), parent.schema(), pageToken, ResourcesPageSize)
			items := make([]securable, 0, len(tables))
			for _, table := range tables {
				if table.Name == "" {
					continue
				}
				items = append(items, securable{
					name:  table.Name,
					owner: table.Owner,
					profile: map[string]any{
						profileKeyCatalogName: parent.catalog(),
						profileKeySchemaName:  parent.schema(),
						profileKeyTableName:   table.Name,
						profileKeyTableType:   table.TableType,
					},
				})
			}

			return items, next, rateLimit, err
		})
}

// privilegesForTable is the set this table instance reports. A missing
// table_type falls back to the union rather than to a single variant.
func privilegesForTable(ctx context.Context, resource *v2.Resource) []string {
	variant := profileString(resource, profileKeyTableType)
	if variant == "" {
		ctxzap.Extract(ctx).Debug("databricks-connector: securable has no table_type, using every table variant's privileges",
			zap.String("securable", resource.GetId().GetResource()),
		)

		return anyTablePrivileges
	}

	return privilegesForTableType(variant)
}

func tableKind(privileges []string) securableKind {
	return securableKind{
		name:        databricks.SecurableTable,
		parse:       parseSecurableRef,
		privileges:  privileges,
		writeRemedy: catalogTrustRemedy,
		goneCodes: []string{
			databricks.ErrorCodeTableDoesNotExist,
			databricks.ErrorCodeSchemaDoesNotExist,
			databricks.ErrorCodeCatalogDoesNotExist,
		},
		access: func(ctx context.Context, d *securableDeps, resource *v2.Resource, ref securableRef) (string, accessState, *v2.RateLimitDescription, error) {
			return d.catalogAccess(ctx, resource, ref, true)
		},
	}
}

func (b *tableBuilder) Entitlements(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	ref, err := parseSecurableRef(resource.GetId().GetResource())
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse table resource id: %w", err)
	}

	variant := profileString(resource, profileKeyTableType)
	grantable := ent.WithGrantableTo(userResourceType, groupResourceType, servicePrincipalResourceType)
	var rv []*v2.Entitlement
	for _, privilege := range privilegesForTable(ctx, resource) {
		options := []ent.EntitlementOption{
			ent.WithDisplayName(fmt.Sprintf("%s %s", resource.GetDisplayName(), privilege)),
			ent.WithDescription(fmt.Sprintf("%s privilege on table %s in Databricks", privilege, ref.permissionsName())),
		}
		// ALL_PRIVILEGES on a view is reported when SQL already granted it. The
		// grants API rejects adding it, so it is not requestable.
		if privilege != privilegeAllPrivileges || !isViewLike(variant) {
			options = append([]ent.EntitlementOption{grantable}, options...)
		}
		rv = append(rv, ent.NewPermissionEntitlement(resource, privilege, options...))
	}

	rv = append(rv, ent.NewOwnershipEntitlement(resource, ownerEntitlement,
		ent.WithDisplayName(fmt.Sprintf("%s owner", resource.GetDisplayName())),
		ent.WithDescription(fmt.Sprintf("Owns table %s in Databricks", ref.permissionsName())),
	))

	return rv, nil, nil
}

func (b *tableBuilder) Grants(ctx context.Context, resource *v2.Resource, attr rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return b.grantsOn(ctx, resource, attr, databricks.SecurableTable, privilegesForTable(ctx, resource))
}

func (b *tableBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	resource := entitlement.GetResource()
	variant := profileString(resource, profileKeyTableType)
	if privilege, err := parseEntitlementPrivilege(entitlement.GetId()); err == nil &&
		privilege == privilegeAllPrivileges && isViewLike(variant) {
		return nil, status.Errorf(codes.InvalidArgument,
			"databricks-connector: the grants API rejects ALL_PRIVILEGES on a %s; grant SELECT or MANAGE instead",
			strings.ToUpper(strings.TrimSpace(variant)))
	}

	return b.grantSecurable(ctx, principal, entitlement, tableKind(privilegesForTable(ctx, resource)))
}

func (b *tableBuilder) Revoke(ctx context.Context, revoked *v2.Grant) (annotations.Annotations, error) {
	resource := revoked.GetEntitlement().GetResource()

	return b.revokeSecurable(ctx, revoked, tableKind(privilegesForTable(ctx, resource)))
}
