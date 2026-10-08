package connector

import (
	"fmt"
	"slices"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const (
	securableTypeMetastore = "METASTORE"
	securableTypeCatalog   = "CATALOG"
	securableTypeSchema    = "SCHEMA"
	securableTypeTable     = "TABLE"
	securableTypeVolume    = "VOLUME"

	profileKeyMetastoreID   = "metastore_id"
	profileKeyFullName      = "full_name"
	profileKeyOwner         = "owner"
	profileKeySecurableType = "securable_type"
	profileKeyCatalogName   = "catalog_name"
	profileKeyCatalogType   = "catalog_type"
	profileKeyIsolationMode = "isolation_mode"
	profileKeySchemaName    = "schema_name"
	profileKeyRegion        = "region"

	// Not a dot: a securable's full name is already dot-separated.
	securableKeySeparator = "::"

	informationSchemaName = "information_schema"
)

// securableRef is a securable keyed by its metastore plus its name components,
// catalog first. Databricks rejects a name holding a space, a period or a forward
// slash, so splitting a full name on "." is unambiguous.
type securableRef struct {
	metastoreID string
	parts       []string
}

// parseSecurableRef reads a securable resource id, "{metastore_id}::{full_name}".
func parseSecurableRef(resourceID string) (securableRef, error) {
	metastoreID, fullName, ok := strings.Cut(resourceID, securableKeySeparator)
	if !ok || metastoreID == "" || fullName == "" {
		return securableRef{}, fmt.Errorf("invalid securable resource id %q", resourceID)
	}

	return securableRef{metastoreID: metastoreID, parts: strings.Split(fullName, ".")}, nil
}

func (r securableRef) fullName() string {
	return strings.Join(r.parts, ".")
}

// permissionsName is the dotted full name, or the metastore's UUID for the metastore
// itself, whose name the endpoint rejects with 400 "Invalid UUID string".
func (r securableRef) permissionsName() string {
	if len(r.parts) == 0 {
		return r.metastoreID
	}

	return r.fullName()
}

// resourceKey is the resource id, which every level also carries as its RawId.
func (r securableRef) resourceKey() string {
	if len(r.parts) == 0 {
		return r.metastoreID
	}

	return r.metastoreID + securableKeySeparator + r.fullName()
}

func (r securableRef) catalog() string {
	return r.part(0)
}

func (r securableRef) part(i int) string {
	if i < len(r.parts) {
		return r.parts[i]
	}

	return ""
}

func (r securableRef) child(name string) securableRef {
	return securableRef{metastoreID: r.metastoreID, parts: append(slices.Clone(r.parts), name)}
}

// information_schema is skipped: its system views are owned by principals that do
// not exist in SCIM and all carry the same default SELECT. parts[1] is the schema
// at every level that has one.
func isUnderInformationSchema(ref securableRef) bool {
	return len(ref.parts) >= 2 && strings.EqualFold(ref.parts[1], informationSchemaName)
}

// securable is one row of a child listing, before it becomes a resource.
type securable struct {
	name    string
	owner   string
	profile map[string]any
}

// securableDeps is the client state every Unity Catalog builder shares.
type securableDeps struct {
	client   *databricks.Client
	uc       *unityCatalog
	willSync func(string) bool
}

// RawId equals the resource id: C1 matches preloaded resources on that annotation.
func newSecurableResource(
	resourceType *v2.ResourceType,
	securableTypeEnum string,
	childTypes []*v2.ResourceType,
	item securable,
	ref securableRef,
	parent *v2.ResourceId,
) (*v2.Resource, error) {
	profile := map[string]any{
		profileKeyMetastoreID:   ref.metastoreID,
		profileKeyFullName:      ref.fullName(),
		profileKeyOwner:         item.owner,
		profileKeySecurableType: securableTypeEnum,
	}
	for key, value := range item.profile {
		profile[key] = value
	}

	resourceKey := ref.resourceKey()
	options := []rs.ResourceOption{
		rs.WithResourceProfile(profile),
		rs.WithParentResourceID(parent),
		rs.WithAnnotation(&v2.RawId{Id: resourceKey}),
	}

	for _, childType := range childTypes {
		// Without this annotation the child's List is never invoked.
		options = append(options, rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: childType.Id}))
	}

	// The display name is the full name rather than the leaf: main.sales.orders and
	// main.finance.orders would otherwise be two rows reading "orders" in C1.
	return rs.NewAppResource(ref.fullName(), resourceType, resourceKey, nil, options...)
}
