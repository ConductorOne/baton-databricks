package connector

import (
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

const (
	securableTypeMetastore = "METASTORE"

	profileKeyMetastoreID   = "metastore_id"
	profileKeyOwner         = "owner"
	profileKeySecurableType = "securable_type"
	profileKeyRegion        = "region"

	// Not a dot: a securable's full name is already dot-separated.
	securableKeySeparator = "::"
)

// securableRef is a securable keyed by its metastore plus its name components,
// catalog first. Databricks rejects a name holding a space, a period or a forward
// slash, so splitting a full name on "." is unambiguous.
type securableRef struct {
	metastoreID string
	parts       []string
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

// securableDeps is the client state every Unity Catalog builder shares.
type securableDeps struct {
	client   *databricks.Client
	uc       *unityCatalog
	willSync func(string) bool
}
