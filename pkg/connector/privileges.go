package connector

import (
	"slices"
	"strings"
)

// The canonical UPPER_SNAKE form the API answers with. It accepts lowercase and
// aliases on input, so a privilege offered under another spelling cannot round-trip.
const (
	privilegeAllPrivileges = "ALL_PRIVILEGES"
	privilegeManage        = "MANAGE"
	privilegeRefresh       = "REFRESH"

	// A view and a materialized view are both securable_type "table", so table_type
	// is the only field that says which privileges apply.
	tableTypeManaged          = "MANAGED"
	tableTypeExternal         = "EXTERNAL"
	tableTypeView             = "VIEW"
	tableTypeMaterializedView = "MATERIALIZED_VIEW"
	tableTypeStreamingTable   = "STREAMING_TABLE"
	tableTypeMetricView       = "METRIC_VIEW"

	// ownerEntitlement names the securable's owner. Ownership is single-valued and
	// Databricks has no notion of removing an owner, so it is never grantable.
	ownerEntitlement = "owner"
)

var (
	// catalogPrivileges is the widest set; BROWSE, CREATE_SCHEMA and USE_CATALOG stop here.
	// Source: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges.
	// The PATCH accepts and echoes every entry, including ones missing from the
	// databricks-sdk-go Privilege enum.
	catalogPrivileges = []string{
		privilegeAllPrivileges,
		"APPLY_TAG",
		"BROWSE",
		"CREATE_SCHEMA",
		"USE_CATALOG",
		"USE_SCHEMA",
		"CREATE_FUNCTION",
		"CREATE_TABLE",
		"CREATE_MATERIALIZED_VIEW",
		"CREATE_MODEL",
		"CREATE_VOLUME",
		"CREATE_FEATURE",
		"CREATE_SECRET",
		"CREATE_SERVICE",
		"SELECT",
		"MODIFY",
		"DELETE",
		"INSERT",
		"UPDATE",
		"EXECUTE",
		privilegeRefresh,
		"READ_VOLUME",
		"WRITE_VOLUME",
		"READ_METADATA",
		"READ_FEATURE",
		"READ_SECRET",
		"REFERENCE_SECRET",
		"WRITE_SECRET",
		"EXTERNAL_USE_SCHEMA",
		privilegeManage,
	}

	// schemaPrivileges is the catalog set minus the three a schema rejects.
	schemaPrivileges = withoutPrivileges(catalogPrivileges, "BROWSE", "CREATE_SCHEMA", "USE_CATALOG")

	// metastorePrivileges is the set for the metastore itself. ALL_PRIVILEGES and MANAGE
	// are both rejected here, which is why a metastore's grant visibility cannot be
	// answered by a privilege read. READ_METADATA is the only entry that inherits down.
	metastorePrivileges = []string{
		"CREATE_CATALOG",
		"CREATE_CLEAN_ROOM",
		"CREATE_CONNECTION",
		"CREATE_EXTERNAL_LOCATION",
		"CREATE_EXTERNAL_METADATA",
		"CREATE_PROVIDER",
		"CREATE_RECIPIENT",
		"CREATE_SHARE",
		"CREATE_SERVICE_CREDENTIAL",
		"CREATE_STORAGE_CREDENTIAL",
		"MANAGE_ALLOWLIST",
		"READ_METADATA",
		"SET_SHARE_PERMISSION",
		"USE_MARKETPLACE_ASSETS",
		"USE_PROVIDER",
		"USE_RECIPIENT",
		"USE_SHARE",
	}

	// tablePrivileges is the set for a real table. BROWSE is catalog-only.
	tablePrivileges = []string{
		privilegeAllPrivileges,
		"APPLY_TAG",
		"DELETE",
		"INSERT",
		privilegeManage,
		"MODIFY",
		"READ_METADATA",
		"SELECT",
		"UPDATE",
	}

	// viewPrivileges matches the privileges reference, including ALL_PRIVILEGES.
	// The REST grants API still 400s when adding ALL_PRIVILEGES on a VIEW; Grant
	// rejects that write. Grants() must still emit it when SQL already granted it.
	viewPrivileges = []string{
		privilegeAllPrivileges,
		"APPLY_TAG",
		privilegeManage,
		"READ_METADATA",
		"SELECT",
	}

	// A materialized view accepts ALL_PRIVILEGES, and a permissions read returns
	// only it, not the implied SELECT and REFRESH.
	materializedViewPrivileges = append(slices.Clone(viewPrivileges), privilegeRefresh)

	// Streaming tables take REFRESH and MODIFY, not the table DML trio.
	streamingTablePrivileges = []string{
		privilegeAllPrivileges,
		"APPLY_TAG",
		privilegeManage,
		"MODIFY",
		"READ_METADATA",
		privilegeRefresh,
		"SELECT",
	}

	// For a grant whose table variant is not knowable.
	anyTablePrivileges = append(slices.Clone(tablePrivileges), privilegeRefresh)

	volumePrivileges = []string{
		privilegeAllPrivileges,
		"APPLY_TAG",
		privilegeManage,
		"READ_METADATA",
		"READ_VOLUME",
		"WRITE_VOLUME",
	}

	// legacyPrivileges are Hive-era aliases the API accepts on input and silently
	// rewrites: CREATE on a schema becomes CREATE_TABLE plus CREATE_FUNCTION. They are
	// never echoed back, so they must not be offered.
	legacyPrivileges = []string{
		"CREATE",
		"CREATE_VIEW",
		"USAGE",
	}
)

func withoutPrivileges(privileges []string, drop ...string) []string {
	return slices.DeleteFunc(slices.Clone(privileges), func(privilege string) bool {
		return slices.Contains(drop, privilege)
	})
}

// isViewLike reports a view or metric view. Both reject the table DML set, and
// the grants API rejects adding ALL_PRIVILEGES on either.
func isViewLike(tableType string) bool {
	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case tableTypeView, tableTypeMetricView:
		return true
	default:
		return false
	}
}

// privilegesForTableType returns the set for a known table_type. MANAGED and
// EXTERNAL are named so they stay on the table set. Anything else, including a
// type Databricks adds later, gets the union so a held REFRESH is not dropped.
func privilegesForTableType(tableType string) []string {
	if isViewLike(tableType) {
		return viewPrivileges
	}

	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case tableTypeMaterializedView:
		return materializedViewPrivileges
	case tableTypeStreamingTable:
		return streamingTablePrivileges
	case tableTypeManaged, tableTypeExternal:
		return tablePrivileges
	default:
		return anyTablePrivileges
	}
}

// An alias is never in a level's set, so this only changes the explanation.
func isLegacyPrivilege(privilege string) bool {
	return slices.Contains(legacyPrivileges, strings.ToUpper(strings.TrimSpace(privilege)))
}
