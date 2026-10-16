package connector

import (
	"slices"
	"strings"
)

// The canonical UPPER_SNAKE form the API answers with. It accepts lowercase and
// aliases on input, so a privilege offered under another spelling cannot round-trip.
const (
	// ownerEntitlement names the securable's owner. Ownership is single-valued and
	// Databricks has no notion of removing an owner, so it is never grantable.
	ownerEntitlement = "owner"
)

var (
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

	// legacyPrivileges are Hive-era aliases the API accepts on input and silently
	// rewrites: CREATE on a schema becomes CREATE_TABLE plus CREATE_FUNCTION. They are
	// never echoed back, so they must not be offered.
	legacyPrivileges = []string{
		"CREATE",
		"CREATE_VIEW",
		"USAGE",
	}
)

// An alias is never in a level's set, so this only changes the explanation.
func isLegacyPrivilege(privilege string) bool {
	return slices.Contains(legacyPrivileges, strings.ToUpper(strings.TrimSpace(privilege)))
}
