package databricks

import (
	"encoding/json"
	"slices"
)

type BaseResponse struct {
	ID string `json:"id"`
}

type PermissionValue struct {
	Value string `json:"value"`
}

type Permissions struct {
	Roles        []PermissionValue `json:"roles,omitempty"`
	Entitlements []PermissionValue `json:"entitlements,omitempty"`
}

type User struct {
	BaseResponse
	Permissions
	Emails []struct {
		Primary bool   `json:"primary"`
		Value   string `json:"value"`
	} `json:"emails"`
	UserName    string `json:"userName"`
	DisplayName string `json:"displayName"`
	Active      bool   `json:"active,omitempty"`
}

func (u User) HaveRole(role string) bool {
	for _, r := range u.Roles {
		if r.Value == role {
			return true
		}
	}

	return false
}

func (u User) HaveEntitlement(entitlement string) bool {
	for _, e := range u.Entitlements {
		if e.Value == entitlement {
			return true
		}
	}

	return false
}

type Member struct {
	ID          string `json:"value"`
	DisplayName string `json:"display"`
	Ref         string `json:"$ref"`
}

type Group struct {
	BaseResponse
	Permissions
	DisplayName string   `json:"displayName"`
	Members     []Member `json:"members,omitempty"`
	Meta        struct {
		Type string `json:"resourceType"`
	} `json:"meta,omitempty"`
	Schemas []string `json:"schemas,omitempty"`
}

const PatchOpSchema = "urn:ietf:params:scim:api:messages:2.0:PatchOp"

// PatchOperation is one SCIM PATCH operation, e.g. {"op":"add","path":"members","value":[...]}
// or {"op":"remove","path":"members[value eq \"<id>\"]"}.
type PatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

type PatchRequest struct {
	Schemas    []string         `json:"schemas"`
	Operations []PatchOperation `json:"Operations"`
}

func (g Group) HaveRole(role string) bool {
	for _, r := range g.Roles {
		if r.Value == role {
			return true
		}
	}

	return false
}

func (g Group) HaveEntitlement(entitlement string) bool {
	for _, e := range g.Entitlements {
		if e.Value == entitlement {
			return true
		}
	}

	return false
}

func (g Group) IsAccountGroup() bool {
	return g.Meta.Type == "Group"
}

type ServicePrincipal struct {
	BaseResponse
	Permissions
	DisplayName   string `json:"displayName"`
	Active        bool   `json:"active"`
	ApplicationID string `json:"applicationId"`
}

func (s ServicePrincipal) HaveRole(role string) bool {
	for _, r := range s.Roles {
		if r.Value == role {
			return true
		}
	}

	return false
}

func (s ServicePrincipal) HaveEntitlement(entitlement string) bool {
	for _, e := range s.Entitlements {
		if e.Value == entitlement {
			return true
		}
	}

	return false
}

type Workspace struct {
	ID             int    `json:"workspace_id"`
	Name           string `json:"workspace_name"`
	Status         string `json:"workspace_status"`
	DeploymentName string `json:"deployment_name"`
}

type WorkspacePrincipal struct {
	ServicePrincipalAppID string `json:"service_principal_name"`
	GroupDisplayName      string `json:"group_name"`
	UserName              string `json:"user_name"`
	ID                    int    `json:"principal_id"`
}

// WorkspaceAdminPermission marks a principal as a workspace admin; WorkspaceUserPermission is basic access.
const (
	WorkspaceAdminPermission = "ADMIN"
	WorkspaceUserPermission  = "USER"
)

type WorkspaceAssignment struct {
	Principal   *WorkspacePrincipal `json:"principal"`
	Permissions []string            `json:"permissions"`
	// Error is the error response associated with this assignment, if any.
	Error string `json:"error"`
}

type Role struct {
	Name string `json:"name"`
}

type RuleSet struct {
	Principals []string `json:"principals"`
	Role       string   `json:"role"`
}

type Metastore struct {
	MetastoreID string `json:"metastore_id"`
	Name        string `json:"name"`
	Region      string `json:"region"`
	Owner       string `json:"owner"`
}

type Catalog struct {
	Name          string `json:"name"`
	MetastoreID   string `json:"metastore_id"`
	Owner         string `json:"owner"`
	CatalogType   string `json:"catalog_type"`
	IsolationMode string `json:"isolation_mode"`
}

type Schema struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`

	// FullName is "{catalog}.{schema}", which a single schema read is checked against.
	FullName string `json:"full_name"`
}

type Table struct {
	Name      string `json:"name"`
	Owner     string `json:"owner"`
	TableType string `json:"table_type"`
}

type Volume struct {
	Name       string `json:"name"`
	Owner      string `json:"owner"`
	VolumeType string `json:"volume_type"`
}

type PrivilegeAssignment struct {
	Principal   string      `json:"principal"`
	PrincipalID json.Number `json:"principal_id"`
	Privileges  []string    `json:"privileges"`
}

type PermissionsChange struct {
	Principal   string      `json:"principal,omitempty"`
	PrincipalID json.Number `json:"principal_id,omitempty"`
	Add         []string    `json:"add,omitempty"`
	Remove      []string    `json:"remove,omitempty"`
}

func (p PermissionsChange) validate() error {
	switch {
	case p.Principal == "" && p.PrincipalID == "":
		return invalidArgument("a principal or a principal id is required")
	case p.Principal != "" && p.PrincipalID != "":
		return invalidArgument("principal %q and principal id %s cannot both be set", p.Principal, p.PrincipalID)
	case p.PrincipalID != "" && len(p.Add) > 0:
		return invalidArgument("principal id %s addresses removals only, so it cannot add %v", p.PrincipalID, p.Add)
	case len(p.Add) == 0 && len(p.Remove) == 0:
		return invalidArgument("no privileges to add or remove")
	}

	return nil
}

type metastoresResponse struct {
	Metastores []Metastore `json:"metastores"`
}

type metastoreWorkspacesResponse struct {
	WorkspaceIDs []json.Number `json:"workspace_ids"`
}

type catalogsResponse struct {
	Catalogs      []Catalog `json:"catalogs"`
	NextPageToken string    `json:"next_page_token"`
}

type schemasResponse struct {
	Schemas       []Schema `json:"schemas"`
	NextPageToken string   `json:"next_page_token"`
}

type tablesResponse struct {
	Tables        []Table `json:"tables"`
	NextPageToken string  `json:"next_page_token"`
}

type volumesResponse struct {
	Volumes       []Volume `json:"volumes"`
	NextPageToken string   `json:"next_page_token"`
}

type permissionsResponse struct {
	PrivilegeAssignments []PrivilegeAssignment `json:"privilege_assignments"`
	NextPageToken        string                `json:"next_page_token"`
}

// EffectivePrivilege is one privilege an effective-permissions answer reports.
// The inherited fields are empty when it is held on the securable itself, and
// otherwise name the parent that grants it.
type EffectivePrivilege struct {
	Privilege         string `json:"privilege"`
	InheritedFromType string `json:"inherited_from_type,omitempty"`
	InheritedFromName string `json:"inherited_from_name,omitempty"`
}

// EffectivePrivilegeAssignment is what one principal holds on a securable once
// inheritance is applied. It carries no principal_id: this endpoint names a
// principal only.
type EffectivePrivilegeAssignment struct {
	Principal  string               `json:"principal"`
	Privileges []EffectivePrivilege `json:"privileges"`
}

// HoldsAny reports whether the assignment carries one of wanted, counting a
// privilege a parent grants the same as one held directly.
func (a EffectivePrivilegeAssignment) HoldsAny(wanted []string) bool {
	return slices.ContainsFunc(a.Privileges, func(held EffectivePrivilege) bool {
		return slices.Contains(wanted, held.Privilege)
	})
}

type effectivePermissionsResponse struct {
	PrivilegeAssignments []EffectivePrivilegeAssignment `json:"privilege_assignments"`
	NextPageToken        string                         `json:"next_page_token"`
}
