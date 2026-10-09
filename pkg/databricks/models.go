package databricks

import "encoding/json"

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

type WorkspaceAssignment struct {
	Principal   *WorkspacePrincipal `json:"principal"`
	Permissions []string            `json:"permissions"`
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
	Name                         string `json:"name"`
	MetastoreID                  string `json:"metastore_id"`
	Owner                        string `json:"owner"`
	CatalogType                  string `json:"catalog_type"`
	IsolationMode                string `json:"isolation_mode"`
	AccessibleInCurrentWorkspace *bool  `json:"accessible_in_current_workspace"`
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

type metastoreAssignment struct {
	MetastoreID string `json:"metastore_id"`
}

type workspaceMetastoreResponse struct {
	MetastoreAssignment metastoreAssignment `json:"metastore_assignment"`
}

type catalogsResponse struct {
	Catalogs      []Catalog `json:"catalogs"`
	NextPageToken string    `json:"next_page_token"`
}

type permissionsResponse struct {
	PrivilegeAssignments []PrivilegeAssignment `json:"privilege_assignments"`
	NextPageToken        string                `json:"next_page_token"`
}
