package databricks

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

// UsablePrincipal reports whether the assignment carries a principal the
// workspace grants sync can turn into a resource. The Databricks
// permission-assignments API emits entries whose principal object is absent
// (omitempty) or present but empty, and entries carrying a per-entry error
// alongside a valid principal. Only the three identifying name fields are
// tested — Principal.ID is deliberately NOT required, so identity derivation
// (rs.NewResourceID(prepareResourceType(p), p.ID)) is unchanged for every
// entry that syncs today. A valid principal with a non-empty Error is still
// usable: the Error is ignored, exactly as today. Callers that must skip
// principal-less entries should guard on this method and log the skip.
func (a *WorkspaceAssignment) UsablePrincipal() bool {
	return a.Principal != nil &&
		(a.Principal.UserName != "" || a.Principal.GroupDisplayName != "" || a.Principal.ServicePrincipalAppID != "")
}

type Role struct {
	Name string `json:"name"`
}

type RuleSet struct {
	Principals []string `json:"principals"`
	Role       string   `json:"role"`
}
