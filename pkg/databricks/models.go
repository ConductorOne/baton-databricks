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
	Principal *WorkspacePrincipal `json:"principal"`
}

type Role struct {
	Name string `json:"name"`
}

type RuleSet struct {
	Principals []string `json:"principals"`
	Role       string   `json:"role"`
}

// SecretInfo is returned by GET /api/2.0/accounts/{account_id}/servicePrincipals/{sp_id}/credentials/secrets.
// The secret value is never returned; only metadata.
type SecretInfo struct {
	ID         string `json:"id"`
	CreateTime string `json:"create_time"`
	UpdateTime string `json:"update_time"`
	SecretHash string `json:"secret_hash"`
	Status     string `json:"status"`
}

// TokenInfo is returned by GET /api/2.0/token-management/tokens (workspace-admin endpoint).
// The token value is never returned; only metadata.
type TokenInfo struct {
	TokenID           string `json:"token_id"`
	Comment           string `json:"comment"`
	CreationTime      int64  `json:"creation_time"`
	ExpiryTime        int64  `json:"expiry_time"`
	CreatedByID       int64  `json:"created_by_id"`
	CreatedByUsername string `json:"created_by_username"`
}
