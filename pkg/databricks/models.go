package databricks

type User struct {
	ID     string `json:"id"`
	Emails []struct {
		Primary bool   `json:"primary"`
		Value   string `json:"value"`
	} `json:"emails"`
	UserName    string `json:"userName"`
	DisplayName string `json:"displayName"`
	Active      bool   `json:"active"`
	Roles       []struct {
		Value string `json:"value"`
	} `json:"roles"`
	Entitlements []struct {
		Value string `json:"value"`
	} `json:"entitlements"`
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

type Group struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Members     []struct {
		ID          string `json:"value"`
		DisplayName string `json:"display"`
		Ref         string `json:"$ref"`
	} `json:"members"`
	Entitlements []struct {
		Value string `json:"value"`
	} `json:"entitlements"`
}

func (g Group) HaveEntitlement(entitlement string) bool {
	for _, e := range g.Entitlements {
		if e.Value == entitlement {
			return true
		}
	}

	return false
}

type ServicePrincipal struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Active      bool   `json:"active"`
	Roles       []struct {
		Value string `json:"value"`
	} `json:"roles"`
	Entitlements []struct {
		Value string `json:"value"`
	} `json:"entitlements"`
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
	ID     int    `json:"workspace_id"`
	Name   string `json:"workspace_name"`
	Status string `json:"workspace_status"`
}

type Role struct {
	Name string `json:"name"`
}

type RuleSet struct {
	Principals []string `json:"principals"`
	Role       string   `json:"role"`
}
