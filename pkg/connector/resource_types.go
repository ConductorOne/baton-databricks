package connector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

var (
	// The user resource type is for all user objects from the database.
	userResourceType = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotationsForUserResourceType(),
	}

	// The group resource type is for all group objects from the database.
	groupResourceType = &v2.ResourceType{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}

	// The service principal resource type is for all service principal objects from the database.
	servicePrincipalResourceType = &v2.ResourceType{
		Id:          "service_principal",
		DisplayName: "Service Principal",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}

	// The role resource type is for all static roles and entitlements available in API.
	roleResourceType = &v2.ResourceType{
		Id:          "role",
		DisplayName: "Role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
	}

	// The workspace resource type is for all workspace objects from the database.
	workspaceResourceType = &v2.ResourceType{
		Id:          "workspace",
		DisplayName: "Workspace",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}

	// The account resource type is for top level resource type.
	accountResourceType = &v2.ResourceType{
		Id:          "account",
		DisplayName: "Account",
	}
)
