package connector

import (
	"context"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/helpers"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type userBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (u *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

func userResource(ctx context.Context, user *databricks.User) (*v2.Resource, error) {
	var emailOptions []rs.UserTraitOption
	var primaryEmail string
	for _, email := range user.Emails {
		if email.Primary {
			primaryEmail = email.Value
		}

		emailOptions = append(
			emailOptions,
			rs.WithEmail(email.Value, email.Primary),
		)
	}

	var status v2.UserTrait_Status_Status
	if user.Active {
		status = v2.UserTrait_Status_STATUS_ENABLED
	} else {
		status = v2.UserTrait_Status_STATUS_DISABLED
	}

	firstName, lastName := helpers.SplitFullName(user.DisplayName)
	profile := map[string]interface{}{
		"first_name": firstName,
		"last_name":  lastName,
		"email":      primaryEmail,
		"user_id":    user.ID,
		"login":      user.UserName,
	}

	userTraitOptions := []rs.UserTraitOption{
		rs.WithUserProfile(profile),
		rs.WithStatus(status),
		rs.WithUserLogin(user.UserName),
	}

	userTraitOptions = append(userTraitOptions, emailOptions...)

	resource, err := rs.NewUserResource(
		user.DisplayName,
		userResourceType,
		user.ID,
		userTraitOptions,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (u *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var rv []*v2.Resource

	users, err := u.client.ListUsers(ctx, nil)
	if err != nil {
		return nil, "", nil, err
	}

	for _, user := range users {
		uCopy := user

		ur, err := userResource(ctx, &uCopy)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, ur)
	}

	return rv, "", nil, nil
}

// Entitlements always returns an empty slice for users.
func (u *userBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (u *userBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newUserBuilder(client *databricks.Client) *userBuilder {
	return &userBuilder{
		client:       client,
		resourceType: userResourceType,
	}
}
