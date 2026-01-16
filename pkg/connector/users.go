package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type userBuilder struct {
	client       *databricks.Client
	resourceType *v2.ResourceType
}

func (u *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

func (u *userBuilder) userResource(ctx context.Context, user *databricks.User, parent *v2.ResourceId) (*v2.Resource, error) {
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

	firstName, lastName := rs.SplitFullName(user.DisplayName)
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
		rs.WithEmail(primaryEmail, true),
	}

	userTraitOptions = append(userTraitOptions, emailOptions...)

	// keep the parent resource id, only if the parent resource is account
	var options []rs.ResourceOption
	if parent.ResourceType == accountResourceType.Id {
		options = append(options, rs.WithParentResourceID(parent))
	}

	resource, err := rs.NewUserResource(
		user.DisplayName,
		userResourceType,
		user.ID,
		userTraitOptions,
		options...,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (u *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, attr rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil {
		return nil, nil, nil
	}

	var workspaceId string
	if parentResourceID.ResourceType == workspaceResourceType.Id {
		workspaceId = parentResourceID.Resource
	}

	bag, page, err := parsePageToken(attr.PageToken.Token, &v2.ResourceId{ResourceType: userResourceType.Id})
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to parse page token: %w", err)
	}

	users, total, _, err := u.client.ListUsers(
		ctx,
		workspaceId,
		databricks.NewPaginationVars(page, ResourcesPageSize),
		databricks.NewUserAttrVars(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list users: %w", err)
	}

	var rv []*v2.Resource
	for _, user := range users {
		uCopy := user

		ur, err := u.userResource(ctx, &uCopy, parentResourceID)
		if err != nil {
			return nil, nil, err
		}

		rv = append(rv, ur)
	}

	token := prepareNextToken(page, len(users), total)
	nextPage, err := bag.NextToken(token)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to create next page token: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

// Entitlements always returns an empty slice for users.
func (u *userBuilder) Entitlements(
	_ context.Context,
	_ *v2.Resource,
	_ rs.SyncOpAttrs,
) (
	[]*v2.Entitlement,
	*rs.SyncOpResults,
	error,
) {
	return nil, nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (u *userBuilder) Grants(
	_ context.Context,
	_ *v2.Resource,
	_ rs.SyncOpAttrs,
) (
	[]*v2.Grant,
	*rs.SyncOpResults,
	error,
) {
	return nil, nil, nil
}

func (o *userBuilder) CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
	}, nil, nil
}

func (o *userBuilder) CreateAccount(ctx context.Context, accountInfo *v2.AccountInfo, credentialOptions *v2.LocalCredentialOptions) (
	connectorbuilder.CreateAccountResponse,
	[]*v2.PlaintextData,
	annotations.Annotations,
	error,
) {
	pMap := accountInfo.Profile.AsMap()
	body := &databricks.CreateUserBody{}

	// Use email as userName since Databricks API expects email in userName field
	if email, ok := pMap["email"]; ok {
		if email == "" {
			return nil, nil, nil, fmt.Errorf("baton-databricks: email is required to create a user")
		}
		body.UserName = email.(string)
	} else {
		return nil, nil, nil, fmt.Errorf("baton-databricks: email field is required in profile")
	}

	if displayName, ok := pMap["displayName"]; ok {
		if displayName == "" {
			return nil, nil, nil, fmt.Errorf("baton-databricks: displayName is required to create a user")
		}
		body.DisplayName = displayName.(string)
	}

	if name, ok := pMap["familyName"]; ok {
		body.Name.FamilyName = name.(string)
	}

	if name, ok := pMap["givenName"]; ok {
		body.Name.GivenName = name.(string)
	}

	if id, ok := pMap["id"]; ok {
		body.Id = id.(string)
	}

	if active, ok := pMap["active"]; ok {
		body.Active = active.(bool)
	}

	res, _, err := o.client.CreateUser(ctx, "", body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-databricks: failed to create user: %w", err)
	}

	user, _, err := o.client.GetUser(ctx, "", res.Id)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-databricks: failed to get user after creation: %w", err)
	}

	parentResourceId, err := rs.NewResourceID(accountResourceType, o.client.GetAccountId())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-databricks: failed to create resource ID for account: %w", err)
	}
	resource, err := o.userResource(ctx, user, parentResourceId)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-databricks: failed to create user resource: %w", err)
	}

	return &v2.CreateAccountResponse_SuccessResult{
		Resource:              resource,
		IsCreateAccountResult: true,
	}, nil, nil, nil
}

func (o *userBuilder) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	_, err := o.client.DeleteUser(ctx, "", resourceId.Resource)
	if err != nil {
		return nil, fmt.Errorf("baton-databricks: failed to delete user: %w", err)
	}
	return nil, nil
}

func newUserBuilder(client *databricks.Client) *userBuilder {
	return &userBuilder{
		client:       client,
		resourceType: userResourceType,
	}
}
