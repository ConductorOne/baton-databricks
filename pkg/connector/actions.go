package connector

import (
	"context"
	"fmt"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	actionDisableUser = "disable_user"
	actionEnableUser  = "enable_user"

	argUserIDKey     = "user_id"
	argUserIDDisplay = "User ID"
	retSuccessKey    = "success"
)

var successReturnType = []*config.Field{
	{Name: retSuccessKey, DisplayName: "Success", Field: &config.Field_BoolField{}},
}

var disableUserSchema = &v2.BatonActionSchema{
	Name:        actionDisableUser,
	DisplayName: "Disable User",
	Description: "Deactivates a Databricks account user (sets active=false). The user keeps their permissions and memberships.",
	Arguments: []*config.Field{
		{Name: argUserIDKey, DisplayName: argUserIDDisplay, Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: successReturnType,
	ActionType:  []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT, v2.ActionType_ACTION_TYPE_ACCOUNT_DISABLE},
}

var enableUserSchema = &v2.BatonActionSchema{
	Name:        actionEnableUser,
	DisplayName: "Enable User",
	Description: "Reactivates a Databricks account user (sets active=true) with the permissions and memberships they had.",
	Arguments: []*config.Field{
		{Name: argUserIDKey, DisplayName: argUserIDDisplay, Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: successReturnType,
	ActionType:  []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT, v2.ActionType_ACTION_TYPE_ACCOUNT_ENABLE},
}

// enable_user and disable_user are account lifecycle actions, which C1 resolves as global
// actions, so they are registered here and not through a resource builder.
var _ connectorbuilder.GlobalActionProvider = (*Databricks)(nil)

// GlobalActions registers the account lifecycle actions.
func (d *Databricks) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := registry.Register(ctx, disableUserSchema, d.disableUserHandler); err != nil {
		return fmt.Errorf("databricks-connector: register disable_user: %w", err)
	}
	if err := registry.Register(ctx, enableUserSchema, d.enableUserHandler); err != nil {
		return fmt.Errorf("databricks-connector: register enable_user: %w", err)
	}
	return nil
}

func successStruct() *structpb.Struct {
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			retSuccessKey: {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		},
	}
}

func (d *Databricks) disableUserHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	return d.setUserActive(ctx, args, false)
}

func (d *Databricks) enableUserHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	return d.setUserActive(ctx, args, true)
}

// setUserActive is shared by both handlers so they validate input and report errors identically.
// The SCIM replace is idempotent, so a user already in the requested state succeeds.
func (d *Databricks) setUserActive(ctx context.Context, args *structpb.Struct, active bool) (*structpb.Struct, annotations.Annotations, error) {
	verb := "disable"
	if active {
		verb = "enable"
	}

	userID, err := actions.RequireStringArg(args, argUserIDKey)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "databricks-connector: %s user: %s: %v", verb, argUserIDKey, err)
	}

	rl, err := d.client.SetUserActive(ctx, userID, active)
	var annos annotations.Annotations
	if rl != nil {
		annos = annotations.New(rl)
	}
	if err != nil {
		return nil, annos, actionError(verb, userID, err)
	}

	return successStruct(), annos, nil
}

// actionError keeps the gRPC code of err so a missing user (NotFound), missing permissions
// (PermissionDenied/Unauthenticated), bad input (InvalidArgument) and transient failures
// (Unavailable, retryable) stay distinguishable to the caller. Errors without a code are
// returned wrapped and unchanged. The cause stays in the chain so errors.As still reaches it.
func actionError(verb, userID string, err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() == codes.OK || st.Code() == codes.Unknown {
		return fmt.Errorf("databricks-connector: %s user %s: %w", verb, userID, err)
	}

	switch st.Code() {
	case codes.NotFound:
		return uhttp.WrapErrors(codes.NotFound, fmt.Sprintf("databricks-connector: %s user %s: user not found in the account", verb, userID), err)
	default:
		return uhttp.WrapErrors(st.Code(), fmt.Sprintf("databricks-connector: %s user %s", verb, userID), err)
	}
}
