package connector

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

// C1 matches Terraform-preloaded resources and entitlements (match_baton_id) against the
// RawId annotation on the resource, so every resource must carry one equal to its resource ID.
func TestResourcesHaveRawId(t *testing.T) {
	ctx := context.Background()
	account := &v2.ResourceId{ResourceType: accountResourceType.Id, Resource: "acc-1"}
	workspace := &v2.ResourceId{ResourceType: workspaceResourceType.Id, Resource: "ws-1"}

	build := func(r *v2.Resource, err error) *v2.Resource {
		t.Helper()
		if err != nil {
			t.Fatalf("failed to build resource: %v", err)
		}
		return r
	}

	cases := []struct {
		name     string
		resource *v2.Resource
		wantId   string
	}{
		{
			"account group",
			build(groupResource(ctx, &databricks.Group{BaseResponse: databricks.BaseResponse{ID: "g1"}}, account)),
			"account/acc-1/group/g1",
		},
		{
			"workspace group",
			build(groupResource(ctx, &databricks.Group{BaseResponse: databricks.BaseResponse{ID: "g1"}}, workspace)),
			"workspace/ws-1/group/g1",
		},
		{
			"workspace",
			build(workspaceResource(ctx, &databricks.Workspace{Name: "ws", DeploymentName: "ws-1"}, account)),
			"ws-1",
		},
		{
			"account role",
			build(roleResource(ctx, "account_admin", account)),
			"account_admin",
		},
		{
			"workspace role",
			build(roleResource(ctx, "workspace_access", workspace)),
			"ws-1:workspace_access",
		},
		{
			"user",
			build((&userBuilder{}).userResource(ctx, &databricks.User{BaseResponse: databricks.BaseResponse{ID: "u1"}}, account)),
			"u1",
		},
		{
			"service principal",
			build((&servicePrincipalBuilder{}).servicePrincipalResource(ctx, &databricks.ServicePrincipal{BaseResponse: databricks.BaseResponse{ID: "sp1"}}, account)),
			"sp1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rawId := &v2.RawId{}
			annos := annotations.Annotations(tc.resource.GetAnnotations())
			ok, err := annos.Pick(rawId)
			if err != nil {
				t.Fatalf("failed to read RawId annotation: %v", err)
			}
			if !ok {
				t.Fatal("resource is missing RawId annotation")
			}
			if rawId.GetId() != tc.wantId {
				t.Errorf("RawId = %q, want %q", rawId.GetId(), tc.wantId)
			}
			if rawId.GetId() != tc.resource.GetId().GetResource() {
				t.Errorf("RawId %q does not match resource ID %q", rawId.GetId(), tc.resource.GetId().GetResource())
			}
		})
	}
}
