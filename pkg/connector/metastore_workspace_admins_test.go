package connector

import (
	"context"
	"net/http"
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// syncEverything is the scope a deployment with metastores turned on runs with.
var syncEverything = syncPrincipals{groups: true, servicePrincipals: true, workspaceGroups: true}

// The metastore in defaultFakeAccount is owned by an account group, so every Grants call
// emits that owner grant beside whatever the privilege rows resolve to.
var ownerGrant = "account/acc-1/group/2 metastore:ms-a:owner"

// adminsAliasAccount is defaultFakeAccount carrying the privilege row Unity Catalog
// writes for a workspace's built-in admins group: the account answers the principal's id
// with a 404 and only workspace wsa serves it.
func adminsAliasAccount(principalID any) *fakeAccount {
	f := defaultFakeAccount()
	f.permissions = []map[string]any{
		{"principal": "_workspace_admins_prod_1", "principal_id": principalID, "privileges": []string{"CREATE_CATALOG", "CREATE_SHARE"}},
	}
	f.workspaceGroups = map[string]map[string]string{"wsa": {"9": "admins"}}

	return f
}

// The row CXH-3600 reports, and every reason the connector may decline to resolve it. A
// row it cannot place must never land on another principal, and must never point at a
// group this sync does not emit as a resource.
func TestMetastoreGrantsResolveTheWorkspaceAdminsAlias(t *testing.T) {
	adminsGrants := []string{
		ownerGrant,
		"workspace/wsa/group/9 metastore:ms-a:CREATE_CATALOG",
		"workspace/wsa/group/9 metastore:ms-a:CREATE_SHARE",
	}

	for _, tc := range []struct {
		name    string
		account func() *fakeAccount
		sync    syncPrincipals
		allow   []string
		want    []string
		wantErr codes.Code
	}{
		{
			name:    "by principal id",
			account: func() *fakeAccount { return adminsAliasAccount(9) },
			sync:    syncEverything,
			want:    adminsGrants,
		},
		{
			name:    "by name when the row carries no id",
			account: func() *fakeAccount { return adminsAliasAccount(0) },
			sync:    syncEverything,
			want:    adminsGrants,
		},
		{
			// Never map the alias onto another group, or onto a reused id's new owner.
			name: "id is not the workspace's admins group",
			account: func() *fakeAccount {
				f := adminsAliasAccount(9)
				f.workspaceGroups = map[string]map[string]string{"wsa": {"9": "engineering"}}
				return f
			},
			sync: syncEverything,
			want: []string{ownerGrant},
		},
		{
			// An allowlisted sync emits no resource for wsb, so its admins group is not there.
			name: "workspace outside the allowlist",
			account: func() *fakeAccount {
				f := adminsAliasAccount(9)
				f.workspaces = append(f.workspaces, map[string]any{
					"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": "RUNNING",
				})
				f.permissions[0]["principal"] = "_workspace_admins_dev_2"
				return f
			},
			sync:  syncEverything,
			allow: []string{"wsa"},
			want:  []string{ownerGrant},
		},
		{
			name:    "workspace groups not in sync scope",
			account: func() *fakeAccount { return adminsAliasAccount(9) },
			sync:    syncPrincipals{groups: true, servicePrincipals: true},
			want:    []string{ownerGrant},
		},
		{
			// A workspace being deleted never becomes RUNNING, so holding the sync for it
			// would fail every run from then on.
			name: "workspace is being deleted",
			account: func() *fakeAccount {
				f := adminsAliasAccount(9)
				f.workspaces = []map[string]any{
					{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": workspaceStatusCancelling},
				}
				return f
			},
			sync: syncEverything,
			want: []string{ownerGrant},
		},
		{
			// One that can still come back is worth waiting for: reporting the metastore
			// without its privileges would read as revoked access.
			name: "workspace is still provisioning",
			account: func() *fakeAccount {
				f := adminsAliasAccount(9)
				f.workspaces = []map[string]any{
					{"workspace_id": 1, "workspace_name": "prod", "deployment_name": "wsa", "workspace_status": workspaceStatusProvisioning},
				}
				return f
			},
			sync:    syncEverything,
			wantErr: codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestMetastoreBuilder(t, tc.account(), tc.allow, tc.sync)

			grants, _, err := m.Grants(context.Background(), testSyncedMetastore(t, "wsa"), rs.SyncOpAttrs{})
			if status.Code(err) != tc.wantErr {
				t.Fatalf("Grants error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != codes.OK {
				return
			}
			if got := grantKeys(grants); !slices.Equal(got, tc.want) {
				t.Fatalf("grants = %v, want %v", got, tc.want)
			}
			for _, g := range grants {
				if g.GetPrincipal().GetId().GetResource() != "workspace/wsa/group/9" {
					continue
				}
				annos := annotations.Annotations(g.GetAnnotations())
				if !annos.Contains(&v2.GrantImmutable{}) {
					t.Errorf("%s is not immutable: Databricks does not allow granting a system privilege back",
						g.GetEntitlement().GetId())
				}
			}
		})
	}
}

// Revoking one of these is irreversible, so neither side of provisioning attempts it.
func TestMetastoreProvisioningRejectsAWorkspaceLocalGroup(t *testing.T) {
	f := adminsAliasAccount(9)
	m := newTestMetastoreBuilder(t, f, nil, syncEverything)
	principal, err := rs.NewResourceID(groupResourceType, "workspace/wsa/group/9")
	if err != nil {
		t.Fatalf("NewResourceID: %v", err)
	}
	entitlement := ent.NewPermissionEntitlement(testSyncedMetastore(t, "wsa"), "CREATE_CATALOG")

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"grant", func() error {
			_, err := m.Grant(context.Background(), &v2.Resource{Id: principal}, entitlement)
			return err
		}},
		{"revoke", func() error {
			_, err := m.Revoke(context.Background(), &v2.Grant{
				Principal:   &v2.Resource{Id: principal},
				Entitlement: entitlement,
			})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); status.Code(err) != codes.FailedPrecondition {
				t.Errorf("%s error = %v, want FailedPrecondition", tc.name, err)
			}
			if len(f.patches) != 0 {
				t.Errorf("%s wrote %v, want no write", tc.name, f.patches)
			}
		})
	}
}

// Metastores are opt-in while workspaces always sync, so reading Unity Catalog to decide
// what a workspace offers would fail the whole workspace type on a tenant that cannot read
// that plane. Only a RUNNING workspace serves SCIM, so only it offers its admins group.
func TestWorkspaceListDoesNotDependOnUnityCatalog(t *testing.T) {
	f := defaultFakeAccount()
	f.assignments = map[string]any{"ms-a": http.StatusForbidden}
	f.workspaces = append(f.workspaces, map[string]any{
		"workspace_id": 2, "workspace_name": "dev", "deployment_name": "wsb", "workspace_status": workspaceStatusProvisioning,
	})
	client, _ := newWorkspaceRoutedClient(t, f.handler(t))

	for _, syncAdmins := range []bool{false, true} {
		w := newWorkspaceBuilder(client, nil, syncAdmins)
		resources, _, err := w.List(context.Background(), testAccountResourceID, rs.SyncOpAttrs{})
		if err != nil {
			t.Fatalf("List(syncAdmins=%v) = %v, want no error when Unity Catalog is unreadable", syncAdmins, err)
		}
		if len(resources) != 2 {
			t.Fatalf("List(syncAdmins=%v) returned %d workspaces, want 2", syncAdmins, len(resources))
		}
		if got := declaresChild(resources[0], groupResourceType.Id); got != syncAdmins {
			t.Errorf("RUNNING workspace declares group child = %v, want %v", got, syncAdmins)
		}
		if declaresChild(resources[1], groupResourceType.Id) {
			t.Error("a workspace that is still provisioning offers an admins group it cannot serve")
		}
	}
}

// declaresChild reports whether a resource offers resourceType as a child to sync.
func declaresChild(r *v2.Resource, resourceType string) bool {
	child := &v2.ChildResourceType{}
	for _, a := range r.GetAnnotations() {
		if a.MessageIs(child) {
			if err := a.UnmarshalTo(child); err == nil && child.GetResourceTypeId() == resourceType {
				return true
			}
		}
	}

	return false
}
