package connector

import (
	"context"
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-databricks/pkg/databricks"
)

func newTestVolumeBuilder(t *testing.T, f *fakeSchemaChildAccount) *volumeBuilder {
	return newTestVolumeBuilderAs(t, f, testOwnPrincipal)
}

func newTestVolumeBuilderAs(t *testing.T, f *fakeSchemaChildAccount, clientID string) *volumeBuilder {
	return newVolumeBuilder(f.client(t), clientID, nil, syncPrincipals{groups: true})
}

// testVolume is a volume resource as List writes it, routed through wsa.
func testVolume(t *testing.T, name, owner, schemaOwner, catalogOwner, metastoreOwner string) *v2.Resource {
	t.Helper()
	r, err := volumeResource(&databricks.Volume{Name: name, Owner: owner, VolumeType: "MANAGED"}, "ms-1", "main", "sales", testSchemaResourceID,
		schemaChildRoute{
			Workspace: "wsa", WorkspaceID: "1", WorkspaceName: "prod",
			MetastoreOwner: metastoreOwner, CatalogOwner: catalogOwner, SchemaOwner: schemaOwner,
		})
	if err != nil {
		t.Fatalf("volumeResource: %v", err)
	}

	return r
}

// A volume is keyed by its metastore and its dotted full name, which extends the
// schema's id, and carries the route and the owners its Grants are decided from.
func TestVolumeListEmitsTheVolumesOfItsSchema(t *testing.T) {
	f := &fakeSchemaChildAccount{
		catalogOwner: "someone",
		schemaOwner:  "someone-else",
		volumes: []map[string]any{
			{"name": "files", "owner": "someone", "volume_type": "MANAGED"},
			{"name": "landing", "owner": "someone", "volume_type": "EXTERNAL"},
		},
	}
	b := newTestVolumeBuilder(t, f)

	resources, err := listAll(t, b.List)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("listed %d volumes, want 2", len(resources))
	}

	first := resources[0]
	if got := first.GetId().GetResource(); got != "ms-1::main.sales.files" {
		t.Errorf("resource id = %q, want ms-1::main.sales.files", got)
	}
	if first.GetDisplayName() != "main.sales.files" {
		t.Errorf("display name = %q, want main.sales.files", first.GetDisplayName())
	}
	if first.GetParentResourceId().GetResource() != "ms-1::main.sales" {
		t.Errorf("parent = %v, want the schema ms-1::main.sales", first.GetParentResourceId())
	}
	profile := rs.GetProfile(first)
	for key, want := range map[string]string{
		profileKeyMetastoreID: "ms-1", profileKeyFullName: "main.sales.files",
		profileKeyCatalogName: "main", profileKeySchemaName: "sales",
		profileKeyVolumeName: "files", profileKeyVolumeType: "MANAGED",
		profileKeyOwner: "someone", profileKeySecurableType: securableTypeVolume,
		profileKeyWorkspace: "wsa", profileKeyWorkspaceID: "1", profileKeyWorkspaceName: "prod",
		profileKeyMetastoreOwner: testOwnPrincipal, profileKeyCatalogOwner: "someone",
		profileKeySchemaOwner: "someone-else",
	} {
		if got, _ := rs.GetProfileStringValue(profile, key); got != want {
			t.Errorf("profile[%s] = %q, want %q", key, got, want)
		}
	}
}

// A volume's files are read and written rather than queried, so it takes its own
// privilege set: a table privilege held on it is neither reported nor grantable.
func TestVolumeGrantsReportOnlyVolumePrivileges(t *testing.T) {
	f := &fakeSchemaChildAccount{
		catalogOwner: "someone",
		schemaOwner:  "someone",
		users:        map[string]string{"1": "alice@example.com"},
		groups:       map[string]string{"11": "data-eng"},
		assignments: map[string][]map[string]any{"main.sales.files": {
			{"principal": "alice@example.com", "principal_id": 1, "privileges": []string{"READ_VOLUME", "SELECT"}},
			{"principal": "data-eng", "principal_id": 11, "privileges": []string{"WRITE_VOLUME"}},
		}},
	}
	b := newTestVolumeBuilder(t, f)

	grants, _, err := b.Grants(context.Background(), testVolume(t, "files", "someone", "someone", "someone", testOwnPrincipal), rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Grants: %v", err)
	}

	want := []string{
		"1 volume:ms-1::main.sales.files:READ_VOLUME",
		"account/" + testAccountId + "/group/11 volume:ms-1::main.sales.files:WRITE_VOLUME",
	}
	slices.Sort(want)
	if got := grantKeys(grants); !slices.Equal(got, want) {
		t.Fatalf("grants = %v, want %v (SELECT is not a volume privilege)", got, want)
	}

	g := grant.NewGrant(testVolume(t, "files", "someone", "someone", "someone", testOwnPrincipal), "SELECT", alice.GetId())
	if _, err := b.Grant(context.Background(), alice, g.GetEntitlement()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("granting SELECT on a volume = %v, want InvalidArgument", err)
	}
	if len(f.patches) != 0 {
		t.Errorf("patches = %v, want none", f.patches)
	}
}
