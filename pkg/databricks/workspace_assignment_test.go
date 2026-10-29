package databricks

import "testing"

// TestWorkspaceAssignmentUsablePrincipal pins the skip predicate used by the
// workspace grants sync (pkg/connector/workspaces.go). See the doc comment on
// WorkspaceAssignment.UsablePrincipal for the API quirk it accommodates.
func TestWorkspaceAssignmentUsablePrincipal(t *testing.T) {
	tests := []struct {
		name       string
		assignment WorkspaceAssignment
		want       bool
	}{
		{
			name:       "nil principal",
			assignment: WorkspaceAssignment{},
			want:       false,
		},
		{
			name:       "empty principal object",
			assignment: WorkspaceAssignment{Principal: &WorkspacePrincipal{}},
			want:       false,
		},
		{
			name:       "user name alone",
			assignment: WorkspaceAssignment{Principal: &WorkspacePrincipal{UserName: "user@example.com"}},
			want:       true,
		},
		{
			name:       "group display name alone",
			assignment: WorkspaceAssignment{Principal: &WorkspacePrincipal{GroupDisplayName: "admins"}},
			want:       true,
		},
		{
			name:       "service principal app id alone",
			assignment: WorkspaceAssignment{Principal: &WorkspacePrincipal{ServicePrincipalAppID: "app-123"}},
			want:       true,
		},
		{
			name:       "id-only principal without any name field is not usable",
			assignment: WorkspaceAssignment{Principal: &WorkspacePrincipal{ID: 300}},
			want:       false,
		},
		{
			name:       "id plus a name field is usable",
			assignment: WorkspaceAssignment{Principal: &WorkspacePrincipal{ID: 100, UserName: "user@example.com"}},
			want:       true,
		},
		{
			name: "valid principal with a non-empty Error stays usable",
			assignment: WorkspaceAssignment{
				Principal: &WorkspacePrincipal{ID: 200, UserName: "other@example.com"},
				Error:     "failed to resolve principal 200",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.assignment.UsablePrincipal(); got != tt.want {
				t.Errorf("UsablePrincipal() = %v, want %v", got, tt.want)
			}
		})
	}
}
