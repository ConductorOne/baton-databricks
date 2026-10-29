package databricks

import (
	"net/url"
	"slices"
	"strings"
	"testing"
)

func TestAttrVarsApplySingleJoinedKey(t *testing.T) {
	tests := []struct {
		name string
		vars *AttrVars
	}{
		{"user", NewUserAttrVars()},
		{"user roles", NewUserRolesAttrVars()},
		{"group", NewGroupAttrVars()},
		{"group members", NewGroupMembersAttrVars()},
		{"group roles", NewGroupRolesAttrVars()},
		{"service principal", NewServicePrincipalAttrVars()},
		{"service principal roles", NewServicePrincipalRolesAttrVars()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := url.Values{}
			tt.vars.Apply(v)
			got := v["attributes"]
			if len(got) != 1 {
				t.Fatalf("expected exactly one attributes key, got %d: %v", len(got), got)
			}
			if parts := strings.Split(got[0], ","); !slices.Equal(parts, tt.vars.Attrs) {
				t.Fatalf("expected %v, got %v", tt.vars.Attrs, parts)
			}
		})
	}
}