package databricks

import (
	"encoding/json"
	"testing"
)

func TestGroupClassDecodeStates(t *testing.T) {
	tests := []struct {
		name string
		body string
		want GroupClass
	}{
		{"meta absent", `{"id":"g1"}`, UnknownGroupClass},
		{"meta empty", `{"id":"g1","meta":{}}`, UnknownGroupClass},
		{"account group", `{"id":"g1","meta":{"resourceType":"Group"}}`, AccountGroup},
		{"workspace group", `{"id":"g1","meta":{"resourceType":"WorkspaceGroup"}}`, WorkspaceGroup},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var g Group
			if err := json.Unmarshal([]byte(tc.body), &g); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := g.GroupClass(); got != tc.want {
				t.Errorf("GroupClass = %v, want %v", got, tc.want)
			}
		})
	}
}
