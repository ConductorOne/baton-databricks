package connector

import (
	"strings"
	"testing"
)

func uintPtr(v uint) *uint { return &v }

func TestPrepareNextTokenAbsentTotal(t *testing.T) {
	tests := []struct {
		name      string
		page      uint
		pageTotal int
		total     *uint
		want      string
		wantErr   bool
	}{
		{"absent total, full page keeps paginating", 1, 50, nil, "51", false},
		{"absent total, short page terminates", 1, 17, nil, "", false},
		{"absent total, oversized server page keeps paginating", 1, 60, nil, "61", false},
		{"absent total, empty page terminates", 1, 0, nil, "", false},
		{"present total, mid-list empty page fails closed", 1, 0, uintPtr(96), "", true},
		{"present total, empty page after total terminates", 97, 0, uintPtr(96), "", false},
		{"present total, full page continues", 1, 50, uintPtr(96), "51", false},
		{"present total, last page returns final offset", 51, 45, uintPtr(96), "96", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prepareNextToken(tt.page, tt.pageTotal, tt.total)
			if (err != nil) != tt.wantErr {
				t.Fatalf("prepareNextToken(%d, %d, %v) error = %v, wantErr %v", tt.page, tt.pageTotal, tt.total, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("prepareNextToken(%d, %d, %v) = %q, want %q", tt.page, tt.pageTotal, tt.total, got, tt.want)
			}
		})
	}
}

func TestPrepareNextTokenForwardProgressGuard(t *testing.T) {
	// A server that omits totalResults AND ignores startIndex returns the same
	// full page for every offset; the guard must fail closed on the second
	// identical page instead of walking until the sync deadline.
	p := newPaginationProgress()
	first := pageProgressCheck{p, "groups", func() string { return "g1" }}

	// First page: records its first item, continues.
	got, err := prepareNextToken(1, 50, nil, first)
	if err != nil {
		t.Fatalf("first page: unexpected error: %v", err)
	}
	if got != "51" {
		t.Fatalf("first page token = %q, want %q", got, "51")
	}

	// Same first item on the next page: fail closed with a gRPC code.
	_, err = prepareNextToken(51, 50, nil, first)
	if err == nil {
		t.Fatal("second identical page: nil error, want fail-closed on non-advancing pagination")
	}
	if !strings.Contains(err.Error(), "databricks-connector:") || !strings.Contains(err.Error(), "did not advance") {
		t.Errorf("error %q does not name the non-advancing pagination", err)
	}

	// A different first item continues normally.
	advanced := pageProgressCheck{p, "groups", func() string { return "g2" }}
	got, err = prepareNextToken(101, 50, nil, advanced)
	if err != nil {
		t.Fatalf("advancing page: unexpected error: %v", err)
	}
	if got != "151" {
		t.Fatalf("advancing page token = %q, want %q", got, "151")
	}

	// With totalResults present the guard is not consulted (a repeated first
	// item across pages can be legitimate when the server honors offsets and
	// items shift between pages).
	p2 := newPaginationProgress()
	check := pageProgressCheck{p2, "users", func() string { return "u1" }}
	for _, page := range []uint{1, 2, 3} {
		if _, err := prepareNextToken(page, 50, uintPtr(200), check); err != nil {
			t.Fatalf("page %d with present total: unexpected error: %v", page, err)
		}
	}
}

// Two consecutive walks on the same builder (e.g. a second role's Grants after the
// first) can legitimately start at the same first item; a walk's page 1 must reset
// the guard rather than compare against the previous walk's last page.
func TestPaginationProgressResetsOnWalkStart(t *testing.T) {
	p := newPaginationProgress()
	check := pageProgressCheck{p, "roles/users/admin", func() string { return "u1" }}

	// Walk 1: full page 1, then a short page 2 (terminates without storing).
	got, err := prepareNextToken(1, 50, nil, check)
	if err != nil || got != "51" {
		t.Fatalf("walk 1 page 1: got %q, err %v", got, err)
	}
	got, err = prepareNextToken(51, 17, nil, check)
	if err != nil || got != "" {
		t.Fatalf("walk 1 page 2: got %q, err %v", got, err)
	}

	// Walk 2 of the same builder starts at the same first item u1: must NOT fail.
	got, err = prepareNextToken(1, 50, nil, check)
	if err != nil {
		t.Fatalf("walk 2 page 1: unexpected error from stale previous-walk state: %v", err)
	}
	if got != "51" {
		t.Fatalf("walk 2 page 1 token = %q, want %q", got, "51")
	}

	// But within walk 2, a page starting at u1 again IS non-advancing.
	_, err = prepareNextToken(51, 50, nil, check)
	if err == nil || !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("walk 2 page 2: error = %v, want non-advancing pagination failure", err)
	}

	// Role-scoped walk keys do not cross-contaminate.
	other := pageProgressCheck{p, "roles/users/viewer", func() string { return "u1" }}
	if _, err := prepareNextToken(51, 50, nil, other); err != nil {
		t.Fatalf("different role's walk page 2: unexpected error: %v", err)
	}
}

// Parent-scoped walk keys keep concurrent walks for same-named roles in
// sibling workspaces from interleaving on one paginationProgress entry.
func TestPaginationProgressScopesByParent(t *testing.T) {
	p := newPaginationProgress()
	wsA := pageProgressCheck{p, "ws-a/roles/users/admin", func() string { return "u1" }}
	wsB := pageProgressCheck{p, "ws-b/roles/users/admin", func() string { return "u1" }}

	if _, err := prepareNextToken(1, 50, nil, wsA); err != nil {
		t.Fatalf("workspace A page 1: %v", err)
	}
	if _, err := prepareNextToken(1, 50, nil, wsB); err != nil {
		t.Fatalf("workspace B page 1 interleaved: unexpected error: %v", err)
	}
	// Workspace A page 2 starting at u1 again is non-advancing; B's interleaved
	// walk must not have overwritten A's state.
	if _, err := prepareNextToken(51, 50, nil, wsA); err == nil || !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("workspace A page 2: error = %v, want non-advancing pagination failure", err)
	}
	if _, err := prepareNextToken(51, 50, nil, wsB); err == nil || !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("workspace B page 2: error = %v, want non-advancing pagination failure", err)
	}
}
