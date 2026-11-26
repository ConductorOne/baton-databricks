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
