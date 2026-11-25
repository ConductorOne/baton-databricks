package connector

import "testing"

func uintPtr(v uint) *uint { return &v }

func TestPrepareNextTokenAbsentTotal(t *testing.T) {
	tests := []struct {
		name      string
		page      uint
		pageTotal int
		total     *uint
		want      string
	}{
		{"absent total, full page keeps paginating", 1, 50, nil, "51"},
		{"absent total, short page terminates", 1, 17, nil, ""},
		{"absent total, oversized server page keeps paginating", 1, 60, nil, "61"},
		{"absent total, empty page terminates", 1, 0, nil, ""},
		{"present total, empty page terminates", 1, 0, uintPtr(96), ""},
		{"present total, full page continues", 1, 50, uintPtr(96), "51"},
		{"present total, last page returns final offset", 51, 45, uintPtr(96), "96"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := prepareNextToken(tt.page, tt.pageTotal, tt.total); got != tt.want {
				t.Errorf("prepareNextToken(%d, %d, %v) = %q, want %q", tt.page, tt.pageTotal, tt.total, got, tt.want)
			}
		})
	}
}
