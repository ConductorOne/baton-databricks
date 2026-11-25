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
