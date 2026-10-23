package connector

import (
	"fmt"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

const ResourcesPageSize uint = 50

// GrantsPageSize caps grants built from a single unpaginated API response (e.g. group members).
// Each grant serializes to ~550 bytes, so a page is ~1.1MB, well under the 6MB Lambda response limit.
const GrantsPageSize uint = 2000

func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, uint, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, 0, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	page, err := convertPageToken(b.PageToken())
	if err != nil {
		return nil, 0, err
	}

	return b, page, nil
}

// convertPageToken converts a string token into an int.
func convertPageToken(token string) (uint, error) {
	if token == "" {
		return 1, nil
	}

	page, err := strconv.ParseUint(token, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse page token: %w", err)
	}

	return uint(page), nil
}

// prepareNextToken prepares the next page token.
// It calculates the next page number and returns it as a string.
func prepareNextToken(page uint, pageTotal int, total uint) string {
	var token string

	next := page + uint(pageTotal) // #nosec G115
	if next < total+1 {
		token = strconv.FormatUint(uint64(next), 10)
	}

	return token
}

// pageBounds returns the slice bounds of the page starting at the 1-based offset page.
func pageBounds(page uint, total int) (int, int) {
	page = max(page, 1)
	start := min(int(page-1), total) // #nosec G115
	end := min(start+int(GrantsPageSize), total)
	return start, end
}
