package connector

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

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

// parseCursorToken returns the pagination bag and the cursor stored in its current page token,
// which is empty on the first page.
func parseCursorToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, string, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, "", err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	return b, b.PageToken(), nil
}

// pageAfter sorts items by key and returns up to GrantsPageSize of them that sort after cursor,
// plus the cursor for the next page (empty when this is the last page). Keys must be unique.
// Paging from the last emitted key rather than an offset keeps page boundaries stable when items
// are added or removed between page fetches.
func pageAfter[T any](items []T, key func(T) string, cursor string) ([]T, string) {
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(key(a), key(b)) })

	start := 0
	if cursor != "" {
		var found bool
		start, found = slices.BinarySearchFunc(items, cursor, func(item T, c string) int { return strings.Compare(key(item), c) })
		if found {
			start++
		}
	}

	end := min(start+int(GrantsPageSize), len(items))
	var next string
	if end < len(items) {
		next = key(items[end-1])
	}

	return items[start:end], next
}
