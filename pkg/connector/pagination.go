package connector

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
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

// paginationProgress remembers the first item key of the previous page for
// each walk so a server that both omits totalResults and ignores startIndex
// (returning the same full page for any offset) fails closed instead of being
// walked forever until the sync deadline.
type paginationProgress struct {
	mu    sync.Mutex
	lasts map[string]string
}

func newPaginationProgress() *paginationProgress {
	return &paginationProgress{lasts: make(map[string]string)}
}

// check records the first item key of the current page and fails closed when it
// equals the previous page's, i.e. when pagination made no forward progress.
func (p *paginationProgress) check(walk, firstItem string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	prev, seen := p.lasts[walk]
	p.lasts[walk] = firstItem
	if seen && prev == firstItem {
		return uhttp.WrapErrors(codes.Internal, fmt.Sprintf(
			"databricks-connector: pagination for %s did not advance (page starts at the same item %s as the previous page); "+
				"the server appears to ignore pagination offsets while totalResults is absent; "+
				"failing sync so existing grants are preserved", walk, firstItem))
	}
	return nil
}

// pageProgressCheck carries the guard and the walk key plus a lazy accessor for
// the current page's first item identity (nil-safe: only evaluated in the
// absent-totalResults continuation path, where the page is known non-empty).
type pageProgressCheck struct {
	progress  *paginationProgress
	walk      string
	firstItem func() string
}

// prepareNextToken prepares the next page token.
// It calculates the next page number and returns it as a string, or an error
// when an empty page arrives while totalResults says more items remain: ending
// the sync there would make C1 treat the missing items as deleted, so we fail
// closed instead.
func prepareNextToken(page uint, pageTotal int, total *uint, progressChecks ...pageProgressCheck) (string, error) {
	if pageTotal == 0 {
		// empty page is terminal evidence only when totalResults is absent or
		// already reached; otherwise the server dropped items mid-list
		if total != nil && page < *total+1 {
			return "", uhttp.WrapErrors(codes.Internal, fmt.Sprintf(
				"databricks-connector: empty page at offset %d but totalResults=%d; failing sync so missing items are not treated as deleted", page, *total))
		}
		return "", nil // kills the same-token non-termination
	}
	if total == nil {
		// absent totalResults: continue while the page is at least as full as
		// the page we asked for (>=, not ==, so a server that clamps or ignores
		// pageSize still advances by its own page size instead of truncating)
		if pageTotal >= int(ResourcesPageSize) {
			for _, pc := range progressChecks {
				if err := pc.progress.check(pc.walk, pc.firstItem()); err != nil {
					return "", err
				}
			}
			return strconv.FormatUint(uint64(page+uint(pageTotal)), 10), nil // #nosec G115
		}
		return "", nil
	}
	next := page + uint(pageTotal) // #nosec G115
	if next < *total+1 {
		return strconv.FormatUint(uint64(next), 10), nil
	}
	return "", nil
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
