package databricks

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	// The SDK caps the walks it drives; this one is driven here.
	maxCursorPages = 1000
)

// forEachPageFrom walks listPage from start. A page can come back empty and still
// carry a next token, so only an absent token ends the walk, and a repeated token
// is an error: Databricks restarts at page one on a token it does not recognize.
func forEachPageFrom[T any](
	ctx context.Context,
	start string,
	listPage func(pageToken string) ([]T, string, *v2.RateLimitDescription, error),
	visit func(page []T) (stop bool, err error),
) (*v2.RateLimitDescription, error) {
	var rateLimit *v2.RateLimitDescription
	seen := map[string]struct{}{}

	for pageToken, pages := start, 0; ; pages++ {
		if pageToken != "" {
			if _, ok := seen[pageToken]; ok {
				return rateLimit, fmt.Errorf("cursor walk repeated a page token after %d pages", pages)
			}
			seen[pageToken] = struct{}{}
		}
		if pages >= maxCursorPages {
			return rateLimit, fmt.Errorf("cursor walk exceeded %d pages", maxCursorPages)
		}
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		page, next, pageRateLimit, err := listPage(pageToken)
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return rateLimit, err
		}

		stop, err := visit(page)
		if err != nil {
			return rateLimit, err
		}
		if stop || next == "" {
			return rateLimit, nil
		}
		pageToken = next
	}
}

// drainPagesFrom walks listPage to exhaustion.
func drainPagesFrom[T any](
	ctx context.Context,
	start string,
	listPage func(pageToken string) ([]T, string, *v2.RateLimitDescription, error),
) (
	[]T,
	*v2.RateLimitDescription,
	error,
) {
	var items []T
	rateLimit, err := forEachPageFrom(ctx, start, listPage, func(page []T) (bool, error) {
		items = append(items, page...)
		return false, nil
	})
	if err != nil {
		return nil, rateLimit, err
	}

	return items, rateLimit, nil
}
