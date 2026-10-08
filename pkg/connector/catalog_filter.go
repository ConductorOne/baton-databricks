package connector

import (
	"fmt"
	"slices"
	"strings"
)

// catalogFilter scopes a whole subtree per entry. Matching is case-insensitive
// because Unity Catalog resolves catalog names case-insensitively.
type catalogFilter struct {
	include map[string]struct{}
	exclude map[string]struct{}
}

// The two config fields are mutually exclusive. Re-checked here because New is
// exported and bypasses the SDK's check.
func newCatalogFilter(include, exclude []string) (catalogFilter, error) {
	in := newNameSet(include)
	ex := newNameSet(exclude)

	if len(in) > 0 && len(ex) > 0 {
		return catalogFilter{}, fmt.Errorf(
			"databricks-catalogs and databricks-exclude-catalogs are mutually exclusive: set the catalogs to sync, or the catalogs to skip, not both")
	}

	return catalogFilter{include: in, exclude: ex}, nil
}

func newNameSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		set[name] = struct{}{}
	}

	if len(set) == 0 {
		return nil
	}

	return set
}

func (f catalogFilter) covers(name string) bool {
	key := strings.ToLower(strings.TrimSpace(name))

	if len(f.include) > 0 {
		_, ok := f.include[key]

		return ok
	}

	_, excluded := f.exclude[key]

	return !excluded
}

func (f catalogFilter) configured() bool {
	return len(f.include) > 0 || len(f.exclude) > 0
}

func (f catalogFilter) unmatched(seen []string) []string {
	if !f.configured() {
		return nil
	}

	present := make(map[string]struct{}, len(seen))
	for _, name := range seen {
		present[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}

	configured := f.include
	if len(configured) == 0 {
		configured = f.exclude
	}

	missing := make([]string, 0, len(configured))
	for name := range configured {
		if _, ok := present[name]; !ok {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)

	if len(missing) == 0 {
		return nil
	}

	return missing
}

func (f catalogFilter) describe() string {
	if len(f.include) > 0 {
		return "databricks-catalogs"
	}

	return "databricks-exclude-catalogs"
}
