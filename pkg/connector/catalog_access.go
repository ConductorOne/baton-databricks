package connector

import (
	"context"
	"fmt"
	"slices"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// catalogFacts is one catalog and the in-scope workspaces that list it. An
// ISOLATED catalog is listed only by the workspaces bound to it.
type catalogFacts struct {
	name          string
	owner         string
	catalogType   string
	isolationMode string
	workspaces    []string
}

// catalogsOf unions a metastore's catalogs across its usable workspaces. A catalog
// no usable workspace lists is outside the sync scope and is not returned. A
// workspace whose listing fails leaves the set unknown, so that fails instead:
// reporting it short would read as a deletion in C1.
func (u *unityCatalog) catalogsOf(
	ctx context.Context,
	snap routingSnapshot,
	metastoreID string,
) (map[string]catalogFacts, accessState, *v2.RateLimitDescription, error) {
	workspaces, state, err := usableWorkspaces(snap, metastoreID)
	if err != nil || state != securableInScope {
		return nil, state, nil, err
	}

	var rateLimit *v2.RateLimitDescription
	facts := make(map[string]catalogFacts)
	for _, workspace := range workspaces {
		catalogs, pageRateLimit, err := u.client.DrainCatalogs(ctx, workspace, ResourcesPageSize)
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return nil, securableInScope, rateLimit, catalogListingError(metastoreID, workspace, err)
		}

		for _, catalog := range catalogs {
			if catalog.Name == "" || (catalog.MetastoreID != "" && catalog.MetastoreID != metastoreID) {
				continue
			}

			entry, ok := facts[catalog.Name]
			if !ok {
				entry = catalogFacts{
					name:          catalog.Name,
					owner:         catalog.Owner,
					catalogType:   catalog.CatalogType,
					isolationMode: catalog.IsolationMode,
				}
			}
			entry.workspaces = append(entry.workspaces, workspace)
			facts[catalog.Name] = entry
		}
	}

	return facts, securableInScope, rateLimit, nil
}

// catalogListingError reads a workspace that refuses the catalog listing as unknown,
// not as listing nothing: a short list would read as deletions in C1.
func catalogListingError(metastoreID, workspace string, err error) error {
	if isUnreadableWorkspaceError(err) || isNotFoundError(err) {
		return uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("databricks-connector: the catalogs of metastore %s cannot be listed through attached workspace %s, "+
				"so which catalogs are in scope is unknown; make the connector principal an Admin of that workspace, "+
				"or leave the workspace out with workspaces or databricks-exclude-workspaces", metastoreID, workspace),
			err)
	}

	return fmt.Errorf("failed to list catalogs through workspace %s: %w", workspace, err)
}

// catalogAccess names the workspace a catalog's permissions are read through and
// where the catalog stands against the sync scope. Unless confirm is set, the
// workspace the last sync wrote on the profile is tried first.
func (d *securableDeps) catalogAccess(
	ctx context.Context,
	resource *v2.Resource,
	ref securableRef,
	confirm bool,
) (string, accessState, *v2.RateLimitDescription, error) {
	if !confirm {
		route := readProfileRoute(resource)
		workspace, ok, rateLimit, err := d.profileWorkspace(ctx, ref, route, func(ctx context.Context, workspace string) (bool, *v2.RateLimitDescription, error) {
			return d.uc.catalogGrantsAreTrustworthy(ctx, route.metastoreOwner, route.owner, ref, workspace)
		})
		if err != nil {
			return "", securableInScope, rateLimit, err
		}
		if ok {
			return workspace, securableInScope, rateLimit, nil
		}
	}

	snap, rateLimit, err := d.uc.buildRouting(ctx)
	if err != nil {
		return "", securableInScope, rateLimit, err
	}
	catalogs, state, catalogRateLimit, err := d.uc.catalogsOf(ctx, snap, ref.metastoreID)
	if catalogRateLimit != nil {
		rateLimit = catalogRateLimit
	}
	if err != nil || state != securableInScope {
		return "", state, rateLimit, err
	}

	facts, ok := catalogs[ref.catalog()]
	if !ok {
		if !confirm {
			return "", securableOutOfScope, rateLimit, nil
		}
		absence, absenceRateLimit, err := d.uc.catalogAbsence(ctx, snap, ref)
		if absenceRateLimit != nil {
			rateLimit = absenceRateLimit
		}

		return "", absence, rateLimit, err
	}

	metastoreOwner := snap.metastores[ref.metastoreID].Owner
	workspace, probeRateLimit, err := firstTrustedWorkspace(ctx, ref, facts.workspaces, func(ctx context.Context, workspace string) (bool, *v2.RateLimitDescription, error) {
		return d.uc.catalogGrantsAreTrustworthy(ctx, metastoreOwner, facts.owner, ref, workspace)
	})
	if probeRateLimit != nil {
		rateLimit = probeRateLimit
	}
	if err != nil {
		return "", securableInScope, rateLimit, err
	}
	if workspace == "" {
		return "", securableInScope, rateLimit, status.Errorf(codes.PermissionDenied,
			"databricks-connector: cannot read every grant on catalog %s: %s", ref.catalog(), catalogTrustRemedy)
	}

	return workspace, securableInScope, rateLimit, nil
}

// catalogAbsence settles a catalog no in-scope workspace lists: deleted, or bound
// only to workspaces outside the sync. Only include_unbound can tell the two apart,
// and Databricks honours it only for a caller that can update bindings, so only a
// listing by the metastore owner is proof. Without proof the catalog is treated as
// still existing, because a revoke reported done would leave the privilege in place.
func (u *unityCatalog) catalogAbsence(
	ctx context.Context,
	snap routingSnapshot,
	ref securableRef,
) (accessState, *v2.RateLimitDescription, error) {
	metastore := snap.metastores[ref.metastoreID]
	workspaces := snap.workspaces[ref.metastoreID]
	if !u.ownsPrincipal(metastore.Owner) || len(workspaces) == 0 {
		return securableInScope, nil, status.Errorf(codes.FailedPrecondition,
			"databricks-connector: catalog %s is not listed by any workspace this sync can use, and only the metastore owner can "+
				"confirm whether it was deleted or is bound only to workspaces outside the sync, so it cannot be changed",
			ref.catalog())
	}

	all, rateLimit, err := u.client.DrainUnboundCatalogs(ctx, workspaces[0], ResourcesPageSize)
	if err != nil {
		return securableInScope, rateLimit, fmt.Errorf("failed to confirm whether catalog %s still exists: %w", ref.catalog(), err)
	}

	found := slices.ContainsFunc(all, func(catalog databricks.Catalog) bool {
		return catalog.Name == ref.catalog() && (catalog.MetastoreID == "" || catalog.MetastoreID == ref.metastoreID)
	})
	if found {
		return securableOutOfScope, rateLimit, nil
	}

	return securableGone, rateLimit, nil
}
