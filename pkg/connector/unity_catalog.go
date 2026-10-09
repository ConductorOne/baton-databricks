package connector

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	workspaceStatusRunning = "RUNNING"
)

// catalogFacts is one catalog plus the workspace host its subtree is read through.
// An empty workspace means the catalog is listed but no workspace this sync covers
// can reach it, which is a state and not an absence.
type catalogFacts struct {
	name          string
	owner         string
	catalogType   string
	isolationMode string
	workspace     string
}

// routingSnapshot is the account's Unity Catalog access paths for the call that
// just listed them. Nothing on the connector keeps it.
type routingSnapshot struct {
	metastores   map[string]databricks.Metastore
	workspaces   map[string][]string
	workspaceIDs map[string]string
	unusable     []string
	asked        int
	catalogs     map[string]map[string]catalogFacts

	// unreadable names the in-scope workspaces that answered neither their
	// assignment nor a catalog listing, so what they serve is unknown rather than
	// known to be nothing. It is separate from unusable, where a successful but
	// empty catalog listing settles that the workspace carries nothing.
	unreadable []string
}

// unityCatalog holds no state beyond the client and the filters: every call asks
// Databricks and drops the answer when it returns.
type unityCatalog struct {
	client *databricks.Client

	// ownPrincipal is the applicationId of the service principal the connector authenticates as.
	ownPrincipal string

	// scope is applied when catalogs are collected, so a filtered catalog is absent exactly as a deleted one is.
	scope catalogFilter

	// workspaces is the configured allowlist, empty when none was given. Every
	// Unity Catalog read is addressed to a workspace, so routing through one the
	// operator scoped out would read the metastore through a deployment this sync
	// does not cover. ListWorkspaces only applies the exclude list, which is why
	// the allowlist has to be applied here as well as in the workspace builder.
	workspaces map[string]struct{}
}

func newUnityCatalog(
	client *databricks.Client,
	ownPrincipal string,
	scope catalogFilter,
	workspaces map[string]struct{},
) *unityCatalog {
	return &unityCatalog{
		client:       client,
		ownPrincipal: ownPrincipal,
		scope:        scope,
		workspaces:   workspaces,
	}
}

func (u *unityCatalog) catalogsFor(ctx context.Context, metastoreID string) (map[string]catalogFacts, *v2.RateLimitDescription, error) {
	snap, rateLimit, err := u.buildRouting(ctx)
	if err != nil {
		return nil, rateLimit, err
	}

	facts, err := catalogsFromSnapshot(snap, metastoreID)

	return facts, rateLimit, err
}

func catalogsFromSnapshot(snap routingSnapshot, metastoreID string) (map[string]catalogFacts, error) {
	if _, err := requireUsable(snap, metastoreID); err != nil {
		return nil, err
	}

	facts := snap.catalogs[metastoreID]
	if facts == nil {
		facts = map[string]catalogFacts{}
	}

	return facts, nil
}

// accessCatalog resolves the workspace a catalog-scoped securable is read through.
// A catalog recorded with an empty workspace errors, because emitting it as absent
// would delete it. A catalog the snapshot does not hold is gone (or out of scope).
func accessCatalog(
	snap routingSnapshot,
	ref securableRef,
	securableType string,
	rateLimit *v2.RateLimitDescription,
) (string, bool, *v2.RateLimitDescription, error) {
	catalogs, err := catalogsFromSnapshot(snap, ref.metastoreID)
	if err != nil {
		return "", false, rateLimit, err
	}
	facts, ok := catalogs[ref.catalog()]
	if !ok {
		return "", false, rateLimit, nil
	}
	if facts.workspace == "" {
		return "", true, rateLimit, status.Errorf(codes.PermissionDenied,
			"databricks-connector: catalog %s exists in metastore %s but no workspace this sync covers can reach it, "+
				"so %s %s cannot be read; grant the connector principal USE_CATALOG on the catalog, bind it to a workspace "+
				"this sync covers, or remove that workspace from databricks-exclude-workspaces",
			ref.catalog(), ref.metastoreID, securableType, ref.permissionsName())
	}

	return facts.workspace, true, rateLimit, nil
}

func workspaceFromSnapshot(snap routingSnapshot, metastoreID string, rateLimit *v2.RateLimitDescription) (string, bool, *v2.RateLimitDescription, error) {
	workspaces, exists, err := usableWorkspaces(snap, metastoreID)
	if err != nil || !exists {
		return "", exists, rateLimit, err
	}

	return workspaces[0], true, rateLimit, nil
}

// requireUsable fails when the metastore is on the account and no workspace can
// read it. A metastore that is gone is an absence, not an error.
func requireUsable(snap routingSnapshot, metastoreID string) ([]string, error) {
	workspaces, exists, err := usableWorkspaces(snap, metastoreID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	return workspaces, nil
}

func usableWorkspaces(snap routingSnapshot, metastoreID string) ([]string, bool, error) {
	if workspaces := snap.workspaces[metastoreID]; len(workspaces) > 0 {
		return workspaces, true, nil
	}
	if _, ok := snap.metastores[metastoreID]; !ok {
		return nil, false, nil
	}

	unusable := ""
	if len(snap.unusable) > 0 {
		unusable = fmt.Sprintf("; %d running workspace(s) reported neither an assignment nor a catalog: %s", len(snap.unusable), strings.Join(snap.unusable, ", "))
	}

	return nil, true, status.Errorf(codes.FailedPrecondition,
		"metastore %s exists on the account but no workspace this sync can use is attached to it, so its securables cannot be read: "+
			"every attached workspace is either left out of workspaces, named in databricks-exclude-workspaces, not RUNNING, "+
			"or reports no metastore assignment%s",
		metastoreID, unusable)
}

// deferredWorkspace is a workspace whose metastore assignment could not be read,
// held over for the catalog listing. denied separates a credential that was
// refused the assignment from one that was told there is none, because the two
// give the listing different weight.
type deferredWorkspace struct {
	name   string
	denied bool
}

// buildRouting lists metastores, workspace attachments and reachable catalogs.
func (u *unityCatalog) buildRouting(ctx context.Context) (routingSnapshot, *v2.RateLimitDescription, error) {
	metastores, rateLimit, err := u.listMetastores(ctx)
	if err != nil {
		return routingSnapshot{}, rateLimit, err
	}

	snap := routingSnapshot{
		metastores:   metastores,
		workspaces:   make(map[string][]string),
		workspaceIDs: make(map[string]string),
		catalogs:     make(map[string]map[string]catalogFacts),
	}

	workspaceRateLimit, err := u.fillWorkspaces(ctx, &snap)
	if workspaceRateLimit != nil {
		rateLimit = workspaceRateLimit
	}
	if err != nil {
		return routingSnapshot{}, rateLimit, err
	}

	// With an allowlist, a metastore no in-scope workspace is attached to is out of
	// scope, and out of scope is an absence here: the metastore is not listed, not
	// listed-but-unreachable. Keeping it would make every securable under it fail
	// with FailedPrecondition, so scoping a sync to one workspace would break the
	// sync for every metastore the other workspaces hold.
	//
	// Only when every in-scope workspace answered, though. An unrouted metastore
	// means "out of scope" only if the workspaces left out are the reason; if an
	// in-scope workspace could not be read, the same metastore may well be one it
	// serves, and dropping it here would report it deleted and take its grants
	// with it. Unknown is not absence, so that case keeps the loud failure.
	if len(u.workspaces) > 0 && len(snap.unreadable) == 0 {
		for metastoreID := range snap.metastores {
			if len(snap.workspaces[metastoreID]) == 0 {
				delete(snap.metastores, metastoreID)
			}
		}
	}

	for metastoreID, workspaces := range snap.workspaces {
		if len(workspaces) == 0 {
			continue
		}
		facts, catalogRateLimit, err := u.collectCatalogs(ctx, metastoreID, workspaces)
		if catalogRateLimit != nil {
			rateLimit = catalogRateLimit
		}
		if err != nil {
			return routingSnapshot{}, rateLimit, err
		}
		snap.catalogs[metastoreID] = facts
	}

	return snap, rateLimit, nil
}

func (u *unityCatalog) listMetastores(ctx context.Context) (map[string]databricks.Metastore, *v2.RateLimitDescription, error) {
	metastores, rateLimit, err := u.client.ListMetastores(ctx)
	if err != nil {
		return nil, rateLimit, fmt.Errorf("failed to list metastores: %w", err)
	}

	byID := make(map[string]databricks.Metastore, len(metastores))
	for _, metastore := range metastores {
		if metastore.MetastoreID == "" {
			continue
		}
		byID[metastore.MetastoreID] = metastore
	}

	return byID, rateLimit, nil
}

// fillWorkspaces maps metastores to workspaces and deployment names to numeric ids.
// The securable endpoints take the deployment name; the assignment endpoint does not.
func (u *unityCatalog) fillWorkspaces(ctx context.Context, snap *routingSnapshot) (*v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	workspaces, rateLimit, err := u.client.ListWorkspaces(ctx)
	if err != nil {
		return rateLimit, fmt.Errorf("failed to list workspaces: %w", err)
	}

	var unassigned []deferredWorkspace
	asked := 0
	for _, workspace := range workspaces {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		if workspace.DeploymentName == "" {
			continue
		}
		if workspace.Status != "" && !strings.EqualFold(workspace.Status, workspaceStatusRunning) {
			continue
		}
		if len(u.workspaces) > 0 {
			if _, ok := matchConfiguredWorkspace(u.workspaces, workspace.DeploymentName, workspace.Name, strconv.Itoa(workspace.ID)); !ok {
				continue
			}
		}

		asked++
		accountID := strconv.Itoa(workspace.ID)
		snap.workspaceIDs[workspace.DeploymentName] = accountID

		metastoreID, assignmentRateLimit, err := u.client.GetWorkspaceMetastore(ctx, accountID)
		if assignmentRateLimit != nil {
			rateLimit = assignmentRateLimit
		}
		if err != nil {
			if isUnreadableWorkspaceError(err) {
				// Databricks answers 404 both for a workspace outside Unity Catalog and
				// for an assignment the credential may not read, and 401/403 when the
				// credential is not a member of the workspace at all. None of the three
				// says anything about the other workspaces, so the workspace is set
				// aside rather than failing the snapshot that every securable needs.
				// Which of the three it was decides how much the catalog listing can
				// then settle, so it is carried along.
				unassigned = append(unassigned, deferredWorkspace{
					name:   workspace.DeploymentName,
					denied: isForbiddenError(err) || isUnauthorizedError(err),
				})

				continue
			}

			return rateLimit, fmt.Errorf("failed to resolve metastore for workspace %s: %w", workspace.DeploymentName, err)
		}
		if metastoreID == "" {
			continue
		}

		snap.workspaces[metastoreID] = append(snap.workspaces[metastoreID], workspace.DeploymentName)
	}

	// A catalog payload names its metastore, so the listing settles which metastore
	// this workspace serves, if any.
	var unusable, unreadable []string
	for _, deferred := range unassigned {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		workspace := deferred.name
		metastoreIDs, listRateLimit, err := u.metastoresServedBy(ctx, workspace)
		if listRateLimit != nil {
			rateLimit = listRateLimit
		}
		if err != nil {
			if isUnreadableWorkspaceError(err) {
				// Neither endpoint answered, so this workspace's metastores are unknown.
				unusable = append(unusable, workspace)
				unreadable = append(unreadable, workspace)

				continue
			}

			return rateLimit, fmt.Errorf("failed to list catalogs through workspace %s: %w", workspace, err)
		}
		if len(metastoreIDs) == 0 {
			unusable = append(unusable, workspace)

			// An empty catalog listing is not the same evidence in both cases. The
			// endpoint returns only the catalogs the caller may use, so a credential
			// that was also denied the assignment read can be looking at an empty list
			// purely for want of USE_CATALOG. Only a 404 on the assignment plus an
			// empty listing reads as a workspace outside Unity Catalog; a denial plus
			// an empty listing settles nothing.
			if deferred.denied {
				unreadable = append(unreadable, workspace)
			}

			continue
		}

		for _, metastoreID := range metastoreIDs {
			snap.workspaces[metastoreID] = append(snap.workspaces[metastoreID], workspace)
		}
	}

	// Every workspace carrying no securable is indistinguishable from an account with
	// no Unity Catalog; the metastore list is the authority on which it is.
	if asked > 0 && len(snap.workspaces) == 0 && len(snap.metastores) > 0 {
		return rateLimit, status.Errorf(codes.FailedPrecondition,
			"the account has %d Unity Catalog metastore(s) but none of the %d running workspace(s) reported a metastore assignment or could "+
				"list a catalog, so no securable can be listed; the connector principal needs permission to read workspace metastore "+
				"assignments on the account",
			len(snap.metastores), asked)
	}

	if len(unusable) > 0 {
		l.Debug("databricks-connector: a running workspace reported no metastore assignment and listed no catalog, so it carries nothing this sync can read",
			zap.Strings("workspaces", unusable),
			zap.Int("running_workspaces", asked),
			zap.Int("metastores_with_an_access_path", len(snap.workspaces)),
		)
	}

	snap.unusable = unusable
	snap.unreadable = unreadable
	snap.asked = asked

	return rateLimit, nil
}

// metastoresServedBy names the metastores whose catalogs a workspace can list, which
// separates the two meanings of a 404 from the assignment endpoint. The catalog filter
// is deliberately not applied: an out-of-scope catalog is still an access path.
func (u *unityCatalog) metastoresServedBy(ctx context.Context, workspace string) ([]string, *v2.RateLimitDescription, error) {
	served := make(map[string]struct{})
	rateLimit, err := u.client.ForEachCatalogPage(ctx, workspace, ResourcesPageSize, func(page []databricks.Catalog) (bool, error) {
		for _, catalog := range page {
			if catalog.MetastoreID == "" {
				continue
			}
			served[catalog.MetastoreID] = struct{}{}
		}
		return false, nil
	})
	if err != nil {
		return nil, rateLimit, err
	}

	metastoreIDs := make([]string, 0, len(served))
	for metastoreID := range served {
		metastoreIDs = append(metastoreIDs, metastoreID)
	}
	slices.Sort(metastoreIDs)

	return metastoreIDs, rateLimit, nil
}

// collectCatalogs unions a metastore's catalogs across every attached workspace.
// An ISOLATED catalog is only listable from the workspaces bound to it, and a
// workspace that cannot reach one omits it without failing.
func (u *unityCatalog) collectCatalogs(ctx context.Context, metastoreID string, workspaces []string) (map[string]catalogFacts, *v2.RateLimitDescription, error) {
	var rateLimit *v2.RateLimitDescription
	facts := make(map[string]catalogFacts)
	var seen []string
	for _, workspace := range workspaces {
		catalogs, pageRateLimit, err := u.client.DrainCatalogs(ctx, workspace, ResourcesPageSize)
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return nil, rateLimit, fmt.Errorf("failed to list catalogs through workspace %s: %w", workspace, err)
		}

		for _, catalog := range catalogs {
			name := catalog.Name
			if name == "" {
				continue
			}
			if catalog.MetastoreID != "" && catalog.MetastoreID != metastoreID {
				continue
			}

			seen = append(seen, name)

			// Filtered here so an out-of-scope catalog never enters the snapshot,
			// and every lookup reports it absent rather than unreachable.
			if !u.scope.covers(name) {
				continue
			}

			entry := catalogFacts{
				name:          name,
				owner:         catalog.Owner,
				catalogType:   catalog.CatalogType,
				isolationMode: catalog.IsolationMode,
				workspace:     workspace,
			}

			// An absent accessible_in_current_workspace and a false one are different
			// answers, so the catalog is still recorded: only "not listed" means gone.
			if catalog.AccessibleInCurrentWorkspace != nil && !*catalog.AccessibleInCurrentWorkspace {
				entry.workspace = ""
				if _, ok := facts[name]; !ok {
					facts[name] = entry
				}

				continue
			}
			if existing, ok := facts[name]; ok && existing.workspace != "" {
				// Another workspace already derived the same resource key.
				continue
			}

			facts[name] = entry
		}
	}

	if u.scope.configured() {
		// An entry matching nothing looks identical to a working filter, so it is named.
		ctxzap.Extract(ctx).Debug("databricks-connector: applied the catalog filter",
			zap.String("metastore", metastoreID),
			zap.String("field", u.scope.describe()),
			zap.Int("in_scope", len(facts)),
			zap.Strings("entries_matching_no_catalog", u.scope.unmatched(seen)),
		)
	}

	return facts, rateLimit, nil
}
