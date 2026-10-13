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

// routingSnapshot is the account's Unity Catalog access paths for the call that
// just listed them. Nothing on the connector keeps it.
type routingSnapshot struct {
	metastores     map[string]databricks.Metastore
	workspaces     map[string][]string
	workspaceIDs   map[string]string
	workspaceNames map[string]string
	asked          int

	// unreadable names the metastores whose workspace assignment list answered
	// 401, 403 or 404, so which workspaces serve them is unknown rather than none.
	unreadable []string

	// pending maps a metastore to its in-scope assigned workspaces that are not
	// RUNNING. Whether they will serve it is unknown until they are, so they keep
	// that metastore from being pruned.
	pending map[string][]string

	// pruned names the listed metastores left out because no workspace this sync
	// can use is attached to them. They still exist in Databricks.
	pruned map[string]struct{}
}

// metastoreState is where a metastore stands in a routing snapshot.
type metastoreState int

const (
	// metastoreGone is a metastore the account no longer lists.
	metastoreGone metastoreState = iota
	// metastoreInScope is listed and not pruned.
	metastoreInScope
	// metastoreOutOfScope is listed but pruned: no workspace this sync can use is attached.
	metastoreOutOfScope
)

// unityCatalog holds no state beyond the client and the workspace filter: every
// call asks Databricks and drops the answer when it returns.
type unityCatalog struct {
	client *databricks.Client

	// ownPrincipal is the applicationId of the service principal the connector authenticates as.
	ownPrincipal string

	// workspaces is the configured allowlist, empty when none was given. Every
	// Unity Catalog read is addressed to a workspace, so routing through one the
	// operator scoped out would read the metastore through a deployment this sync
	// does not cover. ListWorkspaces only applies the exclude list, which is why
	// the allowlist has to be applied here as well as in the workspace builder.
	workspaces map[string]struct{}
}

func newUnityCatalog(client *databricks.Client, ownPrincipal string, workspaces map[string]struct{}) *unityCatalog {
	return &unityCatalog{
		client:       client,
		ownPrincipal: ownPrincipal,
		workspaces:   workspaces,
	}
}

// usableWorkspaces returns workspaces only for an in-scope metastore. One that is
// in scope but has no usable workspace yet fails with FailedPrecondition.
func usableWorkspaces(snap routingSnapshot, metastoreID string) ([]string, metastoreState, error) {
	if workspaces := snap.workspaces[metastoreID]; len(workspaces) > 0 {
		return workspaces, metastoreInScope, nil
	}
	if _, ok := snap.pruned[metastoreID]; ok {
		return nil, metastoreOutOfScope, nil
	}
	if _, ok := snap.metastores[metastoreID]; !ok {
		return nil, metastoreGone, nil
	}

	pending := ""
	if waiting := snap.pending[metastoreID]; len(waiting) > 0 {
		pending = fmt.Sprintf("; %d attached workspace(s) are not RUNNING: %s", len(waiting), strings.Join(waiting, ", "))
	}

	return nil, metastoreInScope, status.Errorf(codes.FailedPrecondition,
		"metastore %s exists on the account but no workspace this sync can use is attached to it, so its securables cannot be read: "+
			"every attached workspace is either left out of workspaces, named in databricks-exclude-workspaces, not RUNNING, "+
			"or reports no metastore assignment%s",
		metastoreID, pending)
}

// buildRouting lists metastores and the workspaces each one is reachable through.
func (u *unityCatalog) buildRouting(ctx context.Context) (routingSnapshot, *v2.RateLimitDescription, error) {
	metastores, rateLimit, err := u.listMetastores(ctx)
	if err != nil {
		return routingSnapshot{}, rateLimit, err
	}

	snap := routingSnapshot{
		metastores:     metastores,
		workspaces:     make(map[string][]string),
		workspaceIDs:   make(map[string]string),
		workspaceNames: make(map[string]string),
		pending:        make(map[string][]string),
		pruned:         make(map[string]struct{}),
	}

	workspaceRateLimit, err := u.fillWorkspaces(ctx, &snap)
	if workspaceRateLimit != nil {
		rateLimit = workspaceRateLimit
	}
	if err != nil {
		return routingSnapshot{}, rateLimit, err
	}

	// A metastore with no usable workspace would fail its Grants with
	// FailedPrecondition, which fails the whole sync, so it is pruned. An unknown
	// answer (unreadable list, non-RUNNING workspace) blocks the prune: dropping it
	// would read as a deletion in C1. Pruned is recorded apart from deleted
	// because the metastore still holds its privileges.
	l := ctxzap.Extract(ctx)
	for metastoreID := range snap.metastores {
		if len(snap.workspaces[metastoreID]) > 0 || len(snap.pending[metastoreID]) > 0 || slices.Contains(snap.unreadable, metastoreID) {
			continue
		}
		l.Debug("databricks-connector: no workspace this sync can use is attached to the metastore; leaving it out of the sync",
			zap.String("metastore", metastoreID),
		)
		delete(snap.metastores, metastoreID)
		snap.pruned[metastoreID] = struct{}{}
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
// The reverse assignment list is one call per metastore (usually one per region),
// so a workspace missing from every metastore's list is simply not on Unity Catalog.
func (u *unityCatalog) fillWorkspaces(ctx context.Context, snap *routingSnapshot) (*v2.RateLimitDescription, error) {
	workspaces, rateLimit, err := u.client.ListWorkspaces(ctx)
	if err != nil {
		return rateLimit, fmt.Errorf("failed to list workspaces: %w", err)
	}

	inScope := map[string]databricks.Workspace{}
	notRunning := map[string]string{}
	asked := 0
	for _, workspace := range workspaces {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}
		if workspace.DeploymentName == "" {
			continue
		}
		accountID := strconv.Itoa(workspace.ID)
		if len(u.workspaces) > 0 {
			if _, ok := matchConfiguredWorkspace(u.workspaces, workspace.DeploymentName, workspace.Name, accountID); !ok {
				continue
			}
		}
		if workspace.Status != "" && !strings.EqualFold(workspace.Status, workspaceStatusRunning) {
			notRunning[accountID] = workspace.DeploymentName
			continue
		}

		asked++
		inScope[accountID] = workspace
		snap.workspaceIDs[workspace.DeploymentName] = accountID
		snap.workspaceNames[workspace.DeploymentName] = workspace.Name
	}

	if len(u.workspaces) > 0 && asked == 0 && len(notRunning) == 0 && len(snap.metastores) > 0 {
		configured := configuredWorkspaceNames(u.workspaces)
		slices.Sort(configured)

		return rateLimit, status.Errorf(codes.FailedPrecondition,
			"none of the workspaces in the workspaces allowlist (%s) matches a workspace on the account by deployment name, name or id, "+
				"so no metastore can be scoped to it",
			strings.Join(configured, ", "))
	}

	var unreadable []string
	for metastoreID := range snap.metastores {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		ids, assignmentRateLimit, err := u.client.ListMetastoreWorkspaces(ctx, metastoreID)
		if assignmentRateLimit != nil {
			rateLimit = assignmentRateLimit
		}
		if err != nil {
			// Databricks does not document what a 404 here means, so it is read as
			// unknown rather than as a deletion: pruning on it would make C1 delete
			// the metastore and its grants. A metastore that really is gone stops
			// appearing in ListMetastores on the next sync.
			if isUnreadableWorkspaceError(err) || isNotFoundError(err) {
				unreadable = append(unreadable, metastoreID)
				continue
			}

			return rateLimit, fmt.Errorf("failed to list workspaces for metastore %s: %w", metastoreID, err)
		}

		for _, id := range ids {
			if deployment, ok := notRunning[id]; ok {
				snap.pending[metastoreID] = append(snap.pending[metastoreID], deployment)
				continue
			}
			workspace, ok := inScope[id]
			if !ok {
				continue
			}
			snap.workspaces[metastoreID] = append(snap.workspaces[metastoreID], workspace.DeploymentName)
		}
	}

	snap.unreadable = unreadable
	snap.asked = asked

	return rateLimit, nil
}
