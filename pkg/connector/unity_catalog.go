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
	metastores   map[string]databricks.Metastore
	workspaces   map[string][]string
	workspaceIDs map[string]string
	unusable     []string
	asked        int
}

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

func workspaceFromSnapshot(snap routingSnapshot, metastoreID string, rateLimit *v2.RateLimitDescription) (string, bool, *v2.RateLimitDescription, error) {
	workspaces, exists, err := usableWorkspaces(snap, metastoreID)
	if err != nil || !exists {
		return "", exists, rateLimit, err
	}

	return workspaces[0], true, rateLimit, nil
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
			"every attached workspace is either named in databricks-exclude-workspaces, not RUNNING, or reports no metastore assignment%s",
		metastoreID, unusable)
}

// buildRouting lists metastores and the workspaces each one is reachable through.
func (u *unityCatalog) buildRouting(ctx context.Context) (routingSnapshot, *v2.RateLimitDescription, error) {
	metastores, rateLimit, err := u.listMetastores(ctx)
	if err != nil {
		return routingSnapshot{}, rateLimit, err
	}

	snap := routingSnapshot{
		metastores:   metastores,
		workspaces:   make(map[string][]string),
		workspaceIDs: make(map[string]string),
	}

	workspaceRateLimit, err := u.fillWorkspaces(ctx, &snap)
	if workspaceRateLimit != nil {
		rateLimit = workspaceRateLimit
	}
	if err != nil {
		return routingSnapshot{}, rateLimit, err
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

	var unassigned []string
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
				unassigned = append(unassigned, workspace.DeploymentName)

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
	var unusable []string
	for _, workspace := range unassigned {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		metastoreIDs, listRateLimit, err := u.metastoresServedBy(ctx, workspace)
		if listRateLimit != nil {
			rateLimit = listRateLimit
		}
		if err != nil {
			if isUnreadableWorkspaceError(err) {
				unusable = append(unusable, workspace)

				continue
			}

			return rateLimit, fmt.Errorf("failed to list catalogs through workspace %s: %w", workspace, err)
		}
		if len(metastoreIDs) == 0 {
			unusable = append(unusable, workspace)

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
