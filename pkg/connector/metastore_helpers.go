package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	securableTypeMetastore = "METASTORE"

	// workspacePermissionAdmin is the workspace-level assignment that makes its
	// holder a metastore admin for the workspace's Unity Catalog reads.
	workspacePermissionAdmin = "ADMIN"

	// Statuses the account API documents for a workspace. PROVISIONING and
	// NOT_PROVISIONED can still become RUNNING. CANCELLING is a workspace being
	// deleted: it will not become RUNNING. FAILED and BANNED cannot either.
	workspaceStatusRunning        = "RUNNING"
	workspaceStatusProvisioning   = "PROVISIONING"
	workspaceStatusNotProvisioned = "NOT_PROVISIONED"
	workspaceStatusCancelling     = "CANCELLING"

	profileKeyMetastoreID   = "metastore_id"
	profileKeyOwner         = "owner"
	profileKeySecurableType = "securable_type"
	profileKeyRegion        = "region"
	profileKeyWorkspace     = "workspace"
	profileKeyWorkspaceID   = "workspace_id"
	profileKeyWorkspaceName = "workspace_name"

	// ownerEntitlement is single-valued and Databricks cannot remove an owner, so it is never grantable.
	ownerEntitlement = "owner"
)

var (
	// Canonical UPPER_SNAKE names, the form the permissions API answers with.
	// ALL_PRIVILEGES and MANAGE are rejected on a metastore.
	metastorePrivileges = []string{
		"CREATE_CATALOG",
		"CREATE_CLEAN_ROOM",
		"CREATE_CONNECTION",
		"CREATE_EXTERNAL_LOCATION",
		"CREATE_EXTERNAL_METADATA",
		"CREATE_PROVIDER",
		"CREATE_RECIPIENT",
		"CREATE_SHARE",
		"CREATE_SERVICE_CREDENTIAL",
		"CREATE_STORAGE_CREDENTIAL",
		"MANAGE_ALLOWLIST",
		"READ_METADATA",
		"SET_SHARE_PERMISSION",
		"USE_MARKETPLACE_ASSETS",
		"USE_PROVIDER",
		"USE_RECIPIENT",
		"USE_SHARE",
	}

	// Hive-era aliases Databricks accepts and rewrites server-side, so they never read back.
	legacyPrivileges = []string{"CREATE", "CREATE_VIEW", "USAGE"}
)

// syncPrincipals says which principal types this sync includes.
type syncPrincipals struct {
	groups            bool
	servicePrincipals bool
}

func emitPrincipal(sync syncPrincipals, resourceType string) bool {
	switch resourceType {
	case userResourceType.Id:
		return true
	case groupResourceType.Id:
		return sync.groups
	case servicePrincipalResourceType.Id:
		return sync.servicePrincipals
	default:
		return false
	}
}

func parseEntitlementPrivilege(entitlementID string) (string, error) {
	separator := strings.LastIndex(entitlementID, ":")
	if separator < 0 || separator == len(entitlementID)-1 {
		return "", status.Errorf(codes.InvalidArgument, "databricks-connector: invalid entitlement id %q", entitlementID)
	}

	return entitlementID[separator+1:], nil
}

// matchHeldPrivilege finds the assignment carrying privilege for this principal.
// The SCIM id wins; the name is used only when the assignment has no id.
func matchHeldPrivilege(assignments []databricks.PrivilegeAssignment, scimID, principalName, privilege string) (databricks.PrivilegeAssignment, bool) {
	var nameOnly databricks.PrivilegeAssignment
	var sawNameOnly bool
	for _, assignment := range assignments {
		if !slices.Contains(assignment.Privileges, privilege) {
			continue
		}

		assignmentID := assignment.PrincipalID.String()
		if scimID != "" && assignmentID == scimID && assignmentID != "0" {
			return assignment, true
		}
		if principalName == "" || assignment.Principal != principalName {
			continue
		}
		if scimID != "" && assignmentID != "" && assignmentID != "0" && assignmentID != scimID {
			continue
		}
		if !sawNameOnly {
			nameOnly, sawNameOnly = assignment, true
		}
	}

	return nameOnly, sawNameOnly
}

// groupSecurableGrantOptions lets C1 expand a group-held privilege through the
// group's members. Shallow stays unset so nested groups propagate.
func groupSecurableGrantOptions(principalId *v2.ResourceId) []grant.GrantOption {
	if principalId.GetResourceType() != groupResourceType.Id {
		return nil
	}

	return []grant.GrantOption{grant.WithAnnotation(&v2.GrantExpandable{
		EntitlementIds:  []string{fmt.Sprintf("group:%s:%s", principalId.GetResource(), groupMemberEntitlement)},
		ResourceTypeIds: []string{userResourceType.Id, groupResourceType.Id, servicePrincipalResourceType.Id},
	})}
}

func isForbiddenError(err error) bool {
	return apiStatus(err) == http.StatusForbidden
}

func apiStatus(err error) int {
	var apiErr *databricks.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}

	return 0
}

// refuseUnreadable keeps a NotFound from reading as an empty result: the SDK
// skips a child List that answers NotFound, and ConductorOne then deletes what
// that parent contained. Every other status, a throttle included, keeps the
// code the client returned so the SDK can still back off and retry.
func refuseUnreadable(msg string, err error) error {
	if err == nil {
		return nil
	}
	if apiStatus(err) == http.StatusNotFound || status.Code(err) == codes.NotFound {
		return uhttp.WrapErrors(codes.FailedPrecondition, msg, err)
	}

	return fmt.Errorf("%s: %w", msg, err)
}

// unityPrincipalScimID is the numeric SCIM id a privilege assignment carries in principal_id.
func unityPrincipalScimID(principal *v2.ResourceId) (string, error) {
	if principal.GetResourceType() != groupResourceType.Id {
		return principal.GetResource(), nil
	}

	_, parsed, err := parseResourceId(principal.GetResource())
	if err != nil {
		return "", fmt.Errorf("failed to parse group resource id: %w", err)
	}

	return parsed.GetResource(), nil
}

// unityPrincipalName is the user's userName, the group's displayName or the
// service principal's applicationId. Empty when the principal no longer exists.
func unityPrincipalName(ctx context.Context, c *databricks.Client, principal *v2.ResourceId) (string, error) {
	principalId, err := unityPrincipalScimID(principal)
	if err != nil {
		return "", err
	}

	qualified, err := preparePrincipalId(ctx, c, "", principal.GetResourceType(), principalId)
	if err != nil {
		return "", err
	}

	_, name, _ := strings.Cut(qualified, "/")

	return name, nil
}

// workspaceScope is the set of workspaces this sync may read Unity Catalog
// through, indexed by numeric id, with the RUNNING ones marked. Both the
// metastore and the catalog syncer resolve their route through it.
type workspaceScope struct {
	byID    map[string]databricks.Workspace
	running map[string]bool
}

// scopedWorkspaces reads the account's workspaces and keeps the ones the
// allowlist admits. ListWorkspaces already drops the excluded ones.
func scopedWorkspaces(
	ctx context.Context,
	client *databricks.Client,
	allowlist map[string]struct{},
	annos *annotations.Annotations,
) (workspaceScope, error) {
	workspaces, rateLimit, err := client.ListWorkspaces(ctx)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return workspaceScope{}, refuseUnreadable("databricks-connector: failed to list workspaces", err)
	}

	scope := workspaceScope{
		byID:    make(map[string]databricks.Workspace, len(workspaces)),
		running: make(map[string]bool, len(workspaces)),
	}
	for _, workspace := range workspaces {
		id := strconv.Itoa(workspace.ID)
		if workspace.DeploymentName == "" {
			continue
		}
		if len(allowlist) > 0 {
			if _, ok := matchConfiguredWorkspace(allowlist, workspace.DeploymentName, workspace.Name, id); !ok {
				continue
			}
		}
		// A workspace that will not become RUNNING is left out of scope entirely,
		// so a metastore attached only to one is skipped rather than failing every
		// sync. Only a status that can still change keeps the metastore waiting.
		if !workspaceIsUsable(workspace.Status) {
			continue
		}
		scope.byID[id] = workspace
		scope.running[id] = workspace.Status == "" || strings.EqualFold(workspace.Status, workspaceStatusRunning)
	}

	return scope, nil
}

// workspaceIsUsable reports a status that is RUNNING or can still become it.
// FAILED and BANNED are terminal, and a status Databricks has not documented
// takes that path too: treating one as pending would fail the sync on every run.
func workspaceIsUsable(workspaceStatus string) bool {
	switch strings.ToUpper(workspaceStatus) {
	case "", workspaceStatusRunning, workspaceStatusProvisioning, workspaceStatusNotProvisioned, workspaceStatusCancelling:
		return true
	default:
		return false
	}
}

// errMetastoreGone marks a metastore the account no longer lists.
var errMetastoreGone = errors.New("metastore is no longer on the account")

// attached lists the running in-scope workspaces the metastore is assigned to,
// in assignment order. inScope reports whether any in-scope workspace is
// attached at all, running or not: that is what tells "outside this sync" apart
// from "in this sync but not reachable right now".
func (s workspaceScope) attached(
	ctx context.Context,
	client *databricks.Client,
	metastoreID string,
	annos *annotations.Annotations,
) ([]databricks.Workspace, bool, error) {
	ids, rateLimit, err := client.ListMetastoreWorkspaces(ctx, metastoreID)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return nil, false, err
	}

	var usable []databricks.Workspace
	inScope := false
	for _, id := range ids {
		workspace, ok := s.byID[id]
		if !ok {
			continue
		}
		inScope = true
		if s.running[id] {
			usable = append(usable, workspace)
		}
	}

	return usable, inScope, nil
}

// pickReadableWorkspace is the first running workspace the connector can read
// every privilege through. An owner sees them all; otherwise it is the first
// workspace where the connector is a direct Admin.
func pickReadableWorkspace(
	ctx context.Context,
	client *databricks.Client,
	ownPrincipal, owner, metastoreID string,
	usable []databricks.Workspace,
	annos *annotations.Annotations,
) (*databricks.Workspace, error) {
	if ownsPrincipal(ownPrincipal, owner) {
		return &usable[0], nil
	}

	for i := range usable {
		admin, err := isWorkspaceAdmin(ctx, client, ownPrincipal, strconv.Itoa(usable[i].ID), annos)
		if err != nil {
			return nil, err
		}
		if admin {
			return &usable[i], nil
		}
	}

	names := make([]string, 0, len(usable))
	for _, workspace := range usable {
		names = append(names, workspace.DeploymentName)
	}

	return nil, status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot read every privilege on metastore %s through any workspace attached to it (%s), "+
			"and Databricks answers a partial read with 200, "+
			"so reporting it would revoke the access ConductorOne cannot see; "+
			"the connector's service principal must own the metastore or be assigned Admin directly on one of those workspaces",
		metastoreID, strings.Join(names, ", "))
}

// metastoreGrantsAreTrustworthy is true when the connector owns the metastore
// or is a direct Admin of the routed workspace. MANAGE is not a metastore
// privilege.
func metastoreGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	if ownsPrincipal(trust.ownPrincipal, trust.owner) {
		return true, nil
	}

	return isWorkspaceAdmin(ctx, client, trust.ownPrincipal, trust.workspaceID, annos)
}

// ownsPrincipal reports whether name is the service principal the connector
// authenticates as. Databricks names a service principal by its applicationId
// in an owner field, and does not normalise its case.
func ownsPrincipal(ownPrincipal, name string) bool {
	return ownPrincipal != "" && strings.EqualFold(name, ownPrincipal)
}

// isWorkspaceAdmin lists the workspace's permission assignments on every call.
// An assignment list the connector may not read leaves Admin unproven, which is
// the same answer as not holding it.
func isWorkspaceAdmin(
	ctx context.Context,
	client *databricks.Client,
	ownPrincipal, workspaceID string,
	annos *annotations.Annotations,
) (bool, error) {
	if ownPrincipal == "" || workspaceID == "" {
		return false, nil
	}

	assignments, rateLimit, err := client.ListWorkspaceMembers(ctx, workspaceID)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		if !deniesTrust(err) {
			return false, fmt.Errorf("databricks-connector: failed to list the permission assignments of workspace %s: %w", workspaceID, err)
		}
		ctxzap.Extract(ctx).Debug("databricks-connector: the permission assignments of the workspace cannot be read, so Admin stays unproven",
			zap.String("workspace_id", workspaceID),
			zap.Error(err))

		return false, nil
	}

	return holdsWorkspaceAdmin(ctx, client, ownPrincipal, assignments, annos)
}

// holdsWorkspaceAdmin is true only when the connector principal itself holds ADMIN. Admin inherited through a group does not count.
func holdsWorkspaceAdmin(
	ctx context.Context,
	client *databricks.Client,
	ownPrincipal string,
	assignments []databricks.WorkspaceAssignment,
	annos *annotations.Annotations,
) (bool, error) {
	var adminIDs []string
	for _, assignment := range assignments {
		principal := assignment.Principal
		if principal == nil || !slices.Contains(assignment.Permissions, workspacePermissionAdmin) {
			continue
		}
		if ownsPrincipal(ownPrincipal, principal.ServicePrincipalAppID) {
			return true, nil
		}
		if principal.ID != 0 {
			adminIDs = append(adminIDs, strconv.Itoa(principal.ID))
		}
	}

	if len(adminIDs) == 0 {
		return false, nil
	}

	ownID, err := ownPrincipalScimID(ctx, client, ownPrincipal, annos)
	if err != nil {
		return false, err
	}

	return ownID != "" && slices.Contains(adminIDs, ownID), nil
}

// ownPrincipalScimID looks up the connector's service principal by application id.
func ownPrincipalScimID(
	ctx context.Context,
	client *databricks.Client,
	ownPrincipal string,
	annos *annotations.Annotations,
) (string, error) {
	id, rateLimit, err := client.FindServicePrincipalID(ctx, "", ownPrincipal)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return "", fmt.Errorf("databricks-connector: failed to resolve the connector's own service principal: %w", err)
	}

	return id, nil
}

// managePrivilege is what a trust rule looks for when ownership did not settle
// it. ALL_PRIVILEGES is deliberately not here: Databricks leaves MANAGE out of
// it, so its holder still reads a partial privilege list.
// https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
var managePrivilege = []string{privilegeManage}

// holdsPrivilegeOn reports whether the connector holds one of wanted. A refusal leaves it unproven. The read is not cached.
func holdsPrivilegeOn(
	ctx context.Context,
	client *databricks.Client,
	trust securableTrust,
	securableType, fullName string,
	wanted []string,
	annos *annotations.Annotations,
) (bool, error) {
	if trust.ownPrincipal == "" || trust.workspace == "" {
		return false, nil
	}

	var holds bool
	rateLimit, err := client.ForEachUncachedPermissionsPage(ctx, trust.workspace, securableType, fullName, trust.ownPrincipal,
		func(page []databricks.PrivilegeAssignment) (bool, error) {
			for _, assignment := range page {
				for _, privilege := range assignment.Privileges {
					if slices.Contains(wanted, privilege) {
						holds = true

						return true, nil
					}
				}
			}

			return false, nil
		})
	noteRateLimit(annos, rateLimit)
	if err != nil {
		if !deniesTrust(err) {
			return false, fmt.Errorf("databricks-connector: failed to read the connector's own privileges on %s %s: %w", securableType, fullName, err)
		}
		ctxzap.Extract(ctx).Debug("databricks-connector: the connector's own privileges on the securable cannot be read, so they stay unproven",
			zap.String("securable_type", securableType),
			zap.String("securable", fullName),
			zap.Error(err))

		return false, nil
	}

	return holds, nil
}

// assignmentListError explains a failed read of the workspaces a metastore is
// assigned to, which decides which workspaces its securables are read through.
// A 404 is not an empty assignment list: the SDK would skip the child List and
// ConductorOne would delete what it contained.
func assignmentListError(metastoreID string, err error) error {
	return refuseUnreadable(
		fmt.Sprintf("databricks-connector: failed to list the workspaces assigned to metastore %s", metastoreID), err)
}

// workspaceForCatalog returns the first in-scope workspace whose catalog listing
// contains catalogName. None listing it is an error, never an absence: a catalog
// bound to a workspace outside the sync, or one the connector lost USE_CATALOG
// on, is not listed either and still exists. Reporting it as gone would delete
// its subtree on a sync, and confirm a revoke that never happened.
func workspaceForCatalog(
	ctx context.Context,
	client *databricks.Client,
	metastoreID, catalogName string,
	usable []databricks.Workspace,
	annos *annotations.Annotations,
) (*databricks.Workspace, *databricks.Catalog, error) {
	var lastDeny error
	for i := range usable {
		catalog, err := catalogThrough(ctx, client, usable[i].DeploymentName, metastoreID, catalogName, annos)
		if err != nil {
			if deniesTrust(err) {
				lastDeny = err
				continue
			}
			return nil, nil, err
		}
		if catalog != nil {
			return &usable[i], catalog, nil
		}
	}

	msg := fmt.Sprintf("databricks-connector: no workspace in sync scope lists catalog %s of metastore %s, so whether it still exists is unknown",
		catalogName, metastoreID)
	if lastDeny != nil {
		msg = fmt.Sprintf("%s: %s", msg, lastDeny.Error())
	}
	return nil, nil, status.Errorf(codes.FailedPrecondition, "%s", msg)
}

// deniesTrust is an answer that withholds what the connector asked for, as
// opposed to one that says the account plane is unavailable.
func deniesTrust(err error) bool {
	switch apiStatus(err) {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return true
	default:
		return false
	}
}

// principalResolver maps a privilege assignment to a C1 resource id. The id is used when present; the name is only the fallback.
type principalResolver struct {
	client *databricks.Client
	annos  *annotations.Annotations
	byID   map[string]*v2.ResourceId
	byName map[string]*v2.ResourceId
}

func newPrincipalResolver(client *databricks.Client, annos *annotations.Annotations) *principalResolver {
	return &principalResolver{client: client, annos: annos, byID: map[string]*v2.ResourceId{}, byName: map[string]*v2.ResourceId{}}
}

// resolve prefers principal_id and never falls back to the name when the id is
// present: a deleted principal keeps only its id, and the name is ambiguous.
func (p *principalResolver) resolve(ctx context.Context, id, name string) (*v2.ResourceId, error) {
	if id != "" && id != "0" {
		if resourceId, ok := p.byID[id]; ok {
			return resourceId, nil
		}
		resourceId, err := p.lookupID(ctx, id)
		if err != nil {
			return nil, err
		}
		p.byID[id] = resourceId
		if _, ok := p.byName[name]; !ok && name != "" && resourceId != nil {
			p.byName[name] = resourceId
		}

		return resourceId, nil
	}
	if name == "" {
		return nil, nil
	}
	if resourceId, ok := p.byName[name]; ok {
		return resourceId, nil
	}
	resourceId, err := p.lookupName(ctx, name)
	if err != nil {
		return nil, err
	}
	p.byName[name] = resourceId

	return resourceId, nil
}

func (p *principalResolver) lookupID(ctx context.Context, id string) (*v2.ResourceId, error) {
	user, rateLimit, err := p.client.GetUser(ctx, "", id)
	noteRateLimit(p.annos, rateLimit)
	if err != nil && !isNotFoundError(err) {
		return nil, fmt.Errorf("failed to resolve principal %s as a user: %w", id, err)
	}
	if err == nil && user != nil && user.ID != "" {
		return &v2.ResourceId{ResourceType: userResourceType.Id, Resource: user.ID}, nil
	}

	// A filtered list answers this without pulling the group's members.
	displayName, rateLimit, err := p.client.FindGroupDisplayName(ctx, "", id)
	noteRateLimit(p.annos, rateLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve principal %s as a group: %w", id, err)
	}
	if displayName != "" {
		return p.groupID(ctx, id)
	}

	servicePrincipal, rateLimit, err := p.client.GetServicePrincipal(ctx, "", id)
	noteRateLimit(p.annos, rateLimit)
	if err != nil && !isNotFoundError(err) {
		return nil, fmt.Errorf("failed to resolve principal %s as a service principal: %w", id, err)
	}
	if err == nil && servicePrincipal != nil && servicePrincipal.ID != "" {
		return &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: servicePrincipal.ID}, nil
	}

	return nil, nil
}

func (p *principalResolver) lookupName(ctx context.Context, name string) (*v2.ResourceId, error) {
	userID, rateLimit, err := p.client.FindUserID(ctx, "", name)
	noteRateLimit(p.annos, rateLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %q as a user: %w", name, err)
	}
	if userID != "" {
		return &v2.ResourceId{ResourceType: userResourceType.Id, Resource: userID}, nil
	}

	groupID, rateLimit, err := p.client.FindGroupID(ctx, "", name)
	noteRateLimit(p.annos, rateLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %q as a group: %w", name, err)
	}
	if groupID != "" {
		return p.groupID(ctx, groupID)
	}

	servicePrincipalID, rateLimit, err := p.client.FindServicePrincipalID(ctx, "", name)
	noteRateLimit(p.annos, rateLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %q as a service principal: %w", name, err)
	}
	if servicePrincipalID != "" {
		return &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: servicePrincipalID}, nil
	}

	return nil, nil
}

// groupID matches the id the group syncer emits for an account-level group.
func (p *principalResolver) groupID(ctx context.Context, groupID string) (*v2.ResourceId, error) {
	account, err := rs.NewResourceID(accountResourceType, p.client.GetAccountId())
	if err != nil {
		return nil, err
	}

	return rs.NewResourceID(groupResourceType, groupResourceId(ctx, groupID, account))
}
