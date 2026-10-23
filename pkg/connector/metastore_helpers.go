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

	// Statuses the account API documents for a workspace. PROVISIONING,
	// NOT_PROVISIONED and CANCELLING can still change; FAILED and BANNED cannot.
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

// scopedWorkspaces indexes, by numeric id, the workspaces this sync may read
// through, and marks which of them are RUNNING. ListWorkspaces already drops the
// excluded ones.
func (m *metastoreBuilder) scopedWorkspaces(ctx context.Context, annos *annotations.Annotations) (map[string]databricks.Workspace, map[string]bool, error) {
	workspaces, rateLimit, err := m.client.ListWorkspaces(ctx)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("databricks-connector: failed to list workspaces: %w", err)
	}

	scoped := make(map[string]databricks.Workspace, len(workspaces))
	running := make(map[string]bool, len(workspaces))
	for _, workspace := range workspaces {
		id := strconv.Itoa(workspace.ID)
		if workspace.DeploymentName == "" {
			continue
		}
		if len(m.workspaces) > 0 {
			if _, ok := matchConfiguredWorkspace(m.workspaces, workspace.DeploymentName, workspace.Name, id); !ok {
				continue
			}
		}
		// A workspace that will not become RUNNING is left out of scope entirely,
		// so a metastore attached only to one is skipped rather than failing every
		// sync. Only a status that can still change keeps the metastore waiting.
		if !workspaceIsUsable(workspace.Status) {
			continue
		}
		scoped[id] = workspace
		running[id] = workspace.Status == "" || strings.EqualFold(workspace.Status, workspaceStatusRunning)
	}

	return scoped, running, nil
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

// routeMetastore picks the first running in-scope workspace attached to the
// metastore. inScope reports whether any in-scope workspace is attached at all.
func (m *metastoreBuilder) routeMetastore(
	ctx context.Context,
	metastoreID string,
	scoped map[string]databricks.Workspace,
	running map[string]bool,
	annos *annotations.Annotations,
) (*databricks.Workspace, bool, error) {
	ids, rateLimit, err := m.client.ListMetastoreWorkspaces(ctx, metastoreID)
	noteRateLimit(annos, rateLimit)
	if err != nil {
		return nil, false, err
	}

	inScope := false
	for _, id := range ids {
		workspace, ok := scoped[id]
		if !ok {
			continue
		}
		if running[id] {
			return &workspace, true, nil
		}
		inScope = true
	}

	return nil, inScope, nil
}

// metastoreGrantsAreTrustworthy is true when the connector owns the metastore or is a direct Admin of the routed workspace. MANAGE is not a metastore privilege.
func metastoreGrantsAreTrustworthy(
	ctx context.Context,
	client *databricks.Client,
	ownPrincipal, metastoreOwner, workspaceID string,
	annos *annotations.Annotations,
) (bool, error) {
	// An owner holds every privilege implicitly, which is not reported as a grant.
	if ownsPrincipal(ownPrincipal, metastoreOwner) {
		return true, nil
	}

	return isWorkspaceAdmin(ctx, client, ownPrincipal, workspaceID, annos)
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

// ensureGrantsAreTrustworthy refuses the call unless the privilege list is
// complete. Databricks returns HTTP 200 with only the rows the caller may see,
// and reporting that subset would make ConductorOne revoke the rest.
// The check is not cached.
func (m *metastoreBuilder) ensureGrantsAreTrustworthy(
	ctx context.Context,
	resource *v2.Resource,
	metastoreID, workspace string,
	annos *annotations.Annotations,
) error {
	profile := rs.GetProfile(resource)
	owner, _ := rs.GetProfileStringValue(profile, profileKeyOwner)
	workspaceID, _ := rs.GetProfileStringValue(profile, profileKeyWorkspaceID)

	trustworthy, err := metastoreGrantsAreTrustworthy(ctx, m.client, m.clientID, owner, workspaceID, annos)
	if err != nil {
		return err
	}
	if !trustworthy {
		return untrustedMetastoreError(metastoreID, workspace)
	}

	return nil
}

// untrustedMetastoreError names the metastore, the workspace the read goes
// through and what an operator has to change.
func untrustedMetastoreError(metastoreID, workspace string) error {
	return status.Errorf(codes.PermissionDenied,
		"databricks-connector: cannot read every privilege on metastore %s through workspace %s, and Databricks answers a partial read with 200, "+
			"so reporting it would revoke the access ConductorOne cannot see; "+
			"the connector's service principal must own the metastore or be assigned Admin directly on the workspace",
		metastoreID, workspace)
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
