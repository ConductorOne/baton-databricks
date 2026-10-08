package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// principalIndex resolves the principal a privilege assignment names. The payload
// carries no type discriminator and a name-built SCIM filter cannot settle it:
// Databricks answers an unmatched filter with 200 and zero results. Built from
// account SCIM, never workspace SCIM.
type principalIndex struct {
	byID   map[string]*v2.ResourceId
	byName map[string]*v2.ResourceId
}

// lookup prefers the numeric id and never falls back to the name when the id is
// present but unknown: userName, displayName and applicationId share that field.
// A deleted principal arrives with an empty name and only the id.
func (p *principalIndex) lookup(assignment databricks.PrivilegeAssignment) (*v2.ResourceId, bool) {
	if id := assignment.PrincipalID.String(); id != "" && id != "0" {
		resourceId, ok := p.byID[id]
		return resourceId, ok
	}

	return p.lookupName(assignment.Principal)
}

func (p *principalIndex) lookupName(name string) (*v2.ResourceId, bool) {
	if name == "" {
		return nil, false
	}

	resourceId, ok := p.byName[name]

	return resourceId, ok
}

// add records a principal; the first writer of a key wins. A name collision is expected
// because the three namespaces share one field. An id collision is not: SCIM ids are
// unique across them, so one means a grant will resolve to the wrong principal.
func (p *principalIndex) add(ctx context.Context, id string, names []string, resourceId *v2.ResourceId) {
	if id != "" {
		if existing, ok := p.byID[id]; ok {
			if existing.GetResourceType() != resourceId.GetResourceType() || existing.GetResource() != resourceId.GetResource() {
				ctxzap.Extract(ctx).Debug("databricks-connector: two principals share one SCIM id; keeping the first",
					zap.String("principal_id", id),
					zap.String("kept", fmt.Sprintf("%s:%s", existing.GetResourceType(), existing.GetResource())),
					zap.String("dropped", fmt.Sprintf("%s:%s", resourceId.GetResourceType(), resourceId.GetResource())),
				)
			}
		} else {
			p.byID[id] = resourceId
		}
	}

	for _, name := range names {
		if name == "" {
			continue
		}
		if _, ok := p.byName[name]; !ok {
			p.byName[name] = resourceId
		}
	}
}

func newPrincipalIndex() *principalIndex {
	return &principalIndex{
		byID:   make(map[string]*v2.ResourceId),
		byName: make(map[string]*v2.ResourceId),
	}
}

func (u *unityCatalog) ownsPrincipal(name string) bool {
	return u.ownPrincipal != "" && strings.EqualFold(name, u.ownPrincipal)
}

func (u *unityCatalog) listPrincipals(ctx context.Context) (*principalIndex, *v2.RateLimitDescription, error) {
	accountResourceId, err := rs.NewResourceID(accountResourceType, u.client.GetAccountId())
	if err != nil {
		return nil, nil, err
	}

	index := newPrincipalIndex()

	rateLimit, err := u.indexUsers(ctx, index)
	if err != nil {
		return nil, rateLimit, err
	}
	groupRateLimit, err := u.indexGroups(ctx, index, accountResourceId)
	if groupRateLimit != nil {
		rateLimit = groupRateLimit
	}
	if err != nil {
		return nil, rateLimit, err
	}
	servicePrincipalRateLimit, err := u.indexServicePrincipals(ctx, index)
	if servicePrincipalRateLimit != nil {
		rateLimit = servicePrincipalRateLimit
	}
	if err != nil {
		return nil, rateLimit, err
	}

	return index, rateLimit, nil
}

func (u *unityCatalog) indexUsers(ctx context.Context, index *principalIndex) (*v2.RateLimitDescription, error) {
	var rateLimit *v2.RateLimitDescription

	for page := uint(1); ; {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		users, total, pageRateLimit, err := u.client.ListUsers(ctx, "", databricks.NewPaginationVars(page, ResourcesPageSize), databricks.NewUserAttrVars())
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return rateLimit, fmt.Errorf("failed to list account users: %w", err)
		}
		if len(users) == 0 {
			return rateLimit, nil
		}

		for _, user := range users {
			index.add(ctx, user.ID, []string{user.UserName}, &v2.ResourceId{ResourceType: userResourceType.Id, Resource: user.ID})
		}

		next, err := nextScimPage(page, len(users), total)
		if err != nil {
			return rateLimit, err
		}
		if next == 0 {
			return rateLimit, nil
		}
		page = next
	}
}

func (u *unityCatalog) indexGroups(ctx context.Context, index *principalIndex, parent *v2.ResourceId) (*v2.RateLimitDescription, error) {
	var rateLimit *v2.RateLimitDescription

	for page := uint(1); ; {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		groups, total, pageRateLimit, err := u.client.ListGroups(ctx, "", databricks.NewPaginationVars(page, ResourcesPageSize), databricks.NewGroupAttrVars())
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return rateLimit, fmt.Errorf("failed to list account groups: %w", err)
		}
		if len(groups) == 0 {
			return rateLimit, nil
		}

		for _, group := range groups {
			// Groups sync under the account with a composite resource id; the index must
			// hold the same form or a grant points at a resource that was never synced.
			resourceId, err := rs.NewResourceID(groupResourceType, groupResourceId(ctx, group.ID, parent))
			if err != nil {
				return rateLimit, err
			}
			index.add(ctx, group.ID, []string{group.DisplayName}, resourceId)
		}

		next, err := nextScimPage(page, len(groups), total)
		if err != nil {
			return rateLimit, err
		}
		if next == 0 {
			return rateLimit, nil
		}
		page = next
	}
}

func (u *unityCatalog) indexServicePrincipals(ctx context.Context, index *principalIndex) (*v2.RateLimitDescription, error) {
	var rateLimit *v2.RateLimitDescription

	for page := uint(1); ; {
		if err := ctx.Err(); err != nil {
			return rateLimit, err
		}

		servicePrincipals, total, pageRateLimit, err := u.client.ListServicePrincipals(ctx, "", databricks.NewPaginationVars(page, ResourcesPageSize), databricks.NewServicePrincipalAttrVars())
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return rateLimit, fmt.Errorf("failed to list account service principals: %w", err)
		}
		if len(servicePrincipals) == 0 {
			return rateLimit, nil
		}

		for _, servicePrincipal := range servicePrincipals {
			// A privilege assignment names a service principal by its applicationId,
			// not its displayName and not its SCIM id.
			index.add(
				ctx,
				servicePrincipal.ID,
				[]string{servicePrincipal.ApplicationID},
				&v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: servicePrincipal.ID},
			)
		}

		next, err := nextScimPage(page, len(servicePrincipals), total)
		if err != nil {
			return rateLimit, err
		}
		if next == 0 {
			return rateLimit, nil
		}
		page = next
	}
}

// ownPrincipalScimID lists account SCIM for this call only. A name-built filter
// cannot tell a service principal from a user or a group that shares the field.
func (u *unityCatalog) ownPrincipalScimID(ctx context.Context) (string, *v2.RateLimitDescription, error) {
	if u.ownPrincipal == "" {
		return "", nil, nil
	}

	index, rateLimit, err := u.listPrincipals(ctx)
	if err != nil {
		return "", rateLimit, err
	}

	resourceId, ok := index.lookupName(u.ownPrincipal)
	if !ok || resourceId.GetResourceType() != servicePrincipalResourceType.Id {
		// The three namespaces share one name field, so a hit elsewhere is somebody else's.
		return "", rateLimit, nil
	}

	return resourceId.GetResource(), rateLimit, nil
}

// nextScimPage returns zero once the listing is exhausted.
func nextScimPage(page uint, pageTotal int, total uint) (uint, error) {
	token := prepareNextToken(page, pageTotal, total)
	if token == "" {
		return 0, nil
	}

	next, err := convertPageToken(token)
	if err != nil {
		return 0, err
	}

	return next, nil
}
