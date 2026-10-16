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

// principalsForGrants builds the index Grants uses for assignments and owner.
// Each distinct principal is resolved on demand: Get* by principal_id, then
// Find* by name.
func (u *unityCatalog) principalsForGrants(
	ctx context.Context,
	assignments []databricks.PrivilegeAssignment,
	owner string,
) (*principalIndex, *v2.RateLimitDescription, error) {
	if len(assignments) == 0 {
		if owner == "" {
			return newPrincipalIndex(), nil, nil
		}

		return u.indexOwner(ctx, owner)
	}

	return u.indexAssignments(ctx, assignments, owner)
}

// indexOwner asks each principal namespace for that one name. The first hit
// wins, in the same order lookupPrincipalByID tries (users, groups,
// service principals).
func (u *unityCatalog) indexOwner(ctx context.Context, owner string) (*principalIndex, *v2.RateLimitDescription, error) {
	index := newPrincipalIndex()

	userID, rateLimit, err := u.client.FindUserID(ctx, "", owner)
	if err != nil {
		return nil, rateLimit, fmt.Errorf("failed to resolve owner %q as a user: %w", owner, err)
	}
	if userID != "" {
		index.add(ctx, userID, []string{owner}, &v2.ResourceId{ResourceType: userResourceType.Id, Resource: userID})

		return index, rateLimit, nil
	}

	groupID, groupRateLimit, err := u.client.FindGroupID(ctx, "", owner)
	if groupRateLimit != nil {
		rateLimit = groupRateLimit
	}
	if err != nil {
		return nil, rateLimit, fmt.Errorf("failed to resolve owner %q as a group: %w", owner, err)
	}
	if groupID != "" {
		accountResourceId, err := rs.NewResourceID(accountResourceType, u.client.GetAccountId())
		if err != nil {
			return nil, rateLimit, err
		}
		resourceId, err := rs.NewResourceID(groupResourceType, groupResourceId(ctx, groupID, accountResourceId))
		if err != nil {
			return nil, rateLimit, err
		}
		index.add(ctx, groupID, []string{owner}, resourceId)

		return index, rateLimit, nil
	}

	servicePrincipalID, servicePrincipalRateLimit, err := u.client.FindServicePrincipalID(ctx, "", owner)
	if servicePrincipalRateLimit != nil {
		rateLimit = servicePrincipalRateLimit
	}
	if err != nil {
		return nil, rateLimit, fmt.Errorf("failed to resolve owner %q as a service principal: %w", owner, err)
	}
	if servicePrincipalID != "" {
		index.add(ctx, servicePrincipalID, []string{owner}, &v2.ResourceId{
			ResourceType: servicePrincipalResourceType.Id,
			Resource:     servicePrincipalID,
		})

		return index, rateLimit, nil
	}

	return index, rateLimit, nil
}

func (u *unityCatalog) indexAssignments(
	ctx context.Context,
	assignments []databricks.PrivilegeAssignment,
	owner string,
) (*principalIndex, *v2.RateLimitDescription, error) {
	index := newPrincipalIndex()
	var rateLimit *v2.RateLimitDescription
	seenID := map[string]struct{}{}
	seenName := map[string]struct{}{}

	resolve := func(assignment databricks.PrivilegeAssignment) error {
		id := assignment.PrincipalID.String()
		if id != "" && id != "0" {
			if _, ok := seenID[id]; ok {
				return nil
			}
			seenID[id] = struct{}{}
			resourceId, pageRateLimit, err := u.lookupPrincipalByID(ctx, id)
			if pageRateLimit != nil {
				rateLimit = pageRateLimit
			}
			if err != nil {
				return err
			}
			if resourceId != nil {
				index.add(ctx, id, []string{assignment.Principal}, resourceId)
			}

			return nil
		}

		name := assignment.Principal
		if name == "" {
			return nil
		}
		if _, ok := seenName[name]; ok {
			return nil
		}
		seenName[name] = struct{}{}
		partial, pageRateLimit, err := u.indexOwner(ctx, name)
		if pageRateLimit != nil {
			rateLimit = pageRateLimit
		}
		if err != nil {
			return err
		}
		if resourceId, ok := partial.lookupName(name); ok {
			index.add(ctx, "", []string{name}, resourceId)
		}

		return nil
	}

	for _, assignment := range assignments {
		if err := resolve(assignment); err != nil {
			return nil, rateLimit, err
		}
	}
	if owner != "" {
		if err := resolve(databricks.PrivilegeAssignment{Principal: owner}); err != nil {
			return nil, rateLimit, err
		}
	}

	return index, rateLimit, nil
}

func (u *unityCatalog) lookupPrincipalByID(
	ctx context.Context,
	id string,
) (*v2.ResourceId, *v2.RateLimitDescription, error) {
	user, rateLimit, err := u.client.GetUser(ctx, "", id)
	if err != nil && !isNotFoundError(err) {
		return nil, rateLimit, fmt.Errorf("failed to resolve principal %s as a user: %w", id, err)
	}
	if err == nil && user != nil && user.ID != "" {
		return &v2.ResourceId{ResourceType: userResourceType.Id, Resource: user.ID}, rateLimit, nil
	}

	// Account GET /Groups/{id} returns members. A filtered list with count 1
	// answers the same question without pulling that list into the cache.
	displayName, groupRateLimit, err := u.client.FindGroupDisplayName(ctx, "", id)
	if groupRateLimit != nil {
		rateLimit = groupRateLimit
	}
	if err != nil {
		return nil, rateLimit, fmt.Errorf("failed to resolve principal %s as a group: %w", id, err)
	}
	if displayName != "" {
		accountResourceId, err := rs.NewResourceID(accountResourceType, u.client.GetAccountId())
		if err != nil {
			return nil, rateLimit, err
		}
		resourceId, err := rs.NewResourceID(groupResourceType, groupResourceId(ctx, id, accountResourceId))
		if err != nil {
			return nil, rateLimit, err
		}

		return resourceId, rateLimit, nil
	}

	servicePrincipal, servicePrincipalRateLimit, err := u.client.GetServicePrincipal(ctx, "", id)
	if servicePrincipalRateLimit != nil {
		rateLimit = servicePrincipalRateLimit
	}
	if err != nil && !isNotFoundError(err) {
		return nil, rateLimit, fmt.Errorf("failed to resolve principal %s as a service principal: %w", id, err)
	}
	if err == nil && servicePrincipal != nil && servicePrincipal.ID != "" {
		return &v2.ResourceId{ResourceType: servicePrincipalResourceType.Id, Resource: servicePrincipal.ID}, rateLimit, nil
	}

	return nil, rateLimit, nil
}

// ownPrincipalScimID resolves the connector's own service principal. The index
// cannot answer this: it is keyed by a name field the three principal namespaces
// share, so a hit there may be somebody else's. An applicationId filter on the
// service principal endpoint has no such ambiguity — it can only match a service
// principal — which is why this asks for the one record instead of walking all
// of account SCIM to find it.
func (u *unityCatalog) ownPrincipalScimID(ctx context.Context) (string, *v2.RateLimitDescription, error) {
	if u.ownPrincipal == "" {
		return "", nil, nil
	}

	id, rateLimit, err := u.client.FindServicePrincipalID(ctx, "", u.ownPrincipal)
	if err != nil {
		return "", rateLimit, fmt.Errorf("failed to resolve the connector's own service principal: %w", err)
	}

	return id, rateLimit, nil
}
