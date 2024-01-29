package databricks

import (
	"fmt"
	"net/url"
)

type Vars interface {
	Apply(params *url.Values)
}

// Pagination vars are used for paginating results from the API.
type PaginationVars struct {
	Start uint `json:"startIndex"`
	Count uint `json:"count"`
}

func (p *PaginationVars) Apply(params *url.Values) {
	if p.Start > 0 {
		params.Add("startIndex", fmt.Sprintf("%d", p.Start))
	}

	if p.Count > 0 {
		params.Add("count", fmt.Sprintf("%d", p.Count))
	}
}

func NewPaginationVars(start uint, count uint) *PaginationVars {
	return &PaginationVars{
		Start: start,
		Count: count,
	}
}

// Filter vars are used for filtering results from the API.
type FilterVars struct {
	Filter string `json:"filter"`
}

func (f *FilterVars) Apply(params *url.Values) {
	if f.Filter != "" {
		params.Add("filter", f.Filter)
	}
}

func NewFilterVars(filter string) *FilterVars {
	return &FilterVars{
		Filter: filter,
	}
}

// Attribute vars are used to specify which attributes to return from the API.
type AttrVars struct {
	Attrs []string `json:"attributes"`
}

func (a *AttrVars) Apply(params *url.Values) {
	if len(a.Attrs) > 0 {
		for _, attr := range a.Attrs {
			params.Add("attributes", attr)
		}
	}
}

func NewUserAttrVars() *AttrVars {
	return &AttrVars{
		Attrs: []string{
			"id",
			"emails",
			"userName",
			"displayName",
			"active",
		},
	}
}

func NewUserRolesAttrVars() *AttrVars {
	return &AttrVars{
		Attrs: []string{
			"roles",
			"entitlements",
		},
	}
}

func NewGroupAttrVars() *AttrVars {
	return &AttrVars{
		Attrs: []string{
			"id",
			"displayName",
			"members",
		},
	}
}

func NewGroupRolesAttrVars() *AttrVars {
	return &AttrVars{
		Attrs: []string{
			"roles",
			"entitlements",
			"meta",
		},
	}
}

func NewServicePrincipalAttrVars() *AttrVars {
	return &AttrVars{
		Attrs: []string{
			"id",
			"displayName",
			"active",
			"applicationId",
		},
	}
}

func NewServicePrincipalRolesAttrVars() *AttrVars {
	return &AttrVars{
		Attrs: []string{
			"roles",
			"entitlements",
		},
	}
}

type ResourceVars struct {
	Payload string `json:"resource"`
}

func (r *ResourceVars) Apply(params *url.Values) {
	if r.Payload != "" {
		params.Add("resource", r.Payload)
	}
}

func NewResourceVars(resource string) *ResourceVars {
	return &ResourceVars{
		Payload: resource,
	}
}

type NameVars struct {
	Etag    string `json:"etag"`
	Payload string `json:"name"`
}

func (n *NameVars) Apply(params *url.Values) {
	params.Set("etag", n.Etag)

	if n.Payload != "" {
		params.Add("name", n.Payload)
	}
}

func NewNameVars(name string, etag string) *NameVars {
	return &NameVars{
		Payload: name,
		Etag:    etag,
	}
}
