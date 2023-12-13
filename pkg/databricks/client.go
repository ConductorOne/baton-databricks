package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

const (
	Base        = "cloud.databricks.com"
	APIEndpoint = "/api/2.0"

	AccountBaseHost   = "accounts." + Base + APIEndpoint
	WorkspaceBaseHost = "%s." + Base + APIEndpoint

	AccountEndpoint   = "/accounts/%s/scim/v2"
	WorkspaceEndpoint = "/preview/scim/v2"

	UsersEndpoint = "/Users"
)

type Client struct {
	httpClient *http.Client
	baseUrl    *url.URL
	cfg        Config
}

func NewClient(httpClient *http.Client, cfg Config) *Client {
	baseUrl := cfg.BaseUrl()

	return &Client{
		httpClient,
		baseUrl,
		cfg,
	}
}

func (c *Client) UpdateConfig(cfg Config) {
	baseUrl := cfg.BaseUrl()
	c.baseUrl = baseUrl
	c.cfg = cfg
}

type ListResponse[T any] struct {
	Resources []T `json:"Resources"`
	Total     int `json:"totalResults"`
	PageTotal int `json:"itemsPerPage"`
}

type PaginationVars struct {
	Start int `json:"startIndex"`
	Count int `json:"count"`
}

func (c *Client) composeURL(endpoint, query string) *url.URL {
	path := c.baseUrl.Path + endpoint

	u := &url.URL{
		Scheme:   "https",
		Host:     c.baseUrl.Host,
		Path:     path,
		RawQuery: query,
	}

	return u
}

func (c *Client) ListUsers(ctx context.Context, _ *PaginationVars) ([]User, error) {
	var res ListResponse[User]
	u := c.composeURL(
		UsersEndpoint,
		"attributes=id,emails,userName,displayName,active",
	)

	err := c.Get(ctx, u, &res)

	if err != nil {
		return nil, err
	}

	return res.Resources, nil
}

func (c *Client) Get(ctx context.Context, urlAddress *url.URL, response interface{}) error {
	return c.doRequest(ctx, urlAddress, http.MethodGet, nil, response)
}

func (c *Client) doRequest(ctx context.Context, urlAddress *url.URL, method string, body io.Reader, response interface{}) error {
	req, err := http.NewRequestWithContext(ctx, method, urlAddress.String(), body)
	if err != nil {
		return err
	}

	fmt.Printf("## HTTP %s - %s\tbody: %s\n", method, urlAddress.String(), body)
	c.cfg.ApplyAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return err
	}

	return nil
}
