package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

func (c *Client) Get(
	ctx context.Context,
	urlAddress *url.URL,
	response interface{},
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		response,
		params...,
	)
}

func (c *Client) Put(
	ctx context.Context,
	urlAddress *url.URL,
	body interface{},
	response interface{},
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodPut,
		body,
		response,
		params...,
	)
}

func parseJSON(body io.Reader, res interface{}) error {
	// Databricks seems to return content-type text/plain even though it's json,
	// so don't check content type.
	if err := json.NewDecoder(body).Decode(res); err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}

	return nil
}

func (c *Client) doRequest(
	ctx context.Context,
	urlAddress *url.URL,
	method string,
	body interface{},
	response interface{},
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	// TODO(marcos): Refactor URLs so that we don't have to unescape.
	u, err := url.PathUnescape(urlAddress.String())
	if err != nil {
		return nil, err
	}

	uri, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	options := []uhttp.RequestOption{
		uhttp.WithAcceptJSONHeader(),
	}
	if body != nil {
		options = append(options, uhttp.WithJSONBody(body))
	}

	req, err := c.httpClient.NewRequest(ctx, method, uri, options...)
	if err != nil {
		return nil, err
	}

	if len(params) > 0 {
		query := url.Values{}
		for _, param := range params {
			param.Apply(&query)
		}

		req.URL.RawQuery = query.Encode()
	}

	c.auth.Apply(req)

	ratelimitData := &v2.RateLimitDescription{}
	resp, err := c.httpClient.Do(
		req,
		uhttp.WithAlwaysJSONResponse(&response),
		uhttp.WithRatelimitData(ratelimitData),
	)
	if resp == nil {
		return ratelimitData, err
	}

	defer resp.Body.Close()

	if err == nil {
		return ratelimitData, nil
	}

	var errorResponse struct {
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if err := parseJSON(resp.Body, &errorResponse); err != nil {
		return nil, err
	}

	return ratelimitData, fmt.Errorf(
		"unexpected status code %d: %s %s %w",
		resp.StatusCode,
		errorResponse.Detail,
		errorResponse.Message,
		err,
	)
}
