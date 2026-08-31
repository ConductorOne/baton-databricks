package databricks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
)

const (
	AlreadyExists = "AlreadyExists"
)

// wrapTransportAuthError maps an OAuth2 token-retrieval failure (bad client
// id/secret) to a gRPC Unauthenticated status. That failure happens in the
// oauth2 transport before any API response, so uhttp never sees an HTTP status
// to map, and the error would otherwise surface as codes.Unknown.
func wrapTransportAuthError(err error) error {
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		return uhttp.WrapErrors(codes.Unauthenticated, "databricks-connector: authentication failed", err)
	}
	return err
}

// APIError represents an error response from the Databricks API.
type APIError struct {
	StatusCode int
	Detail     string
	Message    string
	Err        error
}

func (e *APIError) Error() string {
	return fmt.Sprintf(
		"unexpected status code %d: %s %s %v",
		e.StatusCode,
		e.Detail,
		e.Message,
		e.Err,
	)
}

func (e *APIError) Unwrap() error {
	return e.Err
}

// nameWorkspace403Remedy enriches a 403 from a workspace-scoped call with the
// fix: excluding the workspace scopes it out of the sync. workspaceId is empty
// for account-scoped calls, where a 403 is not a per-workspace access problem,
// so those pass through untouched.
func nameWorkspace403Remedy(workspaceId string, err error) error {
	if workspaceId == "" || err == nil {
		return err
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusForbidden {
		return fmt.Errorf(
			"workspace %s is inaccessible (403); remove it from --databricks-workspaces, or scope it out with --databricks-exclude-workspaces (BATON_DATABRICKS_EXCLUDE_WORKSPACES): %w",
			workspaceId, err,
		)
	}
	return err
}

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

func (c *Client) Post(
	ctx context.Context,
	urlAddress *url.URL,
	body interface{},
	response interface{},
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodPost,
		body,
		response,
		params...,
	)
}

func (c *Client) Delete(
	ctx context.Context,
	urlAddress *url.URL,
) (*v2.RateLimitDescription, error) {
	response := struct{}{}
	return c.doRequestNoResponse(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		response,
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
		return ratelimitData, wrapTransportAuthError(err)
	}

	defer resp.Body.Close()

	if err == nil {
		l := ctxzap.Extract(ctx)
		l.Debug("do request response", zap.Any("response", response))
		return ratelimitData, nil
	}

	var errorResponse struct {
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if err := parseJSON(resp.Body, &errorResponse); err != nil {
		return nil, err
	}

	return ratelimitData, &APIError{
		StatusCode: resp.StatusCode,
		Detail:     errorResponse.Detail,
		Message:    errorResponse.Message,
		Err:        err,
	}
}

func (c *Client) doRequestNoResponse(
	ctx context.Context,
	urlAddress *url.URL,
	method string,
	body interface{},
	response interface{},
	params ...Vars,
) (*v2.RateLimitDescription, error) {
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
		uhttp.WithRatelimitData(ratelimitData),
	)
	if resp == nil {
		return ratelimitData, wrapTransportAuthError(err)
	}

	defer resp.Body.Close()

	if err == nil {
		l := ctxzap.Extract(ctx)
		l.Debug("do request response", zap.Any("response", response))
		return ratelimitData, nil
	}

	return ratelimitData, &APIError{
		StatusCode: resp.StatusCode,
		Err:        err,
	}
}
