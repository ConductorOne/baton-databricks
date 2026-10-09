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

// wrapTransportAuthError maps an OAuth2 token-retrieval failure to a gRPC
// status. It happens in the oauth2 transport before any API response, so uhttp
// never sees an HTTP status and the error would otherwise surface as Unknown.
// oauth2 returns RetrieveError for any non-2xx from the token endpoint, so a
// transient 5xx must stay retryable (Unavailable) rather than look like bad
// credentials. A missing Response falls back to Unauthenticated.
func wrapTransportAuthError(err error) error {
	var retrieveErr *oauth2.RetrieveError
	if !errors.As(err, &retrieveErr) {
		return err
	}

	code := codes.Unauthenticated
	if retrieveErr.Response != nil {
		switch status := retrieveErr.Response.StatusCode; {
		case status == http.StatusForbidden:
			code = codes.PermissionDenied
		case status == http.StatusTooManyRequests:
			code = codes.Unavailable
		case status >= 500:
			code = codes.Unavailable
		}
	}

	return uhttp.WrapErrors(code, "databricks-connector: authentication failed", err)
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
			"workspace %s is inaccessible (403); remove it from --workspaces, or scope it out with --databricks-exclude-workspaces (BATON_DATABRICKS_EXCLUDE_WORKSPACES): %w",
			workspaceId, err,
		)
	}
	return err
}

// invalidArgument marks a malformed request so it does not reach C1 as Unknown and get retried.
func invalidArgument(format string, args ...any) error {
	return uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf(format, args...))
}

func (c *Client) Get(
	ctx context.Context,
	urlAddress *url.URL,
	response any,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		response,
		nil,
		params...,
	)
}

// GetUncached is Get with the uhttp response cache bypassed on the read side.
// The cache holds a GET 200 for an hour and a PATCH does not invalidate it, so a
// read that decides whether a write still has to happen must not be served from
// it: the write would be skipped against a page that predates the last change.
// uhttp still records the fresh response, so the entry a later reader sees is the
// one this call just observed.
func (c *Client) GetUncached(
	ctx context.Context,
	urlAddress *url.URL,
	response any,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		response,
		[]uhttp.RequestOption{uhttp.WithNoCache()},
		params...,
	)
}

func (c *Client) Put(
	ctx context.Context,
	urlAddress *url.URL,
	body any,
	response any,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodPut,
		body,
		response,
		nil,
		params...,
	)
}

func (c *Client) Post(
	ctx context.Context,
	urlAddress *url.URL,
	body any,
	response any,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodPost,
		body,
		response,
		nil,
		params...,
	)
}

func (c *Client) Patch(
	ctx context.Context,
	urlAddress *url.URL,
	body any,
	response any,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodPatch,
		body,
		response,
		nil,
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

func parseJSON(body io.Reader, res any) error {
	// Databricks seems to return content-type text/plain even though it's json,
	// so don't check content type.
	if err := json.NewDecoder(body).Decode(res); err != nil {
		return fmt.Errorf("failed to decode response body: %w", err)
	}

	return nil
}

// cacheScopeHeader pins a cached response to the host it was read from: uhttp
// keys its cache on path, query and headers with no host component, so two
// workspace deployments answering the same path share one entry. net/http builds
// the Host line from the URL and never sends this header on the wire.
const cacheScopeHeader = "Host"

// prepareRequest builds every request this client sends, so none can reach the
// cache without the scope header. uhttp writes an entry even for a caller that
// asked not to read one.
func (c *Client) prepareRequest(
	ctx context.Context,
	urlAddress *url.URL,
	method string,
	body any,
	requestOptions []uhttp.RequestOption,
	params ...Vars,
) (*http.Request, error) {
	// TODO(marcos): Refactor URLs so that we don't have to unescape.
	unescaped, err := url.PathUnescape(urlAddress.String())
	if err != nil {
		return nil, err
	}

	requestURL, err := url.Parse(unescaped)
	if err != nil {
		return nil, err
	}

	options := []uhttp.RequestOption{
		uhttp.WithAcceptJSONHeader(),
	}
	if body != nil {
		options = append(options, uhttp.WithJSONBody(body))
	}
	options = append(options, requestOptions...)

	req, err := c.httpClient.NewRequest(ctx, method, requestURL, options...)
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

	req.Header.Set(cacheScopeHeader, req.URL.Host)

	c.auth.Apply(req)

	return req, nil
}

func (c *Client) doRequest(
	ctx context.Context,
	urlAddress *url.URL,
	method string,
	body any,
	response any,
	requestOptions []uhttp.RequestOption,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	req, err := c.prepareRequest(ctx, urlAddress, method, body, requestOptions, params...)
	if err != nil {
		return nil, err
	}

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
	body any,
	response any,
	params ...Vars,
) (*v2.RateLimitDescription, error) {
	req, err := c.prepareRequest(ctx, urlAddress, method, body, nil, params...)
	if err != nil {
		return nil, err
	}

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
