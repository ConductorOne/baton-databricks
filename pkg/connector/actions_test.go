package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type recordedAction struct {
	method string
	path   string
	body   map[string]any
}

// patchTransport answers account user PATCHes with a fixed status and records
// every request, so tests assert on what went over the wire. An empty respBody is
// answered with `{}`, except on 204 (and when emptyBody is set), which carry zero bytes.
type patchTransport struct {
	mu        sync.Mutex
	status    int
	respBody  string
	emptyBody bool
	requests  []recordedAction
}

func (p *patchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body map[string]any
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
	}
	p.mu.Lock()
	p.requests = append(p.requests, recordedAction{method: req.Method, path: req.URL.Path, body: body})
	p.mu.Unlock()

	respBody := p.respBody
	if respBody == "" && !p.emptyBody && p.status != http.StatusNoContent {
		respBody = `{}`
	}
	return &http.Response{
		StatusCode: p.status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(respBody)),
		Request:    req,
	}, nil
}

func newActionsConnector(t *testing.T, tr *patchTransport) *Databricks {
	t.Helper()
	client, err := databricks.NewClient(
		context.Background(), &http.Client{Transport: tr},
		"cloud.databricks.com", "accounts.cloud.databricks.com",
		"acct-1", "", &databricks.NoAuth{}, nil,
	)
	require.NoError(t, err)
	return &Databricks{client: client}
}

func userArgs(t *testing.T, id string) *structpb.Struct {
	t.Helper()
	args, err := structpb.NewStruct(map[string]any{"user_id": id})
	require.NoError(t, err)
	return args
}

func TestSetUserActivePatchesOnlyActive(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(*Databricks) func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error)
		active  bool
	}{
		{"disable", func(d *Databricks) func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
			return d.disableUserHandler
		}, false},
		{"enable", func(d *Databricks) func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
			return d.enableUserHandler
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &patchTransport{status: http.StatusOK}
			d := newActionsConnector(t, tr)

			res, _, err := tc.handler(d)(context.Background(), userArgs(t, "76832170785741"))
			require.NoError(t, err)
			require.True(t, res.GetFields()["success"].GetBoolValue())

			// Exactly one request: a PATCH, never a PUT/GET that could rewrite other attributes.
			require.Len(t, tr.requests, 1)
			req := tr.requests[0]
			require.Equal(t, http.MethodPatch, req.method)
			require.Equal(t, "/api/2.0/accounts/acct-1/scim/v2/Users/76832170785741", req.path)
			require.Equal(t, map[string]any{
				"schemas": []any{"urn:ietf:params:scim:api:messages:2.0:PatchOp"},
				"Operations": []any{
					map[string]any{"op": "replace", "path": "active", "value": tc.active},
				},
			}, req.body)
		})
	}
}

// Both actions run twice in a row: the second call, against a user already in the
// target state, must succeed and send the same request.
func TestActionsAreIdempotent(t *testing.T) {
	tr := &patchTransport{status: http.StatusOK}
	d := newActionsConnector(t, tr)

	for i := 0; i < 2; i++ {
		_, _, err := d.disableUserHandler(context.Background(), userArgs(t, "42"))
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		_, _, err := d.enableUserHandler(context.Background(), userArgs(t, "42"))
		require.NoError(t, err)
	}
	require.Len(t, tr.requests, 4)
	require.Equal(t, tr.requests[0].body, tr.requests[1].body)
	require.Equal(t, tr.requests[2].body, tr.requests[3].body)
}

// The API answers a successful PATCH with 204 and no bytes at all; the transport sends
// exactly that by default for 204. An explicit `{}` 200 is covered too.
func TestSetUserActiveAcceptsEmptyBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		tr   *patchTransport
	}{
		{"204 zero bytes", &patchTransport{status: http.StatusNoContent}},
		{"200 zero bytes", &patchTransport{status: http.StatusOK, emptyBody: true}},
		{"200 explicit {}", &patchTransport{status: http.StatusOK, respBody: `{}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newActionsConnector(t, tc.tr)
			for name, h := range map[string]func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error){
				"disable": d.disableUserHandler, "enable": d.enableUserHandler,
			} {
				res, _, err := h(context.Background(), userArgs(t, "42"))
				require.NoError(t, err, name)
				require.True(t, res.GetFields()["success"].GetBoolValue(), name)
			}
			require.Len(t, tc.tr.requests, 2)
		})
	}
}

// A 204 mock must really carry zero bytes, or the tests above would not exercise the empty-body read.
func TestPatchTransportEmptyBodyIsZeroBytes(t *testing.T) {
	for _, tr := range []*patchTransport{
		{status: http.StatusNoContent},
		{status: http.StatusOK, emptyBody: true},
	} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPatch, "https://example.invalid/", nil)
		require.NoError(t, err)
		resp, err := tr.RoundTrip(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Empty(t, raw)
	}
}

func TestActionInputValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		args *structpb.Struct
	}{
		{"nil args", nil},
		{"missing user_id", &structpb.Struct{Fields: map[string]*structpb.Value{}}},
		{"empty user_id", userArgs(t, "")},
		{"non-string user_id", func() *structpb.Struct {
			s, _ := structpb.NewStruct(map[string]any{"user_id": 42})
			return s
		}()},
		{"compound id", userArgs(t, "account/acct-1/user/42")},
		{"path traversal", userArgs(t, "42/../../Groups/7")},
		{"encoded slash", userArgs(t, "42%2F..")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &patchTransport{status: http.StatusOK}
			d := newActionsConnector(t, tr)

			for _, h := range []func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error){d.disableUserHandler, d.enableUserHandler} {
				_, _, err := h(context.Background(), tc.args)
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			}
			require.Empty(t, tr.requests, "invalid input must not reach the API")
		})
	}
}

func TestActionErrorsKeepTheirMeaning(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		respBody string
		want     codes.Code
		contains string
	}{
		{"missing user", http.StatusNotFound, `{"detail":"User with id 42 does not exist.","status":"404"}`, codes.NotFound, "user not found"},
		{"forbidden", http.StatusForbidden, `{"message":"not an account admin"}`, codes.PermissionDenied, ""},
		{"unauthorized", http.StatusUnauthorized, `{"message":"invalid token"}`, codes.Unauthenticated, ""},
		{"rate limited", http.StatusTooManyRequests, `{"message":"slow down"}`, codes.Unavailable, ""},
		{"server error", http.StatusServiceUnavailable, `{"message":"try later"}`, codes.Unavailable, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &patchTransport{status: tc.status, respBody: tc.respBody}
			d := newActionsConnector(t, tr)

			for name, h := range map[string]func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error){
				"disable": d.disableUserHandler, "enable": d.enableUserHandler,
			} {
				res, _, err := h(context.Background(), userArgs(t, "42"))
				require.Error(t, err, name)
				require.Nil(t, res, name)
				require.Equal(t, tc.want, status.Code(err), "%s: %v", name, err)
				require.Contains(t, err.Error(), "databricks-connector:")
				require.Contains(t, err.Error(), "42")
				var apiErr *databricks.APIError
				require.ErrorAs(t, err, &apiErr, "%s: the API error must stay reachable", name)
				require.Equal(t, tc.status, apiErr.StatusCode, name)
				if tc.contains != "" {
					require.Contains(t, err.Error(), tc.contains)
				}
			}
		})
	}
}

func TestGlobalActionSchemas(t *testing.T) {
	for _, tc := range []struct {
		schema *v2.BatonActionSchema
		typ    v2.ActionType
	}{
		{disableUserSchema, v2.ActionType_ACTION_TYPE_ACCOUNT_DISABLE},
		{enableUserSchema, v2.ActionType_ACTION_TYPE_ACCOUNT_ENABLE},
	} {
		require.Equal(t, []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT, tc.typ}, tc.schema.GetActionType())
		require.Len(t, tc.schema.GetArguments(), 1, "C1 lifecycle actions accept exactly one argument")
		arg := tc.schema.GetArguments()[0]
		require.Equal(t, "user_id", arg.GetName())
		require.True(t, arg.GetIsRequired())
	}
}

// Gateways answer errors with HTML, whitespace or nothing. The HTTP cause must survive:
// gRPC code, *APIError with the status, and the rate-limit annotation.
func TestActionErrorsWithNonJSONBody(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		respBody string
		want     codes.Code
		contains string
	}{
		{"403 html", http.StatusForbidden, "<html>denied</html>", codes.PermissionDenied, ""},
		{"403 empty", http.StatusForbidden, "", codes.PermissionDenied, ""},
		{"404 html", http.StatusNotFound, "<html>nope</html>", codes.NotFound, "user not found"},
		{"429 whitespace", http.StatusTooManyRequests, " ", codes.Unavailable, ""},
		{"429 empty", http.StatusTooManyRequests, "", codes.Unavailable, ""},
		{"503 html", http.StatusServiceUnavailable, "<html>unavailable</html>", codes.Unavailable, ""},
		{"503 empty", http.StatusServiceUnavailable, "", codes.Unavailable, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &patchTransport{status: tc.status, respBody: tc.respBody, emptyBody: tc.respBody == ""}
			d := newActionsConnector(t, tr)

			for name, h := range map[string]func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error){
				"disable": d.disableUserHandler, "enable": d.enableUserHandler,
			} {
				res, annos, err := h(context.Background(), userArgs(t, "42"))
				require.Error(t, err, name)
				require.Nil(t, res, name)
				require.Equal(t, tc.want, status.Code(err), "%s: %v", name, err)
				require.Contains(t, err.Error(), "42")
				if tc.contains != "" {
					require.Contains(t, err.Error(), tc.contains)
				}
				var apiErr *databricks.APIError
				require.ErrorAs(t, err, &apiErr, name)
				require.Equal(t, tc.status, apiErr.StatusCode, name)
				require.NotContains(t, err.Error(), "failed to decode response body", name)

				rl := &v2.RateLimitDescription{}
				ok, aerr := annos.Pick(rl)
				require.NoError(t, aerr)
				require.True(t, ok, "%s: rate-limit annotation must be kept", name)
			}
		})
	}
}
