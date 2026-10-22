package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// capturedCreate records the raw and decoded body of the CreateUser request
// sent by CreateAccount.
type capturedCreate struct {
	raw  []byte
	body databricks.CreateUserBody
}

// newCapturingUserBuilder backs a user builder with a test server that
// captures the CreateUser body and also answers the post-create GetUser fetch.
func newCapturingUserBuilder(t *testing.T, captured *capturedCreate) *userBuilder {
	t.Helper()
	return newTestUserBuilder(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read create request body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			captured.raw = raw
			if err := json.Unmarshal(raw, &captured.body); err != nil {
				t.Errorf("decode create request body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(databricks.CreateUserResponse{
				Id:          "u1",
				UserName:    captured.body.UserName,
				DisplayName: captured.body.DisplayName,
				Active:      captured.body.Active,
			})
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"u1","userName":%q,"displayName":%q,"active":%t,"emails":[{"primary":true,"value":%q}]}`,
				captured.body.UserName, captured.body.DisplayName, captured.body.Active, captured.body.UserName)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

// newTestUserBuilder backs a user builder with a test server running handler.
func newTestUserBuilder(t *testing.T, handler http.HandlerFunc) *userBuilder {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}

	httpClient := &http.Client{Transport: &hostRewriter{target: target}}
	c, err := databricks.NewClient(context.Background(), httpClient, testHostname, testAccountHost, testAccountId, "", &databricks.NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return newUserBuilder(c)
}

// CreateUserBody.Active has no `omitempty`, so a zero-valued body silently
// sends "active": false even when the profile never set it.
func TestCreateAccountActiveDefault(t *testing.T) {
	tests := []struct {
		name       string
		profile    map[string]interface{}
		wantActive bool
	}{
		{
			name: "no active field in profile defaults to active",
			profile: map[string]interface{}{
				"email":       "alice@example.com",
				"displayName": "Alice",
			},
			wantActive: true,
		},
		{
			name: "explicit active true overrides default",
			profile: map[string]interface{}{
				"email":       "alice@example.com",
				"displayName": "Alice",
				"active":      true,
			},
			wantActive: true,
		},
		{
			name: "explicit active false overrides default",
			profile: map[string]interface{}{
				"email":       "alice@example.com",
				"displayName": "Alice",
				"active":      false,
			},
			wantActive: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var captured capturedCreate
			b := newCapturingUserBuilder(t, &captured)

			profile, err := structpb.NewStruct(tc.profile)
			if err != nil {
				t.Fatalf("structpb.NewStruct: %v", err)
			}

			_, _, _, err = b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			if err != nil {
				t.Fatalf("CreateAccount: %v", err)
			}

			if captured.body.Active != tc.wantActive {
				t.Errorf("serialized request body active = %v, want %v", captured.body.Active, tc.wantActive)
			}

			// No omitempty: the key must always be present on the wire.
			var raw map[string]interface{}
			if err := json.Unmarshal(captured.raw, &raw); err != nil {
				t.Fatalf("unmarshal raw request body: %v", err)
			}
			if _, ok := raw["active"]; !ok {
				t.Error("serialized request body is missing the active field")
			}
		})
	}
}

// A non-boolean active profile value must error, not panic the type assertion.
func TestCreateAccountActiveNonBoolProfileValue(t *testing.T) {
	var captured capturedCreate
	b := newCapturingUserBuilder(t, &captured)

	profile, err := structpb.NewStruct(map[string]interface{}{
		"email":       "alice@example.com",
		"displayName": "Alice",
		"active":      "yes",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}

	_, _, _, err = b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
	if err == nil {
		t.Fatal("CreateAccount succeeded with a non-bool active profile value, want error")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("status.Code(err) = %v, want %v", got, codes.InvalidArgument)
	}
}

// Non-string profile values, null included, must error naming the field
// before any request is sent, not panic the type assertion.
func TestCreateAccountNonStringProfileValues(t *testing.T) {
	badValues := map[string]interface{}{
		"number": 42,
		"bool":   true,
		"null":   nil,
		"object": map[string]interface{}{"value": "x"},
	}

	for _, field := range []string{"email", "displayName", "familyName", "givenName", "id"} {
		for kind, value := range badValues {
			t.Run(field+"/"+kind, func(t *testing.T) {
				requests := 0
				b := newTestUserBuilder(t, func(w http.ResponseWriter, r *http.Request) {
					requests++
					w.WriteHeader(http.StatusInternalServerError)
				})

				profileMap := map[string]interface{}{
					"email":       "alice@example.com",
					"displayName": "Alice",
				}
				profileMap[field] = value
				profile, err := structpb.NewStruct(profileMap)
				if err != nil {
					t.Fatalf("structpb.NewStruct: %v", err)
				}

				_, _, _, err = b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
				want := fmt.Sprintf("baton-databricks: %s field must be a string", field)
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("CreateAccount error = %v, want %q", err, want)
				}
				if got := status.Code(err); got != codes.InvalidArgument {
					t.Errorf("status.Code(err) = %v, want %v", got, codes.InvalidArgument)
				}
				if requests != 0 {
					t.Errorf("sent %d requests, want 0", requests)
				}
			})
		}
	}
}

// A missing or empty email or displayName is rejected as invalid input before
// any request is sent.
func TestCreateAccountMissingRequiredProfileValues(t *testing.T) {
	tests := []struct {
		name    string
		profile map[string]interface{}
		want    string
	}{
		{
			name:    "missing email",
			profile: map[string]interface{}{"displayName": "Alice"},
			want:    "baton-databricks: email field is required in profile",
		},
		{
			name:    "empty email",
			profile: map[string]interface{}{"email": "", "displayName": "Alice"},
			want:    "baton-databricks: email is required to create a user",
		},
		{
			name:    "displayName missing",
			profile: map[string]interface{}{"email": "alice@example.com"},
			want:    "baton-databricks: displayName field is required in profile",
		},
		{
			name:    "empty displayName",
			profile: map[string]interface{}{"email": "alice@example.com", "displayName": ""},
			want:    "baton-databricks: displayName is required to create a user",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			b := newTestUserBuilder(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.WriteHeader(http.StatusInternalServerError)
			})

			profile, err := structpb.NewStruct(tc.profile)
			if err != nil {
				t.Fatalf("structpb.NewStruct: %v", err)
			}

			_, _, _, err = b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("CreateAccount error = %v, want %q", err, tc.want)
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("status.Code(err) = %v, want %v", got, codes.InvalidArgument)
			}
			if requests != 0 {
				t.Errorf("sent %d requests, want 0", requests)
			}
		})
	}
}

// createConflictServer answers POST with createStatus and the filtered
// userName lookup with lookupBody, or with lookupStatus when it is set,
// counting both.
type createConflictServer struct {
	createStatus int
	lookupStatus int
	lookupBody   string
	posts        int
	lookups      int
	lookupFilter string
}

func (s *createConflictServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodPost:
		s.posts++
		w.WriteHeader(s.createStatus)
		fmt.Fprintf(w, `{"detail":"error %d","status":"%d"}`, s.createStatus, s.createStatus)
	case http.MethodGet:
		s.lookups++
		s.lookupFilter = r.URL.Query().Get("filter")
		if s.lookupStatus != 0 {
			w.WriteHeader(s.lookupStatus)
			fmt.Fprintf(w, `{"detail":"error %d","status":"%d"}`, s.lookupStatus, s.lookupStatus)
			return
		}
		fmt.Fprint(w, s.lookupBody)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func createAliceAccount(t *testing.T, b *userBuilder) (connectorbuilder.CreateAccountResponse, error) {
	t.Helper()
	profile, err := structpb.NewStruct(map[string]interface{}{
		"email":       "alice@example.com",
		"displayName": "Alice",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	res, _, _, err := b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
	return res, err
}

// A 409 for an existing userName returns the existing user as already-exists.
func TestCreateAccountDuplicateEmailReturnsAlreadyExists(t *testing.T) {
	srv := &createConflictServer{
		createStatus: http.StatusConflict,
		lookupBody: `{"totalResults":1,"Resources":[{"id":"existing-1","userName":"alice@example.com",` +
			`"displayName":"Alice","active":true,"emails":[{"primary":true,"value":"alice@example.com"}]}]}`,
	}
	b := newTestUserBuilder(t, srv.handle)

	res, err := createAliceAccount(t, b)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	already, ok := res.(*v2.CreateAccountResponse_AlreadyExistsResult)
	if !ok {
		t.Fatalf("CreateAccount result = %T, want *v2.CreateAccountResponse_AlreadyExistsResult", res)
	}
	if got := already.GetResource().GetId().GetResource(); got != "existing-1" {
		t.Errorf("resource id = %q, want %q", got, "existing-1")
	}
	if !already.GetIsCreateAccountResult() {
		t.Error("IsCreateAccountResult = false, want true")
	}
	if srv.posts != 1 || srv.lookups != 1 {
		t.Errorf("posts = %d, lookups = %d, want 1 and 1", srv.posts, srv.lookups)
	}
	if want := "userName eq 'alice@example.com'"; srv.lookupFilter != want {
		t.Errorf("lookup filter = %q, want %q", srv.lookupFilter, want)
	}
}

// A 409 whose lookup finds no user keeps the original create error.
func TestCreateAccountConflictWithoutExistingUserErrors(t *testing.T) {
	srv := &createConflictServer{
		createStatus: http.StatusConflict,
		lookupBody:   `{"totalResults":0,"Resources":[]}`,
	}
	b := newTestUserBuilder(t, srv.handle)

	_, err := createAliceAccount(t, b)
	if err == nil || !strings.HasPrefix(err.Error(), "baton-databricks: failed to create user:") {
		t.Fatalf("CreateAccount error = %v, want the wrapped create error", err)
	}
	if strings.Contains(err.Error(), "existing-user lookup failed") {
		t.Errorf("CreateAccount error = %v, want no lookup failure note", err)
	}
	if srv.posts != 1 || srv.lookups != 1 {
		t.Errorf("posts = %d, lookups = %d, want 1 and 1", srv.posts, srv.lookups)
	}
}

// A 409 whose lookup itself fails reports the lookup failure with the create
// error as text, and resolves to the lookup error's code so a transient
// failure stays retryable.
func TestCreateAccountConflictWithFailedLookupReportsLookupError(t *testing.T) {
	tests := []struct {
		name         string
		lookupStatus int
		wantCode     codes.Code
	}{
		{name: "lookup 500", lookupStatus: http.StatusInternalServerError, wantCode: codes.Unavailable},
		{name: "lookup 429", lookupStatus: http.StatusTooManyRequests, wantCode: codes.Unavailable},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &createConflictServer{
				createStatus: http.StatusConflict,
				lookupStatus: tc.lookupStatus,
			}
			b := newTestUserBuilder(t, srv.handle)

			res, err := createAliceAccount(t, b)
			if err == nil {
				t.Fatalf("CreateAccount result = %T, want an error", res)
			}
			if res != nil {
				t.Errorf("CreateAccount result = %T, want nil (no already-exists result)", res)
			}
			if !strings.Contains(err.Error(), "baton-databricks: failed to create user:") {
				t.Errorf("CreateAccount error = %v, want the create error text", err)
			}
			if !strings.Contains(err.Error(), "existing-user lookup failed") {
				t.Errorf("CreateAccount error = %v, want the lookup failure note", err)
			}
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("status.Code(err) = %v, want %v", got, tc.wantCode)
			}
			if srv.posts != 1 || srv.lookups != 1 {
				t.Errorf("posts = %d, lookups = %d, want 1 and 1", srv.posts, srv.lookups)
			}
		})
	}
}

// A 409 whose lookup returns a different userName keeps the original create
// error rather than returning that other user as already-exists.
func TestCreateAccountConflictWithDifferentUserNameErrors(t *testing.T) {
	srv := &createConflictServer{
		createStatus: http.StatusConflict,
		lookupBody: `{"totalResults":1,"Resources":[{"id":"other-1","userName":"bob@example.com",` +
			`"displayName":"Bob","active":true,"emails":[{"primary":true,"value":"bob@example.com"}]}]}`,
	}
	b := newTestUserBuilder(t, srv.handle)

	res, err := createAliceAccount(t, b)
	if err == nil || !strings.HasPrefix(err.Error(), "baton-databricks: failed to create user:") {
		t.Fatalf("CreateAccount error = %v, want the wrapped create error", err)
	}
	if res != nil {
		t.Errorf("CreateAccount result = %T, want nil", res)
	}
	if strings.Contains(err.Error(), "existing-user lookup failed") {
		t.Errorf("CreateAccount error = %v, want no lookup failure note", err)
	}
	if srv.posts != 1 || srv.lookups != 1 {
		t.Errorf("posts = %d, lookups = %d, want 1 and 1", srv.posts, srv.lookups)
	}
}

// A 409 whose lookup returns the userName in different case is the same user.
func TestCreateAccountConflictCaseInsensitiveUserNameReturnsAlreadyExists(t *testing.T) {
	srv := &createConflictServer{
		createStatus: http.StatusConflict,
		lookupBody: `{"totalResults":1,"Resources":[{"id":"existing-1","userName":"Alice@Example.COM",` +
			`"displayName":"Alice","active":true,"emails":[{"primary":true,"value":"Alice@Example.COM"}]}]}`,
	}
	b := newTestUserBuilder(t, srv.handle)

	res, err := createAliceAccount(t, b)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	already, ok := res.(*v2.CreateAccountResponse_AlreadyExistsResult)
	if !ok {
		t.Fatalf("CreateAccount result = %T, want *v2.CreateAccountResponse_AlreadyExistsResult", res)
	}
	if got := already.GetResource().GetId().GetResource(); got != "existing-1" {
		t.Errorf("resource id = %q, want %q", got, "existing-1")
	}
}

// Any other create error is returned wrapped, with no lookup.
func TestCreateAccountOtherErrorIsWrapped(t *testing.T) {
	srv := &createConflictServer{createStatus: http.StatusBadRequest}
	b := newTestUserBuilder(t, srv.handle)

	_, err := createAliceAccount(t, b)
	if err == nil || !strings.HasPrefix(err.Error(), "baton-databricks: failed to create user:") {
		t.Fatalf("CreateAccount error = %v, want the wrapped create error", err)
	}
	if srv.posts != 1 || srv.lookups != 0 {
		t.Errorf("posts = %d, lookups = %d, want 1 and 0", srv.posts, srv.lookups)
	}
}

// A valid profile still creates the user and returns success.
func TestCreateAccountValidProfileSucceeds(t *testing.T) {
	var captured capturedCreate
	b := newCapturingUserBuilder(t, &captured)

	profile, err := structpb.NewStruct(map[string]interface{}{
		"email":       "alice@example.com",
		"displayName": "Alice",
		"familyName":  "Smith",
		"givenName":   "Alice",
		"id":          "ext-1",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}

	res, _, _, err := b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	success, ok := res.(*v2.CreateAccountResponse_SuccessResult)
	if !ok {
		t.Fatalf("CreateAccount result = %T, want *v2.CreateAccountResponse_SuccessResult", res)
	}
	if got := success.GetResource().GetId().GetResource(); got != "u1" {
		t.Errorf("resource id = %q, want %q", got, "u1")
	}
	body := captured.body
	if body.UserName != "alice@example.com" || body.DisplayName != "Alice" ||
		body.Name.FamilyName != "Smith" || body.Name.GivenName != "Alice" || body.Id != "ext-1" || !body.Active {
		t.Errorf("create body = %+v, want the profile values with active true", body)
	}
}
