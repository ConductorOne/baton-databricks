package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
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
}
