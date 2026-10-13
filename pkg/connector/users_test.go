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

// newCapturingUserBuilder returns a user builder backed by a test server that
// records the CreateUser request body into cap and answers both the create
// call and the post-create GetUser fetch that CreateAccount performs.
func newCapturingUserBuilder(t *testing.T, cap *capturedCreate) *userBuilder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read create request body: %v", err)
			}
			cap.raw = raw
			if err := json.Unmarshal(raw, &cap.body); err != nil {
				t.Fatalf("decode create request body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(databricks.CreateUserResponse{
				Id:          "u1",
				UserName:    cap.body.UserName,
				DisplayName: cap.body.DisplayName,
				Active:      cap.body.Active,
			})
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"u1","userName":%q,"displayName":%q,"active":%t,"emails":[{"primary":true,"value":%q}]}`,
				cap.body.UserName, cap.body.DisplayName, cap.body.Active, cap.body.UserName)
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

// Regression for CXH-2345: Databricks defaults a user to active when the
// field is absent from the request, but CreateUserBody.Active has no
// `omitempty`, so a zero-valued body silently sent "active": false on every
// created account, leaving it unable to authenticate.
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
			var cap capturedCreate
			b := newCapturingUserBuilder(t, &cap)

			profile, err := structpb.NewStruct(tc.profile)
			if err != nil {
				t.Fatalf("structpb.NewStruct: %v", err)
			}

			_, _, _, err = b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			if err != nil {
				t.Fatalf("CreateAccount: %v", err)
			}

			if cap.body.Active != tc.wantActive {
				t.Errorf("serialized request body active = %v, want %v", cap.body.Active, tc.wantActive)
			}

			// Active has no `omitempty` tag, so the key must always be present
			// on the wire regardless of its value, or Databricks falls back to
			// its own default instead of honoring what the connector intended.
			var raw map[string]interface{}
			if err := json.Unmarshal(cap.raw, &raw); err != nil {
				t.Fatalf("unmarshal raw request body: %v", err)
			}
			if _, ok := raw["active"]; !ok {
				t.Error("serialized request body is missing the active field")
			}
		})
	}
}

// Regression for CXH-2345: a non-boolean `active` profile value must error
// instead of panicking the type assertion.
func TestCreateAccountActiveNonBoolProfileValue(t *testing.T) {
	var cap capturedCreate
	b := newCapturingUserBuilder(t, &cap)

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
