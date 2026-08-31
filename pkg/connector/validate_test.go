package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// rolesTransport answers the assignable-roles calls Validate makes. failAccount
// makes the account-plane probe (host "accounts.*") fail so isAccAPIAvailable stays
// false while the workspace probe still succeeds.
type rolesTransport struct{ failAccount bool }

func (t rolesTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.failAccount && strings.HasPrefix(req.URL.Host, "accounts.") {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"boom"}`)),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"roles":[]}`)),
		Request:    req,
	}, nil
}

func captureLogs(ctx context.Context, buf *bytes.Buffer) context.Context {
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(buf),
		zapcore.DebugLevel,
	)
	return ctxzap.ToContext(ctx, zap.New(core))
}

func newValidateConnector(t *testing.T, auth databricks.Auth, tr http.RoundTripper) *Databricks {
	t.Helper()
	client, err := databricks.NewClient(
		context.Background(), &http.Client{Transport: tr},
		"cloud.databricks.com", "accounts.cloud.databricks.com",
		"acct-1", "", auth, nil,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return &Databricks{client: client, workspaces: []string{"ws1"}}
}

// levelFor scans the captured JSON log lines for the first entry whose message
// contains want and returns its level. Empty string means no such entry.
func levelFor(t *testing.T, buf *bytes.Buffer, want string) string {
	t.Helper()
	for _, line := range strings.Split(buf.String(), "\n") {
		if line == "" {
			continue
		}
		var entry struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if strings.Contains(entry.Msg, want) {
			return entry.Level
		}
	}
	return ""
}

const accountUnreachableMsg = "account API unreachable"

// CXH-2350: dropping the whole account plane is a customer-visible degradation, so the
// startup notice must be visible. It logs at warn, not debug (a debug line is invisible at
// the default info level, which is the silent degradation the ticket was filed to fix).
func TestValidateWorkspaceTokenLogsAtWarn(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := captureLogs(context.Background(), buf)
	d := newValidateConnector(t, databricks.NewTokenAuth([]string{"ws1"}, []string{"tok"}), rolesTransport{})

	if _, err := d.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got := levelFor(t, buf, accountUnreachableMsg); got != "warn" {
		t.Errorf("token-auth notice logged at %q, want %q", got, "warn")
	}
}

// OAuth with a failed account probe is the same whole-tenant drop, also at warn.
func TestValidateOAuthProbeFailureLogsAtWarn(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := captureLogs(context.Background(), buf)
	d := newValidateConnector(t, &databricks.NoAuth{}, rolesTransport{failAccount: true})

	if _, err := d.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got := levelFor(t, buf, accountUnreachableMsg); got != "warn" {
		t.Errorf("OAuth-probe-failure notice logged at %q, want %q", got, "warn")
	}
}

// When the account API is reachable (non-token auth), the notice must not fire at all.
func TestValidateAccountReachableNoNotice(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := captureLogs(context.Background(), buf)
	d := newValidateConnector(t, &databricks.NoAuth{}, rolesTransport{})

	if _, err := d.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got := levelFor(t, buf, accountUnreachableMsg); got != "" {
		t.Errorf("notice fired (level %q) when account API was reachable", got)
	}
}
